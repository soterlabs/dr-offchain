# dr-offchain

Off-chain replication of Spark's **Distribution Rewards** (XR / Accessibility Rewards) for deposits captured by the **Skybase** frontend that don't emit on-chain `Referral` events.

Spark's on-chain DR pipeline keys off the `Referral` event emitted by the PSM / sUSDS deposit flow. Skybase-routed CowSwap swaps, PSM USDC→USDS conversions, and Morpho VaultV2 deposits bypass that event, so they need an off-chain equivalent.

Methodology reference: [`SPARK_DR_METHODOLOGY.md`](./SPARK_DR_METHODOLOGY.md).

## How it works

1. **Synthetic `Referral` events** — Skybase frontend logs in `data/` (one JSON per destination/month) are loaded as `SyntheticReferral(ts, depositor, dest_kind, scope_id, amount, tx_hash)`. Each `tx_hash` is treated as if it emitted a `Referral` with `ref_code = 0`.
2. **Balance events fetched from Dune** — both directions are pulled per scope:
   - `wallet_usds` (CowSwap + PSM): every inflow (`to = depositor`) and outflow (`from = depositor`) on `sky_ethereum.usds_evt_transfer`.
   - `morpho`: every `Deposit` and `Withdraw` on `morpho_v2_ethereum.vaultv2_evt_{deposit,withdraw}` where `onBehalf = depositor` and the vault is Spark-curated.
3. **Dedupe vs. tagged referrals** — a Skybase-tagged referral is co-emitted with an on-chain USDS inflow; the CSV loaders drop any Dune inflow whose `(tx_hash, depositor)` matches a tagged referral in the same scope kind, preventing double-counting.
4. **Sticky eligibility** (matches Spark's on-chain `last_value(ref_code) ignore nulls`) — once a depositor has their **first tagged inflow** in a scope, the entire running balance in that scope becomes eligible. Subsequent untagged inflows grow it; outflows reduce it (clamped at 0). The attribution latch never releases — re-deposits after a full drain are still eligible.
5. **Time-weighted integration** — The piecewise-constant `eligible` series is integrated into daily TW averages with a 00:00 UTC synthetic anchor.
6. **XR rate applied per day** — `reward_per = 365 * (exp(ln(1+APY)/365) - 1)`. APY: **0.6%** for calendar 2025, **0.5%** from 2026-01-01 onward.
7. **USD conversion** — USDS is $1-pegged, so `tw_reward_usd = tw_reward`.

**Scope granularity**

| Destination | `scope_id` | Rationale |
|---|---|---|
| CowSwap + PSM (USDC→USDS) | `wallet_usds:{depositor}` | Both deliver USDS to the depositor wallet; balance events are any USDS transfer touching the wallet. Shared sticky bucket per wallet — acquisition route is not distinguishable from the transfer stream. |
| Morpho VaultV2 | `morpho:{vault}` | Balance events are Deposit/Withdraw on a specific vault; one bucket per (user, vault). |

## Layout

```
src/dr_offchain/
  config.py     addresses + XR_RATE_PERIODS
  loader.py     JSON → SyntheticReferral
  events.py     SQL prep (addr substitution) + CSV loaders w/ tagged-inflow dedupe
  pipeline.py   sticky eligibility + TW integration + rate + monthly rollup
  cli.py        prepare-sql, run
queries/
  wallet_usds_transfers.sql         template (__DEPOSITORS__) — CowSwap + PSM, both directions
  morpho_vaultv2_events.sql         template (__DEPOSITORS__, __VAULTS__) — both directions
data/
  skybase_*.json                    frontend capture logs
tests/
  test_pipeline.py                  sticky + TW + rate-switch unit tests
  test_events.py                    CSV loader + dedupe unit tests
```

## Install

```bash
pip install -e .
```

Requires Python 3.11+.

## Usage

### Option A — MCP (no API key)

If your shell is set up with the Dune MCP server, everything runs through it:

1. Ask Claude (or any MCP client) to create + execute the two fetch queries using [`queries/wallet_usds_transfers.sql`](queries/wallet_usds_transfers.sql) and [`queries/morpho_vaultv2_events.sql`](queries/morpho_vaultv2_events.sql) with the depositor/vault lists baked in. The wallet-USDS depositor list is the **union** of CowSwap and PSM depositors.
2. Save the results as `out/events/wallet_usds_transfers.csv` and `out/events/morpho_events.csv` (columns must match the CSV schemas loaded in `events.py`).
3. Run the pipeline.

### Option B — Manual SQL

```bash
# Emit Dune SQL with depositor/vault addresses substituted
python -m dr_offchain.cli prepare-sql --data data --out out/sql

# → out/sql/wallet_usds_transfers.sql
# → out/sql/morpho_vaultv2_events.sql
```

Run both in Dune, export to CSV, save as:

- `out/events/wallet_usds_transfers.csv` — `ts, depositor, direction, amount, tx_hash, block_number, log_index`
- `out/events/morpho_events.csv` — `ts, depositor, vault, direction, amount, tx_hash, block_number, log_index`

`direction` is `'in'` for inflows/deposits and `'out'` for outflows/withdraws.

### Run the pipeline

```bash
python -m dr_offchain.cli run              # through = today (UTC)
python -m dr_offchain.cli run --through 2026-03-31
```

Writes:

- `out/daily_rewards.csv` — `dt, depositor, scope_id, tw_eligible, reward_per, tw_reward, tw_reward_usd`
- `out/monthly_rewards.csv` — `month, wallet_usds_dr_usd, morpho_dr_usd, total_dr_usd`

### Tests

```bash
pytest
```

## Published artifacts

Monthly rollup (values re-embedded from each local run):

- https://dune.com/queries/7332495 — Skybase DR monthly (wallet-USDS + Morpho)

Source fetch queries:

- https://dune.com/queries/7331963 — wallet-USDS transfers (CowSwap + PSM, both directions)
- https://dune.com/queries/7331968 — Morpho VaultV2 deposits + withdraws

## Known caveats

- **CowSwap DR is near-zero for true pass-through wallets.** CowSwap is a router, not a yield venue: USDS acquired via a Skybase-routed swap typically leaves the depositor wallet within minutes. If the wallet never holds USDS again, TW eligibility is negligible. Under sticky attribution, however, *any* later USDS inflow to the same wallet becomes eligible via the attribution latch — so CowSwap accrual is shaped by subsequent wallet activity, not just the swap itself.
- **PSM and CowSwap share one sticky bucket per wallet.** Both channels deliver USDS to the depositor wallet, and the transfer stream cannot distinguish which channel a given USDS unit came from. Per-channel accounting is therefore not derivable from `daily_rewards.csv` alone.
- **Pure sticky — no partial deallocation.** Once a wallet is tagged in a scope, the latch holds for that scope's lifetime. This matches Spark's on-chain `last_value(ref_code) ignore nulls` semantics. An untagged scope that a depositor later funds would only become eligible if a future tagged referral fires in that same scope.
- **Refresh cadence is manual.** Re-run the fetch queries, overwrite the CSVs, re-run the pipeline, re-embed into the monthly Dune query (7332495).
