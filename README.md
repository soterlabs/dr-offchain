# dr-offchain

Off-chain replication of Spark's **Distribution Rewards** (XR / Accessibility Rewards) for deposits captured by the **Skybase** frontend that don't emit on-chain `Referral` events.

Spark's on-chain DR pipeline keys off the `Referral` event emitted by the PSM / sUSDS deposit flow. Skybase-routed CowSwap swaps and Morpho VaultV2 deposits bypass that event, so they need an off-chain equivalent.

Methodology reference: [`SPARK_DR_METHODOLOGY.md`](./SPARK_DR_METHODOLOGY.md).

## How it works

1. **Synthetic `Referral` events** — Skybase frontend logs in `data/` (one JSON per destination/month) are loaded as `SyntheticReferral(ts, depositor, dest_kind, scope_id, amount, tx_hash)`. Each `tx_hash` is treated as if it emitted a `Referral` with `ref_code = 0`.
2. **Outflows fetched from Dune** — CowSwap: `sky_ethereum.usds_evt_transfer` where `from = depositor`. Morpho: `morpho_v2_ethereum.vaultv2_evt_withdraw` where `onBehalf = depositor`.
3. **FIFO eligibility** (not sticky) — On each outflow, the tagged eligible balance is burned FIFO, clamped at 0. Untagged inflows do NOT grow eligible.
4. **Time-weighted integration** — The piecewise-constant `eligible` series is integrated into daily TW averages with a 00:00 UTC synthetic anchor.
5. **XR rate applied per day** — `reward_per = 365 * (exp(ln(1+APY)/365) - 1)`. APY: **0.6%** for calendar 2025, **0.5%** from 2026-01-01 onward.
6. **USD conversion** — USDS is $1-pegged, so `tw_reward_usd = tw_reward`.

**Scope granularity**

| Destination | `scope_id` | Rationale |
|---|---|---|
| CowSwap | `cowswap:{depositor}` | Outflow is any USDS transfer from the wallet; one bucket per user. |
| Morpho VaultV2 | `morpho:{vault}` | Outflow is a Withdraw event on a specific vault; one bucket per (user, vault). |

## Layout

```
src/dr_offchain/
  config.py     addresses + XR_RATE_PERIODS
  loader.py     JSON → SyntheticReferral
  events.py     SQL prep (addr substitution) + CSV loaders for outflows
  pipeline.py   FIFO + TW integration + rate application + monthly rollup
  cli.py        prepare-sql, run
queries/
  cowswap_usds_outflows.sql        template (__DEPOSITORS__)
  morpho_vaultv2_withdraws.sql     template (__DEPOSITORS__, __VAULTS__)
data/
  skybase_*.json                   frontend capture logs
tests/
  test_pipeline.py                 FIFO + TW + rate-switch unit tests
```

## Install

```bash
pip install -e .
```

Requires Python 3.11+.

## Usage

### Option A — MCP (no API key)

If your shell is set up with the Dune MCP server, everything runs through it:

1. Ask Claude (or any MCP client) to create + execute the two fetch queries using [`queries/cowswap_usds_outflows.sql`](queries/cowswap_usds_outflows.sql) and [`queries/morpho_vaultv2_withdraws.sql`](queries/morpho_vaultv2_withdraws.sql) with the depositor/vault lists baked in.
2. Save the results as `out/events/cowswap_outflows.csv` and `out/events/morpho_withdraws.csv` (columns must match the CSV schemas loaded in `events.py`).
3. Run the pipeline.

### Option B — Manual SQL

```bash
# Emit Dune SQL with depositor/vault addresses substituted
python -m dr_offchain.cli prepare-sql --data data --out out/sql

# → out/sql/cowswap_usds_outflows.sql
# → out/sql/morpho_vaultv2_withdraws.sql
```

Run both in Dune, export to CSV, save as:

- `out/events/cowswap_outflows.csv` — `ts, depositor, amount, tx_hash, block_number, log_index`
- `out/events/morpho_withdraws.csv` — `ts, depositor, vault, amount, tx_hash, block_number, log_index`

### Run the pipeline

```bash
python -m dr_offchain.cli run              # through = today (UTC)
python -m dr_offchain.cli run --through 2026-03-31
```

Writes:

- `out/daily_rewards.csv` — `dt, depositor, scope_id, tw_eligible, reward_per, tw_reward, tw_reward_usd`
- `out/monthly_rewards.csv` — `month, scope_kind, tw_reward_usd`

### Tests

```bash
pytest
```

## Published artifacts

Monthly rollup (values re-embedded from each local run):

- https://dune.com/queries/7332495 — Skybase DR monthly (CowSwap + Morpho)

Source fetch queries (temp):

- https://dune.com/queries/7331963 — CowSwap USDS outflows
- https://dune.com/queries/7331968 — Morpho VaultV2 withdraws

## Known caveats

- **CowSwap DR is near-zero by design.** CowSwap is a router, not a yield venue: USDS acquired via a Skybase-routed swap typically leaves the depositor wallet within minutes, so FIFO burns the tagged balance before meaningful TW accrues. The CowSwap methodology is flagged for review before any payout decisions.
- **Refresh cadence is manual.** Re-run the fetch queries, overwrite the CSVs, re-run the pipeline, re-embed into the monthly Dune query (7332495).
