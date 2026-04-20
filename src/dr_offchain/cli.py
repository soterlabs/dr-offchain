"""CLI.

Subcommands:
  prepare-sql   Emit Dune SQL with depositor/vault lists substituted.
  run           Compute daily & monthly XR rewards from JSON + balance CSVs.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from .config import MORPHO_VAULTV2_ADDRESSES
from .events import (
    build_morpho_sql,
    build_wallet_usds_sql,
    load_morpho_events,
    load_wallet_usds_events,
)
from .loader import load_dir
from .pipeline import (
    apply_rewards,
    build_sticky_trajectory,
    compute_daily_tw,
    monthly_rollup_by_scope,
)


def cmd_prepare_sql(args: argparse.Namespace) -> None:
    referrals = load_dir(args.data)
    wallet_depositors = sorted({
        r.depositor for r in referrals if r.scope_id.startswith("wallet_usds:")
    })
    morpho_depositors = sorted({
        r.depositor for r in referrals if r.scope_id.startswith("morpho:")
    })
    cowswap_count = len({r.depositor for r in referrals if r.dest_kind == "cowswap"})
    psm_count = len({r.depositor for r in referrals if r.dest_kind == "psm"})
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "wallet_usds_transfers.sql").write_text(build_wallet_usds_sql(wallet_depositors))
    (args.out / "morpho_vaultv2_events.sql").write_text(
        build_morpho_sql(morpho_depositors, sorted(MORPHO_VAULTV2_ADDRESSES))
    )
    print(f"Wrote SQL to {args.out}/")
    print(f"  wallet-USDS depositors: {len(wallet_depositors)} "
          f"(CowSwap: {cowswap_count}, PSM: {psm_count})")
    print(f"  Morpho depositors:      {len(morpho_depositors)}")


def cmd_run(args: argparse.Namespace) -> None:
    referrals = load_dir(args.data)
    events = []
    if args.wallet_usds_csv.exists():
        loaded = load_wallet_usds_events(args.wallet_usds_csv, referrals)
        events.extend(loaded)
        print(f"Loaded {len(loaded)} wallet-USDS events from {args.wallet_usds_csv} "
              f"(post-dedupe vs. tagged referrals)")
    else:
        print(f"[skip] {args.wallet_usds_csv} not found — no wallet-USDS events applied")
    if args.morpho_csv.exists():
        loaded = load_morpho_events(args.morpho_csv, referrals)
        events.extend(loaded)
        print(f"Loaded {len(loaded)} Morpho events from {args.morpho_csv} "
              f"(post-dedupe vs. tagged referrals)")
    else:
        print(f"[skip] {args.morpho_csv} not found — no Morpho events applied")

    traj = build_sticky_trajectory(referrals, events)
    through = args.through or datetime.now(timezone.utc).date()
    daily = compute_daily_tw(traj, through)
    daily = apply_rewards(daily)
    monthly = monthly_rollup_by_scope(daily)

    args.out.mkdir(parents=True, exist_ok=True)
    daily_out = args.out / "daily_rewards.csv"
    monthly_out = args.out / "monthly_rewards.csv"
    daily.to_csv(daily_out, index=False)
    monthly.to_csv(monthly_out, index=False)
    print(f"\nWrote {daily_out} ({len(daily)} rows)")
    print(f"Wrote {monthly_out} ({len(monthly)} rows)")
    if not daily.empty:
        daily = daily.copy()
        daily["scope_kind"] = daily["scope_id"].str.split(":", n=1).str[0]
        per_scope = daily.groupby("scope_kind")["tw_reward_usd"].sum()
        print("\nTotals by scope (USD):")
        for k in ("wallet_usds", "morpho"):
            print(f"  {k:<12} ${per_scope.get(k, 0.0):>14,.4f}")
        print(f"  {'total':<12} ${daily['tw_reward_usd'].sum():>14,.4f}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="dr-offchain")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_prep = sub.add_parser("prepare-sql", help="emit Dune SQL with addresses substituted")
    p_prep.add_argument("--data", type=Path, default=Path("data"))
    p_prep.add_argument("--out", type=Path, default=Path("out/sql"))
    p_prep.set_defaults(func=cmd_prepare_sql)

    p_run = sub.add_parser("run", help="compute daily & monthly XR rewards")
    p_run.add_argument("--data", type=Path, default=Path("data"))
    p_run.add_argument("--wallet-usds-csv", type=Path,
                       default=Path("out/events/wallet_usds_transfers.csv"))
    p_run.add_argument("--morpho-csv", type=Path,
                       default=Path("out/events/morpho_events.csv"))
    p_run.add_argument("--out", type=Path, default=Path("out"))
    p_run.add_argument("--through", type=lambda s: date.fromisoformat(s), default=None,
                       help="last day to include (UTC), default=today")
    p_run.set_defaults(func=cmd_run)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
