"""CLI.

Subcommands:
  prepare-sql   Emit Dune SQL with depositor/vault lists substituted.
  run           Compute daily & monthly XR rewards from JSON + outflow CSVs.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from .config import MORPHO_VAULTV2_ADDRESSES
from .events import (
    build_cowswap_sql,
    build_morpho_sql,
    load_cowswap_outflows,
    load_morpho_outflows,
)
from .loader import load_dir
from .pipeline import (
    apply_rewards,
    build_eligibility_trajectory,
    compute_daily_tw,
    monthly_rollup,
)


def cmd_prepare_sql(args: argparse.Namespace) -> None:
    referrals = load_dir(args.data)
    cowswap_depositors = sorted({r.depositor for r in referrals if r.dest_kind == "cowswap"})
    morpho_depositors = sorted({r.depositor for r in referrals if r.dest_kind == "morpho"})
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "cowswap_usds_outflows.sql").write_text(build_cowswap_sql(cowswap_depositors))
    (args.out / "morpho_vaultv2_withdraws.sql").write_text(
        build_morpho_sql(morpho_depositors, sorted(MORPHO_VAULTV2_ADDRESSES))
    )
    print(f"Wrote SQL to {args.out}/")
    print(f"  CowSwap depositors: {len(cowswap_depositors)}")
    print(f"  Morpho depositors:  {len(morpho_depositors)}")


def cmd_run(args: argparse.Namespace) -> None:
    referrals = load_dir(args.data)
    outflows = []
    if args.cowswap_csv.exists():
        outflows.extend(load_cowswap_outflows(args.cowswap_csv))
        print(f"Loaded {len(outflows)} CowSwap outflows from {args.cowswap_csv}")
    else:
        print(f"[skip] {args.cowswap_csv} not found — no CowSwap outflows applied")
    before = len(outflows)
    if args.morpho_csv.exists():
        outflows.extend(load_morpho_outflows(args.morpho_csv))
        print(f"Loaded {len(outflows) - before} Morpho outflows from {args.morpho_csv}")
    else:
        print(f"[skip] {args.morpho_csv} not found — no Morpho outflows applied")

    traj = build_eligibility_trajectory(referrals, outflows)
    through = args.through or datetime.now(timezone.utc).date()
    daily = compute_daily_tw(traj, through)
    daily = apply_rewards(daily)
    monthly = monthly_rollup(daily)

    args.out.mkdir(parents=True, exist_ok=True)
    daily_out = args.out / "daily_rewards.csv"
    monthly_out = args.out / "monthly_rewards.csv"
    daily.to_csv(daily_out, index=False)
    monthly.to_csv(monthly_out, index=False)
    print(f"\nWrote {daily_out} ({len(daily)} rows)")
    print(f"Wrote {monthly_out} ({len(monthly)} rows)")
    if not daily.empty:
        print(f"\nTotal tw_reward_usd: ${daily['tw_reward_usd'].sum():,.4f}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="dr-offchain")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_prep = sub.add_parser("prepare-sql", help="emit Dune SQL with addresses substituted")
    p_prep.add_argument("--data", type=Path, default=Path("data"))
    p_prep.add_argument("--out", type=Path, default=Path("out/sql"))
    p_prep.set_defaults(func=cmd_prepare_sql)

    p_run = sub.add_parser("run", help="compute daily & monthly XR rewards")
    p_run.add_argument("--data", type=Path, default=Path("data"))
    p_run.add_argument("--cowswap-csv", type=Path, default=Path("out/events/cowswap_outflows.csv"))
    p_run.add_argument("--morpho-csv", type=Path, default=Path("out/events/morpho_withdraws.csv"))
    p_run.add_argument("--out", type=Path, default=Path("out"))
    p_run.add_argument("--through", type=lambda s: date.fromisoformat(s), default=None,
                       help="last day to include (UTC), default=today")
    p_run.set_defaults(func=cmd_run)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
