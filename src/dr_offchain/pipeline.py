"""Sticky attribution + time-weighted average balance + XR reward.

Reference: SPARK_DR_METHODOLOGY.md.

Attribution is STICKY, matching Spark's on-chain `last_value(ref_code) ignore
nulls` semantics: once a depositor has their first tagged inflow in a given
scope, the entire running balance becomes eligible for rewards. Subsequent
untagged inflows grow eligible; outflows reduce it (clamped at 0). The
attribution latch never releases, so a balance that hits 0 and regrows from
untagged deposits is still fully eligible.

Scopes:
  * `wallet_usds:{depositor}`  — USDS held in the depositor's wallet. Tagged
    inflows come from CowSwap or PSM routes; both deliver USDS to the wallet
    and share this single balance bucket.
  * `morpho:{vault}`            — Shares owned in a Spark-curated Morpho
    VaultV2. Tagged inflows are Skybase-routed deposits; untagged inflows
    are Deposit events fired by the vault for the same `onBehalf` from any
    other entry point.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from .config import XR_RATE_PERIODS
from .loader import SyntheticReferral


@dataclass(frozen=True)
class BalanceEvent:
    """Untagged balance change observed on-chain (from Dune).

    `direction` is "in" for inflows (balance grows) or "out" for outflows
    (balance shrinks, clamped at 0).
    """
    ts: datetime
    depositor: str
    scope_id: str
    direction: str  # "in" | "out"
    amount: float
    tx_hash: str


def _reward_per(apy: float) -> float:
    return 365.0 * (math.exp(math.log(1.0 + apy) / 365.0) - 1.0)


def _rate_for_date(d: date) -> float:
    for period in XR_RATE_PERIODS:
        if period.start <= d <= period.end:
            return _reward_per(period.apy)
    return 0.0


def build_sticky_trajectory(
    referrals: list[SyntheticReferral],
    events: list[BalanceEvent],
) -> dict[tuple[str, str], list[tuple[datetime, float]]]:
    """Return {(depositor, scope_id): [(ts, eligible_after_event), ...]}.

    Merges tagged referrals (which inject an inflow AND arm the attribution
    latch) with untagged Dune balance events. For each scope, iterates events
    in chronological order maintaining a non-negative running balance and a
    one-way `attributed` latch. Eligible after each event is the running
    balance if the latch has ever fired, else 0.
    """
    # (ts, delta, is_tagged) tuples per (depositor, scope_id)
    merged: dict[tuple[str, str], list[tuple[datetime, float, bool]]] = defaultdict(list)
    for r in referrals:
        merged[(r.depositor, r.scope_id)].append((r.ts, +r.amount, True))
    for e in events:
        delta = e.amount if e.direction == "in" else -e.amount
        merged[(e.depositor, e.scope_id)].append((e.ts, delta, False))

    result: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    for key, evts in merged.items():
        # Stable sort so tagged + untagged events at the same ts preserve
        # input order; both yield the same post-event balance regardless.
        evts.sort(key=lambda x: x[0])
        balance = 0.0
        attributed = False
        snaps: list[tuple[datetime, float]] = []
        for ts, delta, is_tagged in evts:
            balance = max(0.0, balance + delta)
            if is_tagged:
                attributed = True
            snaps.append((ts, balance if attributed else 0.0))
        result[key] = snaps
    return result


def compute_daily_tw(
    trajectory: dict[tuple[str, str], list[tuple[datetime, float]]],
    through: date,
) -> pd.DataFrame:
    """Integrate piecewise-constant eligible series into daily TW averages
    through `through` (inclusive, UTC). Emits one row per (day, depositor,
    scope_id) with `tw_eligible = sum(elig_i * duration_i) / 86400`.
    """
    end_ts = datetime.combine(through + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    rows: list[dict] = []

    for (depositor, scope_id), points in trajectory.items():
        if not points:
            continue
        day_sum: dict[date, float] = defaultdict(float)
        # points[i] = (t_i, elig after event i). Segment i runs [t_i, t_{i+1}).
        # Final segment extends to end_ts with the last eligible value.
        segments = list(zip(points, points[1:] + [(end_ts, points[-1][1])]))
        for (t0, elig), (t1, _) in segments:
            if t0 >= end_ts:
                break
            t1 = min(t1, end_ts)
            cur = t0
            while cur < t1:
                day_end = datetime.combine(cur.date() + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
                seg_end = min(t1, day_end)
                duration = (seg_end - cur).total_seconds()
                day_sum[cur.date()] += elig * duration / 86400.0
                cur = seg_end
        for d, tw in sorted(day_sum.items()):
            rows.append({
                "dt": d,
                "depositor": depositor,
                "scope_id": scope_id,
                "tw_eligible": tw,
            })
    if not rows:
        return pd.DataFrame(columns=["dt", "depositor", "scope_id", "tw_eligible"])
    return pd.DataFrame(rows)


def apply_rewards(daily: pd.DataFrame) -> pd.DataFrame:
    """Attach reward_per / tw_reward / tw_reward_usd (USDS price = 1)."""
    if daily.empty:
        return daily.assign(reward_per=[], tw_reward=[], tw_reward_usd=[])
    df = daily.copy()
    df["reward_per"] = df["dt"].apply(_rate_for_date)
    df["tw_reward"] = df["tw_eligible"] / 365.0 * df["reward_per"]
    df["tw_reward_usd"] = df["tw_reward"]  # USDS is $1-pegged
    return df


_SCOPE_KINDS = ("wallet_usds", "morpho")


def monthly_rollup_by_scope(daily: pd.DataFrame) -> pd.DataFrame:
    """Wide-format monthly rollup: one column per scope kind + total.

    Expects `daily` to carry scope_id of the form `<kind>:<suffix>` where
    kind ∈ {wallet_usds, morpho}. Emits columns
    `month, wallet_usds_dr_usd, morpho_dr_usd, total_dr_usd`.
    """
    cols = ["month", *[f"{k}_dr_usd" for k in _SCOPE_KINDS], "total_dr_usd"]
    if daily.empty:
        return pd.DataFrame(columns=cols)
    df = daily.copy()
    df["month"] = pd.to_datetime(df["dt"]).dt.to_period("M").dt.to_timestamp().dt.date
    df["scope_kind"] = df["scope_id"].str.split(":", n=1).str[0]
    pivot = (
        df.groupby(["month", "scope_kind"])["tw_reward_usd"].sum()
        .unstack("scope_kind", fill_value=0.0)
        .reset_index()
    )
    for k in _SCOPE_KINDS:
        if k not in pivot.columns:
            pivot[k] = 0.0
    pivot["total_dr_usd"] = pivot[list(_SCOPE_KINDS)].sum(axis=1)
    pivot = pivot.rename(columns={k: f"{k}_dr_usd" for k in _SCOPE_KINDS})
    return pivot[cols].sort_values("month").reset_index(drop=True)
