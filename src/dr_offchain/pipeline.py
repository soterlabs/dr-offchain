"""FIFO eligibility + time-weighted average balance + XR reward.

Reference: SPARK_DR_METHODOLOGY.md.
Attribution is NOT sticky: on each outflow, the tagged eligible balance is
burned FIFO, clamped at 0. Untagged inflows do NOT grow eligible, so the
pipeline only needs tagged Referrals (from JSON) + outflows (from Dune).
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
class OutflowEvent:
    ts: datetime
    depositor: str
    scope_id: str
    amount: float   # positive USDS amount leaving the tracked balance
    tx_hash: str


def _reward_per(apy: float) -> float:
    return 365.0 * (math.exp(math.log(1.0 + apy) / 365.0) - 1.0)


def _rate_for_date(d: date) -> float:
    for period in XR_RATE_PERIODS:
        if period.start <= d <= period.end:
            return _reward_per(period.apy)
    return 0.0


def build_eligibility_trajectory(
    referrals: list[SyntheticReferral],
    outflows: list[OutflowEvent],
) -> dict[tuple[str, str], list[tuple[datetime, float]]]:
    """Return {(depositor, scope_id): [(ts, eligible_after_event), ...]}
    sorted chronologically.
    """
    events: dict[tuple[str, str], list[tuple[datetime, float]]] = defaultdict(list)
    for r in referrals:
        events[(r.depositor, r.scope_id)].append((r.ts, +r.amount))
    for o in outflows:
        events[(o.depositor, o.scope_id)].append((o.ts, -o.amount))

    traj: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
    for key, evts in events.items():
        evts.sort(key=lambda x: x[0])
        eligible = 0.0
        points: list[tuple[datetime, float]] = []
        for ts, delta in evts:
            eligible = eligible + delta if delta >= 0 else max(0.0, eligible + delta)
            points.append((ts, eligible))
        traj[key] = points
    return traj


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


def monthly_rollup(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(columns=["month", "scope_kind", "tw_reward_usd"])
    df = daily.copy()
    df["month"] = pd.to_datetime(df["dt"]).dt.to_period("M").dt.to_timestamp().dt.date
    df["scope_kind"] = df["scope_id"].str.split(":", n=1).str[0]
    return (
        df.groupby(["month", "scope_kind"], as_index=False)["tw_reward_usd"]
        .sum()
        .sort_values(["month", "scope_kind"])
        .reset_index(drop=True)
    )
