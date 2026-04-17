"""Unit tests for FIFO eligibility + TW integration."""
from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pandas as pd

from dr_offchain.loader import SyntheticReferral
from dr_offchain.pipeline import (
    OutflowEvent,
    apply_rewards,
    build_eligibility_trajectory,
    compute_daily_tw,
)


UTC = timezone.utc


def _ref(ts, depositor, scope, amount):
    return SyntheticReferral(ts=ts, depositor=depositor, dest_kind="cowswap",
                             scope_id=scope, amount=amount, tx_hash="0x00")


def _out(ts, depositor, scope, amount):
    return OutflowEvent(ts=ts, depositor=depositor, scope_id=scope,
                        amount=amount, tx_hash="0x00")


# ---------------- FIFO eligibility ----------------


def test_fifo_simple_deposit():
    t = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    traj = build_eligibility_trajectory([_ref(t, "0xaa", "s1", 100.0)], [])
    assert traj[("0xaa", "s1")] == [(t, 100.0)]


def test_fifo_outflow_burns():
    t1 = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)
    traj = build_eligibility_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0)],
        [_out(t2, "0xaa", "s1", 60.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 40.0)]


def test_fifo_outflow_clamps_at_zero():
    t1 = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)
    traj = build_eligibility_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0)],
        [_out(t2, "0xaa", "s1", 150.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 0.0)]


def test_fifo_multiple_deposits_stack():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 15, 0, 0, tzinfo=UTC)
    t3 = datetime(2026, 3, 20, 0, 0, tzinfo=UTC)
    traj = build_eligibility_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0), _ref(t2, "0xaa", "s1", 50.0)],
        [_out(t3, "0xaa", "s1", 120.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 150.0), (t3, 30.0)]


def test_fifo_outflow_before_any_deposit_is_noop():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    traj = build_eligibility_trajectory(
        [_ref(t2, "0xaa", "s1", 100.0)],
        [_out(t1, "0xaa", "s1", 50.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 0.0), (t2, 100.0)]


def test_fifo_independent_scopes():
    t = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    traj = build_eligibility_trajectory(
        [_ref(t, "0xaa", "s1", 100.0), _ref(t, "0xaa", "s2", 200.0)],
        [_out(t, "0xaa", "s1", 30.0)],
    )
    # FIFO burn applies only to s1
    assert ("0xaa", "s1") in traj and ("0xaa", "s2") in traj
    assert traj[("0xaa", "s1")][-1][1] == 70.0
    assert traj[("0xaa", "s2")][-1][1] == 200.0


# ---------------- TW integration ----------------


def test_tw_full_day():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    daily = compute_daily_tw({("0xaa", "s1"): [(t1, 100.0)]}, date(2026, 3, 10))
    assert len(daily) == 1
    assert daily.iloc[0]["tw_eligible"] == 100.0
    assert daily.iloc[0]["dt"] == date(2026, 3, 10)


def test_tw_mid_day_deposit():
    t1 = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)  # noon
    daily = compute_daily_tw({("0xaa", "s1"): [(t1, 100.0)]}, date(2026, 3, 10))
    # 100 eligible for exactly 12h = 43200s → TW = 100 * 43200/86400 = 50
    assert daily.iloc[0]["tw_eligible"] == 50.0


def test_tw_forward_fills_after_last_event():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    daily = compute_daily_tw({("0xaa", "s1"): [(t1, 100.0)]}, date(2026, 3, 12))
    assert len(daily) == 3
    assert all(daily["tw_eligible"] == 100.0)


def test_tw_outflow_within_day():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    daily = compute_daily_tw(
        {("0xaa", "s1"): [(t1, 100.0), (t2, 40.0)]}, date(2026, 3, 10),
    )
    # (100 * 43200 + 40 * 43200) / 86400 = 70
    assert daily.iloc[0]["tw_eligible"] == 70.0


def test_tw_spans_day_boundary():
    t1 = datetime(2026, 3, 10, 18, 0, tzinfo=UTC)  # 6h left in day
    daily = compute_daily_tw({("0xaa", "s1"): [(t1, 100.0)]}, date(2026, 3, 11))
    d1 = daily[daily["dt"] == date(2026, 3, 10)].iloc[0]["tw_eligible"]
    d2 = daily[daily["dt"] == date(2026, 3, 11)].iloc[0]["tw_eligible"]
    assert math.isclose(d1, 100.0 * 6 / 24)
    assert d2 == 100.0


# ---------------- Rate application ----------------


def test_reward_rate_2025_vs_2026_boundary():
    df = pd.DataFrame([
        {"dt": date(2025, 6, 1), "depositor": "0xaa", "scope_id": "s1", "tw_eligible": 365.0},
        {"dt": date(2026, 6, 1), "depositor": "0xaa", "scope_id": "s1", "tw_eligible": 365.0},
    ])
    df = apply_rewards(df)
    r2025 = df[df["dt"] == date(2025, 6, 1)].iloc[0]["tw_reward"]
    r2026 = df[df["dt"] == date(2026, 6, 1)].iloc[0]["tw_reward"]
    # tw_reward with tw_eligible=365 equals reward_per = 365*(exp(ln(1+APY)/365)-1).
    expected_2025 = 365.0 * (math.exp(math.log(1.006) / 365.0) - 1.0)
    expected_2026 = 365.0 * (math.exp(math.log(1.005) / 365.0) - 1.0)
    assert math.isclose(r2025, expected_2025, rel_tol=1e-9)
    assert math.isclose(r2026, expected_2026, rel_tol=1e-9)
    assert math.isclose(df.iloc[0]["tw_reward_usd"], df.iloc[0]["tw_reward"])


def test_empty_trajectory_yields_empty_df():
    daily = compute_daily_tw({}, date(2026, 3, 10))
    assert daily.empty
    assert list(daily.columns) == ["dt", "depositor", "scope_id", "tw_eligible"]
