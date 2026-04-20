"""Unit tests for sticky eligibility + TW integration."""
from __future__ import annotations

import math
from datetime import date, datetime, timezone

import pandas as pd

from dr_offchain.loader import SyntheticReferral
from dr_offchain.pipeline import (
    BalanceEvent,
    apply_rewards,
    build_sticky_trajectory,
    compute_daily_tw,
    monthly_rollup_by_scope,
)


UTC = timezone.utc


def _ref(ts, depositor, scope, amount, dest_kind="cowswap", tx_hash="0x00"):
    return SyntheticReferral(ts=ts, depositor=depositor, dest_kind=dest_kind,
                             scope_id=scope, amount=amount, tx_hash=tx_hash)


def _evt(ts, depositor, scope, direction, amount, tx_hash="0x00"):
    return BalanceEvent(ts=ts, depositor=depositor, scope_id=scope,
                        direction=direction, amount=amount, tx_hash=tx_hash)


# ---------------- Sticky eligibility ----------------


def test_sticky_single_tagged_inflow_marks_balance_eligible():
    t = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory([_ref(t, "0xaa", "s1", 100.0)], [])
    assert traj[("0xaa", "s1")] == [(t, 100.0)]


def test_sticky_untagged_inflow_before_tag_has_zero_eligible():
    # Pre-tag: balance accrues but eligible stays 0 until first tag fires.
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t2, "0xaa", "s1", 50.0)],
        [_evt(t1, "0xaa", "s1", "in", 100.0)],
    )
    # t1: untagged inflow, balance=100 but eligible=0 (not yet tagged).
    # t2: tagged inflow, balance=150, latch fires, eligible=150.
    assert traj[("0xaa", "s1")] == [(t1, 0.0), (t2, 150.0)]


def test_sticky_untagged_inflow_after_tag_grows_eligible():
    # Pure sticky: once tagged, every subsequent inflow (tagged or not)
    # increases eligible balance.
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0)],
        [_evt(t2, "0xaa", "s1", "in", 40.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 140.0)]


def test_sticky_outflow_reduces_balance():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0)],
        [_evt(t2, "0xaa", "s1", "out", 60.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 40.0)]


def test_sticky_outflow_clamps_at_zero():
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0)],
        [_evt(t2, "0xaa", "s1", "out", 150.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 0.0)]


def test_sticky_latch_persists_through_zero_balance():
    # Tag, drain to 0, then untagged inflow — attribution remains, so the
    # regrown balance is fully eligible (pure sticky).
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    t3 = datetime(2026, 3, 12, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t1, "0xaa", "s1", 100.0)],
        [_evt(t2, "0xaa", "s1", "out", 100.0),
         _evt(t3, "0xaa", "s1", "in", 25.0)],
    )
    assert traj[("0xaa", "s1")] == [(t1, 100.0), (t2, 0.0), (t3, 25.0)]


def test_sticky_outflow_before_any_tag_only_reduces_balance():
    # Pre-tag outflow clamps balance but doesn't affect eligible (still 0).
    # Tagged inflow at t3 then arms the latch on whatever balance remains.
    t1 = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 3, 11, 0, 0, tzinfo=UTC)
    t3 = datetime(2026, 3, 12, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t3, "0xaa", "s1", 30.0)],
        [_evt(t1, "0xaa", "s1", "in", 100.0),
         _evt(t2, "0xaa", "s1", "out", 40.0)],
    )
    # t1: bal=100 elig=0; t2: bal=60 elig=0; t3: bal=90 elig=90.
    assert traj[("0xaa", "s1")] == [(t1, 0.0), (t2, 0.0), (t3, 90.0)]


def test_sticky_independent_scopes():
    t = datetime(2026, 3, 10, 0, 0, tzinfo=UTC)
    traj = build_sticky_trajectory(
        [_ref(t, "0xaa", "s1", 100.0), _ref(t, "0xaa", "s2", 200.0)],
        [_evt(t, "0xaa", "s1", "out", 30.0)],
    )
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
    expected_2025 = 365.0 * (math.exp(math.log(1.006) / 365.0) - 1.0)
    expected_2026 = 365.0 * (math.exp(math.log(1.005) / 365.0) - 1.0)
    assert math.isclose(r2025, expected_2025, rel_tol=1e-9)
    assert math.isclose(r2026, expected_2026, rel_tol=1e-9)
    assert math.isclose(df.iloc[0]["tw_reward_usd"], df.iloc[0]["tw_reward"])


def test_empty_trajectory_yields_empty_df():
    daily = compute_daily_tw({}, date(2026, 3, 10))
    assert daily.empty
    assert list(daily.columns) == ["dt", "depositor", "scope_id", "tw_eligible"]


# ---------------- Monthly rollup ----------------


def test_monthly_rollup_by_scope_wide_schema():
    df = pd.DataFrame([
        {"dt": date(2026, 3, 1), "depositor": "0xaa",
         "scope_id": "wallet_usds:0xaa", "tw_eligible": 100.0},
        {"dt": date(2026, 3, 1), "depositor": "0xaa",
         "scope_id": "morpho:0xv", "tw_eligible": 300.0},
    ])
    df = apply_rewards(df)
    wide = monthly_rollup_by_scope(df)
    assert list(wide.columns) == ["month", "wallet_usds_dr_usd",
                                  "morpho_dr_usd", "total_dr_usd"]
    row = wide.iloc[0]
    assert math.isclose(row["total_dr_usd"],
                        row["wallet_usds_dr_usd"] + row["morpho_dr_usd"])


def test_monthly_rollup_empty_yields_expected_columns():
    empty = pd.DataFrame(columns=["dt", "depositor", "scope_id", "tw_eligible",
                                  "reward_per", "tw_reward", "tw_reward_usd"])
    wide = monthly_rollup_by_scope(empty)
    assert wide.empty
    assert list(wide.columns) == ["month", "wallet_usds_dr_usd",
                                  "morpho_dr_usd", "total_dr_usd"]
