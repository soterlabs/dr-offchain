"""Unit tests for CSV loaders and dedupe against tagged Skybase referrals."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dr_offchain.events import load_morpho_events, load_wallet_usds_events
from dr_offchain.loader import SyntheticReferral


UTC = timezone.utc


def _ref(depositor, scope, tx_hash, amount=100.0, dest_kind="cowswap"):
    return SyntheticReferral(
        ts=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
        depositor=depositor,
        dest_kind=dest_kind,
        scope_id=scope,
        amount=amount,
        tx_hash=tx_hash,
    )


# ---------------- Wallet USDS loader ----------------


def test_wallet_usds_loader_drops_tagged_inflow(tmp_path: Path) -> None:
    csv = tmp_path / "w.csv"
    csv.write_text(
        "ts,depositor,direction,amount,tx_hash,block_number,log_index\n"
        "2026-03-10 00:00:00+00:00,0xaa,in,100.0,0xtag,1,0\n"    # tagged → drop
        "2026-03-11 00:00:00+00:00,0xaa,in,50.0,0xuntag,2,0\n"   # untagged → keep
        "2026-03-12 00:00:00+00:00,0xaa,out,20.0,0xout,3,0\n"    # outflow → keep
    )
    refs = [_ref("0xaa", "wallet_usds:0xaa", "0xtag")]
    events = load_wallet_usds_events(csv, refs)
    assert len(events) == 2
    kinds = {(e.direction, e.amount) for e in events}
    assert kinds == {("in", 50.0), ("out", 20.0)}
    assert all(e.scope_id == "wallet_usds:0xaa" for e in events)


def test_wallet_usds_loader_keeps_outflow_matching_tagged_tx(tmp_path: Path) -> None:
    # Dedupe only applies to inflows — an outflow in the same tx (rare but
    # possible with routers) must still be counted.
    csv = tmp_path / "w.csv"
    csv.write_text(
        "ts,depositor,direction,amount,tx_hash,block_number,log_index\n"
        "2026-03-10 00:00:00+00:00,0xaa,out,10.0,0xtag,1,0\n"
    )
    refs = [_ref("0xaa", "wallet_usds:0xaa", "0xtag")]
    events = load_wallet_usds_events(csv, refs)
    assert len(events) == 1
    assert events[0].direction == "out"


def test_wallet_usds_loader_dedupe_is_per_depositor(tmp_path: Path) -> None:
    # A tagged tx for 0xaa must not suppress an inflow row targeting 0xbb.
    csv = tmp_path / "w.csv"
    csv.write_text(
        "ts,depositor,direction,amount,tx_hash,block_number,log_index\n"
        "2026-03-10 00:00:00+00:00,0xbb,in,100.0,0xtag,1,0\n"
    )
    refs = [_ref("0xaa", "wallet_usds:0xaa", "0xtag")]
    events = load_wallet_usds_events(csv, refs)
    assert len(events) == 1
    assert events[0].depositor == "0xbb"


# ---------------- Morpho loader ----------------


def test_morpho_loader_drops_tagged_deposit(tmp_path: Path) -> None:
    csv = tmp_path / "m.csv"
    csv.write_text(
        "ts,depositor,vault,direction,amount,tx_hash,block_number,log_index\n"
        "2026-03-10 00:00:00+00:00,0xaa,0xv1,in,100.0,0xtag,1,0\n"
        "2026-03-11 00:00:00+00:00,0xaa,0xv1,in,50.0,0xuntag,2,0\n"
        "2026-03-12 00:00:00+00:00,0xaa,0xv1,out,30.0,0xwd,3,0\n"
    )
    refs = [_ref("0xaa", "morpho:0xv1", "0xtag", dest_kind="morpho")]
    events = load_morpho_events(csv, refs)
    assert len(events) == 2
    assert {(e.direction, e.amount) for e in events} == {("in", 50.0), ("out", 30.0)}
    assert all(e.scope_id == "morpho:0xv1" for e in events)


def test_morpho_loader_dedupe_scoped_by_kind(tmp_path: Path) -> None:
    # A wallet_usds-scoped referral with tx_hash=0xtag should NOT suppress
    # a morpho-scope deposit row with the same tx_hash.
    csv = tmp_path / "m.csv"
    csv.write_text(
        "ts,depositor,vault,direction,amount,tx_hash,block_number,log_index\n"
        "2026-03-10 00:00:00+00:00,0xaa,0xv1,in,100.0,0xtag,1,0\n"
    )
    refs = [_ref("0xaa", "wallet_usds:0xaa", "0xtag")]
    events = load_morpho_events(csv, refs)
    assert len(events) == 1
