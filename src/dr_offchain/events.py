"""Dune SQL prep (depositor/vault substitution) + CSV loaders for balance events.

The Dune exports carry BOTH directions (inflows + outflows). Inflows that
correspond to Skybase-tagged referrals (same tx_hash + depositor) are
dropped by the loaders to avoid double-counting; the Skybase record is the
authoritative tagged source and is consumed from JSON via `loader.py`.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from .loader import SyntheticReferral
from .pipeline import BalanceEvent


_SQL_DIR = Path(__file__).resolve().parents[2] / "queries"


def _fmt_addrs(addrs: list[str]) -> str:
    return ", ".join(a if a.startswith("0x") else f"0x{a}" for a in addrs)


def build_wallet_usds_sql(depositors: list[str]) -> str:
    sql = (_SQL_DIR / "wallet_usds_transfers.sql").read_text()
    return sql.replace("__DEPOSITORS__", _fmt_addrs(depositors))


def build_morpho_sql(depositors: list[str], vaults: list[str]) -> str:
    sql = (_SQL_DIR / "morpho_vaultv2_events.sql").read_text()
    return (
        sql.replace("__DEPOSITORS__", _fmt_addrs(depositors))
           .replace("__VAULTS__", _fmt_addrs(vaults))
    )


def _ts(val) -> datetime:
    return pd.to_datetime(val, utc=True).to_pydatetime()


def _tagged_keys(
    referrals: list[SyntheticReferral], scope_kind: str
) -> set[tuple[str, str]]:
    """Return {(tx_hash, depositor)} for referrals whose scope_id starts with
    `scope_kind:`. Used to drop the corresponding Dune inflow rows.
    """
    return {
        (r.tx_hash, r.depositor)
        for r in referrals
        if r.scope_id.startswith(f"{scope_kind}:")
    }


def load_wallet_usds_events(
    csv_path: Path, referrals: list[SyntheticReferral]
) -> list[BalanceEvent]:
    """Dune-exported CSV: ts, depositor, direction, amount, tx_hash, block_number, log_index

    Inflow rows matching a Skybase-tagged wallet_usds referral by
    (tx_hash, depositor) are dropped — the tagged referral provides the
    authoritative inflow already.
    """
    df = pd.read_csv(csv_path)
    tagged = _tagged_keys(referrals, "wallet_usds")
    out: list[BalanceEvent] = []
    for _, r in df.iterrows():
        dep = str(r["depositor"]).lower()
        tx = str(r["tx_hash"]).lower()
        direction = str(r["direction"])
        if direction == "in" and (tx, dep) in tagged:
            continue
        out.append(BalanceEvent(
            ts=_ts(r["ts"]),
            depositor=dep,
            scope_id=f"wallet_usds:{dep}",
            direction=direction,
            amount=float(r["amount"]),
            tx_hash=tx,
        ))
    return out


def load_morpho_events(
    csv_path: Path, referrals: list[SyntheticReferral]
) -> list[BalanceEvent]:
    """Dune-exported CSV: ts, depositor, vault, direction, amount, tx_hash, block_number, log_index

    Deposit rows matching a Skybase-tagged morpho referral by
    (tx_hash, depositor) are dropped to avoid double-counting.
    """
    df = pd.read_csv(csv_path)
    tagged = _tagged_keys(referrals, "morpho")
    out: list[BalanceEvent] = []
    for _, r in df.iterrows():
        dep = str(r["depositor"]).lower()
        vault = str(r["vault"]).lower()
        tx = str(r["tx_hash"]).lower()
        direction = str(r["direction"])
        if direction == "in" and (tx, dep) in tagged:
            continue
        out.append(BalanceEvent(
            ts=_ts(r["ts"]),
            depositor=dep,
            scope_id=f"morpho:{vault}",
            direction=direction,
            amount=float(r["amount"]),
            tx_hash=tx,
        ))
    return out
