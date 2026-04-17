"""Dune SQL prep (depositor/vault substitution) + CSV loaders for outflows."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from .pipeline import OutflowEvent


_SQL_DIR = Path(__file__).resolve().parents[2] / "queries"


def _fmt_addrs(addrs: list[str]) -> str:
    return ", ".join(a if a.startswith("0x") else f"0x{a}" for a in addrs)


def build_cowswap_sql(depositors: list[str]) -> str:
    sql = (_SQL_DIR / "cowswap_usds_outflows.sql").read_text()
    return sql.replace("__DEPOSITORS__", _fmt_addrs(depositors))


def build_morpho_sql(depositors: list[str], vaults: list[str]) -> str:
    sql = (_SQL_DIR / "morpho_vaultv2_withdraws.sql").read_text()
    return (
        sql.replace("__DEPOSITORS__", _fmt_addrs(depositors))
           .replace("__VAULTS__", _fmt_addrs(vaults))
    )


def _ts(val) -> datetime:
    return pd.to_datetime(val, utc=True).to_pydatetime()


def load_cowswap_outflows(csv_path: Path) -> list[OutflowEvent]:
    """Dune-exported CSV: ts, depositor, amount, tx_hash, block_number, log_index"""
    df = pd.read_csv(csv_path)
    return [
        OutflowEvent(
            ts=_ts(r["ts"]),
            depositor=str(r["depositor"]).lower(),
            scope_id=f"cowswap:{str(r['depositor']).lower()}",
            amount=float(r["amount"]),
            tx_hash=str(r["tx_hash"]).lower(),
        )
        for _, r in df.iterrows()
    ]


def load_morpho_outflows(csv_path: Path) -> list[OutflowEvent]:
    """Dune-exported CSV: ts, depositor, vault, amount, tx_hash, block_number, log_index"""
    df = pd.read_csv(csv_path)
    return [
        OutflowEvent(
            ts=_ts(r["ts"]),
            depositor=str(r["depositor"]).lower(),
            scope_id=f"morpho:{str(r['vault']).lower()}",
            amount=float(r["amount"]),
            tx_hash=str(r["tx_hash"]).lower(),
        )
        for _, r in df.iterrows()
    ]
