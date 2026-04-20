"""Load Skybase frontend-capture JSON logs into synthetic Referral records."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import COWSWAP_SETTLEMENT, MORPHO_VAULTV2_ADDRESSES, SPARK_PSM_USDC_TO_USDS


@dataclass(frozen=True)
class SyntheticReferral:
    ts: datetime
    depositor: str  # lowercase 0x hex
    dest_kind: str  # "cowswap" | "psm" | "morpho"
    scope_id: str   # FIFO sub-account key
    amount: float   # human-units USDS (wei / 1e18)
    tx_hash: str


def _scope_id(depositor: str, dest_kind: str, target_contract: str) -> str:
    # CowSwap acquisitions and PSM USDC→USDS conversions both deliver USDS to
    # the depositor wallet; they share one FIFO bucket because outflows (any
    # USDS transfer from the wallet) cannot distinguish the acquisition route.
    if dest_kind in ("cowswap", "psm"):
        return f"wallet_usds:{depositor}"
    return f"morpho:{target_contract}"


def _classify(target_contract: str) -> str:
    t = target_contract.lower()
    if t == COWSWAP_SETTLEMENT:
        return "cowswap"
    if t == SPARK_PSM_USDC_TO_USDS:
        return "psm"
    if t in MORPHO_VAULTV2_ADDRESSES:
        return "morpho"
    raise ValueError(f"unknown target_contract {target_contract}")


def load_file(path: Path) -> list[SyntheticReferral]:
    with path.open() as f:
        rows = json.load(f)
    out: list[SyntheticReferral] = []
    for r in rows:
        dep = r["depositor"].lower()
        target = r["target_contract"].lower()
        kind = _classify(target)
        ts = datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc)
        amount = int(r["amount_wei"]) / 1e18
        out.append(SyntheticReferral(
            ts=ts,
            depositor=dep,
            dest_kind=kind,
            scope_id=_scope_id(dep, kind, target),
            amount=amount,
            tx_hash=r["tx_hash"].lower(),
        ))
    return out


def load_dir(data_dir: Path) -> list[SyntheticReferral]:
    out: list[SyntheticReferral] = []
    for path in sorted(data_dir.glob("skybase_*.json")):
        out.extend(load_file(path))
    return out
