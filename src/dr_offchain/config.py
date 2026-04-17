"""Addresses, rate periods, and token constants for the DR off-chain pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


USDS_ETHEREUM = "0xdc035d45d973e3ec169d2276ddab16f1e407384f"
USDS_DECIMALS = 18

COWSWAP_SETTLEMENT = "0x9008d19f58aabd9ed0d60971565aa8510560ab41"

MORPHO_VAULTV2_ADDRESSES = {
    "0xe15fcc81118895b67b6647bbd393182df44e11e0",
    "0xf42bca228d9bd3e2f8ee65fec3d21de1063882d4",
}


@dataclass(frozen=True)
class RatePeriod:
    start: date
    end: date
    apy: float  # e.g. 0.005 = 0.5% APY


# XR "Accessibility Rewards" APY schedule — see SPARK_DR_METHODOLOGY.md §1.
# Values are the full-APY inputs; pipeline converts to per-day rate via
#   reward_per = 365 * (exp(ln(1 + APY) / 365) - 1)
XR_RATE_PERIODS: tuple[RatePeriod, ...] = (
    RatePeriod(date(2025, 1, 1), date(2025, 12, 31), 0.006),
    RatePeriod(date(2026, 1, 1), date(2030, 12, 31), 0.005),
)
