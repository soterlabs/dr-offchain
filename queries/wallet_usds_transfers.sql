-- USDS transfers touching Skybase-tagged depositor wallets on Ethereum,
-- BOTH directions. Under pure sticky attribution, inflows (including
-- untagged) grow the depositor's eligible balance once they have been
-- tagged by any prior Skybase deposit; outflows reduce it (clamped at 0).
-- Burns (to = 0x0) are included as genuine outflows.
-- The placeholder below is substituted by dr_offchain.events.build_wallet_usds_sql
-- with a comma-separated list of 0x-prefixed hex depositor addresses.

with inflows as (
  select
    evt_block_time as ts,
    "to" as depositor,
    'in' as direction,
    cast(value as double) / 1e18 as amount,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index as log_index
  from sky_ethereum.usds_evt_transfer
  where "to" in (__DEPOSITORS__)
),
outflows as (
  select
    evt_block_time as ts,
    "from" as depositor,
    'out' as direction,
    cast(value as double) / 1e18 as amount,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index as log_index
  from sky_ethereum.usds_evt_transfer
  where "from" in (__DEPOSITORS__)
)
select * from inflows
union all
select * from outflows
order by block_number, log_index
