-- USDS outflows from Skybase-tagged CowSwap depositors' wallets on Ethereum.
-- Each outflow burns the depositor's FIFO-tagged eligible balance.
-- The placeholder below is substituted by dr_offchain.events.build_cowswap_sql
-- with a comma-separated list of 0x-prefixed hex addresses.

select
  evt_block_time as ts,
  "from" as depositor,
  cast(value as double) / 1e18 as amount,
  evt_tx_hash as tx_hash,
  evt_block_number as block_number,
  evt_index as log_index
from sky_ethereum.usds_evt_transfer
where "from" in (__DEPOSITORS__)
  and "to" != 0x0000000000000000000000000000000000000000
order by evt_block_number, evt_index
