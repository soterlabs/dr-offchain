-- Withdraws from Skybase-targeted Morpho VaultV2 vaults by tagged depositors.
-- Each withdraw burns the depositor's FIFO-tagged eligible balance in that
-- vault. `onBehalf` is the owner of the shares burned.
-- The placeholders below are substituted by dr_offchain.events.build_morpho_sql
-- with comma-separated 0x-prefixed hex address lists.

select
  evt_block_time as ts,
  "onBehalf" as depositor,
  contract_address as vault,
  cast(assets as double) / 1e18 as amount,
  evt_tx_hash as tx_hash,
  evt_block_number as block_number,
  evt_index as log_index
from morpho_v2_ethereum.vaultv2_evt_withdraw
where contract_address in (__VAULTS__)
  and "onBehalf" in (__DEPOSITORS__)
order by evt_block_number, evt_index
