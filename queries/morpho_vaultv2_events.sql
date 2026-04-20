-- Morpho VaultV2 Deposit + Withdraw events for tagged depositors on
-- Spark-curated vaults. Under pure sticky attribution, any Deposit to a
-- depositor's `onBehalf` grows eligible shares once they have been tagged;
-- Withdraws reduce the balance (clamped at 0).
-- The placeholders below are substituted by dr_offchain.events.build_morpho_sql
-- with comma-separated 0x-prefixed hex address lists.

with deposits as (
  select
    evt_block_time as ts,
    "onBehalf" as depositor,
    contract_address as vault,
    'in' as direction,
    cast(assets as double) / 1e18 as amount,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index as log_index
  from morpho_v2_ethereum.vaultv2_evt_deposit
  where contract_address in (__VAULTS__)
    and "onBehalf" in (__DEPOSITORS__)
),
withdraws as (
  select
    evt_block_time as ts,
    "onBehalf" as depositor,
    contract_address as vault,
    'out' as direction,
    cast(assets as double) / 1e18 as amount,
    evt_tx_hash as tx_hash,
    evt_block_number as block_number,
    evt_index as log_index
  from morpho_v2_ethereum.vaultv2_evt_withdraw
  where contract_address in (__VAULTS__)
    and "onBehalf" in (__DEPOSITORS__)
)
select * from deposits
union all
select * from withdraws
order by block_number, log_index
