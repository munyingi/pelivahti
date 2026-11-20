-- The batch layer's own version of the wallet ledger, rebuilt from bronze only.
-- It must agree with the stream engine's ledger to the cent (see rpt_realtime_vs_batch_ledger).
select player_id, 'DEPOSIT' as movement, 'wallet' as product, transaction_id as ref_id,
       amount_eur, deposited_at_utc as at_utc
from {{ ref('stg_deposits') }}
union all
select player_id, 'WITHDRAWAL', 'wallet', transaction_id, -amount_eur, withdrawn_at_utc
from {{ ref('stg_withdrawals') }}
union all
select player_id, 'STAKE', 'sports', bet_id, -stake_eur, placed_at_utc
from {{ ref('int_bet_settlements') }}
union all
select player_id, case status when 'void' then 'REFUND' else 'PAYOUT' end, 'sports', bet_id, return_eur, settled_at_utc
from {{ ref('int_bet_settlements') }}
where status in ('won', 'void')
union all
select player_id, 'STAKE', 'casino', round_id, -stake_eur, played_at_utc
from {{ ref('stg_casino_rounds') }}
union all
select player_id, 'WIN', 'casino', round_id, win_eur, played_at_utc
from {{ ref('stg_casino_rounds') }}
where win_eur > 0
