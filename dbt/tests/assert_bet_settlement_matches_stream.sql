-- Per bet, the stream's net settlement (payouts + refunds - reversals) must equal the batch result.
with rt as (
    select ref_id as bet_id, sum(amount_eur) as rt_return_eur
    from {{ source('realtime', 'ledger') }}
    where product = 'sports' and entry_type in ('PAYOUT', 'REFUND', 'REVERSAL')
    group by 1
)

select b.bet_id, b.status, b.return_eur, coalesce(rt.rt_return_eur, 0) as rt_return_eur
from {{ ref('fct_sports_bets') }} as b
left join rt using (bet_id)
where b.return_eur <> coalesce(rt.rt_return_eur, 0)
