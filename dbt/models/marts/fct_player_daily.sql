-- Batch truth for every player and local day, including late events the stream skipped.
with movements as (
    select *, {{ local_date('at_utc') }} as business_date
    from {{ ref('int_wallet_movements') }}
),

daily as (
    select
        player_id,
        business_date,
        sum(-amount_eur) filter (where movement = 'STAKE') as stakes_eur,
        sum(amount_eur) filter (where movement in ('PAYOUT', 'REFUND', 'WIN')) as returns_eur,
        sum(amount_eur) filter (where movement = 'DEPOSIT') as deposits_eur,
        sum(-amount_eur) filter (where movement = 'WITHDRAWAL') as withdrawals_eur,
        count(*) filter (where movement = 'STAKE' and product = 'sports') as n_bets,
        count(*) filter (where movement = 'STAKE' and product = 'casino') as n_rounds
    from movements
    group by 1, 2
),

limits as (
    select player_id, effective_from_utc, limit_eur
    from {{ ref('int_player_limits') }}
    where limit_type = 'daily'
),

with_day_end as (
    select
        *,
        -- last instant of the local day, expressed in UTC
        timezone('UTC', timezone('{{ var("business_timezone") }}', cast(business_date + 1 as timestamp))) - interval 1 microsecond
            as day_end_utc
    from daily
)

select
    d.player_id,
    d.business_date,
    {{ money('coalesce(d.stakes_eur, 0)') }} as stakes_eur,
    {{ money('coalesce(d.returns_eur, 0)') }} as returns_eur,
    {{ money('coalesce(d.stakes_eur, 0) - coalesce(d.returns_eur, 0)') }} as net_loss_eur,
    {{ money('coalesce(d.deposits_eur, 0)') }} as deposits_eur,
    {{ money('coalesce(d.withdrawals_eur, 0)') }} as withdrawals_eur,
    d.n_bets,
    d.n_rounds,
    l.limit_eur as daily_loss_limit_eur,
    coalesce(d.stakes_eur, 0) - coalesce(d.returns_eur, 0) > l.limit_eur as limit_breached
from with_day_end as d
asof left join limits as l
    on d.player_id = l.player_id and d.day_end_utc >= l.effective_from_utc
