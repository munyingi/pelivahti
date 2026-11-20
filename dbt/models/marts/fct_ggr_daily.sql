-- Gross gaming revenue (GGR) = stakes - winnings, per Helsinki business day and product.
-- Sports bets count on the day they settle; void and still-open bets are excluded.
with sports as (
    select
        settled_date as business_date,
        'sports' as product,
        player_id,
        stake_eur as turnover_eur,
        return_eur as winnings_eur
    from {{ ref('fct_sports_bets') }}
    where status in ('won', 'lost')
),

casino as (
    select played_date, 'casino', player_id, stake_eur, win_eur
    from {{ ref('fct_casino_rounds') }}
),

unioned as (
    select * from sports
    union all
    select * from casino
)

select
    business_date,
    product,
    count(distinct player_id) as active_players,
    {{ money('sum(turnover_eur)') }} as turnover_eur,
    {{ money('sum(winnings_eur)') }} as winnings_eur,
    {{ money('sum(turnover_eur) - sum(winnings_eur)') }} as ggr_eur,
    round((sum(turnover_eur) - sum(winnings_eur)) / nullif(sum(turnover_eur), 0), 4) as hold_pct
from unioned
group by business_date, product
