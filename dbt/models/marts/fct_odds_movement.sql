with odds as (
    select o.*, m.kickoff_utc
    from {{ ref('stg_odds_changes') }} as o
    join {{ ref('stg_matches') }} as m using (match_id)
    where o.changed_at_utc < m.kickoff_utc
),

bets as (
    select match_id, selection, count(*) as n_bets, sum(stake_eur) as stakes_eur
    from {{ ref('stg_bets') }}
    group by 1, 2
)

select
    o.match_id,
    o.selection,
    arg_min(o.odds, o.changed_at_utc) as opening_odds,
    arg_max(o.odds, o.changed_at_utc) as closing_odds,
    count(*) - 1 as n_moves,
    round((arg_max(o.odds, o.changed_at_utc) - arg_min(o.odds, o.changed_at_utc))
        / arg_min(o.odds, o.changed_at_utc), 4) as drift_pct,
    coalesce(max(b.n_bets), 0) as n_bets,
    {{ money('coalesce(max(b.stakes_eur), 0)') }} as stakes_eur
from odds as o
left join bets as b using (match_id, selection)
group by o.match_id, o.selection
