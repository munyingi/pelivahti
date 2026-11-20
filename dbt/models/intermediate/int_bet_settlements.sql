with bets as (
    select * from {{ ref('stg_bets') }}
),

results as (
    select * from {{ ref('int_match_final_results') }}
)

select
    b.bet_id,
    b.player_id,
    b.match_id,
    b.selection,
    b.odds,
    b.stake_eur,
    b.channel,
    b.placed_at_utc,
    r.outcome,
    r.result_version,
    coalesce(r.was_corrected, false) as result_corrected,
    case
        when r.match_id is null then 'open'
        when r.outcome = 'void' then 'void'
        when r.outcome = b.selection then 'won'
        else 'lost'
    end as status,
    -- a bet placed after the result was known settles at bet time (same rule as the stream engine)
    case when r.match_id is not null then greatest(r.resulted_at_utc, b.placed_at_utc) end as settled_at_utc,
    {{ money("case
        when r.outcome = 'void' then b.stake_eur
        when r.outcome = b.selection then round(b.stake_eur * b.odds, 2)
        else 0
    end") }} as return_eur
from bets as b
left join results as r using (match_id)
