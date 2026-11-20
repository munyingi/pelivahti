with bets as (
    select
        match_id,
        count(*) as n_bets,
        sum(stake_eur) as turnover_eur,
        sum(return_eur) filter (where status = 'won') as paid_out_eur
    from {{ ref('int_bet_settlements') }}
    group by match_id
)

select
    m.match_id,
    m.sport,
    m.league,
    m.home_team,
    m.away_team,
    m.home_team || ' - ' || m.away_team as fixture,
    m.kickoff_utc,
    {{ to_helsinki('m.kickoff_utc') }} as kickoff_helsinki,
    r.outcome as final_outcome,
    r.result_version,
    coalesce(r.was_corrected, false) as result_corrected,
    coalesce(b.n_bets, 0) as n_bets,
    {{ money('coalesce(b.turnover_eur, 0)') }} as turnover_eur,
    {{ money('coalesce(b.paid_out_eur, 0)') }} as paid_out_eur
from {{ ref('stg_matches') }} as m
left join {{ ref('int_match_final_results') }} as r using (match_id)
left join bets as b using (match_id)
