select
    payload ->> 'match_id' as match_id,
    payload ->> 'sport' as sport,
    payload ->> 'league' as league,
    payload ->> 'home_team' as home_team,
    payload ->> 'away_team' as away_team,
    cast(replace(payload ->> 'kickoff_time', 'Z', '') as timestamp) as kickoff_utc,
    event_time_utc as published_at_utc
from {{ ref('stg_events') }}
where event_type = 'match_scheduled'
