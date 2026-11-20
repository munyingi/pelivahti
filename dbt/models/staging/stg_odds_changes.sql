select
    event_id,
    payload ->> 'match_id' as match_id,
    payload ->> 'selection' as selection,
    cast(payload ->> 'odds' as decimal(8, 2)) as odds,
    event_time_utc as changed_at_utc
from {{ ref('stg_events') }}
where event_type = 'odds_changed'
