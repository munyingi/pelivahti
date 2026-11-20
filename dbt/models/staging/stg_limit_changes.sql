select
    event_id,
    player_id,
    payload ->> 'limit_type' as limit_type,
    {{ money("payload ->> 'new_limit_eur'") }} as new_limit_eur,
    event_time_utc as changed_at_utc
from {{ ref('stg_events') }}
where event_type = 'limit_changed'
