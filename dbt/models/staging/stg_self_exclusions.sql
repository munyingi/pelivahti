select
    event_id,
    player_id,
    event_time_utc as excluded_from_utc,
    cast(replace(payload ->> 'excluded_until', 'Z', '') as timestamp) as excluded_until_utc
from {{ ref('stg_events') }}
where event_type = 'self_exclusion_set'
