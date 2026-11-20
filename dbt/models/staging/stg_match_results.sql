select
    event_id,
    payload ->> 'match_id' as match_id,
    payload ->> 'outcome' as outcome,
    cast(payload ->> 'result_version' as integer) as result_version,
    event_time_utc as resulted_at_utc
from {{ ref('stg_events') }}
where event_type = 'match_result'
