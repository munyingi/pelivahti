-- One row per event. Bronze is already idempotent on event_id; the qualify is a second safety net.
select
    event_id,
    event_type,
    schema_version,
    timezone('UTC', event_time) as event_time_utc,
    player_id,
    payload,
    source_partition,
    source_offset,
    ingested_at
from {{ source('bronze', 'events') }}
qualify row_number() over (partition by event_id order by ingested_at, source_offset) = 1
