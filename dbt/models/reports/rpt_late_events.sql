-- Late arrivals seen by the stream, copied into the warehouse for the dashboard.
select event_id, event_type, event_time as event_time_utc, watermark as watermark_utc, lateness_seconds
from {{ source('realtime', 'late_events') }}
