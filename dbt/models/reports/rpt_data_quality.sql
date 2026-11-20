-- One row per quality metric, for the dashboard and for alerting.
select 'bronze_events' as metric, count(*)::bigint as value, 'unique valid events in Delta bronze' as note
from {{ source('bronze', 'events') }}
union all
select 'bronze_dlq', count(*), 'messages that broke their data contract'
from {{ source('bronze', 'dlq') }}
union all
select 'stream_duplicates_dropped', count(*), 'redelivered messages ignored by event_id'
from {{ source('realtime', 'duplicates') }}
union all
select 'stream_late_events', count(*), 'events older than the watermark (booked, but skipped windowed rules)'
from {{ source('realtime', 'late_events') }}
union all
select 'stream_dlq', count(*), 'contract violations seen by the stream'
from {{ source('realtime', 'dlq') }}
union all
select 'ledger_players_out_of_balance', count(*) filter (where diff_eur <> 0), 'players where stream and batch disagree'
from {{ ref('rpt_realtime_vs_batch_ledger') }}
union all
select 'stream_batches', count(*), 'committed micro-batches'
from {{ source('realtime', 'batches') }}
