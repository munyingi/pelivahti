-- Micro-batch throughput and watermark progress of the stream engine.
select
    batch_no,
    records,
    watermark as watermark_utc,
    seconds,
    round(records / nullif(seconds, 0)) as records_per_second,
    committed_at
from {{ source('realtime', 'batches') }}
