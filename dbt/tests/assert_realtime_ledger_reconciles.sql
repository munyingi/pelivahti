-- The stream and the batch layer must book exactly the same money for every player.
select * from {{ ref('rpt_realtime_vs_batch_ledger') }}
where diff_eur <> 0
