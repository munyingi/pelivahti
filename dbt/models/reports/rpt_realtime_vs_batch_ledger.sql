-- Lambda check: the stream engine's ledger and the batch rebuild must agree per player, to the cent.
with rt as (
    select player_id, sum(amount_eur) as realtime_net_eur, count(*) as realtime_entries
    from {{ source('realtime', 'ledger') }}
    group by 1
),

batch as (
    select player_id, sum(amount_eur) as batch_net_eur, count(*) as batch_entries
    from {{ ref('int_wallet_movements') }}
    group by 1
)

select
    coalesce(rt.player_id, batch.player_id) as player_id,
    {{ money('coalesce(realtime_net_eur, 0)') }} as realtime_net_eur,
    {{ money('coalesce(batch_net_eur, 0)') }} as batch_net_eur,
    {{ money('coalesce(realtime_net_eur, 0) - coalesce(batch_net_eur, 0)') }} as diff_eur,
    coalesce(realtime_entries, 0) as realtime_entries,
    coalesce(batch_entries, 0) as batch_entries
from rt
full outer join batch using (player_id)
