-- How far the real-time daily windows drift from batch truth. The gap comes from events that
-- arrived after the watermark: the stream still booked their money but skipped the closed window.
with rt as (
    select player_id, day as business_date, net_loss as realtime_net_loss_eur
    from {{ source('realtime', 'player_daily') }}
),

batch as (
    select player_id, business_date, net_loss_eur as batch_net_loss_eur
    from {{ ref('fct_player_daily') }}
)

select
    coalesce(rt.player_id, batch.player_id) as player_id,
    coalesce(rt.business_date, batch.business_date) as business_date,
    {{ money('coalesce(realtime_net_loss_eur, 0)') }} as realtime_net_loss_eur,
    {{ money('coalesce(batch_net_loss_eur, 0)') }} as batch_net_loss_eur,
    {{ money('coalesce(batch_net_loss_eur, 0) - coalesce(realtime_net_loss_eur, 0)') }} as late_data_gap_eur
from rt
full outer join batch using (player_id, business_date)
