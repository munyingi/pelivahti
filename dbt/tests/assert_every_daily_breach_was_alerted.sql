-- Every day where batch truth shows a limit breach should have a real-time alert.
-- Set to warn: a breach caused only by late events can legitimately be missed by the stream,
-- which is exactly the gap this test makes visible.
{{ config(severity='warn') }}
select d.player_id, d.business_date, d.net_loss_eur, d.daily_loss_limit_eur
from {{ ref('fct_player_daily') }} as d
left join {{ ref('fct_rg_alerts') }} as a
    on a.player_id = d.player_id
    and a.triggered_date = d.business_date
    and a.rule = 'DAILY_LOSS_LIMIT_BREACH'
where d.limit_breached and a.alert_id is null
