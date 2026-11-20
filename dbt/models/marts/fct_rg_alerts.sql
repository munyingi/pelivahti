select
    a.alert_id,
    a.rule,
    a.severity,
    a.category,
    a.player_id,
    a.event_time as triggered_at_utc,
    {{ to_local('a.event_time') }} as triggered_at_local,
    {{ local_date('a.event_time') }} as triggered_date,
    a.details,
    a.processed_at,
    p.age_band,
    p.region,
    p.risk_tier
from {{ source('realtime', 'alerts') }} as a
left join {{ ref('dim_players') }} as p using (player_id)
