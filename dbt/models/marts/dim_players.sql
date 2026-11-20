with players as (
    select * from {{ ref('stg_players') }}
),

current_limits as (
    select
        player_id,
        max_by(limit_eur, effective_from_utc) filter (where limit_type = 'daily') as daily_loss_limit_eur,
        max_by(limit_eur, effective_from_utc) filter (where limit_type = 'monthly') as monthly_loss_limit_eur
    from {{ ref('int_player_limits') }}
    group by player_id
),

exclusions as (
    select player_id, max(excluded_until_utc) as excluded_until_utc
    from {{ ref('stg_self_exclusions') }}
    group by player_id
),

alerts as (
    select
        player_id,
        count(*) as alerts_total,
        count(*) filter (where severity = 'critical') as alerts_critical,
        count(*) filter (where severity = 'high') as alerts_high
    from {{ source('realtime', 'alerts') }}
    where category = 'responsible_gaming'
    group by player_id
)

select
    p.player_id,
    p.birth_year,
    case
        when 2026 - p.birth_year < 25 then '18-24'
        when 2026 - p.birth_year < 35 then '25-34'
        when 2026 - p.birth_year < 50 then '35-49'
        when 2026 - p.birth_year < 65 then '50-64'
        else '65+'
    end as age_band,
    p.region,
    p.registered_at_utc,
    l.daily_loss_limit_eur,
    l.monthly_loss_limit_eur,
    e.excluded_until_utc,
    e.player_id is not null as has_self_excluded,
    coalesce(a.alerts_total, 0) as rg_alerts_total,
    coalesce(a.alerts_critical, 0) as rg_alerts_critical,
    coalesce(a.alerts_high, 0) as rg_alerts_high,
    case
        when coalesce(a.alerts_critical, 0) > 0 or coalesce(a.alerts_high, 0) >= 3 then 'red'
        when coalesce(a.alerts_high, 0) > 0 or coalesce(a.alerts_total, 0) >= 3 then 'amber'
        else 'green'
    end as risk_tier
from players as p
left join current_limits as l using (player_id)
left join exclusions as e using (player_id)
left join alerts as a using (player_id)
