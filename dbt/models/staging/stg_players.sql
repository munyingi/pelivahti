select
    player_id,
    cast(payload ->> 'birth_year' as integer) as birth_year,
    payload ->> 'municipality' as municipality,
    {{ money("payload ->> 'daily_loss_limit_eur'") }} as daily_loss_limit_eur,
    {{ money("payload ->> 'monthly_loss_limit_eur'") }} as monthly_loss_limit_eur,
    event_time_utc as registered_at_utc
from {{ ref('stg_events') }}
where event_type = 'player_registered'
