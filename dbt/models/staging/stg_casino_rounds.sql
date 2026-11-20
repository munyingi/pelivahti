select
    payload ->> 'round_id' as round_id,
    player_id,
    payload ->> 'game_id' as game_id,
    {{ money("payload ->> 'stake_eur'") }} as stake_eur,
    {{ money("payload ->> 'win_eur'") }} as win_eur,
    event_time_utc as played_at_utc
from {{ ref('stg_events') }}
where event_type = 'casino_round'
