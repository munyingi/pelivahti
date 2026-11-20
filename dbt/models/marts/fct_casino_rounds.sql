select
    round_id,
    player_id,
    game_id,
    split_part(game_id, '_', 1) as game_type,
    stake_eur,
    win_eur,
    played_at_utc,
    {{ helsinki_date('played_at_utc') }} as played_date
from {{ ref('stg_casino_rounds') }}
