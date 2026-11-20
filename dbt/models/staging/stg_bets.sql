select
    payload ->> 'bet_id' as bet_id,
    player_id,
    payload ->> 'match_id' as match_id,
    payload ->> 'selection' as selection,
    cast(payload ->> 'odds' as decimal(8, 2)) as odds,
    {{ money("payload ->> 'stake_eur'") }} as stake_eur,
    payload ->> 'channel' as channel,
    event_time_utc as placed_at_utc
from {{ ref('stg_events') }}
where event_type = 'bet_placed'
-- a bet re-sent under a new event_id is still one bet: keep the first one, like the stream engine
qualify row_number() over (partition by payload ->> 'bet_id' order by source_partition, source_offset) = 1
