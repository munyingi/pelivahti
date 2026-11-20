select
    payload ->> 'transaction_id' as transaction_id,
    player_id,
    {{ money("payload ->> 'amount_eur'") }} as amount_eur,
    event_time_utc as withdrawn_at_utc
from {{ ref('stg_events') }}
where event_type = 'withdrawal'
