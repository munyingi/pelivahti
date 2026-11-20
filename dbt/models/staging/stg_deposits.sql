select
    payload ->> 'transaction_id' as transaction_id,
    player_id,
    {{ money("payload ->> 'amount_eur'") }} as amount_eur,
    payload ->> 'method' as method,
    event_time_utc as deposited_at_utc
from {{ ref('stg_events') }}
where event_type = 'deposit'
