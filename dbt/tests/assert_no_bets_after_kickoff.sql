select bet_id, placed_at_utc, kickoff_utc
from {{ ref('fct_sports_bets') }}
where placed_at_utc >= kickoff_utc
