select
    s.*,
    {{ to_local('s.placed_at_utc') }} as placed_at_local,
    {{ local_date('s.placed_at_utc') }} as placed_date,
    {{ local_date('s.settled_at_utc') }} as settled_date,
    m.sport,
    m.league,
    m.fixture,
    m.kickoff_utc
from {{ ref('int_bet_settlements') }} as s
left join {{ ref('dim_matches') }} as m using (match_id)
