-- Loss limits as effective-dated rows. Lowering a limit applies at once;
-- raising it only applies after a 24 hour cooling-off period.
with changes as (
    select player_id, 'daily' as limit_type, daily_loss_limit_eur as limit_eur, registered_at_utc as changed_at_utc
    from {{ ref('stg_players') }}
    union all
    select player_id, 'monthly', monthly_loss_limit_eur, registered_at_utc
    from {{ ref('stg_players') }}
    union all
    select player_id, limit_type, new_limit_eur, changed_at_utc
    from {{ ref('stg_limit_changes') }}
),

ordered as (
    select
        *,
        lag(limit_eur) over (partition by player_id, limit_type order by changed_at_utc) as previous_limit_eur
    from changes
)

select
    player_id,
    limit_type,
    limit_eur,
    previous_limit_eur,
    changed_at_utc,
    case
        when previous_limit_eur is null or limit_eur <= previous_limit_eur then changed_at_utc
        else changed_at_utc + interval 24 hour
    end as effective_from_utc
from ordered
