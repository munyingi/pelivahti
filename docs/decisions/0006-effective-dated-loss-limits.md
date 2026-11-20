# 0006. Loss limits as effective-dated rows: decreases apply at once, increases after 24 hours

**Status:** Accepted

## Context

Players set daily and monthly loss limits and can change them. Responsible gaming practice is asymmetric: a player who lowers a limit is protecting themselves and it should apply immediately; a player who raises a limit may be chasing losses, so the increase should wait. To judge a breach correctly the warehouse must know which limit was in force at the moment of each bet, not the limit today.

## Decision

- Model limits as effective-dated rows in [`int_player_limits`](../../dbt/models/intermediate/int_player_limits.sql), built from the registration limit plus every `limit_changed` event.
- `effective_from` is the change time for a decrease, and the change time **plus 24 hours** for an increase.
- Join each player-day to the limit in force with an `ASOF` join in [`fct_player_daily`](../../dbt/models/marts/fct_player_daily.sql).

## Consequences

- Breaches are judged against the rule that applied at the time, which is what a regulator would check.
- Historical reports stay correct when a player later changes their limit.
- The 24-hour cooling period is a single constant in the model and could be made configurable per jurisdiction.
