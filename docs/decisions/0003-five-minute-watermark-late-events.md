# 0003. A 5-minute watermark, and late events never reopen a closed window

**Status:** Accepted

## Context

Responsible gaming rules (daily loss limits, session length, night-time play) must fire within seconds, which means closing windows quickly. But events arrive late: mobile clients reconnect, partitions lag. A long watermark catches more late events but delays every alert; a short one alerts fast but misses some events.

## Decision

- Watermark = highest event time seen minus `allowed_lateness`, default **5 minutes**.
- An event older than the watermark is **late**. It still settles and still hits the ledger, because money is never dropped.
- A late event **does not reopen** a window that has already closed. It skips the windowed risk rules and is logged to `late_events`.
- The batch layer is the correction path. dbt rebuilds daily player results from bronze and measures the gap in [`rpt_realtime_vs_batch_daily`](../../dbt/models/reports/rpt_realtime_vs_batch_daily.sql).

Code: `watermark` and `allowed_lateness` in [`engine.py`](../../src/pelivahti/stream/engine.py)

## Consequences

- Alerts stay fast and the stream never rewrites an alert it already sent.
- Some breaches can be missed in real time. [`assert_every_daily_breach_was_alerted`](../../dbt/tests/assert_every_daily_breach_was_alerted.sql) is deliberately a **warning**: on the default seed it flags one player-day where late events pushed a player over the limit after the window closed. That is the visible cost of a 5-minute watermark, reported rather than hidden.
- The lateness is configurable, so the trade-off can be tuned per rule if a regulator requires it.
