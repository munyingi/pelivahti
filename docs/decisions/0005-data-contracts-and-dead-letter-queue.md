# 0005. Versioned JSON Schema contracts with a dead-letter queue

**Status:** Accepted

## Context

Ten event types come from several producers. A malformed message (a missing `player_id`, a negative stake, a string where a number belongs) can crash the engine or, worse, be booked as money. Stopping the whole stream for one bad message is not acceptable either.

## Decision

- Every event type has a JSON Schema in [`contracts/v1/`](../../contracts/v1). Schemas are checked for validity when they load.
- Every message is validated before the engine sees it ([`contracts.py`](../../src/pelivahti/contracts.py)).
- Invalid messages go to a **dead-letter queue** table with the raw payload, the source offset and the reason. The stream continues.
- The folder name is the contract version. A breaking change ships as `v2` alongside `v1`, so producers can migrate one at a time.

## Consequences

- Bad data is visible and countable instead of silently dropped. [`rpt_data_quality`](../../dbt/models/reports/rpt_data_quality.sql) reports DLQ volume for both the bronze load and the stream.
- DLQ rows are committed in the same transaction as results ([0001](0001-exactly-once-in-one-transaction.md)), so a replay never double-counts them.
- Reprocessing a fixed DLQ message is a manual step today. A production setup would add a replay command.
