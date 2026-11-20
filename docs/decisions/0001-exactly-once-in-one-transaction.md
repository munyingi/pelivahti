# 0001. Exactly-once results by committing results, offsets and state in one transaction

**Status:** Accepted

## Context

Bets and casino rounds arrive from Kafka (Redpanda) with at-least-once delivery. The engine keeps state in memory (open bets, wallet balances, windows). If the process crashes after writing ledger entries but before saving its offset, a restart replays those events and books the same stake or payout twice. In a wallet ledger that is real money, so "at least once" is not acceptable for results.

Kafka transactions solve this when the sink is also Kafka. Here the sink is a database, so they do not help on their own.

## Decision

Every micro-batch is committed in **one DuckDB transaction** that writes:

1. the batch's results: ledger entries, alerts, closed windows, DLQ rows, late and duplicate logs
2. the next offset for every source partition
3. a pickled checkpoint of the engine state

On restart the engine loads the last checkpoint and seeks to the stored offsets, so it resumes from a state that matches the committed results exactly. As a second line of defence every output row has a deterministic id (for example `{bet_id}:PAYOUT:v{version}`) and is written with `INSERT OR IGNORE`.

Code: [`src/pelivahti/stream/runner.py`](../../src/pelivahti/stream/runner.py)

## Proof

[`tests/test_exactly_once.py`](../../tests/test_exactly_once.py) crashes the engine inside a transaction after three batches, restarts it, and asserts every output table is identical to a clean run. A second test deletes the offsets and checkpoint, replays the whole stream on a finished database and asserts nothing changes.

## Consequences

- Results, offsets and state can never disagree, which is what makes the stream ledger safe to reconcile against the batch rebuild ([0004](0004-append-only-ledger-with-reversals.md)).
- Alerts published to the `rg-alerts` topic are sent after the commit, so they are at-least-once. Consumers deduplicate on `alert_id`.
- The checkpoint is a pickle. That is fine for one engine version, but a schema change in engine state would need a migration or a replay from bronze.
