# 0004. An append-only wallet ledger with reversals for corrected results

**Status:** Accepted

## Context

Match results get corrected after bets have already been paid: a goal is overturned, a match is voided. The simple fix is to update the settled row in place. That destroys the history of what the player was actually paid and when, which is exactly what an auditor or a tax report needs.

## Decision

The ledger is **append-only**. Nothing is updated or deleted.

- Each result carries a version. A bet is only re-settled if the new version is higher than the one it was settled with.
- On re-settlement the engine first writes a `REVERSAL` entry for the old amount, then a new `PAYOUT`, `REFUND` (for a void) or nothing (for a loss).
- Entry ids are deterministic, `{bet_id}:{kind}:v{version}`, so replays are idempotent ([0001](0001-exactly-once-in-one-transaction.md)).

Code: `_settle()` in [`engine.py`](../../src/pelivahti/stream/engine.py)

## Consequences

- Any balance at any point in time can be rebuilt by summing entries up to that time.
- The stream ledger and an independent batch rebuild from bronze must agree to the cent, enforced by [`assert_realtime_ledger_reconciles`](../../dbt/tests/assert_realtime_ledger_reconciles.sql).
- A correction made after payout is recorded as a new reversal entry with its own timestamp, so the history of what was paid, and when, is never lost.
- The ledger grows with every correction. That is the intended trade: storage is cheap, lost audit history is not.
