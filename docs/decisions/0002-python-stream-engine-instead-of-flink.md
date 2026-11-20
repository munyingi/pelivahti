# 0002. A small Python stream engine instead of Flink

**Status:** Accepted

## Context

The project needs event-time windows, watermarks, keyed state and checkpoints tied to source offsets. Apache Flink provides all of these, but it needs a JVM cluster, is hard to run in GitHub Actions, and hides the semantics behind a framework. The goal here is to show the semantics clearly and to run everything for free on a laptop and in CI.

## Decision

Write the engine in plain Python, about 500 lines in [`src/pelivahti/stream/engine.py`](../../src/pelivahti/stream/engine.py). It implements:

- event time rather than arrival time for every window
- a watermark of highest event time seen minus allowed lateness
- tumbling day windows, sliding windows and session windows
- keyed state per player and per match
- checkpoints committed together with offsets ([0001](0001-exactly-once-in-one-transaction.md))

## Consequences

- Every rule is readable in one file, and the full pipeline, including a Redpanda integration test, runs in CI on every push.
- **One engine instance only.** Settlement needs bets (keyed by player) and match results (keyed by match) together. At scale you would partition by player and broadcast match results to every partition, which Flink's broadcast state does directly.
- Throughput is limited to one Python process. That is enough for the synthetic workload (about 79,000 events) but not for a real operator.
- In production the same design maps to Flink with a two-phase-commit sink. The semantics, tests and dbt reconciliation would carry over unchanged.
