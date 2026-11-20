# Design decisions

Short records of the choices that shape Pelivahti: what was decided, why, and what it costs. Each one points to the code or test that enforces it.

| # | Decision | Status |
|---|---|---|
| [0001](0001-exactly-once-in-one-transaction.md) | Exactly-once results by committing results, offsets and state in one transaction | Accepted |
| [0002](0002-python-stream-engine-instead-of-flink.md) | A small Python stream engine instead of Flink | Accepted |
| [0003](0003-five-minute-watermark-late-events.md) | A 5-minute watermark, and late events never reopen a closed window | Accepted |
| [0004](0004-append-only-ledger-with-reversals.md) | An append-only wallet ledger with reversals for corrected results | Accepted |
| [0005](0005-data-contracts-and-dead-letter-queue.md) | Versioned JSON Schema contracts with a dead-letter queue | Accepted |
| [0006](0006-effective-dated-loss-limits.md) | Loss limits as effective-dated rows: decreases apply at once, increases after 24 hours | Accepted |
