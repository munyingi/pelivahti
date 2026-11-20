"""Runs the engine against a source with exactly-once results in DuckDB.

Every micro-batch is committed in ONE DuckDB transaction that contains:

1. the batch's results (ledger, alerts, closed windows, DLQ, late and duplicate logs)
2. the next source offset for every partition
3. a checkpoint of the engine's state

If the process dies anywhere before the commit, the whole batch rolls back. On restart
the engine reloads the last checkpoint, seeks to the stored offsets and replays the
batch. Because the outputs also use deterministic ids with INSERT OR IGNORE, a replay
can never double-count money or alerts.
"""

from __future__ import annotations

import pickle
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from ..sources import FileSource, KafkaAlertSink, KafkaSource
from .engine import Engine, StreamConfig, naive

DDL = """
CREATE SCHEMA IF NOT EXISTS rt;
CREATE TABLE IF NOT EXISTS rt.ledger (
    entry_id VARCHAR PRIMARY KEY, player_id VARCHAR NOT NULL, entry_type VARCHAR NOT NULL, product VARCHAR NOT NULL,
    ref_id VARCHAR NOT NULL, amount_eur DECIMAL(12,2) NOT NULL, event_id VARCHAR NOT NULL,
    event_time TIMESTAMP NOT NULL, is_late BOOLEAN NOT NULL, batch_no INTEGER NOT NULL,
    processed_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS rt.alerts (
    alert_id VARCHAR PRIMARY KEY, rule VARCHAR NOT NULL, severity VARCHAR NOT NULL, category VARCHAR NOT NULL,
    player_id VARCHAR NOT NULL, event_time TIMESTAMP NOT NULL, details VARCHAR, batch_no INTEGER NOT NULL,
    processed_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS rt.player_daily (
    player_id VARCHAR, day DATE, stakes DECIMAL(12,2), returns DECIMAL(12,2), net_loss DECIMAL(12,2),
    deposits DECIMAL(12,2), withdrawals DECIMAL(12,2), n_bets INTEGER, n_rounds INTEGER, daily_limit DECIMAL(12,2),
    batch_no INTEGER, processed_at TIMESTAMP, PRIMARY KEY (player_id, day));
CREATE TABLE IF NOT EXISTS rt.dlq (
    source_partition INTEGER, source_offset BIGINT, raw VARCHAR, error VARCHAR, batch_no INTEGER,
    processed_at TIMESTAMP, PRIMARY KEY (source_partition, source_offset));
CREATE TABLE IF NOT EXISTS rt.late_events (
    event_id VARCHAR PRIMARY KEY, event_type VARCHAR, event_time TIMESTAMP, watermark TIMESTAMP,
    lateness_seconds DOUBLE, batch_no INTEGER, processed_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS rt.duplicates (
    source_partition INTEGER, source_offset BIGINT, event_id VARCHAR, batch_no INTEGER, processed_at TIMESTAMP,
    PRIMARY KEY (source_partition, source_offset));
CREATE TABLE IF NOT EXISTS rt.consumer_offsets (
    source VARCHAR, source_partition INTEGER, next_offset BIGINT, updated_at TIMESTAMP,
    PRIMARY KEY (source, source_partition));
CREATE TABLE IF NOT EXISTS rt.checkpoint (id INTEGER PRIMARY KEY, state BLOB, batch_no INTEGER, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS rt.batches (
    batch_no INTEGER PRIMARY KEY, records INTEGER, watermark TIMESTAMP, seconds DOUBLE, committed_at TIMESTAMP);
"""


class SimulatedCrash(RuntimeError):
    """Raised on purpose inside a transaction to prove that recovery is exactly-once."""


@dataclass
class RunSummary:
    batches: int
    records: int
    stats: dict
    seconds: float


def _insert(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict], batch_no: int, now: datetime) -> None:
    if not rows:
        return
    for r in rows:
        r["batch_no"] = batch_no
        r["processed_at"] = now
    tbl = pa.Table.from_pylist(rows)  # noqa: F841  (referenced by name in the SQL below)
    con.execute(f"INSERT OR IGNORE INTO {table} BY NAME SELECT * FROM tbl")


def _load(con, source_name: str) -> tuple[Engine | None, dict[int, int]]:
    row = con.execute("SELECT state FROM rt.checkpoint WHERE id = 1").fetchone()
    engine = pickle.loads(row[0]) if row else None
    offsets = dict(
        con.execute(
            "SELECT source_partition, next_offset FROM rt.consumer_offsets WHERE source = ?", [source_name]
        ).fetchall()
    )
    return engine, offsets


def run_stream(
    db_path: str | Path,
    *,
    file: str | Path | None = None,
    bootstrap: str | None = None,
    topic: str = "pelivahti.events",
    batch_size: int = 2000,
    crash_after_batches: int | None = None,
    flush_at_end: bool = True,
    idle_timeout_s: float = 10.0,
    publish_alerts: bool = True,
    cfg: StreamConfig | None = None,
    log=print,
) -> RunSummary:
    if (file is None) == (bootstrap is None):
        raise ValueError("Pass exactly one of file= or bootstrap=")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    source_name = f"file:{Path(file).name}" if file else f"kafka:{topic}"
    engine, offsets = _load(con, source_name)
    if engine is None:
        engine = Engine(cfg)
    elif offsets:
        log(f"resuming from checkpoint at batch {engine.batch_no}, offsets {offsets}")

    if file:
        source = FileSource(file, offsets)
        alert_sink = None
    else:
        source = KafkaSource(bootstrap, topic, offsets, idle_timeout_s=idle_timeout_s)
        alert_sink = KafkaAlertSink(bootstrap) if publish_alerts else None

    started, n_batches, n_records = time.monotonic(), 0, 0

    def commit(records: int, positions: dict[int, int], t0: float) -> list[dict]:
        out = engine.drain()
        now = naive(datetime.now(UTC))
        b = engine.batch_no
        con.execute("BEGIN TRANSACTION")
        try:
            _insert(con, "rt.ledger", out.ledger, b, now)
            _insert(con, "rt.alerts", out.alerts, b, now)
            _insert(con, "rt.player_daily", out.player_daily, b, now)
            _insert(con, "rt.dlq", out.dlq, b, now)
            _insert(con, "rt.late_events", out.late, b, now)
            _insert(con, "rt.duplicates", out.duplicates, b, now)
            for p, nxt in positions.items():
                con.execute(
                    "INSERT OR REPLACE INTO rt.consumer_offsets VALUES (?, ?, ?, ?)", [source_name, p, nxt, now]
                )
            con.execute(
                "INSERT OR REPLACE INTO rt.checkpoint VALUES (1, ?, ?, ?)",
                [pickle.dumps(engine, protocol=pickle.HIGHEST_PROTOCOL), b, now],
            )
            wm = engine.watermark
            con.execute(
                "INSERT OR REPLACE INTO rt.batches VALUES (?, ?, ?, ?, ?)",
                [b, records, naive(wm) if wm else None, time.monotonic() - t0, now],
            )
            if crash_after_batches is not None and n_batches >= crash_after_batches:
                raise SimulatedCrash(f"simulated crash before committing batch {b}")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            con.close()
            raise
        return out.alerts

    for batch in source.batches(batch_size):
        t0 = time.monotonic()
        positions: dict[int, int] = {}
        for rec in batch:
            engine.handle(rec)
            positions[rec.partition] = rec.offset + 1
        engine.end_batch()
        alerts = commit(len(batch), positions, t0)
        if alert_sink and alerts:
            alert_sink.send(alerts)
        n_batches += 1
        n_records += len(batch)
        if n_batches % 10 == 0:
            log(f"batch {engine.batch_no}: {n_records} records, watermark {engine.watermark}")

    if flush_at_end:
        t0 = time.monotonic()
        engine.flush()
        engine.batch_no += 1
        alerts = commit(0, {}, t0)
        if alert_sink and alerts:
            alert_sink.send(alerts)

    con.close()
    return RunSummary(n_batches, n_records, dict(engine.stats), round(time.monotonic() - started, 2))
