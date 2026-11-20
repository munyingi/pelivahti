"""Bronze layer on Delta Lake.

The lander is a second, independent consumer of the same events. It validates each
message against its contract and MERGEs it into a Delta table keyed on event_id, so
re-running it (or receiving a redelivered message) never creates duplicate rows.
Invalid messages go to a Delta dead-letter table. Every run is logged.

Delta gives the bronze layer ACID appends and time travel: `as_of_version` lets you
rebuild the warehouse exactly as it looked after any earlier load.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .contracts import validate
from .sources import FileSource, KafkaSource

EVENTS_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("schema_version", pa.int32()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("player_id", pa.string()),
        ("payload", pa.string()),
        ("source_partition", pa.int32()),
        ("source_offset", pa.int64()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("event_date", pa.string()),
    ]
)
DLQ_SCHEMA = pa.schema(
    [
        ("source_partition", pa.int32()),
        ("source_offset", pa.int64()),
        ("raw", pa.string()),
        ("error", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)
LOG_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("source", pa.string()),
        ("started_at", pa.timestamp("us", tz="UTC")),
        ("finished_at", pa.timestamp("us", tz="UTC")),
        ("records_read", pa.int64()),
        ("valid", pa.int64()),
        ("invalid", pa.int64()),
        ("inserted", pa.int64()),
        ("already_present", pa.int64()),
        ("delta_version", pa.int64()),
    ]
)


def _merge(path: Path, table: pa.Table, predicate: str, partition_by: list[str] | None = None) -> int:
    """Insert rows that are not already in the Delta table. Returns the number inserted."""
    if table.num_rows == 0:
        return 0
    if not (path / "_delta_log").exists():
        write_deltalake(str(path), table, partition_by=partition_by)
        return table.num_rows
    metrics = (
        DeltaTable(str(path))
        .merge(table, predicate=predicate, source_alias="s", target_alias="t")
        .when_not_matched_insert_all()
        .execute()
    )
    return int(metrics.get("num_target_rows_inserted", 0))


def land(
    lake: str | Path,
    *,
    file: str | Path | None = None,
    bootstrap: str | None = None,
    topic: str = "pelivahti.events",
    batch_size: int = 25_000,
    idle_timeout_s: float = 10.0,
) -> dict:
    lake = Path(lake)
    events_path, dlq_path, log_path = (
        lake / "bronze" / "events",
        lake / "bronze" / "dlq",
        lake / "bronze" / "ingest_log",
    )
    events_path.parent.mkdir(parents=True, exist_ok=True)
    if file:
        source, source_name = FileSource(file), f"file:{Path(file).name}"
    else:
        source = KafkaSource(bootstrap, topic, idle_timeout_s=idle_timeout_s, group_id="pelivahti-lander")
        source_name = f"kafka:{topic}"

    started = datetime.now(UTC)
    read = valid = invalid = inserted = 0
    for batch in source.batches(batch_size):
        now = datetime.now(UTC)
        rows, bad, seen = [], [], set()
        for rec in batch:
            read += 1
            res = validate(rec.value)
            if not res.ok:
                invalid += 1
                bad.append(
                    {
                        "source_partition": rec.partition,
                        "source_offset": rec.offset,
                        "raw": rec.value[:4000],
                        "error": res.error,
                        "ingested_at": now,
                    }
                )
                continue
            valid += 1
            ev = res.event
            if ev["event_id"] in seen:
                continue
            seen.add(ev["event_id"])
            ts = datetime.fromisoformat(ev["event_time"].replace("Z", "+00:00"))
            rows.append(
                {
                    "event_id": ev["event_id"],
                    "event_type": ev["event_type"],
                    "schema_version": ev["schema_version"],
                    "event_time": ts,
                    "player_id": ev.get("player_id"),
                    "payload": json.dumps(ev, ensure_ascii=False, sort_keys=True),
                    "source_partition": rec.partition,
                    "source_offset": rec.offset,
                    "ingested_at": now,
                    "event_date": ts.date().isoformat(),
                }
            )
        inserted += _merge(
            events_path,
            pa.Table.from_pylist(rows, schema=EVENTS_SCHEMA),
            "t.event_id = s.event_id",
            partition_by=["event_date"],
        )
        _merge(
            dlq_path,
            pa.Table.from_pylist(bad, schema=DLQ_SCHEMA),
            "t.source_partition = s.source_partition AND t.source_offset = s.source_offset",
        )

    version = DeltaTable(str(events_path)).version() if (events_path / "_delta_log").exists() else -1
    summary = {
        "run_id": str(uuid.uuid4()),
        "source": source_name,
        "started_at": started,
        "finished_at": datetime.now(UTC),
        "records_read": read,
        "valid": valid,
        "invalid": invalid,
        "inserted": inserted,
        "already_present": valid - inserted,
        "delta_version": version,
    }
    write_deltalake(str(log_path), pa.Table.from_pylist([summary], schema=LOG_SCHEMA), mode="append")
    return summary


def compact(lake: str | Path) -> None:
    """Small-file housekeeping: compact bronze partitions and drop files older than the retention window."""
    dt = DeltaTable(str(Path(lake) / "bronze" / "events"))
    dt.optimize.compact()
    dt.vacuum(retention_hours=168, dry_run=False, enforce_retention_duration=True)
