"""End-to-end through a real broker. Runs in CI against Redpanda; skipped locally without one.

docker compose up -d redpanda
KAFKA_BOOTSTRAP=localhost:19092 pytest -m kafka
"""

import json
import os
import uuid
from collections import defaultdict
from decimal import Decimal

import duckdb
import pytest

from pelivahti.generator import GeneratorConfig, generate
from pelivahti.lakehouse import land
from pelivahti.sources import produce_file
from pelivahti.stream.runner import run_stream

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BOOTSTRAP, reason="set KAFKA_BOOTSTRAP to run")]


def per_player(db) -> dict:
    con = duckdb.connect(str(db), read_only=True)
    rows = con.execute("SELECT player_id, sum(amount_eur) FROM rt.ledger GROUP BY 1").fetchall()
    con.close()
    return {p: Decimal(a) for p, a in rows}


def test_kafka_pipeline_matches_file_pipeline(tmp_path):
    events = tmp_path / "events.jsonl"
    generate(events, GeneratorConfig(seed=11, players=80, days=2))
    topic = f"pelivahti.test.{uuid.uuid4().hex[:8]}"
    produced = produce_file(events, BOOTSTRAP, topic, partitions=3)
    assert produced == sum(1 for _ in events.open())

    kafka_db, file_db = tmp_path / "kafka.duckdb", tmp_path / "file.duckdb"
    k = run_stream(kafka_db, bootstrap=BOOTSTRAP, topic=topic, batch_size=1000, idle_timeout_s=5, log=lambda *_: None)
    run_stream(file_db, file=events, batch_size=1000, log=lambda *_: None)
    assert k.records == produced

    # Partitioning changes the processing order, so ledger entry ids may differ for corrected results,
    # but the money booked for every player must be identical.
    assert per_player(kafka_db) == per_player(file_db)

    con = duckdb.connect(str(kafka_db), read_only=True)
    offsets = dict(con.execute("SELECT source_partition, next_offset FROM rt.consumer_offsets").fetchall())
    con.close()
    assert len(offsets) == 3 and sum(offsets.values()) == produced

    # Restarting the consumer finds nothing new: offsets were stored with the results.
    again = run_stream(kafka_db, bootstrap=BOOTSTRAP, topic=topic, idle_timeout_s=3, log=lambda *_: None)
    assert again.records == 0 and per_player(kafka_db) == per_player(file_db)

    # The lander, a second consumer group, builds the same bronze layer from Kafka.
    lake = land(tmp_path / "lake", bootstrap=BOOTSTRAP, topic=topic, idle_timeout_s=5)
    assert lake["records_read"] == produced and lake["inserted"] > 0


def test_alerts_are_published(tmp_path):
    from confluent_kafka import Consumer

    events = tmp_path / "events.jsonl"
    generate(events, GeneratorConfig(seed=5, players=120, days=2))
    topic = f"pelivahti.test.{uuid.uuid4().hex[:8]}"
    produce_file(events, BOOTSTRAP, topic)
    run_stream(tmp_path / "alerts.duckdb", bootstrap=BOOTSTRAP, topic=topic, idle_timeout_s=5, log=lambda *_: None)

    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": uuid.uuid4().hex, "auto.offset.reset": "earliest"})
    c.subscribe(["pelivahti.rg-alerts"])
    rules = defaultdict(int)
    for m in c.consume(num_messages=10_000, timeout=10):
        if not m.error():
            rules[json.loads(m.value())["rule"]] += 1
    c.close()
    assert sum(rules.values()) > 0
