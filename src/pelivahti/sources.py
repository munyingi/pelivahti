"""Message sources and sinks.

Both sources yield batches of ``Record`` with a (partition, offset) position, so the
stream engine can store its position next to its results and resume exactly where
it left off after a crash.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TOPIC = "pelivahti.events"
ALERTS_TOPIC = "pelivahti.rg-alerts"


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    value: str


class FileSource:
    """Reads a JSON Lines file as if it were a single-partition topic (offset = line number)."""

    name = "file"

    def __init__(self, path: str | Path, start_offsets: dict[int, int] | None = None):
        self.path = Path(path)
        self.start = (start_offsets or {}).get(0, 0)

    def batches(self, size: int) -> Iterator[list[Record]]:
        batch: list[Record] = []
        with self.path.open(encoding="utf-8") as fh:
            for offset, line in enumerate(fh):
                if offset < self.start:
                    continue
                line = line.rstrip("\n")
                if not line:
                    continue
                batch.append(Record(0, offset, line))
                if len(batch) >= size:
                    yield batch
                    batch = []
        if batch:
            yield batch


class KafkaSource:
    """Reads a Kafka/Redpanda topic with manual partition assignment.

    Offsets are *not* committed to Kafka. The caller stores them in its own sink inside
    the same transaction as the results, which is what makes processing exactly-once.
    """

    name = "kafka"

    def __init__(
        self,
        bootstrap: str,
        topic: str = DEFAULT_TOPIC,
        start_offsets: dict[int, int] | None = None,
        idle_timeout_s: float = 10.0,
        group_id: str = "pelivahti-stream",
    ):
        from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

        self.topic = topic
        self.idle_timeout_s = idle_timeout_s
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        meta = self.consumer.list_topics(topic, timeout=10)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            raise RuntimeError(f"Topic {topic!r} not found on {bootstrap}")
        start_offsets = start_offsets or {}
        self.consumer.assign(
            [
                TopicPartition(topic, p, start_offsets.get(p, OFFSET_BEGINNING))
                for p in sorted(meta.topics[topic].partitions)
            ]
        )

    def batches(self, size: int) -> Iterator[list[Record]]:
        idle_since = time.monotonic()
        try:
            while True:
                msgs = self.consumer.consume(num_messages=size, timeout=1.0)
                batch = []
                for m in msgs:
                    if m.error():
                        continue
                    batch.append(Record(m.partition(), m.offset(), m.value().decode("utf-8", errors="replace")))
                if batch:
                    idle_since = time.monotonic()
                    yield batch
                elif time.monotonic() - idle_since > self.idle_timeout_s:
                    return
        finally:
            self.consumer.close()


def ensure_topics(bootstrap: str, topics: dict[str, int]) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in topics.items() if t not in existing]
    if new:
        for fut in admin.create_topics(new).values():
            fut.result()


def _key_for(line: str) -> str | None:
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(ev, dict):
        return None
    return ev.get("player_id") or ev.get("match_id")


def produce_file(
    path: str | Path, bootstrap: str, topic: str = DEFAULT_TOPIC, partitions: int = 3, rate_per_s: float | None = None
) -> int:
    """Publish a JSONL file to Kafka, keyed by player (or match) so one player's events stay in order."""
    from confluent_kafka import Producer

    ensure_topics(bootstrap, {topic: partitions, ALERTS_TOPIC: 1})
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "enable.idempotence": True})
    n = 0
    with Path(path).open(encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            key = _key_for(line)
            while True:
                try:
                    producer.produce(topic, value=line.encode("utf-8"), key=key.encode() if key else None)
                    break
                except BufferError:
                    producer.poll(0.5)
            n += 1
            if n % 10_000 == 0:
                producer.poll(0)
            if rate_per_s:
                time.sleep(1 / rate_per_s)
    producer.flush(30)
    return n


class KafkaAlertSink:
    """Publishes alerts after the database commit. Delivery is at-least-once; alert_id is the dedup key."""

    def __init__(self, bootstrap: str, topic: str = ALERTS_TOPIC):
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})

    def send(self, alerts: list[dict]) -> None:
        for a in alerts:
            payload = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in a.items()}
            self.producer.produce(
                self.topic, key=a["alert_id"].encode(), value=json.dumps(payload, default=str).encode()
            )
        self.producer.flush(10)
