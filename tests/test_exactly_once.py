"""Crash the stream mid-run and prove the recovered result is identical to a clean run."""

import duckdb
import pytest

from pelivahti.generator import GeneratorConfig, generate
from pelivahti.lakehouse import land
from pelivahti.stream.runner import SimulatedCrash, run_stream

TABLES = {
    "ledger": "entry_id, player_id, entry_type, product, ref_id, amount_eur, event_id, event_time, is_late",
    "alerts": "alert_id, rule, severity, player_id, event_time, details",
    "player_daily": "player_id, day, stakes, returns, net_loss, deposits, withdrawals, n_bets, n_rounds, daily_limit",
    "dlq": "source_partition, source_offset, error",
    "late_events": "event_id, event_type, event_time, watermark, lateness_seconds",
    "duplicates": "source_partition, source_offset, event_id",
}


@pytest.fixture(scope="module")
def events(tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "events.jsonl"
    generate(path, GeneratorConfig(seed=7, players=60, days=2))
    return path


def snapshot(db) -> dict:
    con = duckdb.connect(str(db), read_only=True)
    snap = {t: sorted(con.execute(f"SELECT {cols} FROM rt.{t}").fetchall(), key=str) for t, cols in TABLES.items()}
    con.close()
    return snap


def test_generator_is_deterministic(tmp_path):
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    generate(a, GeneratorConfig(seed=3, players=20, days=1))
    generate(b, GeneratorConfig(seed=3, players=20, days=1))
    assert a.read_bytes() == b.read_bytes()


def test_crash_and_resume_equals_clean_run(events, tmp_path):
    clean = tmp_path / "clean.duckdb"
    run_stream(clean, file=events, batch_size=500, log=lambda *_: None)

    crashed = tmp_path / "crashed.duckdb"
    with pytest.raises(SimulatedCrash):
        run_stream(crashed, file=events, batch_size=500, crash_after_batches=3, log=lambda *_: None)
    partial = snapshot(crashed)
    assert 0 < len(partial["ledger"]) < len(snapshot(clean)["ledger"])  # only 3 batches survived

    run_stream(crashed, file=events, batch_size=500, log=lambda *_: None)  # restart from the checkpoint
    assert snapshot(crashed) == snapshot(clean)


def test_replaying_a_finished_run_changes_nothing(events, tmp_path):
    db = tmp_path / "realtime.duckdb"
    run_stream(db, file=events, batch_size=500, log=lambda *_: None)
    before = snapshot(db)
    con = duckdb.connect(str(db))
    con.execute("DELETE FROM rt.consumer_offsets; DELETE FROM rt.checkpoint;")  # force a full replay from offset 0
    con.close()
    run_stream(db, file=events, batch_size=500, log=lambda *_: None)
    assert snapshot(db) == before


def test_bronze_landing_is_idempotent(events, tmp_path):
    first = land(tmp_path / "lake", file=events)
    second = land(tmp_path / "lake", file=events)
    assert first["inserted"] > 0 and first["invalid"] > 0
    assert second["inserted"] == 0 and second["already_present"] == first["valid"]
