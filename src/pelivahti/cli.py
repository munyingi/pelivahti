"""Command line entry point: `pelivahti <command>`."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(os.environ.get("PELIVAHTI_DATA", ROOT / "data"))
EVENTS = DATA / "events.jsonl"
LAKE = DATA / "lake"
REALTIME = DATA / "realtime.duckdb"
WAREHOUSE = DATA / "warehouse.duckdb"
DBT_DIR = ROOT / "dbt"


def _env() -> dict:
    env = dict(os.environ)
    env.setdefault("PELIVAHTI_LAKE", str(LAKE))
    env.setdefault("PELIVAHTI_REALTIME", str(REALTIME))
    env.setdefault("PELIVAHTI_WAREHOUSE", str(WAREHOUSE))
    env.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))
    return env


def cmd_generate(a) -> None:
    from .generator import GeneratorConfig, generate

    cfg = GeneratorConfig(seed=a.seed, players=a.players, days=a.days)
    print(json.dumps(generate(a.out, cfg), indent=2))


def cmd_produce(a) -> None:
    from .sources import produce_file

    n = produce_file(a.file, a.bootstrap, a.topic, partitions=a.partitions, rate_per_s=a.rate)
    print(f"produced {n} messages to {a.topic}")


def cmd_land(a) -> None:
    from .lakehouse import land

    kw = (
        {"file": a.file}
        if not a.bootstrap
        else {"bootstrap": a.bootstrap, "topic": a.topic, "idle_timeout_s": a.idle_timeout}
    )
    print(json.dumps(land(a.lake, **kw), indent=2, default=str))


def cmd_stream(a) -> None:
    from .stream.runner import run_stream

    kw = (
        {"file": a.file}
        if not a.bootstrap
        else {"bootstrap": a.bootstrap, "topic": a.topic, "idle_timeout_s": a.idle_timeout}
    )
    summary = run_stream(
        a.db, batch_size=a.batch_size, crash_after_batches=a.crash_after, flush_at_end=not a.no_flush, **kw
    )
    print(json.dumps(summary.__dict__, indent=2, default=str))


def cmd_dbt(a) -> None:
    dbt = shutil.which("dbt") or str(Path(sys.executable).with_name("dbt"))
    args = a.args or ["build"]
    sys.exit(subprocess.run([dbt, *args], cwd=DBT_DIR, env=_env()).returncode)


def cmd_run_local(a) -> None:
    """The whole pipeline without Kafka: generate -> land -> stream -> dbt build."""
    from .generator import GeneratorConfig, generate
    from .lakehouse import land
    from .stream.runner import run_stream

    if a.fresh:
        reset()
    if not EVENTS.exists() or a.fresh:
        print(
            "1/4 generate",
            generate(EVENTS, GeneratorConfig(seed=a.seed, players=a.players, days=a.days))["messages"],
            "messages",
        )
    print("2/4 land to Delta bronze", land(LAKE, file=EVENTS)["inserted"], "new rows")
    s = run_stream(REALTIME, file=EVENTS, log=lambda *_: None)
    print("3/4 stream", s.records, "records", s.stats)
    dbt = shutil.which("dbt") or str(Path(sys.executable).with_name("dbt"))
    rc = subprocess.run([dbt, "build", "--quiet"], cwd=DBT_DIR, env=_env()).returncode
    print("4/4 dbt build", "ok" if rc == 0 else "FAILED")
    sys.exit(rc)


def reset() -> None:
    for p in (LAKE, REALTIME, WAREHOUSE, Path(str(REALTIME) + ".wal"), Path(str(WAREHOUSE) + ".wal"), EVENTS):
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="pelivahti", description="Real-time betting and responsible gaming pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write synthetic events to JSONL")
    g.add_argument("--out", default=str(EVENTS))
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--players", type=int, default=800)
    g.add_argument("--days", type=int, default=3)
    g.set_defaults(func=cmd_generate)

    p = sub.add_parser("produce", help="publish the JSONL file to Kafka/Redpanda")
    p.add_argument("--file", default=str(EVENTS))
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--topic", default="pelivahti.events")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--rate", type=float, default=None, help="messages per second (default: as fast as possible)")
    p.set_defaults(func=cmd_produce)

    for name, func, helptext in (
        ("land", cmd_land, "load events into the Delta Lake bronze layer"),
        ("stream", cmd_stream, "run the real-time engine"),
    ):
        s = sub.add_parser(name, help=helptext)
        s.add_argument("--file", default=str(EVENTS))
        s.add_argument("--bootstrap", default=None, help="read from Kafka instead of the file")
        s.add_argument("--topic", default="pelivahti.events")
        s.add_argument("--idle-timeout", type=float, default=15.0, help="stop after this many idle seconds (Kafka)")
        if name == "land":
            s.add_argument("--lake", default=str(LAKE))
        else:
            s.add_argument("--db", default=str(REALTIME))
            s.add_argument("--batch-size", type=int, default=2000)
            s.add_argument("--crash-after", type=int, default=None, help="simulate a crash after N committed batches")
            s.add_argument("--no-flush", action="store_true", help="keep windows open at the end (resumable)")
        s.set_defaults(func=func)

    d = sub.add_parser("dbt", help="run dbt with the project's paths, e.g. `pelivahti dbt build`")
    d.add_argument("args", nargs=argparse.REMAINDER)
    d.set_defaults(func=cmd_dbt)

    r = sub.add_parser("run-local", help="generate, land, stream and build the warehouse")
    r.add_argument("--fresh", action="store_true", help="delete existing data first")
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--players", type=int, default=800)
    r.add_argument("--days", type=int, default=3)
    r.set_defaults(func=cmd_run_local)

    x = sub.add_parser("reset", help="delete generated data")
    x.set_defaults(func=lambda _a: reset())

    a = ap.parse_args(argv)
    a.func(a)


if __name__ == "__main__":
    main()
