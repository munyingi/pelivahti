"""Dagster orchestration for the local (file-based) pipeline.

    dagster dev -m orchestration.definitions

Asset graph: raw_events -> bronze_events (Delta) -> realtime_outputs (stream engine) -> dbt models.
A daily schedule rebuilds everything at 06:15 local time.
"""

import os
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pelivahti.cli import reset
from pelivahti.generator import GeneratorConfig, generate
from pelivahti.lakehouse import land
from pelivahti.stream.runner import run_stream

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("PELIVAHTI_DATA", ROOT / "data"))
EVENTS, LAKE, REALTIME = DATA / "events.jsonl", DATA / "lake", DATA / "realtime.duckdb"
os.environ.setdefault("PELIVAHTI_LAKE", str(LAKE))
os.environ.setdefault("PELIVAHTI_REALTIME", str(REALTIME))
os.environ.setdefault("PELIVAHTI_WAREHOUSE", str(DATA / "warehouse.duckdb"))

dbt_project = DbtProject(project_dir=ROOT / "dbt", profiles_dir=ROOT / "dbt")
dbt_project.prepare_if_dev()


@asset(group_name="ingest", description="Synthetic events in arrival order (JSON Lines).")
def raw_events() -> MaterializeResult:
    reset()  # a new event file means a full rebuild: stored stream offsets would not match it
    summary = generate(EVENTS, GeneratorConfig())
    return MaterializeResult(metadata={"messages": summary["messages"], "events": summary["events"]})


BRONZE_TABLES = ["events", "dlq", "ingest_log"]
REALTIME_TABLES = ["ledger", "alerts", "player_daily", "dlq", "late_events", "duplicates", "batches"]


@multi_asset(
    specs=[AssetSpec(["bronze", t], deps=[raw_events], group_name="ingest") for t in BRONZE_TABLES],
    description="Contract-checked events MERGEd into Delta Lake bronze.",
)
def bronze_lake():
    s = land(LAKE, file=EVENTS)
    meta = {k: s[k] for k in ("records_read", "inserted", "invalid", "delta_version")}
    for t in BRONZE_TABLES:
        yield MaterializeResult(asset_key=AssetKey(["bronze", t]), metadata=meta if t == "events" else {})


@multi_asset(
    specs=[AssetSpec(["realtime", t], deps=[raw_events], group_name="realtime") for t in REALTIME_TABLES],
    description="Stream engine outputs written exactly-once to DuckDB.",
)
def realtime_engine():
    s = run_stream(REALTIME, file=EVENTS, log=lambda *_: None)
    for t in REALTIME_TABLES:
        meta = {"records": s.records, **s.stats} if t == "ledger" else {}
        yield MaterializeResult(asset_key=AssetKey(["realtime", t]), metadata=meta)


@dbt_assets(manifest=dbt_project.manifest_path)
def warehouse(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


everything = define_asset_job("full_pipeline", selection="*")

defs = Definitions(
    assets=[raw_events, bronze_lake, realtime_engine, warehouse],
    jobs=[everything],
    schedules=[
        ScheduleDefinition(
            job=everything, cron_schedule="15 6 * * *", execution_timezone=os.environ.get("PELIVAHTI_TIMEZONE", "EET")
        )
    ],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
