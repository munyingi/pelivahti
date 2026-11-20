"""Data contracts: every event is validated against a versioned JSON Schema.

Events that break their contract never reach the lake or the stream state.
They are routed to a dead-letter queue (DLQ) together with the reason.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
MONEY_FIELDS = {
    "amount_eur",
    "stake_eur",
    "win_eur",
    "daily_loss_limit_eur",
    "monthly_loss_limit_eur",
    "new_limit_eur",
    "odds",
}


@dataclass(frozen=True)
class ContractResult:
    ok: bool
    event: dict | None
    error: str | None


@cache
def _validators(version: str = "v1") -> dict[str, Draft202012Validator]:
    out = {}
    for path in sorted((CONTRACTS_DIR / version).glob("*.json")):
        schema = json.loads(path.read_text())
        Draft202012Validator.check_schema(schema)
        out[path.stem] = Draft202012Validator(schema)
    if not out:
        raise RuntimeError(f"No contracts found in {CONTRACTS_DIR / version}")
    return out


def event_types() -> list[str]:
    return sorted(_validators())


def validate(raw: str | bytes) -> ContractResult:
    """Parse and validate one raw message. Never raises on bad input."""
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return ContractResult(False, None, f"invalid_json: {exc.msg if hasattr(exc, 'msg') else exc}")
    if not isinstance(event, dict):
        return ContractResult(False, None, "invalid_json: message is not an object")

    event_type = event.get("event_type")
    validator = _validators().get(event_type) if isinstance(event_type, str) else None
    if validator is None:
        return ContractResult(False, event, f"unknown_event_type: {event_type!r}")

    errors = sorted(validator.iter_errors(event), key=lambda e: list(e.path))
    if errors:
        first = errors[0]
        where = ".".join(str(p) for p in first.path) or "<root>"
        return ContractResult(False, event, f"contract_violation: {where}: {first.message}")

    for field in MONEY_FIELDS & event.keys():
        if Decimal(str(event[field])).as_tuple().exponent < -2:
            return ContractResult(False, event, f"contract_violation: {field}: more than 2 decimals")

    return ContractResult(True, event, None)
