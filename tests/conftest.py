from __future__ import annotations

import itertools
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from pelivahti.sources import Record

T0 = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)  # 12:00 local time (EET)


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")


class EventFactory:
    """Builds contract-valid events and wraps them as source records."""

    def __init__(self):
        self._offset = itertools.count()
        self._ids = itertools.count(1)

    def event(self, event_type: str, minutes: float, **fields) -> dict:
        return {
            "event_id": str(uuid.UUID(int=next(self._ids), version=4)),
            "event_type": event_type,
            "schema_version": 1,
            "event_time": iso(T0 + timedelta(minutes=minutes)),
            **fields,
        }

    def record(self, ev: dict | str) -> Record:
        value = ev if isinstance(ev, str) else json.dumps(ev)
        return Record(0, next(self._offset), value)

    # shortcuts
    def register(self, pid="P000001", minutes=0, daily=100, monthly=1000):
        return self.event(
            "player_registered",
            minutes,
            player_id=pid,
            birth_year=1990,
            region="North",
            daily_loss_limit_eur=daily,
            monthly_loss_limit_eur=monthly,
        )

    def bet(self, bet_id="B00000001", pid="P000001", match="M00001", sel="home", odds=2.5, stake=10, minutes=1):
        return self.event(
            "bet_placed",
            minutes,
            player_id=pid,
            bet_id=bet_id,
            match_id=match,
            selection=sel,
            odds=odds,
            stake_eur=stake,
            channel="app",
        )

    def result(self, match="M00001", outcome="home", version=1, minutes=120):
        return self.event("match_result", minutes, match_id=match, outcome=outcome, result_version=version)

    def deposit(self, txn, pid="P000001", amount=50, minutes=0):
        return self.event("deposit", minutes, player_id=pid, transaction_id=txn, amount_eur=amount, method="mobilepay")

    def spin(self, rid, pid="P000001", stake=1, win=0, minutes=0):
        return self.event(
            "casino_round", minutes, player_id=pid, round_id=rid, game_id="slot_sisu_gold", stake_eur=stake, win_eur=win
        )


@pytest.fixture
def f() -> EventFactory:
    return EventFactory()
