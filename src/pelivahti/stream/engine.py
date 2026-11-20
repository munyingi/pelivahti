"""Event-time stream engine: settlement, wallet ledger and responsible-gaming rules.

Design rules
------------
* **Event time, not arrival time.** Windows use the time the player acted.
* **Watermark** = highest event time seen minus ``allowed_lateness``. An event older
  than the watermark is *late*.
* **Money is never dropped.** Late events still settle and still hit the ledger.
  They only skip the windowed risk rules (their windows may already be closed) and
  are logged to ``late_events`` so the batch layer can account for them.
* **Idempotent outputs.** Every ledger entry and alert has a deterministic id, so a
  replay after a crash produces exactly the same rows.
* **Corrections are append-only.** A corrected match result writes a REVERSAL entry
  for the old settlement and a new settlement entry. Nothing is updated in place.
"""

from __future__ import annotations

import bisect
import hashlib
import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from ..contracts import validate
from ..sources import Record

HEL = ZoneInfo("Europe/Helsinki")
D2 = Decimal("0.01")
GAMING_ENTRIES = {"STAKE", "PAYOUT", "REFUND", "REVERSAL", "WIN"}

SEVERITY = {
    "SELF_EXCLUSION_BREACH": ("critical", "responsible_gaming"),
    "DAILY_LOSS_LIMIT_BREACH": ("high", "responsible_gaming"),
    "MONTHLY_LOSS_LIMIT_BREACH": ("high", "responsible_gaming"),
    "LOSS_CHASING": ("high", "responsible_gaming"),
    "DEPOSIT_VELOCITY": ("medium", "responsible_gaming"),
    "NIGHT_MARATHON_SESSION": ("medium", "responsible_gaming"),
    "DAILY_LOSS_LIMIT_80PCT": ("low", "responsible_gaming"),
    "STALE_ODDS_ACCEPTED": ("medium", "operations"),
}


def d(x) -> Decimal:
    return Decimal(str(x)).quantize(D2, rounding=ROUND_HALF_UP)


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


def naive(ts: datetime) -> datetime:
    return ts.astimezone(UTC).replace(tzinfo=None)


def local_day(ts: datetime) -> date:
    return ts.astimezone(HEL).date()


def day_end_utc(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=HEL).astimezone(UTC) + timedelta(days=1)


@dataclass
class StreamConfig:
    allowed_lateness: timedelta = timedelta(minutes=5)
    dedup_ttl: timedelta = timedelta(hours=24)
    limit_increase_delay: timedelta = timedelta(hours=24)
    deposit_velocity_count: int = 3
    deposit_velocity_window: timedelta = timedelta(minutes=15)
    chase_deposit_after_loss: timedelta = timedelta(minutes=10)
    chase_window: timedelta = timedelta(minutes=20)
    chase_stake_multiplier: Decimal = Decimal("2")
    session_gap: timedelta = timedelta(minutes=20)
    night_session_min: timedelta = timedelta(minutes=90)
    stale_odds_tolerance: Decimal = Decimal("0.10")


@dataclass
class PlayerState:
    daily_limit: Decimal | None = None
    monthly_limit: Decimal | None = None
    pending_limits: list = field(default_factory=list)  # (effective_from, kind, value)
    excluded_from: datetime | None = None
    excluded_until: datetime | None = None
    month_loss: dict = field(default_factory=dict)  # "YYYY-MM" -> Decimal
    deposits: deque = field(default_factory=deque)
    stake_sum: Decimal = Decimal("0")
    stake_count: int = 0
    last_loss_at: datetime | None = None
    chase_until: datetime | None = None
    chase_base: Decimal = Decimal("0")
    session_start: datetime | None = None
    session_last: datetime | None = None
    alerted: set = field(default_factory=set)


@dataclass
class Bet:
    bet_id: str
    player_id: str
    match_id: str
    selection: str
    odds: Decimal
    stake: Decimal
    settled_version: int = 0
    settled_amount: Decimal = Decimal("0")


@dataclass
class Outputs:
    ledger: list = field(default_factory=list)
    alerts: list = field(default_factory=list)
    player_daily: list = field(default_factory=list)
    dlq: list = field(default_factory=list)
    late: list = field(default_factory=list)
    duplicates: list = field(default_factory=list)


class Engine:
    def __init__(self, cfg: StreamConfig | None = None):
        self.cfg = cfg or StreamConfig()
        self.max_event_time: datetime | None = None
        self.players: dict[str, PlayerState] = defaultdict(PlayerState)
        self.bets: dict[str, Bet] = {}
        self.match_bets: dict[str, list[str]] = defaultdict(list)
        self.results: dict[str, tuple[int, str, datetime]] = {}
        self.odds: dict[tuple[str, str], list[tuple[datetime, Decimal]]] = defaultdict(list)
        self.seen: dict[str, datetime] = {}
        self.days: dict[tuple[str, date], dict] = {}
        self.batch_no = 0
        self.out = Outputs()
        self.stats = defaultdict(int)

    # ------------------------------------------------------------------ plumbing
    @property
    def watermark(self) -> datetime | None:
        return None if self.max_event_time is None else self.max_event_time - self.cfg.allowed_lateness

    def drain(self) -> Outputs:
        out, self.out = self.out, Outputs()
        return out

    def handle(self, rec: Record) -> None:
        res = validate(rec.value)
        if not res.ok:
            self.stats["dlq"] += 1
            self.out.dlq.append(
                {
                    "source_partition": rec.partition,
                    "source_offset": rec.offset,
                    "raw": rec.value[:4000],
                    "error": res.error,
                }
            )
            return
        ev = res.event
        if ev["event_id"] in self.seen:
            self.stats["duplicates"] += 1
            self.out.duplicates.append(
                {"source_partition": rec.partition, "source_offset": rec.offset, "event_id": ev["event_id"]}
            )
            return
        ts = parse_ts(ev["event_time"])
        self.seen[ev["event_id"]] = ts

        wm = self.watermark
        late = wm is not None and ts < wm
        if late:
            self.stats["late"] += 1
            self.out.late.append(
                {
                    "event_id": ev["event_id"],
                    "event_type": ev["event_type"],
                    "event_time": naive(ts),
                    "watermark": naive(wm),
                    "lateness_seconds": (wm - ts).total_seconds(),
                }
            )
        if self.max_event_time is None or ts > self.max_event_time:
            self.max_event_time = ts
        self.stats["processed"] += 1
        getattr(self, f"_on_{ev['event_type']}")(ev, ts, late)

    def end_batch(self) -> None:
        self.batch_no += 1
        wm = self.watermark
        if wm is None:
            return
        self._close_days(wm)
        cutoff = wm - self.cfg.dedup_ttl
        if self.batch_no % 10 == 0:
            self.seen = {k: v for k, v in self.seen.items() if v >= cutoff}

    def flush(self) -> None:
        """End of input: close every open window."""
        self._close_days(datetime.max.replace(tzinfo=UTC))

    # ------------------------------------------------------------------ outputs
    def _ledger(
        self,
        entry_id: str,
        ev: dict,
        ts: datetime,
        player_id: str,
        kind: str,
        product: str,
        ref: str,
        amount: Decimal,
        late: bool,
        trigger_event_id: str | None = None,
    ) -> None:
        self.out.ledger.append(
            {
                "entry_id": entry_id,
                "player_id": player_id,
                "entry_type": kind,
                "product": product,
                "ref_id": ref,
                "amount_eur": amount,
                "event_id": trigger_event_id or ev["event_id"],
                "event_time": naive(ts),
                "is_late": late,
            }
        )
        if kind in GAMING_ENTRIES and not late:
            self._gaming_effect(player_id, ts, kind, amount)
        if not late:
            agg = self._day(player_id, ts)
            if kind == "STAKE":
                agg["stakes"] += -amount
            elif kind in {"PAYOUT", "REFUND", "WIN", "REVERSAL"}:
                agg["returns"] += amount
            elif kind == "DEPOSIT":
                agg["deposits"] += amount
            elif kind == "WITHDRAWAL":
                agg["withdrawals"] += -amount

    def _alert(self, rule: str, player_id: str, key: str, ts: datetime, **details) -> None:
        st = self.players[player_id]
        akey = f"{rule}|{key}"
        if akey in st.alerted:
            return
        st.alerted.add(akey)
        severity, category = SEVERITY[rule]
        alert_id = hashlib.sha1(f"{rule}|{player_id}|{key}".encode()).hexdigest()[:20]
        self.out.alerts.append(
            {
                "alert_id": alert_id,
                "rule": rule,
                "severity": severity,
                "category": category,
                "player_id": player_id,
                "event_time": naive(ts),
                "details": json.dumps(details, default=str, sort_keys=True),
            }
        )

    # ------------------------------------------------------------------ windows
    def _day(self, player_id: str, ts: datetime) -> dict:
        key = (player_id, local_day(ts))
        agg = self.days.get(key)
        if agg is None:
            agg = self.days[key] = {
                "stakes": Decimal("0"),
                "returns": Decimal("0"),
                "deposits": Decimal("0"),
                "withdrawals": Decimal("0"),
                "n_bets": 0,
                "n_rounds": 0,
                "alerted": set(),
            }
        return agg

    def _close_days(self, wm: datetime) -> None:
        for pid, day in [k for k in self.days if day_end_utc(k[1]) <= wm]:
            agg = self.days.pop((pid, day))
            limit = self._limit(pid, "daily", day_end_utc(day) - timedelta(microseconds=1))
            self.out.player_daily.append(
                {
                    "player_id": pid,
                    "day": day,
                    "stakes": agg["stakes"],
                    "returns": agg["returns"],
                    "net_loss": agg["stakes"] - agg["returns"],
                    "deposits": agg["deposits"],
                    "withdrawals": agg["withdrawals"],
                    "n_bets": agg["n_bets"],
                    "n_rounds": agg["n_rounds"],
                    "daily_limit": limit,
                }
            )

    def _limit(self, player_id: str, kind: str, at: datetime) -> Decimal | None:
        st = self.players[player_id]
        keep = []
        for eff, k, v in st.pending_limits:
            if eff <= at:
                if k == "daily":
                    st.daily_limit = v
                else:
                    st.monthly_limit = v
            else:
                keep.append((eff, k, v))
        st.pending_limits = keep
        return st.daily_limit if kind == "daily" else st.monthly_limit

    def _gaming_effect(self, pid: str, ts: datetime, kind: str, amount: Decimal) -> None:
        st = self.players[pid]
        agg = self._day(pid, ts)
        loss_delta = -amount  # a stake (negative entry) adds to the loss, any return reduces it
        # the day aggregate is updated by the caller right after this call, so use the post-entry value here
        net_day = agg["stakes"] - agg["returns"] + loss_delta
        day = local_day(ts)
        month = day.strftime("%Y-%m")
        st.month_loss[month] = st.month_loss.get(month, Decimal("0")) + loss_delta
        daily, monthly = self._limit(pid, "daily", ts), self._limit(pid, "monthly", ts)
        if daily is not None:
            if net_day > daily:
                self._alert("DAILY_LOSS_LIMIT_BREACH", pid, str(day), ts, net_loss=net_day, limit=daily)
            elif net_day >= daily * Decimal("0.8"):
                self._alert("DAILY_LOSS_LIMIT_80PCT", pid, str(day), ts, net_loss=net_day, limit=daily)
        if monthly is not None and st.month_loss[month] > monthly:
            self._alert("MONTHLY_LOSS_LIMIT_BREACH", pid, month, ts, net_loss=st.month_loss[month], limit=monthly)

    # ------------------------------------------------------------------ player activity helpers
    def _activity(self, pid: str, ts: datetime, late: bool) -> None:
        st = self.players[pid]
        if st.excluded_until and st.excluded_from and st.excluded_from <= ts < st.excluded_until:
            # compliance check runs even for late events: it does not depend on a window
            self._alert("SELF_EXCLUSION_BREACH", pid, str(local_day(ts)), ts, excluded_until=st.excluded_until)
        if late:
            return
        if st.session_last is None or ts - st.session_last > self.cfg.session_gap:
            st.session_start = ts
        st.session_last = max(ts, st.session_last or ts)
        dur = st.session_last - st.session_start
        if dur >= self.cfg.night_session_min:
            hours = {st.session_start.astimezone(HEL).hour, ts.astimezone(HEL).hour}
            if any(0 <= h < 5 for h in hours):
                self._alert(
                    "NIGHT_MARATHON_SESSION",
                    pid,
                    st.session_start.isoformat(),
                    ts,
                    session_start=st.session_start,
                    minutes=round(dur.total_seconds() / 60),
                )

    def _stake_rules(self, pid: str, ts: datetime, stake: Decimal, late: bool) -> None:
        st = self.players[pid]
        if not late:
            if st.chase_until and ts <= st.chase_until and stake >= st.chase_base * self.cfg.chase_stake_multiplier:
                self._alert("LOSS_CHASING", pid, str(local_day(ts)), ts, stake=stake, usual_stake=st.chase_base)
                st.chase_until = None
            st.stake_sum += stake
            st.stake_count += 1

    def _loss(self, pid: str, ts: datetime, late: bool) -> None:
        if not late:
            self.players[pid].last_loss_at = ts

    # ------------------------------------------------------------------ event handlers
    def _on_player_registered(self, ev, ts, late):
        st = self.players[ev["player_id"]]
        st.daily_limit = d(ev["daily_loss_limit_eur"])
        st.monthly_limit = d(ev["monthly_loss_limit_eur"])

    def _on_limit_changed(self, ev, ts, late):
        pid, kind, new = ev["player_id"], ev["limit_type"], d(ev["new_limit_eur"])
        st = self.players[pid]
        current = st.daily_limit if kind == "daily" else st.monthly_limit
        effective = ts if current is None or new <= current else ts + self.cfg.limit_increase_delay
        st.pending_limits.append((effective, kind, new))

    def _on_self_exclusion_set(self, ev, ts, late):
        st = self.players[ev["player_id"]]
        st.excluded_from = ts
        st.excluded_until = parse_ts(ev["excluded_until"])

    def _on_deposit(self, ev, ts, late):
        pid, amount = ev["player_id"], d(ev["amount_eur"])
        self._activity(pid, ts, late)
        self._ledger(
            f"{ev['transaction_id']}:DEPOSIT", ev, ts, pid, "DEPOSIT", "wallet", ev["transaction_id"], amount, late
        )
        if late:
            return
        st = self.players[pid]
        st.deposits.append(ts)
        while st.deposits and ts - st.deposits[0] > self.cfg.deposit_velocity_window:
            st.deposits.popleft()
        if len(st.deposits) >= self.cfg.deposit_velocity_count:
            bucket = ts.astimezone(HEL).strftime("%Y-%m-%dT%H")
            self._alert(
                "DEPOSIT_VELOCITY",
                pid,
                bucket,
                ts,
                deposits_in_window=len(st.deposits),
                window_minutes=self.cfg.deposit_velocity_window.total_seconds() / 60,
            )
        if st.last_loss_at and ts - st.last_loss_at <= self.cfg.chase_deposit_after_loss and st.stake_count:
            st.chase_until = ts + self.cfg.chase_window
            st.chase_base = (st.stake_sum / st.stake_count).quantize(D2)

    def _on_withdrawal(self, ev, ts, late):
        pid = ev["player_id"]
        self._ledger(
            f"{ev['transaction_id']}:WITHDRAWAL",
            ev,
            ts,
            pid,
            "WITHDRAWAL",
            "wallet",
            ev["transaction_id"],
            -d(ev["amount_eur"]),
            late,
        )

    def _on_match_scheduled(self, ev, ts, late):
        pass  # reference data, handled by the batch layer

    def _on_odds_changed(self, ev, ts, late):
        hist = self.odds[(ev["match_id"], ev["selection"])]
        bisect.insort(hist, (ts, d(ev["odds"])))

    def _odds_at(self, match_id: str, sel: str, ts: datetime) -> Decimal | None:
        hist = self.odds.get((match_id, sel))
        if not hist:
            return None
        i = bisect.bisect_right(hist, (ts, Decimal("Infinity"))) - 1
        return hist[i][1] if i >= 0 else None

    def _on_bet_placed(self, ev, ts, late):
        pid = ev["player_id"]
        bet = Bet(ev["bet_id"], pid, ev["match_id"], ev["selection"], d(ev["odds"]), d(ev["stake_eur"]))
        if bet.bet_id in self.bets:
            return  # same bet re-sent under a new event_id: the business key wins
        self.bets[bet.bet_id] = bet
        self.match_bets[bet.match_id].append(bet.bet_id)
        self._activity(pid, ts, late)
        self._stake_rules(pid, ts, bet.stake, late)
        self._ledger(f"{bet.bet_id}:STAKE", ev, ts, pid, "STAKE", "sports", bet.bet_id, -bet.stake, late)
        if not late:
            self._day(pid, ts)["n_bets"] += 1
        live = self._odds_at(bet.match_id, bet.selection, ts)
        if live is not None and abs(bet.odds - live) / live > self.cfg.stale_odds_tolerance:
            self._alert(
                "STALE_ODDS_ACCEPTED", pid, bet.bet_id, ts, bet_id=bet.bet_id, accepted_odds=bet.odds, live_odds=live
            )
        if bet.match_id in self.results:
            version, outcome, resulted_at = self.results[bet.match_id]
            self._settle(bet, version, outcome, max(ts, resulted_at), ev, late)

    def _on_match_result(self, ev, ts, late):
        mid, version, outcome = ev["match_id"], ev["result_version"], ev["outcome"]
        if mid in self.results and self.results[mid][0] >= version:
            return  # an older or repeated version: ignore, the newest result is authoritative
        self.results[mid] = (version, outcome, ts)
        for bet_id in self.match_bets.get(mid, []):
            self._settle(self.bets[bet_id], version, outcome, ts, ev, late)

    def _settle(self, bet: Bet, version: int, outcome: str, ts: datetime, ev: dict, late: bool) -> None:
        if bet.settled_version >= version:
            return
        if bet.settled_version and bet.settled_amount:
            self._ledger(
                f"{bet.bet_id}:REVERSAL:v{version}",
                ev,
                ts,
                bet.player_id,
                "REVERSAL",
                "sports",
                bet.bet_id,
                -bet.settled_amount,
                late,
            )
        if outcome == "void":
            kind, amount = "REFUND", bet.stake
        elif outcome == bet.selection:
            kind, amount = "PAYOUT", d(bet.stake * bet.odds)
        else:
            kind, amount = "LOST", Decimal("0")
        if amount:
            self._ledger(
                f"{bet.bet_id}:{kind}:v{version}", ev, ts, bet.player_id, kind, "sports", bet.bet_id, amount, late
            )
        elif bet.settled_version == 0:
            self._loss(bet.player_id, ts, late)
        bet.settled_version, bet.settled_amount = version, amount

    def _on_casino_round(self, ev, ts, late):
        pid, rid = ev["player_id"], ev["round_id"]
        stake, win = d(ev["stake_eur"]), d(ev["win_eur"])
        self._activity(pid, ts, late)
        self._stake_rules(pid, ts, stake, late)
        self._ledger(f"{rid}:STAKE", ev, ts, pid, "STAKE", "casino", rid, -stake, late)
        if win:
            self._ledger(f"{rid}:WIN", ev, ts, pid, "WIN", "casino", rid, win, late)
        if win < stake:
            self._loss(pid, ts, late)
        if not late:
            self._day(pid, ts)["n_rounds"] += 1
