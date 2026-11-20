"""Synthetic event generator for a regulated betting and online casino operator.

Everything is fake and seeded, so the same seed always gives the same file.
The generator also simulates the transport layer on purpose:

* small random delivery delays, so events arrive slightly out of order
* a share of very late events (some beyond the stream's allowed lateness)
* at-least-once redelivery, so some events arrive twice with the same event_id
* a few events that break their data contract

The output is JSON Lines in *arrival* order, which is what a consumer would see.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

REGIONS = [
    "North",
    "South",
    "East",
    "West",
    "Central",
    "Capital",
    "Coastal",
    "Lakeside",
    "Highlands",
    "Islands",
]
LEAGUES = {
    ("ice_hockey", "NHL"): [
        "Bruins",
        "Rangers",
        "Maple Leafs",
        "Canadiens",
        "Penguins",
        "Oilers",
        "Avalanche",
        "Lightning",
        "Panthers",
        "Stars",
        "Kings",
        "Golden Knights",
        "Hurricanes",
        "Devils",
        "Jets",
        "Wild",
    ],
    ("football", "Premier League"): [
        "Arsenal",
        "Chelsea",
        "Liverpool",
        "Man City",
        "Man United",
        "Tottenham",
        "Newcastle",
        "Aston Villa",
        "Brighton",
        "West Ham",
        "Everton",
        "Fulham",
    ],
    ("basketball", "NBA"): ["Celtics", "Lakers", "Warriors", "Bucks", "Nuggets", "Heat", "Knicks", "Suns"],
}
CASINO_GAMES = ["slot_revontulet", "slot_sisu_gold", "slot_kalevala", "blackjack_classic", "roulette_euro"]
CHANNELS = ["app", "app", "app", "web", "web", "retail"]
DEPOSIT_METHODS = ["bank_transfer", "card", "mobilepay", "mobilepay"]
D2 = Decimal("0.01")


def money(x: float | Decimal) -> float:
    return float(Decimal(str(x)).quantize(D2, rounding=ROUND_HALF_UP))


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class GeneratorConfig:
    seed: int = 42
    players: int = 800
    days: int = 3
    start: datetime = datetime(2026, 11, 16, tzinfo=UTC)
    duplicate_rate: float = 0.01
    late_rate: float = 0.015
    invalid_rate: float = 0.002


@dataclass
class Player:
    player_id: str
    profile: str  # casual | regular | at_risk
    daily_limit: float
    monthly_limit: float
    balance: float = 0.0
    avg_stake: float = 2.0
    excluded_until: datetime | None = None
    ignores_exclusion: bool = False


@dataclass
class Match:
    match_id: str
    sport: str
    league: str
    home: str
    away: str
    kickoff: datetime
    probs: dict[str, float]
    odds: dict[str, float] = field(default_factory=dict)


class Generator:
    def __init__(self, cfg: GeneratorConfig):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.end = cfg.start + timedelta(days=cfg.days)
        self.events: list[dict] = []
        self._seq = {"T": 0, "B": 0, "R": 0}

    # ---------- helpers
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _next(self, prefix: str, width: int) -> str:
        self._seq[prefix] += 1
        return f"{prefix}{self._seq[prefix]:0{width}d}"

    def emit(self, event_type: str, ts: datetime, **fields) -> None:
        if ts >= self.end:
            return
        self.events.append(
            {
                "event_id": self._uuid(),
                "event_type": event_type,
                "schema_version": 1,
                "event_time": iso(ts),
                **fields,
            }
        )

    # ---------- world
    def _players(self) -> list[Player]:
        players = []
        for i in range(1, self.cfg.players + 1):
            r = self.rng.random()
            profile = "casual" if r < 0.78 else "regular" if r < 0.94 else "at_risk"
            daily = self.rng.choice([50, 100, 100, 200, 300, 500])
            monthly = max(daily * 3, self.rng.choice([300, 500, 1000, 2000]))
            p = Player(f"P{i:06d}", profile, float(daily), float(monthly))
            p.avg_stake = {"casual": 2.0, "regular": 5.0, "at_risk": 10.0}[profile] * self.rng.uniform(0.6, 1.6)
            players.append(p)
            reg_time = self.cfg.start - timedelta(days=self.rng.randint(1, 900), minutes=self.rng.randint(0, 1440))
            # registrations before the window are emitted at the window start so the stream sees them first
            self.emit(
                "player_registered",
                max(reg_time, self.cfg.start),
                player_id=p.player_id,
                birth_year=self.rng.randint(1950, 2006),
                region=self.rng.choice(REGIONS),
                daily_loss_limit_eur=p.daily_limit,
                monthly_loss_limit_eur=p.monthly_limit,
            )
        return players

    def _matches(self) -> list[Match]:
        matches, n = [], 0
        for day in range(self.cfg.days + 1):
            base = self.cfg.start + timedelta(days=day)
            for (sport, league), teams in LEAGUES.items():
                count = {"ice_hockey": 6, "football": 5, "basketball": 4}[sport]
                hours = {"ice_hockey": [16, 16, 17], "football": [13, 15, 18, 20], "basketball": [0, 1, 2, 3]}[sport]
                pairs = self.rng.sample(teams, k=min(len(teams), count * 2))
                for k in range(0, len(pairs) - 1, 2):
                    n += 1
                    kickoff = base + timedelta(hours=self.rng.choice(hours), minutes=self.rng.choice([0, 30]))
                    h = self.rng.uniform(0.25, 0.6)
                    d = 0.0 if sport == "basketball" else self.rng.uniform(0.18, 0.3)
                    probs = {"home": h, "draw": d, "away": max(0.08, 1 - h - d)}
                    s = sum(probs.values())
                    probs = {k2: v / s for k2, v in probs.items()}
                    matches.append(Match(f"M{n:05d}", sport, league, pairs[k], pairs[k + 1], kickoff, probs))
        return matches

    def _price(self, p: float) -> float:
        margin = 1.07
        return max(1.01, money(1 / (p * margin)))

    def _publish_matches(self, matches: list[Match]) -> None:
        for m in matches:
            published = max(self.cfg.start, m.kickoff - timedelta(hours=36))
            self.emit(
                "match_scheduled",
                published,
                match_id=m.match_id,
                sport=m.sport,
                league=m.league,
                home_team=m.home,
                away_team=m.away,
                kickoff_time=iso(m.kickoff),
            )
            for sel, p in m.probs.items():
                if p <= 0.001:
                    continue
                m.odds[sel] = self._price(p)
                self.emit(
                    "odds_changed",
                    published + timedelta(seconds=1),
                    match_id=m.match_id,
                    selection=sel,
                    odds=m.odds[sel],
                )

    def _odds_movements(self, m: Match) -> list[tuple[datetime, str, float]]:
        """Pre-compute when the odds move, so bets can use the price that was live at bet time."""
        moves = []
        published = max(self.cfg.start, m.kickoff - timedelta(hours=36))
        span = (m.kickoff - published).total_seconds()
        for _ in range(self.rng.randint(1, 4)):
            t = published + timedelta(seconds=self.rng.uniform(0.1, 0.95) * span)
            sel = self.rng.choice([s for s in m.odds])
            new = max(1.01, money(m.odds[sel] * self.rng.uniform(0.85, 1.15)))
            moves.append((t, sel, new))
        moves.sort()
        for t, sel, new in moves:
            self.emit("odds_changed", t, match_id=m.match_id, selection=sel, odds=new)
        return moves

    def _results(self, matches: list[Match]) -> None:
        for m in matches:
            t = m.kickoff + timedelta(hours=2, minutes=self.rng.randint(5, 40))
            if self.rng.random() < 0.03:
                outcome = "void"
            else:
                outcome = self.rng.choices(list(m.probs), weights=list(m.probs.values()))[0]
            self.emit("match_result", t, match_id=m.match_id, outcome=outcome, result_version=1)
            if outcome != "void" and self.rng.random() < 0.025:
                corrected = self.rng.choice([o for o in m.odds if o != outcome])
                self.emit(
                    "match_result",
                    t + timedelta(hours=self.rng.uniform(1, 6)),
                    match_id=m.match_id,
                    outcome=corrected,
                    result_version=2,
                )

    @staticmethod
    def _odds_at(m: Match, moves: list[tuple[datetime, str, float]], sel: str, ts: datetime) -> float:
        price = None
        for t, s, o in moves:
            if s == sel and t <= ts:
                price = o
        if price is not None:
            return price
        return m.initial_odds[sel]  # type: ignore[attr-defined]

    # ---------- player behaviour
    def _session(self, p: Player, start: datetime, matches: list[Match], moves: dict[str, list]) -> None:
        rng = self.rng
        t = start
        length = {"casual": 25, "regular": 45, "at_risk": 110}[p.profile] * rng.uniform(0.4, 1.6)
        session_end = start + timedelta(minutes=length)
        stake_mult = 1.0
        prefers_casino = rng.random() < {"casual": 0.35, "regular": 0.5, "at_risk": 0.7}[p.profile]
        deposits_this_session = 0

        def deposit(ts: datetime, amount: float) -> None:
            nonlocal deposits_this_session
            amount = money(min(max(amount, 10), 2000))
            p.balance += amount
            deposits_this_session += 1
            self.emit(
                "deposit",
                ts,
                player_id=p.player_id,
                transaction_id=self._next("T", 8),
                amount_eur=amount,
                method=rng.choice(DEPOSIT_METHODS),
            )

        excluded = p.excluded_until is not None and start < p.excluded_until
        if excluded and not p.ignores_exclusion:
            return

        while t < session_end:
            stake = money(max(0.2, rng.lognormvariate(0, 0.6) * p.avg_stake * stake_mult))
            if p.balance < stake:
                limit_dep = 6 if p.profile == "at_risk" else 2
                if deposits_this_session >= limit_dep:
                    break
                deposit(t, rng.choice([20, 20, 50, 50, 100, 200]) * (2 if p.profile == "at_risk" else 1))
                t += timedelta(seconds=rng.randint(20, 90))
                if p.profile == "at_risk" and rng.random() < 0.35:
                    # rapid top-ups: several deposits within minutes
                    for _ in range(rng.randint(1, 3)):
                        deposit(t, rng.choice([20, 50, 100]))
                        t += timedelta(seconds=rng.randint(30, 200))

            upcoming = [m for m in matches if t + timedelta(minutes=5) < m.kickoff < t + timedelta(hours=30)]
            if upcoming and not prefers_casino and rng.random() < 0.8:
                m = rng.choice(upcoming)
                sel = rng.choices(list(m.probs), weights=[v + 0.05 for v in m.probs.values()])[0]
                if sel not in m.odds:
                    sel = "home"
                odds = self._odds_at(m, moves[m.match_id], sel, t)
                if rng.random() < 0.01:
                    odds = money(odds * rng.uniform(1.12, 1.3))  # accepted at a stale, too generous price
                stake = min(stake, money(p.balance))
                if stake < 0.1:
                    break
                p.balance = money(p.balance - stake)
                self.emit(
                    "bet_placed",
                    t,
                    player_id=p.player_id,
                    bet_id=self._next("B", 8),
                    match_id=m.match_id,
                    selection=sel,
                    odds=odds,
                    stake_eur=stake,
                    channel=rng.choice(CHANNELS),
                )
                t += timedelta(seconds=rng.randint(40, 240))
            else:
                game = rng.choice(CASINO_GAMES)
                rounds = rng.randint(3, 12)
                for _ in range(rounds):
                    stake_r = money(min(100, max(0.1, p.avg_stake * stake_mult * rng.uniform(0.5, 1.5) / 2)))
                    if p.balance < stake_r:
                        break
                    r = rng.random()
                    mult = (
                        0 if r < 0.6 else rng.choice([0.5, 1, 2, 3]) if r < 0.985 else rng.choice([5, 10, 20, 50])
                    )  # ~94.5% RTP
                    win = money(stake_r * mult)
                    p.balance = money(p.balance - stake_r + win)
                    self.emit(
                        "casino_round",
                        t,
                        player_id=p.player_id,
                        round_id=self._next("R", 9),
                        game_id=game,
                        stake_eur=stake_r,
                        win_eur=win,
                    )
                    t += timedelta(seconds=rng.randint(6, 20))
                    if p.profile == "at_risk" and win == 0 and rng.random() < 0.06:
                        stake_mult = min(stake_mult * 2, 8)  # chasing losses with bigger stakes
                t += timedelta(seconds=rng.randint(60, 300))

        if p.balance > 150 and rng.random() < 0.5:
            amount = money(p.balance * rng.uniform(0.5, 1.0))
            p.balance = money(p.balance - amount)
            self.emit(
                "withdrawal",
                t + timedelta(minutes=1),
                player_id=p.player_id,
                transaction_id=self._next("T", 8),
                amount_eur=amount,
            )

    def _session_starts(self, p: Player) -> list[datetime]:
        starts = []
        per_day = {"casual": 0.5, "regular": 1.3, "at_risk": 2.6}[p.profile]
        for day in range(self.cfg.days):
            base = self.cfg.start + timedelta(days=day)
            for _ in range(self.rng.choices([0, 1, 2, 3, 4], weights=self._poisson(per_day))[0]):
                if p.profile == "at_risk" and self.rng.random() < 0.45:
                    hour = self.rng.choice([21, 22, 23, 0, 1])  # UTC, i.e. late evening / night in local time (UTC+2)
                else:
                    hour = self.rng.choice([5, 9, 10, 12, 14, 15, 16, 17, 18, 19, 20])
                starts.append(base + timedelta(hours=hour, minutes=self.rng.randint(0, 59)))
        return sorted(starts)

    @staticmethod
    def _poisson(lam: float) -> list[float]:
        import math

        return [math.exp(-lam) * lam**k / math.factorial(k) for k in range(5)]

    def _account_events(self, players: list[Player]) -> None:
        for p in players:
            if self.rng.random() < 0.04:
                ts = self.cfg.start + timedelta(hours=self.rng.uniform(1, 24 * self.cfg.days))
                kind = self.rng.choice(["daily", "monthly"])
                cur = p.daily_limit if kind == "daily" else p.monthly_limit
                new = money(max(10, cur * self.rng.choice([0.5, 0.5, 2.0])))
                self.emit("limit_changed", ts, player_id=p.player_id, limit_type=kind, new_limit_eur=new)
            if p.profile == "at_risk" and self.rng.random() < 0.15:
                ts = self.cfg.start + timedelta(hours=self.rng.uniform(4, 24 * self.cfg.days - 12))
                p.excluded_until = ts + timedelta(days=30)
                p.ignores_exclusion = self.rng.random() < 0.3  # simulates an upstream control failure
                self.emit("self_exclusion_set", ts, player_id=p.player_id, excluded_until=iso(p.excluded_until))

    # ---------- transport simulation
    def _deliver(self) -> list[tuple[datetime, str]]:
        rng, out = self.rng, []
        for ev in self.events:
            ts = datetime.fromisoformat(ev["event_time"].replace("Z", "+00:00"))
            delay = timedelta(seconds=rng.expovariate(1 / 1.5))
            if rng.random() < self.cfg.late_rate:
                delay = timedelta(minutes=rng.uniform(2, 45))
            line = json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
            # reference data comes from more reliable upstream systems; only transactional events get corrupted
            if rng.random() < self.cfg.invalid_rate and ev["event_type"] not in {
                "player_registered",
                "match_scheduled",
            }:
                line = self._corrupt(ev)
            out.append((ts + delay, line))
            if rng.random() < self.cfg.duplicate_rate:
                out.append((ts + delay + timedelta(seconds=rng.uniform(1, 600)), line))
        out.sort(key=lambda x: x[0])
        return out

    def _corrupt(self, ev: dict) -> str:
        bad = dict(ev)
        choice = self.rng.randint(0, 3)
        if choice == 0 and "stake_eur" in bad:
            bad["stake_eur"] = -abs(bad["stake_eur"])
        elif choice == 1:
            bad.pop("event_time", None)
        elif choice == 2:
            bad["schema_version"] = 2
        else:
            return json.dumps(bad)[:-7]  # truncated message
        return json.dumps(bad, ensure_ascii=False, separators=(",", ":"))

    # ---------- main
    def run(self) -> list[str]:
        players = self._players()
        matches = self._matches()
        self._publish_matches(matches)
        for m in matches:
            m.initial_odds = dict(m.odds)  # type: ignore[attr-defined]
        moves = {m.match_id: self._odds_movements(m) for m in matches}
        self._results(matches)
        self._account_events(players)
        for p in players:
            for start in self._session_starts(p):
                self._session(p, start, matches, moves)
        return [line for _, line in self._deliver()]


def generate(path: str | Path, cfg: GeneratorConfig | None = None) -> dict:
    cfg = cfg or GeneratorConfig()
    gen = Generator(cfg)
    lines = gen.run()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    counts: dict[str, int] = {}
    for ev in gen.events:
        counts[ev["event_type"]] = counts.get(ev["event_type"], 0) + 1
    return {"path": str(path), "events": len(gen.events), "messages": len(lines), "by_type": counts}
