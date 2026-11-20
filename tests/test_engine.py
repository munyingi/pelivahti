from decimal import Decimal

from pelivahti.stream.engine import Engine


def run(f, events):
    eng = Engine()
    for ev in events:
        eng.handle(f.record(ev))
    eng.end_batch()
    return eng, eng.drain()


def entries(out):
    return {e["entry_id"]: e["amount_eur"] for e in out.ledger}


# ---------------------------------------------------------------- settlement
def test_winning_bet_pays_stake_times_odds(f):
    _, out = run(f, [f.register(), f.bet(odds=2.35, stake=10), f.result(outcome="home")])
    assert entries(out) == {"B00000001:STAKE": Decimal("-10.00"), "B00000001:PAYOUT:v1": Decimal("23.50")}


def test_payout_rounds_half_up(f):
    _, out = run(f, [f.register(), f.bet(odds=1.15, stake=0.5), f.result()])
    assert entries(out)["B00000001:PAYOUT:v1"] == Decimal("0.58")  # 0.575 -> 0.58


def test_void_refunds_stake(f):
    _, out = run(f, [f.register(), f.bet(stake=12), f.result(outcome="void")])
    assert entries(out)["B00000001:REFUND:v1"] == Decimal("12.00")


def test_result_correction_reverses_and_resettles(f):
    _, out = run(
        f,
        [
            f.register(),
            f.bet(sel="home", odds=3, stake=10),
            f.result(outcome="home", version=1),
            f.result(outcome="away", version=2, minutes=300),
        ],
    )
    assert entries(out) == {
        "B00000001:STAKE": Decimal("-10.00"),
        "B00000001:PAYOUT:v1": Decimal("30.00"),
        "B00000001:REVERSAL:v2": Decimal("-30.00"),
    }


def test_older_result_version_is_ignored(f):
    _, out = run(
        f, [f.register(), f.bet(sel="away"), f.result(outcome="away", version=2), f.result(outcome="home", version=1)]
    )
    assert "B00000001:PAYOUT:v2" in entries(out)
    assert not any("REVERSAL" in k for k in entries(out))


def test_bet_arriving_after_its_result_still_settles(f):
    _, out = run(f, [f.register(), f.result(outcome="home", minutes=120), f.bet(odds=2, stake=5, minutes=1)])
    assert entries(out)["B00000001:PAYOUT:v1"] == Decimal("10.00")


def test_redelivered_event_is_dropped(f):
    bet = f.bet()
    eng, out = run(f, [f.register(), bet, bet, bet])
    assert len([e for e in out.ledger if e["entry_type"] == "STAKE"]) == 1
    assert len(out.duplicates) == 2


def test_invalid_message_goes_to_dlq(f):
    _, out = run(f, [f.register(), '{"event_type": "bet_placed", "stake'])
    assert len(out.dlq) == 1 and out.ledger == []


# ---------------------------------------------------------------- watermark and late data
def test_late_event_is_booked_but_skips_windowed_rules(f):
    eng, out = run(
        f,
        [
            f.register(daily=20),
            f.spin("R000000001", minutes=60),  # pushes the watermark to 10:55 UTC
            f.spin("R000000002", stake=50, minutes=5),  # 50 minutes behind the watermark
        ],
    )
    assert len(out.late) == 1 and out.late[0]["lateness_seconds"] == 50 * 60
    assert entries(out)["R000000002:STAKE"] == Decimal("-50.00")  # money is never dropped
    assert not [a for a in out.alerts if a["rule"].startswith("DAILY")]  # but the closed window is untouched


def test_daily_windows_close_when_the_watermark_passes_midnight(f):
    eng, out = run(f, [f.register(), f.spin("R000000001", stake=3, win=1), f.spin("R000000002", minutes=60 * 13)])
    # 10:00 + 13 h = 23:00 UTC = 01:00 local time next day, so the first local day is closed
    assert len(out.player_daily) == 1
    day = out.player_daily[0]
    assert str(day["day"]) == "2026-11-16" and day["net_loss"] == Decimal("2.00")


# ---------------------------------------------------------------- responsible gaming rules
def rules(out):
    return {a["rule"] for a in out.alerts}


def test_daily_loss_limit_breach_and_80pct(f):
    spins = [f.spin(f"R{i:09d}", stake=20, minutes=i) for i in range(1, 7)]
    _, out = run(f, [f.register(daily=100), *spins])
    assert {"DAILY_LOSS_LIMIT_80PCT", "DAILY_LOSS_LIMIT_BREACH"} <= rules(out)
    assert len([a for a in out.alerts if a["rule"] == "DAILY_LOSS_LIMIT_BREACH"]) == 1  # once per day


def test_limit_increase_waits_for_cooling_off(f):
    raise_limit = f.event("limit_changed", 1, player_id="P000001", limit_type="daily", new_limit_eur=1000)
    spins = [f.spin(f"R{i:09d}", stake=30, minutes=10 + i) for i in range(5)]
    _, out = run(f, [f.register(daily=100), raise_limit, *spins])
    assert "DAILY_LOSS_LIMIT_BREACH" in rules(out)  # the higher limit is not active yet


def test_deposit_velocity(f):
    deps = [f.deposit(f"T{i:08d}", minutes=i * 4) for i in range(3)]
    _, out = run(f, [f.register(), *deps])
    assert "DEPOSIT_VELOCITY" in rules(out)


def test_self_exclusion_breach(f):
    excl = f.event("self_exclusion_set", 0, player_id="P000001", excluded_until="2026-12-16T10:00:00.000Z")
    _, out = run(f, [f.register(), excl, f.spin("R000000001", minutes=30)])
    alert = [a for a in out.alerts if a["rule"] == "SELF_EXCLUSION_BREACH"]
    assert alert and alert[0]["severity"] == "critical"


def test_loss_chasing(f):
    events = [f.register(daily=10_000)]
    events += [f.spin(f"R{i:09d}", stake=2, minutes=i) for i in range(1, 6)]  # usual stake 2, all lost
    events += [f.deposit("T00000001", amount=200, minutes=8), f.spin("R000000099", stake=10, minutes=10)]
    _, out = run(f, events)
    assert "LOSS_CHASING" in rules(out)


def test_night_marathon_session(f):
    # 22:30 UTC = 00:30 local time, play every 10 minutes for 100 minutes
    spins = [f.spin(f"R{i:09d}", minutes=12 * 60 + 30 + i * 10) for i in range(11)]
    _, out = run(f, [f.register(daily=10_000), *spins])
    assert "NIGHT_MARATHON_SESSION" in rules(out)


def test_stale_odds_alert(f):
    odds = f.event("odds_changed", 0, match_id="M00001", selection="home", odds=2.0)
    _, out = run(f, [f.register(), odds, f.bet(odds=2.5)])
    assert "STALE_ODDS_ACCEPTED" in rules(out)


def test_alert_ids_are_deterministic(f):
    def ids():
        g = type(f)()
        spins = [g.spin(f"R{i:09d}", stake=40, minutes=i) for i in range(4)]
        return [a["alert_id"] for a in run(g, [g.register(), *spins])[1].alerts]

    assert ids() == ids()
