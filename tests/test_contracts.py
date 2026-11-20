import json

from pelivahti.contracts import event_types, validate


def test_all_contracts_load():
    assert len(event_types()) == 10


def test_valid_event_passes(f):
    assert validate(json.dumps(f.bet())).ok


def test_negative_stake_is_rejected(f):
    ev = f.bet(stake=-5)
    res = validate(json.dumps(ev))
    assert not res.ok and "stake_eur" in res.error


def test_unknown_field_is_rejected(f):
    ev = f.bet()
    ev["promo_code"] = "FREE"
    assert "Additional properties" in validate(json.dumps(ev)).error


def test_wrong_schema_version_is_rejected(f):
    ev = f.deposit("T00000001")
    ev["schema_version"] = 2
    assert not validate(json.dumps(ev)).ok


def test_more_than_two_decimals_is_rejected(f):
    res = validate(json.dumps(f.deposit("T00000001", amount=10.005)))
    assert not res.ok and "decimals" in res.error


def test_truncated_and_unknown_messages_do_not_raise():
    assert validate('{"event_type": "bet_pl').error.startswith("invalid_json")
    assert validate('{"event_type": "jackpot"}').error.startswith("unknown_event_type")
    assert validate("[1, 2]").error.startswith("invalid_json")
