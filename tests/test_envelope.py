import json

from banking_cdc.envelope import ChangeEvent, SourceInfo, make_event


def _source(position="0x01:0x02"):
    return SourceInfo(
        system="sqlserver",
        database="cards",
        table="dbo.accounts",
        position=position,
        commit_ts="2026-10-01T10:00:00+00:00",
    )


def test_event_id_is_deterministic_for_same_change():
    a = make_event("u", _source(), {"account_id": 1}, {"balance": 1}, {"balance": 2}, "run-1")
    b = make_event("u", _source(), {"account_id": 1}, {"balance": 1}, {"balance": 2}, "run-2")
    assert a.event_id == b.event_id
    assert a.trace_id != b.trace_id


def test_event_id_changes_with_source_position():
    a = make_event("u", _source("0x01:0x02"), {"account_id": 1}, None, {"b": 1}, "r")
    b = make_event("u", _source("0x01:0x03"), {"account_id": 1}, None, {"b": 1}, "r")
    assert a.event_id != b.event_id


def test_kafka_key_groups_changes_by_row():
    ev = make_event("c", _source(), {"account_id": 7}, None, {"account_id": 7}, "r")
    assert ev.kafka_key == "dbo.accounts:account_id=7"


def test_rejects_unknown_op():
    import pytest

    with pytest.raises(ValueError):
        make_event("x", _source(), {"id": 1}, None, None, "r")


def test_round_trips_through_json():
    ev = make_event("d", _source(), {"account_id": 1}, {"balance": 5}, None, "r")
    restored = ChangeEvent.from_json(ev.to_json())
    assert restored == ev
    assert json.loads(ev.to_json())["source"]["system"] == "sqlserver"


def test_payload_hash_covers_row_images():
    a = make_event("u", _source(), {"id": 1}, None, {"amount": "10.00"}, "r")
    b = make_event("u", _source(), {"id": 1}, None, {"amount": "10.01"}, "r")
    assert a.payload_hash != b.payload_hash
