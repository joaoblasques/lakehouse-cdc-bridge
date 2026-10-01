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


def test_events_from_both_adapters_match_the_published_json_schema(tmp_path):
    from pathlib import Path

    import jsonschema

    from banking_cdc.generator import _pt_iban
    from banking_cdc.sources.fileshare import FileShareAdapter, LocalDirectoryShare
    from banking_cdc.sources.sqlserver import rows_to_events

    schema = json.loads(
        (Path(__file__).parents[1] / "schemas" / "change_event.schema.json").read_text()
    )
    db_rows = [
        {"__$start_lsn": b"\x01", "__$seqval": b"\x01", "__$operation": 2, "account_id": 1},
        {"__$start_lsn": b"\x02", "__$seqval": b"\x01", "__$operation": 1, "account_id": 1},
    ]
    events = rows_to_events(db_rows, "cards", "dbo.accounts", ["account_id"], "r")
    a, b = _pt_iban("0033000000000000001"), _pt_iban("0035000000000000002")
    (tmp_path / "f.csv").write_text(
        "transfer_id,value_date,booking_ts,debtor_iban,creditor_iban,amount,currency,reference\n"
        f"T1,2026-10-01,2026-10-01T09:00:00+00:00,{a},{b},10.00,EUR,X\n"
    )
    events += FileShareAdapter(LocalDirectoryShare(tmp_path), "share").capture(None, "r").events
    assert len(events) == 3
    for ev in events:
        jsonschema.validate(json.loads(ev.to_json()), schema)
