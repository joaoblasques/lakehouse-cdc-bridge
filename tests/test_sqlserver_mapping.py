from datetime import datetime
from decimal import Decimal

import pytest

from banking_cdc.sources.sqlserver import CdcGapError, check_no_gap, rows_to_events

TS = datetime(2026, 10, 1, 9, 0, 0)


def _row(op, lsn, seq, **cols):
    return {
        "__$start_lsn": bytes.fromhex(lsn),
        "__$seqval": bytes.fromhex(seq),
        "__$operation": op,
        "__$update_mask": b"\x00",
        "__commit_ts": TS,
        **cols,
    }


def _map(rows):
    return rows_to_events(rows, "cards", "dbo.accounts", ["account_id"], "run-1")


def test_insert_maps_to_create_with_after_image_only():
    (ev,) = _map([_row(2, "0001", "0001", account_id=1, balance=Decimal("10.50"))])
    assert ev.op == "c"
    assert ev.before is None
    assert ev.after == {"account_id": 1, "balance": "10.50"}
    assert ev.key == {"account_id": 1}
    assert ev.source.position == "0x0001:0x0001"
    assert ev.source.commit_ts == TS.isoformat()


def test_update_pairs_before_and_after_images():
    (ev,) = _map(
        [
            _row(3, "0002", "0005", account_id=1, balance=Decimal("10.50")),
            _row(4, "0002", "0005", account_id=1, balance=Decimal("8.00")),
        ]
    )
    assert ev.op == "u"
    assert ev.before["balance"] == "10.50"
    assert ev.after["balance"] == "8.00"


def test_delete_keeps_before_image():
    (ev,) = _map([_row(1, "0003", "0001", account_id=9, balance=Decimal("0"))])
    assert ev.op == "d"
    assert ev.after is None
    assert ev.key == {"account_id": 9}


def test_cdc_metadata_columns_are_not_leaked_into_row_images():
    (ev,) = _map([_row(2, "0001", "0001", account_id=1)])
    assert not any(k.startswith("__") for k in ev.after)


def test_events_keep_log_order():
    evs = _map(
        [
            _row(2, "0001", "0001", account_id=1),
            _row(3, "0002", "0001", account_id=1),
            _row(4, "0002", "0001", account_id=1),
            _row(1, "0003", "0001", account_id=1),
        ]
    )
    assert [e.op for e in evs] == ["c", "u", "d"]


def test_gap_detected_when_retention_cleaned_past_watermark():
    with pytest.raises(CdcGapError):
        check_no_gap(last_lsn=bytes.fromhex("0005"), min_lsn=bytes.fromhex("0009"), table="t")
    check_no_gap(last_lsn=bytes.fromhex("0009"), min_lsn=bytes.fromhex("0009"), table="t")
