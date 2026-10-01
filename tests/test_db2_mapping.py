from datetime import datetime, timedelta
from decimal import Decimal

from banking_cdc.sources.db2 import diff_snapshot, row_hash, watermark_rows_to_events

T0 = datetime(2026, 10, 1, 9, 0, 0, 123456)
COLS = dict(customer_id=1, product="PERSONAL", principal=Decimal("5000.00"), status="ACTIVE")


def _row(loan_id, ts, **over):
    return {"LOAN_ID": loan_id, "ROW_CHANGED": ts, **{k.upper(): v for k, v in COLS.items()}} | {
        k.upper(): v for k, v in over.items()
    }


def _map(rows, known, last=None):
    return watermark_rows_to_events(
        rows, known, "corebank", "CORE.LOANS", ["LOAN_ID"], "ROW_CHANGED", last, "run"
    )


def test_new_key_is_insert_and_known_key_is_update():
    known = {}
    evs, known, wm = _map([_row(1, T0)], known)
    assert [e.op for e in evs] == ["c"]
    evs, known, wm = _map([_row(1, T0 + timedelta(seconds=5), status="CLOSED")], known, wm)
    assert [e.op for e in evs] == ["u"]
    assert evs[0].after["STATUS"] == "CLOSED"
    assert evs[0].before is None  # query-based CDC has no before image


def test_watermark_columns_are_not_part_of_the_row_image():
    evs, _, _ = _map([_row(1, T0)], {})
    assert "ROW_CHANGED" not in evs[0].after
    assert evs[0].source.commit_ts == T0.isoformat()


def test_rows_at_the_watermark_timestamp_are_not_emitted_twice():
    evs, known, wm = _map([_row(1, T0), _row(2, T0)], {})
    assert wm == {"ts": T0.isoformat(), "keys_at_ts": ["1", "2"]}
    # The next query uses >= ts, so rows 1 and 2 come back; only row 3 is new.
    evs, known, wm = _map([_row(1, T0), _row(2, T0), _row(3, T0)], known, wm)
    assert [e.key["LOAN_ID"] for e in evs] == [3]
    assert wm["keys_at_ts"] == ["1", "2", "3"]


def test_same_row_version_gets_the_same_event_id():
    a, _, _ = _map([_row(1, T0)], {})
    b, _, _ = _map([_row(1, T0)], {})
    assert a[0].event_id == b[0].event_id


def test_snapshot_finds_deletes_missed_updates_and_missed_inserts():
    r1, r2, r3 = _row(1, T0), _row(2, T0), _row(3, T0)
    known = {"1": row_hash(r1, "ROW_CHANGED"), "2": row_hash(r2, "ROW_CHANGED")}
    source_now = [r1, _row(2, T0, status="CLOSED"), r3]  # 2 changed unseen, 3 new; nothing deleted
    evs, known2 = diff_snapshot(
        source_now,
        known,
        "corebank",
        "CORE.LOANS",
        ["LOAN_ID"],
        "ROW_CHANGED",
        T0 + timedelta(minutes=1),
        "run",
    )
    assert sorted((e.key["LOAN_ID"], e.op) for e in evs) == [(2, "u"), (3, "c")]
    evs, known3 = diff_snapshot(
        [r1],
        known2,
        "corebank",
        "CORE.LOANS",
        ["LOAN_ID"],
        "ROW_CHANGED",
        T0 + timedelta(minutes=2),
        "run",
    )
    assert sorted((e.key["LOAN_ID"], e.op) for e in evs) == [(2, "d"), (3, "d")]
    assert set(known3) == {"1"}


def test_snapshot_delete_event_is_deterministic_for_the_same_lost_version():
    known = {"9": "abc"}
    a, _ = diff_snapshot([], known, "corebank", "CORE.LOANS", ["LOAN_ID"], "ROW_CHANGED", T0, "r1")
    b, _ = diff_snapshot(
        [],
        known,
        "corebank",
        "CORE.LOANS",
        ["LOAN_ID"],
        "ROW_CHANGED",
        T0 + timedelta(hours=1),
        "r2",
    )
    assert a[0].op == "d" and a[0].key == {"LOAN_ID": 9}
    assert a[0].event_id == b[0].event_id  # a replayed scan does not create a second delete


def test_sequence_orders_snapshot_events_after_the_rows_they_follow():
    evs, known, _ = _map([_row(1, T0)], {})
    dels, _ = diff_snapshot(
        [],
        known,
        "corebank",
        "CORE.LOANS",
        ["LOAN_ID"],
        "ROW_CHANGED",
        T0 + timedelta(seconds=1),
        "run",
    )
    assert dels[0].source.order_key() > evs[0].source.order_key()
