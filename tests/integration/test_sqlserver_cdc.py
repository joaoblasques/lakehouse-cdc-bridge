"""Needs `docker compose up -d sqlserver`. Run with: uv run pytest -m integration"""

import time
from datetime import UTC, datetime

import pytest

from banking_cdc.generator import BankSimulator
from banking_cdc.seed import apply_batch, apply_schema, connect, wait_for_capture
from banking_cdc.sources.sqlserver import SqlServerCdcAdapter, TableSpec

pytestmark = pytest.mark.integration

TABLES = [
    TableSpec("dbo", "customers", ["customer_id"]),
    TableSpec("dbo", "accounts", ["account_id"]),
    TableSpec("dbo", "card_transactions", ["tx_id"]),
]


def _reset():
    apply_schema()
    with connect() as conn:
        cur = conn.cursor()
        for t in ("card_transactions", "accounts", "customers"):
            cur.execute(f"DELETE FROM dbo.{t}")
        conn.commit()
        # The capture job reads the log asynchronously: let it catch up with the deletes above
        # before the test takes its baseline, or they leak into the counted events.
        wait_for_capture(conn, beat=int(time.time() * 1000) % 2_000_000_000)


def _capture_until(adapter, watermark, predicate, timeout=60):
    """The Agent capture job polls the log every few seconds; wait for it."""
    deadline = time.time() + timeout
    events = []
    while time.time() < deadline:
        result = adapter.capture(watermark, "it")
        events += result.events
        watermark = result.new_watermark
        if predicate(events):
            return events, watermark
        time.sleep(2)
    raise AssertionError(f"timed out, got {len(events)} events")


def test_end_to_end_capture_from_sql_server_log():
    _reset()
    adapter = SqlServerCdcAdapter(connect, "cards", TABLES)
    # Start from "now": skip whatever earlier test runs left in the change tables.
    _, watermark = _capture_until(adapter, None, lambda e: True)

    sim = BankSimulator(seed=3, start=datetime(2026, 10, 1, 9, tzinfo=UTC), n_customers=5)
    seed = sim.initial_load()
    r1, r2 = sim.next_round(), sim.next_round()
    with connect() as conn:
        for b in (seed, r1, r2):
            apply_batch(conn, b)
    expected = len(seed.sql_ops) + len(r1.sql_ops) + len(r2.sql_ops)

    events, watermark = _capture_until(adapter, watermark, lambda e: len(e) >= expected)
    assert len(events) == expected
    assert {e.op for e in events} == {"c", "u", "d"}
    update = next(e for e in events if e.op == "u" and e.source.table == "dbo.accounts")
    assert update.before["balance"] != update.after["balance"]

    # Nothing new: the watermark holds and no duplicates come back.
    assert adapter.capture(watermark, "it").events == []

    stats = adapter.source_stats()
    assert stats["dbo.customers"]["rows"] == 5
