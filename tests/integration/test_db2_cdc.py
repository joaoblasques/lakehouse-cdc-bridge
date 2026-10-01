"""Needs `docker compose --profile db2 up -d db2`. Run with: uv run pytest -m db2"""

import time
from datetime import UTC, datetime, timedelta

import pytest

from banking_cdc.generator import BankSimulator
from banking_cdc.seed import apply_db2_batch, apply_db2_schema, db2_connect
from banking_cdc.sources.base import jsonable
from banking_cdc.sources.db2 import Db2QueryCdcAdapter, Db2Table

pytestmark = pytest.mark.db2

LAG = timedelta(seconds=1)
TABLE = Db2Table("CORE", "LOANS", ["LOAN_ID"], "ROW_CHANGED")


def _adapter(snapshot_every):
    return Db2QueryCdcAdapter(db2_connect, "COREBANK", [TABLE], snapshot_every, LAG)


def _source_rows() -> dict[int, dict]:
    conn = db2_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM CORE.LOANS")  # ROW_CHANGED is hidden from SELECT *
    cols = [d[0] for d in cur.description]
    rows = {r[0]: jsonable(dict(zip(cols, r, strict=True))) for r in cur.fetchall()}
    conn.close()
    return rows


def _replay(events, state):
    for e in sorted(events, key=lambda e: e.source.order_key()):
        if e.op == "d":
            state.pop(e.key["LOAN_ID"], None)
        else:
            state[e.key["LOAN_ID"]] = e.after
    return state


def test_replayed_events_rebuild_the_db2_table_including_hard_deletes():
    apply_db2_schema()
    conn = db2_connect()
    conn.cursor().execute("DELETE FROM CORE.LOANS")
    conn.commit()
    time.sleep(LAG.total_seconds() + 0.5)

    adapter = _adapter(snapshot_every=1000)  # watermark pass only, until we ask for a snapshot
    baseline = adapter.capture(None, "it")
    assert baseline.events == []

    sim = BankSimulator(seed=11, start=datetime(2026, 10, 1, 9, tzinfo=UTC), n_customers=30)
    # Capture after every batch, like the scheduled job: a loan seen as APPLIED and then
    # cancelled (hard-deleted) is exactly what the watermark pass can't notice.
    state, watermark, seen_deleted = {}, baseline.new_watermark, 0
    for b in [sim.initial_load(), sim.next_round(), sim.next_round(), sim.next_round()]:
        seen_deleted += sum(op == "delete" and row["LOAN_ID"] in state for _, op, row in b.db2_ops)
        apply_db2_batch(conn, b)
        time.sleep(LAG.total_seconds() + 0.5)
        run = adapter.capture(watermark, "it")
        assert {e.op for e in run.events} <= {"c", "u"}  # watermark pass: no deletes
        state, watermark = _replay(run.events, state), run.new_watermark
    conn.close()
    assert seen_deleted > 0
    assert len(state) == len(_source_rows()) + seen_deleted  # stale rows the pass can't see

    # Snapshot pass: the deletes are found, and the replayed state equals the source.
    snap = Db2QueryCdcAdapter(db2_connect, "COREBANK", [TABLE], 1, LAG).capture(watermark, "it")
    assert sum(e.op == "d" for e in snap.events) == seen_deleted
    assert _replay(snap.events, state) == _source_rows()

    # Nothing changed since: no new events, and no repeated deletes.
    again = Db2QueryCdcAdapter(db2_connect, "COREBANK", [TABLE], 1, LAG).capture(
        snap.new_watermark, "it"
    )
    assert again.events == []
