"""Query-based CDC for DB2, for when the transaction log is out of reach.

Reading a mainframe DB2 log needs IBM replication tooling that a downstream team usually can't
install. This adapter uses what any reader can: a `ROW CHANGE TIMESTAMP` column, which DB2 sets
on every insert and update.

Two passes:
  * Watermark pass (every run): rows whose timestamp is at or after the watermark. Catches
    inserts and updates cheaply, but can't see hard deletes.
  * Snapshot pass (every N runs): read every key and compare a fingerprint of each row with the
    last known state. Finds deletes, and anything the watermark pass missed.

State lives in the watermark JSON (committed by the capture step after Kafka confirms):
  {"tables": {"CORE.LOANS": {"ts": ..., "keys_at_ts": [...], "known": {key: fingerprint}}},
   "runs_since_snapshot": n}
Keeping `known` in the watermark is fine for tables up to ~10^5 rows; beyond that it belongs in
its own Delta table (see ADR-008 on the website).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from banking_cdc.envelope import ChangeEvent, SourceInfo, make_event
from banking_cdc.sources.base import CaptureResult, jsonable

SEQ_FMT = "%Y-%m-%dT%H:%M:%S.%f"  # fixed width, so sequences sort as strings


def _key_str(row: dict[str, Any], primary_key: list[str]) -> str:
    return "|".join(json.dumps(row[k], default=str) for k in primary_key)


def _key_dict(key_str: str, primary_key: list[str]) -> dict[str, Any]:
    return {k: json.loads(v) for k, v in zip(primary_key, key_str.split("|"), strict=True)}


def row_hash(row: dict[str, Any], ts_column: str) -> str:
    """Fingerprint of the row's content (the change timestamp itself excluded)."""
    content = {k: v for k, v in jsonable(row).items() if k != ts_column}
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _event(
    op, row, key_str, primary_key, database, table, ts_column, position, seq, commit, trace_id
) -> ChangeEvent:
    image = None if row is None else {k: v for k, v in jsonable(row).items() if k != ts_column}
    source = SourceInfo(
        system="db2",
        database=database,
        table=table,
        position=position,
        commit_ts=commit,
        sequence=seq,
    )
    before = None
    after = image if op != "d" else None
    return make_event(op, source, _key_dict(key_str, primary_key), before, after, trace_id)


def watermark_rows_to_events(
    rows: Iterable[dict[str, Any]],
    known: dict[str, str],
    database: str,
    table: str,
    primary_key: list[str],
    ts_column: str,
    last: dict[str, Any] | None,
    trace_id: str,
) -> tuple[list[ChangeEvent], dict[str, str], dict[str, Any] | None]:
    """Rows from `WHERE ts >= last.ts ORDER BY ts`. Returns events, updated known, new watermark.

    The query is `>=` because several rows can share one timestamp and some of them may not have
    been visible last time. `keys_at_ts` remembers which ones were already emitted.
    """
    known = dict(known)
    prev_ts = last["ts"] if last else None
    seen_at_prev = set(last["keys_at_ts"]) if last else set()
    events: list[ChangeEvent] = []
    max_ts, keys_at_max = prev_ts, set(seen_at_prev)
    for row in rows:
        ts: datetime = row[ts_column]
        ts_iso = ts.isoformat()
        key = _key_str(row, primary_key)
        if ts_iso == prev_ts and key in seen_at_prev:
            continue
        op = "u" if key in known else "c"
        known[key] = row_hash(row, ts_column)
        events.append(
            _event(
                op,
                row,
                key,
                primary_key,
                database,
                table,
                ts_column,
                f"{ts_column}={ts_iso}",
                ts.strftime(SEQ_FMT),
                ts_iso,
                trace_id,
            )
        )
        if max_ts is None or ts_iso > max_ts:
            max_ts, keys_at_max = ts_iso, {key}
        elif ts_iso == max_ts:
            keys_at_max.add(key)
    watermark = None if max_ts is None else {"ts": max_ts, "keys_at_ts": sorted(keys_at_max)}
    return events, known, watermark


def diff_snapshot(
    rows: Iterable[dict[str, Any]],
    known: dict[str, str],
    database: str,
    table: str,
    primary_key: list[str],
    ts_column: str,
    scanned_at: datetime,
    trace_id: str,
) -> tuple[list[ChangeEvent], dict[str, str]]:
    """Compare a full read of the table with the last known state."""
    current: dict[str, str] = {}
    events: list[ChangeEvent] = []
    for row in rows:
        key = _key_str(row, primary_key)
        fp = row_hash(row, ts_column)
        current[key] = fp
        if known.get(key) == fp:
            continue
        ts: datetime = row[ts_column]
        # Same position as the watermark pass would have used: if both see this version,
        # the event_id matches and Bronze keeps one.
        events.append(
            _event(
                "u" if key in known else "c",
                row,
                key,
                primary_key,
                database,
                table,
                ts_column,
                f"{ts_column}={ts.isoformat()}",
                ts.strftime(SEQ_FMT),
                ts.isoformat(),
                trace_id,
            )
        )
    scan_seq = scanned_at.strftime(SEQ_FMT) + "|snapshot"
    for key in sorted(set(known) - set(current)):
        # Identify the delete by the last version we saw, not by scan time: a replayed scan
        # produces the same event_id instead of a second delete.
        events.append(
            _event(
                "d",
                None,
                key,
                primary_key,
                database,
                table,
                ts_column,
                f"deleted-after:{known[key]}",
                scan_seq,
                scanned_at.isoformat(),
                trace_id,
            )
        )
    return events, current


@dataclass(frozen=True)
class Db2Table:
    schema: str
    table: str
    primary_key: list[str]
    ts_column: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


class Db2QueryCdcAdapter:
    name = "db2_core"

    def __init__(
        self,
        connect: Callable[[], Any],
        database: str,
        tables: list[Db2Table],
        snapshot_every: int = 3,
        safety_lag: timedelta = timedelta(seconds=2),
    ):
        self.connect = connect  # DB-API connection (ibm_db_dbi), qmark parameters
        self.database = database
        self.tables = tables
        self.snapshot_every = snapshot_every
        self.safety_lag = safety_lag

    @staticmethod
    def _dicts(cur) -> list[dict[str, Any]]:
        cols = [d[0].upper() for d in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    def capture(self, watermark: dict[str, Any] | None, trace_id: str) -> CaptureResult:
        state = watermark or {}
        tables_state = dict(state.get("tables", {}))
        runs = state.get("runs_since_snapshot", self.snapshot_every)  # first run: snapshot
        do_snapshot = runs + 1 >= self.snapshot_every
        events: list[ChangeEvent] = []
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
            db_now: datetime = cur.fetchone()[0]
            # Rows changed in the last moment may belong to transactions that haven't
            # committed yet; leave them for the next run.
            upper = db_now - self.safety_lag
            for spec in self.tables:
                st = tables_state.get(spec.qualified, {})
                cols = f"{spec.ts_column}, t.*"
                if "ts" in st:
                    cur.execute(
                        f"SELECT {cols} FROM {spec.qualified} t WHERE {spec.ts_column} >= ? "
                        f"AND {spec.ts_column} < ? ORDER BY {spec.ts_column}",
                        (datetime.fromisoformat(st["ts"]), upper),
                    )
                else:
                    cur.execute(
                        f"SELECT {cols} FROM {spec.qualified} t WHERE {spec.ts_column} < ? "
                        f"ORDER BY {spec.ts_column}",
                        (upper,),
                    )
                wm_events, known, wm = watermark_rows_to_events(
                    self._dicts(cur),
                    st.get("known", {}),
                    self.database,
                    spec.qualified,
                    spec.primary_key,
                    spec.ts_column,
                    st if "ts" in st else None,
                    trace_id,
                )
                events += wm_events
                if do_snapshot:
                    cur.execute(
                        f"SELECT {cols} FROM {spec.qualified} t WHERE {spec.ts_column} < ?",
                        (upper,),
                    )
                    snap_events, known = diff_snapshot(
                        self._dicts(cur),
                        known,
                        self.database,
                        spec.qualified,
                        spec.primary_key,
                        spec.ts_column,
                        db_now,
                        trace_id,
                    )
                    events += snap_events
                tables_state[spec.qualified] = (
                    wm or {k: st[k] for k in ("ts", "keys_at_ts") if k in st}
                ) | {"known": known}
        finally:
            conn.close()
        return CaptureResult(
            events,
            {"tables": tables_state, "runs_since_snapshot": 0 if do_snapshot else runs + 1},
        )

    def source_stats(self) -> dict[str, dict[str, Any]]:
        stats = {}
        conn = self.connect()
        try:
            cur = conn.cursor()
            for spec in self.tables:
                cur.execute(f"SELECT COUNT(*) FROM {spec.qualified}")
                stats[spec.qualified] = {"rows": int(cur.fetchone()[0])}
        finally:
            conn.close()
        return stats
