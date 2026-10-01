"""SQLite store for the analyst queue.

SQLite is enough for one service instance and keeps the demo dependency-free; the store sits
behind this small class so a Postgres version would not touch the API or the consumer.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATUSES = ("OPEN", "CONFIRMED", "DISMISSED")

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    alert_id              TEXT PRIMARY KEY,
    rule                  TEXT NOT NULL,
    account_id            INTEGER NOT NULL,
    evidence_keys         TEXT NOT NULL,
    first_evidence_ts     TEXT,
    last_source_commit_ts TEXT,
    alert_created_at      TEXT,
    latency_seconds       REAL,
    status                TEXT NOT NULL DEFAULT 'OPEN',
    note                  TEXT,
    decided_at            TEXT,
    received_at           TEXT NOT NULL
)"""


class AlertStore:
    def __init__(self, path: Path | str = ":memory:"):
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()  # the Kafka consumer thread and API requests share it
        with self._lock:
            self._conn.execute(SCHEMA)

    def upsert(self, alert: dict[str, Any]) -> bool:
        """Insert a new alert, or refresh its evidence. Analyst fields are never overwritten.

        Returns True when the alert is new.
        """
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM alerts WHERE alert_id = ?", (alert["alert_id"],)
            ).fetchone()
            self._conn.execute(
                """INSERT INTO alerts (alert_id, rule, account_id, evidence_keys,
                       first_evidence_ts, last_source_commit_ts, alert_created_at,
                       latency_seconds, received_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(alert_id) DO UPDATE SET evidence_keys = excluded.evidence_keys""",
                (
                    alert["alert_id"],
                    alert["rule"],
                    int(alert["account_id"]),
                    json.dumps(list(alert["evidence_keys"])),
                    alert.get("first_evidence_ts"),
                    alert.get("last_source_commit_ts"),
                    alert.get("alert_created_at"),
                    alert.get("latency_seconds"),
                    datetime.now(UTC).isoformat(),
                ),
            )
        return exists is None

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        out = dict(row)
        out["evidence_keys"] = json.loads(out["evidence_keys"])
        return out

    def get(self, alert_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM alerts WHERE alert_id = ?", (alert_id,))
            return self._row(row.fetchone())

    def list(
        self,
        status: str | None = None,
        rule: str | None = None,
        account_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where, args = [], []
        for col, val in (("status", status), ("rule", rule), ("account_id", account_id)):
            if val is not None:
                where.append(f"{col} = ?")
                args.append(val)
        sql = "SELECT * FROM alerts"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY alert_created_at DESC, alert_id LIMIT ? OFFSET ?"
        with self._lock:
            return [self._row(r) for r in self._conn.execute(sql, (*args, limit, offset))]

    def set_status(self, alert_id: str, status: str, note: str | None) -> dict[str, Any] | None:
        if status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}")
        decided = None if status == "OPEN" else datetime.now(UTC).isoformat()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE alerts SET status = ?, note = ?, decided_at = ? WHERE alert_id = ?",
                (status, note, decided, alert_id),
            )
        return self.get(alert_id) if cur.rowcount else None

    def stats(self) -> dict[str, Any]:
        with self._lock:
            by_status = dict(
                self._conn.execute("SELECT status, COUNT(*) FROM alerts GROUP BY status")
            )
            by_rule = dict(self._conn.execute("SELECT rule, COUNT(*) FROM alerts GROUP BY rule"))
        return {"total": sum(by_status.values()), "by_status": by_status, "by_rule": by_rule}
