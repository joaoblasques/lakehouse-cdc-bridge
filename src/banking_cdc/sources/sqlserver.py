"""Log-based CDC from SQL Server's native change tables.

SQL Server Agent's capture job reads the transaction log into `cdc.<capture_instance>_CT`
tables. This adapter reads those tables between the last processed LSN (the watermark, stored in
Delta) and the current max LSN, and maps every change to the standard envelope.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from banking_cdc.envelope import ChangeEvent, SourceInfo, make_event
from banking_cdc.sources.base import CaptureResult, jsonable

# __$operation codes in cdc.fn_cdc_get_all_changes_*
DELETE, INSERT, UPDATE_BEFORE, UPDATE_AFTER = 1, 2, 3, 4


class CdcGapError(RuntimeError):
    """Retention cleanup removed changes we never read. The table needs a re-snapshot."""


@dataclass(frozen=True)
class TableSpec:
    schema: str
    table: str
    primary_key: list[str]

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def capture_instance(self) -> str:
        return f"{self.schema}_{self.table}"


def _hex(value: bytes) -> str:
    return "0x" + value.hex()


def check_no_gap(last_lsn: bytes, min_lsn: bytes, table: str) -> None:
    if last_lsn < min_lsn:
        raise CdcGapError(
            f"{table}: last processed LSN {_hex(last_lsn)} is older than the oldest change "
            f"still retained ({_hex(min_lsn)}). Changes were lost to CDC cleanup; re-snapshot."
        )


def rows_to_events(
    rows: Iterable[dict[str, Any]],
    database: str,
    table: str,
    primary_key: list[str],
    trace_id: str,
) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    pending_before: dict[tuple[bytes, bytes], dict[str, Any]] = {}
    for row in rows:
        lsn, seq, op = row["__$start_lsn"], row["__$seqval"], row["__$operation"]
        image = jsonable({k: v for k, v in row.items() if not k.startswith("__")})
        if op == UPDATE_BEFORE:
            pending_before[(lsn, seq)] = image
            continue
        commit_ts = row.get("__commit_ts")
        source = SourceInfo(
            system="sqlserver",
            database=database,
            table=table,
            position=f"{_hex(lsn)}:{_hex(seq)}",
            commit_ts=commit_ts.isoformat() if commit_ts else None,
        )
        before = after = None
        if op == INSERT:
            kind, after = "c", image
        elif op == UPDATE_AFTER:
            kind, before, after = "u", pending_before.pop((lsn, seq), None), image
        elif op == DELETE:
            kind, before = "d", image
        else:
            raise ValueError(f"unexpected __$operation {op}")
        key = {k: image[k] for k in primary_key}
        events.append(make_event(kind, source, key, before, after, trace_id))
    return events


class SqlServerCdcAdapter:
    name = "sqlserver_cards"

    def __init__(self, connect: Callable[[], Any], database: str, tables: list[TableSpec]):
        self.connect = connect  # returns a DB-API connection (pymssql)
        self.database = database
        self.tables = tables

    def capture(self, watermark: dict[str, Any] | None, trace_id: str) -> CaptureResult:
        last = dict((watermark or {}).get("lsn", {}))  # table -> hex LSN
        events: list[ChangeEvent] = []
        with self.connect() as conn:
            cur = conn.cursor(as_dict=True)
            cur.execute("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn")
            max_lsn = cur.fetchone()["max_lsn"]
            if max_lsn is None:  # capture job has not run yet
                return CaptureResult([], {"lsn": last})
            for spec in self.tables:
                ci = spec.capture_instance
                cur.execute(f"SELECT sys.fn_cdc_get_min_lsn('{ci}') AS min_lsn")
                min_lsn = cur.fetchone()["min_lsn"]
                if spec.qualified in last:
                    prev = bytes.fromhex(last[spec.qualified][2:])
                    check_no_gap(prev, min_lsn, spec.qualified)
                    cur.execute("SELECT sys.fn_cdc_increment_lsn(%s) AS lsn", (prev,))
                    from_lsn = cur.fetchone()["lsn"]
                else:
                    from_lsn = min_lsn
                if from_lsn > max_lsn:
                    continue
                cur.execute(
                    "SELECT sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS __commit_ts, * "
                    f"FROM cdc.fn_cdc_get_all_changes_{ci}(%s, %s, N'all update old') "
                    "ORDER BY __$start_lsn, __$seqval, __$operation",
                    (from_lsn, max_lsn),
                )
                events += rows_to_events(
                    cur.fetchall(), self.database, spec.qualified, spec.primary_key, trace_id
                )
                last[spec.qualified] = _hex(max_lsn)
        return CaptureResult(events, {"lsn": last})

    def source_stats(self) -> dict[str, dict[str, Any]]:
        stats = {}
        with self.connect() as conn:
            cur = conn.cursor(as_dict=True)
            for spec in self.tables:
                cur.execute(f"SELECT COUNT(*) AS n FROM {spec.qualified}")
                stats[spec.qualified] = {"rows": cur.fetchone()["n"]}
            cur.execute("SELECT SUM(balance) AS s FROM dbo.accounts")
            if "dbo.accounts" in stats:
                stats["dbo.accounts"]["balance_sum"] = str(cur.fetchone()["s"])
        return stats
