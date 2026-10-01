"""Apply simulator batches to the cards database and drop partner files on the share."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from banking_cdc.generator import Batch

PRIMARY_KEYS = {"customers": "customer_id", "accounts": "account_id", "card_transactions": "tx_id"}
SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "sqlserver" / "001_cards_schema.sql"


def connect(database: str = "cards") -> Any:
    import pymssql

    return pymssql.connect(
        server=os.environ.get("MSSQL_HOST", "localhost"),
        port=os.environ.get("MSSQL_PORT", "1433"),
        user=os.environ.get("MSSQL_USER", "sa"),
        password=os.environ.get("MSSQL_SA_PASSWORD", "LocalDev!Passw0rd"),
        database=database,
    )


def apply_schema(sql_path: Path = SCHEMA_SQL) -> None:
    conn = connect("master")
    conn.autocommit(True)
    cur = conn.cursor()
    for statement in sql_path.read_text().split("\nGO\n"):
        if statement.strip():
            cur.execute(statement)
    conn.close()


def _sql(table: str, op: str, row: dict[str, Any]) -> tuple[str, tuple]:
    pk = PRIMARY_KEYS[table]
    if op == "insert":
        cols = list(row)
        return (
            f"INSERT INTO dbo.{table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
            tuple(row[c] for c in cols),
        )
    if op == "update":
        cols = [c for c in row if c != pk]
        sets = ", ".join(f"{c} = %s" for c in cols)
        return f"UPDATE dbo.{table} SET {sets} WHERE {pk} = %s", (*[row[c] for c in cols], row[pk])
    if op == "delete":
        return f"DELETE FROM dbo.{table} WHERE {pk} = %s", (row[pk],)
    raise ValueError(op)


def apply_batch(conn: Any, batch: Batch, commit_every: int = 50) -> None:
    """Commit in small chunks so changes land on many LSNs, like real OLTP traffic."""
    cur = conn.cursor()
    for i, (table, op, row) in enumerate(batch.sql_ops, start=1):
        cur.execute(*_sql(table, op, row))
        if i % commit_every == 0:
            conn.commit()
    conn.commit()


def write_files(share_dir: Path, batch: Batch) -> None:
    share_dir.mkdir(parents=True, exist_ok=True)
    for name, text in batch.files:
        (share_dir / name).write_text(text)


def wait_for_capture(conn: Any, beat: int, timeout: float = 120.0) -> float:
    """Write a heartbeat and block until the CDC capture job has read it. Returns lag seconds."""
    import time

    cur = conn.cursor()
    cur.execute(
        "MERGE dbo.cdc_heartbeat AS t USING (SELECT 1 AS id) AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET beat = %s, beat_at = SYSUTCDATETIME() "
        "WHEN NOT MATCHED THEN INSERT (id, beat, beat_at) VALUES (1, %s, SYSUTCDATETIME());",
        (beat, beat),
    )
    conn.commit()
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        cur.execute("SELECT COUNT(*) FROM cdc.dbo_cdc_heartbeat_CT WHERE beat = %s", (beat,))
        if cur.fetchone()[0]:
            return time.monotonic() - started
        time.sleep(0.5)
    raise TimeoutError(f"CDC capture job did not pick up heartbeat {beat} in {timeout}s")


# -- DB2 (core banking) -------------------------------------------------------------------------

DB2_SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "db2" / "001_core_schema.sql"
DB2_TABLES = {"loans": ("CORE.LOANS", "LOAN_ID")}


def db2_connect() -> Any:
    import ibm_db_dbi

    return ibm_db_dbi.connect(
        f"DATABASE={os.environ.get('DB2_DATABASE', 'COREBANK')};"
        f"HOSTNAME={os.environ.get('DB2_HOST', 'localhost')};"
        f"PORT={os.environ.get('DB2_PORT', '50000')};PROTOCOL=TCPIP;"
        f"UID={os.environ.get('DB2_USER', 'db2inst1')};"
        f"PWD={os.environ.get('DB2_PASSWORD', 'LocalDev!Passw0rd')};",
        "",
        "",
    )


def apply_db2_schema(sql_path: Path = DB2_SCHEMA_SQL) -> None:
    conn = db2_connect()
    cur = conn.cursor()
    for statement in sql_path.read_text().split("\n;\n"):
        body = "\n".join(ln for ln in statement.splitlines() if not ln.strip().startswith("--"))
        if not body.strip():
            continue
        try:
            cur.execute(body)
        except Exception as exc:  # re-running the script: object (42710) or index (01550) exists
            if not any(code in str(exc) for code in ("SQLSTATE=42710", "SQLSTATE=01550")):
                raise
    conn.commit()
    conn.close()


def _db2_sql(table: str, op: str, row: dict[str, Any]) -> tuple[str, tuple]:
    name, pk = DB2_TABLES[table]
    if op == "insert":
        cols = list(row)
        marks = ", ".join(["?"] * len(cols))
        return f"INSERT INTO {name} ({', '.join(cols)}) VALUES ({marks})", tuple(row.values())
    if op == "update":
        cols = [c for c in row if c != pk]
        sets = ", ".join(f"{c} = ?" for c in cols)
        return f"UPDATE {name} SET {sets} WHERE {pk} = ?", (*[row[c] for c in cols], row[pk])
    if op == "delete":
        return f"DELETE FROM {name} WHERE {pk} = ?", (row[pk],)
    raise ValueError(op)


def apply_db2_batch(conn: Any, batch: Batch) -> None:
    cur = conn.cursor()
    for table, op, row in batch.db2_ops:
        cur.execute(*_db2_sql(table, op, row))
    conn.commit()
