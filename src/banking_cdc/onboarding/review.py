"""Check the model's draft against the parsed DDL and the existing config, then build the
proposal file a person reviews. Facts (columns, types, Silver schema) come from the DDL;
the model's answer only fills in judgment, and every claim it makes about a column is checked.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from banking_cdc.onboarding.ddl import TableDef, spark_type

TOPIC = re.compile(r"^banking\.cdc\.[a-z0-9_]+\.[a-z0-9_]+$")
SILVER = re.compile(r"^silver_[a-z0-9_]+$")


@dataclass
class Finding:
    level: str  # "error" blocks the proposal; "warning" is for the reviewer
    message: str


def _used(cfg: dict[str, Any], field: str) -> set[str]:
    return {t[field] for src in cfg["sources"].values() for t in src["tables"]}


def validate(
    draft: dict[str, Any], table: TableDef, source_type: str, cfg: dict[str, Any]
) -> list[Finding]:
    out: list[Finding] = []

    def error(msg: str) -> None:
        out.append(Finding("error", msg))

    names = {c.name for c in table.columns}
    if not table.primary_key:
        error("the DDL declares no primary key; CDC needs a stable key per row")
    for col in table.primary_key:
        if col not in names:
            error(f"declared primary key column {col} is not a column of the table")
    for col in draft["primary_key"]:
        if col not in names:
            error(f"primary_key column {col} does not exist in the DDL")
    if table.primary_key and draft["primary_key"] != table.primary_key:
        error(
            f"primary_key {draft['primary_key']} differs from the declared key {table.primary_key}"
        )

    ts = draft["ts_column"]
    if source_type == "db2":
        column = table.column(ts) if ts else None
        if not ts:
            error("db2 capture needs a ts_column (watermark)")
        elif column is None:
            error(f"ts_column {ts} does not exist in the DDL")
        elif spark_type(column.sql_type) != "TIMESTAMP":
            error(f"ts_column {ts} is {column.sql_type}; the watermark must be a TIMESTAMP")
        elif not column.is_row_change_timestamp:
            out.append(
                Finding(
                    "warning",
                    f"ts_column {ts} is set by the application, not by "
                    "DB2 (ROW CHANGE TIMESTAMP); updates that skip it are missed",
                )
            )
    elif ts:
        out.append(Finding("warning", f"ts_column is ignored for {source_type} sources"))

    if not TOPIC.match(draft["topic"]):
        error(f"topic {draft['topic']} breaks the convention banking.cdc.<domain>.<entity>")
    elif draft["topic"] in _used(cfg, "topic"):
        error(f"topic {draft['topic']} is already used")
    if not SILVER.match(draft["silver_table"]):
        error(f"silver_table {draft['silver_table']} breaks the convention silver_<entity>")
    elif draft["silver_table"] in _used(cfg, "silver_table"):
        error(f"silver_table {draft['silver_table']} is already used")

    for pii in draft["pii_columns"]:
        if pii["column"] not in names:
            error(f"PII column {pii['column']} does not exist in the DDL")
    for c in table.columns:
        if not c.is_row_change_timestamp and spark_type(c.sql_type) is None:
            error(f"column {c.name} has type {c.sql_type} with no safe Spark mapping")
    return out


def silver_schema(table: TableDef) -> str:
    """Spark DDL of the row image. The DB2 row-change timestamp is CDC metadata, not data."""
    return ", ".join(
        f"{c.name} {spark_type(c.sql_type)}" for c in table.columns if not c.is_row_change_timestamp
    )


def build_proposal(
    draft: dict[str, Any],
    findings: list[Finding],
    table: TableDef,
    ddl: str,
    source: str,
    source_type: str,
    provenance: dict[str, str],
) -> dict[str, Any]:
    entry: dict[str, Any] = {"schema": table.schema} if table.schema else {}
    entry |= {"table": table.table, "primary_key": draft["primary_key"]}
    if source_type == "db2":
        entry["ts_column"] = draft["ts_column"]
    entry |= {"topic": draft["topic"], "silver_table": draft["silver_table"]}
    entry["silver_schema"] = silver_schema(table)

    blocked = any(f.level == "error" for f in findings) or any(
        n["severity"] == "blocker" for n in draft["review_notes"]
    )
    return {
        "status": "needs_changes" if blocked else "ready_for_review",
        "source": source,
        "entry": entry,
        "review": {
            "summary": draft["table_summary"],
            "validator": [{"level": f.level, "message": f.message} for f in findings],
            "model_notes": draft["review_notes"],
            "pii_columns": draft["pii_columns"],
        },
        "provenance": provenance
        | {
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "ddl_sha256": hashlib.sha256(ddl.encode()).hexdigest(),
        },
    }
