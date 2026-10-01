"""Just enough CREATE TABLE parsing for onboarding: names, types, nullability, keys.

Covers the DB2 and SQL Server dialects the sources use (quoted or bracketed names, inline or
table-level PRIMARY KEY, DB2 ROW CHANGE TIMESTAMP). It is not a general SQL parser; anything
it cannot read is reported, never guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_CREATE = re.compile(r"CREATE\s+TABLE\s+([^\s(]+)\s*\(", re.IGNORECASE)
_TYPE = re.compile(r"([A-Za-z]+(?:\s+PRECISION)?)\s*(\([^)]*\))?")
_KEY = re.compile(r"PRIMARY\s+KEY\s*\(([^)]*)\)", re.IGNORECASE)


@dataclass
class Column:
    name: str
    sql_type: str
    nullable: bool = True
    is_row_change_timestamp: bool = False


@dataclass
class TableDef:
    schema: str | None
    table: str
    columns: list[Column]
    primary_key: list[str] = field(default_factory=list)

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)

    @property
    def row_change_column(self) -> str | None:
        return next((c.name for c in self.columns if c.is_row_change_timestamp), None)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}" if self.schema else self.table


def _unquote(name: str) -> str:
    return name.strip().strip('"[]`')


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def _split_top_level(body: str) -> list[str]:
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [" ".join(p.split()) for p in parts if p.strip()]


def _key_columns(text: str) -> list[str]:
    return [_unquote(c) for c in text.split(",")]


def parse_ddl(sql: str) -> TableDef:
    sql = _strip_comments(sql)
    m = _CREATE.search(sql)
    if not m:
        raise ValueError("no CREATE TABLE statement found")
    name_parts = [_unquote(p) for p in m.group(1).split(".")]
    schema, table = (name_parts[-2] if len(name_parts) > 1 else None), name_parts[-1]

    # body: from the opening parenthesis to its matching close
    depth, start = 0, m.end() - 1
    for i in range(start, len(sql)):
        depth += {"(": 1, ")": -1}.get(sql[i], 0)
        if depth == 0:
            body = sql[start + 1 : i]
            break
    else:
        raise ValueError("unbalanced parentheses in CREATE TABLE")

    columns, primary_key = [], []
    for item in _split_top_level(body):
        upper = item.upper()
        if upper.startswith(("CONSTRAINT", "PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "CHECK")):
            if key := _KEY.search(item):
                primary_key = _key_columns(key.group(1))
            continue
        name, _, rest = item.partition(" ")
        t = _TYPE.match(rest)
        sql_type = (t.group(1) + (t.group(2) or "")).upper().replace(" ", "") if t else rest
        sql_type = sql_type.replace("DOUBLEPRECISION", "DOUBLE")
        columns.append(
            Column(
                name=_unquote(name),
                sql_type=sql_type,
                nullable="NOT NULL" not in upper,
                is_row_change_timestamp="ROW CHANGE TIMESTAMP" in upper,
            )
        )
        if re.search(r"\bPRIMARY\s+KEY\b", upper):
            primary_key = [_unquote(name)]
    return TableDef(schema, table, columns, primary_key)


_SIMPLE = {
    "INT": "INT",
    "INTEGER": "INT",
    "SMALLINT": "INT",
    "TINYINT": "INT",
    "BIGINT": "BIGINT",
    "MONEY": "DECIMAL(19,4)",
    "SMALLMONEY": "DECIMAL(10,4)",
    "FLOAT": "DOUBLE",
    "REAL": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "DECFLOAT": "DOUBLE",
    "DATE": "DATE",
    "BIT": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
}
_STRING = {
    "CHAR",
    "VARCHAR",
    "NCHAR",
    "NVARCHAR",
    "TEXT",
    "NTEXT",
    "CLOB",
    "DBCLOB",
    "GRAPHIC",
    "VARGRAPHIC",
    "UNIQUEIDENTIFIER",
    "XML",
}
_TIMESTAMP = {"TIMESTAMP", "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET"}
_BINARY = {"BINARY", "VARBINARY", "BLOB", "IMAGE"}


def spark_type(sql_type: str) -> str | None:
    """SQL Server / DB2 column type -> Spark DDL type, or None when there is no safe mapping."""
    base, _, args = sql_type.upper().partition("(")
    if base in ("DECIMAL", "NUMERIC", "DEC"):
        nums = [a.strip() for a in args.rstrip(")").split(",") if a.strip()]
        precision, scale = (nums + ["10", "0"])[:2] if nums else ("10", "0")
        if len(nums) == 1:
            scale = "0"
        return f"DECIMAL({precision},{scale})"
    if base in _SIMPLE:
        return _SIMPLE[base]
    if base in _STRING:
        return "STRING"
    if base in _TIMESTAMP:
        return "TIMESTAMP"
    if base in _BINARY:
        return "BINARY"
    return None
