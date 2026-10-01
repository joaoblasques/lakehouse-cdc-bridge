"""The standard change event every source adapter emits.

One envelope for all sources means downstream (Bronze, Silver, consumers) never needs to know
whether a change came from a database log or a partner file.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

OPS = {"c", "u", "d", "r"}  # create, update, delete, snapshot read


@dataclass(frozen=True)
class SourceInfo:
    system: str
    database: str
    table: str
    position: str  # LSN:seqval for SQL Server, path:line@sha256 for files
    commit_ts: str | None = None


@dataclass(frozen=True)
class ChangeEvent:
    event_id: str
    op: str
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: SourceInfo
    captured_at: str
    trace_id: str
    schema_version: int = 1
    payload_hash: str = field(default="")

    @property
    def kafka_key(self) -> str:
        pk = ",".join(f"{k}={v}" for k, v in sorted(self.key.items()))
        return f"{self.source.table}:{pk}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, default=str)

    @classmethod
    def from_json(cls, raw: str | bytes) -> ChangeEvent:
        data = json.loads(raw)
        data["source"] = SourceInfo(**data["source"])
        return cls(**data)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def make_event(
    op: str,
    source: SourceInfo,
    key: dict[str, Any],
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    trace_id: str,
    captured_at: str | None = None,
) -> ChangeEvent:
    if op not in OPS:
        raise ValueError(f"unknown op {op!r}, expected one of {sorted(OPS)}")
    identity = "|".join([source.system, source.table, _canonical(key), source.position])
    return ChangeEvent(
        event_id=hashlib.sha256(identity.encode()).hexdigest(),
        op=op,
        key=key,
        before=before,
        after=after,
        source=source,
        captured_at=captured_at or datetime.now(UTC).isoformat(),
        trace_id=trace_id,
        payload_hash=hashlib.sha256(_canonical([before, after]).encode()).hexdigest(),
    )
