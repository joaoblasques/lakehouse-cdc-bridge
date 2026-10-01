"""The contract every CDC source adapter implements."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol

from banking_cdc.envelope import ChangeEvent


@dataclass
class Reject:
    """A record that could not become a valid event. Goes to the DLQ topic, never dropped."""

    source_system: str
    position: str
    reason: str
    raw: dict[str, Any]


@dataclass
class CaptureResult:
    events: list[ChangeEvent]
    new_watermark: dict[str, Any]
    rejects: list[Reject] = field(default_factory=list)
    manifest: list[dict[str, Any]] = field(default_factory=list)  # file sources only


class SourceAdapter(Protocol):
    name: str

    def capture(self, watermark: dict[str, Any] | None, trace_id: str) -> CaptureResult:
        """Return every change after `watermark`. Must not advance any state itself."""

    def source_stats(self) -> dict[str, dict[str, Any]]:
        """Per-table counts/sums straight from the source, for reconciliation."""


def jsonable(row: dict[str, Any]) -> dict[str, Any]:
    """Money stays exact (Decimal -> str); timestamps become ISO-8601."""
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = str(v)
        elif isinstance(v, datetime | date):
            out[k] = v.isoformat()
        elif isinstance(v, bytes):
            out[k] = "0x" + v.hex()
        else:
            out[k] = v
    return out
