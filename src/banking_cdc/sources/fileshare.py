"""File-arrival CDC for partner files on an Azure File Share.

Auto Loader reads ADLS, Blob and Volumes, not Azure Files (SMB shares), so change detection is
done here: list the share, compare each file's (name, etag) with the manifest of files already
processed, and emit one event per row of every new or changed file.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from banking_cdc.envelope import SourceInfo, make_event
from banking_cdc.sources.base import CaptureResult, Reject
from banking_cdc.validation import iban_is_valid, parse_amount

TABLE = "partner_transfers"
REQUIRED = ("transfer_id", "debtor_iban", "creditor_iban", "amount", "booking_ts")


@dataclass(frozen=True)
class FileRef:
    name: str
    etag: str
    size: int
    last_modified: str


class FileShare(Protocol):
    def list_files(self) -> list[FileRef]: ...
    def read(self, name: str) -> bytes: ...


class LocalDirectoryShare:
    """Local stand-in for the share (Azurite has no File service emulator)."""

    def __init__(self, root: Path | str):
        self.root = Path(root)

    def list_files(self) -> list[FileRef]:
        refs = []
        for p in sorted(self.root.iterdir()):
            if p.is_file():
                st = p.stat()
                refs.append(
                    FileRef(
                        name=p.name,
                        etag=f"{st.st_mtime_ns}-{st.st_size}",
                        size=st.st_size,
                        last_modified=datetime.fromtimestamp(st.st_mtime, UTC).isoformat(),
                    )
                )
        return refs

    def read(self, name: str) -> bytes:
        return (self.root / name).read_bytes()


class AzureFileShare:
    """Azure Files through the storage SDK (connection string from a Databricks secret)."""

    def __init__(self, connection_string: str, share: str, directory: str = ""):
        from azure.storage.fileshare import ShareClient

        self.dir = ShareClient.from_connection_string(
            connection_string, share
        ).get_directory_client(directory)

    def list_files(self) -> list[FileRef]:
        refs = []
        for item in self.dir.list_directories_and_files():
            if item.get("is_directory"):
                continue
            props = self.dir.get_file_client(item["name"]).get_file_properties()
            refs.append(
                FileRef(
                    name=item["name"],
                    etag=props.etag,
                    size=props.size,
                    last_modified=props.last_modified.isoformat(),
                )
            )
        return sorted(refs, key=lambda r: r.name)

    def read(self, name: str) -> bytes:
        return self.dir.get_file_client(name).download_file().readall()


class FileShareAdapter:
    name = "partner_fileshare"

    def __init__(self, share: FileShare, share_name: str, suffix: str = ".csv"):
        self.share = share
        self.share_name = share_name
        self.suffix = suffix

    def capture(self, watermark: dict[str, Any] | None, trace_id: str) -> CaptureResult:
        seen: dict[str, str] = dict((watermark or {}).get("files", {}))  # name -> etag
        events, rejects, manifest = [], [], []
        for ref in self.share.list_files():
            if not ref.name.endswith(self.suffix) or seen.get(ref.name) == ref.etag:
                continue
            op = "u" if ref.name in seen else "c"
            content = self.share.read(ref.name)
            sha = hashlib.sha256(content).hexdigest()
            n_rows = n_rejected = 0
            for line_no, row in enumerate(csv.DictReader(io.StringIO(content.decode())), start=2):
                position = f"{ref.name}:{line_no}@{sha[:12]}"
                reason = _invalid(row)
                if reason:
                    rejects.append(Reject("azure_fileshare", position, reason, row))
                    n_rejected += 1
                    continue
                source = SourceInfo(
                    system="azure_fileshare",
                    database=self.share_name,
                    table=TABLE,
                    position=position,
                    commit_ts=ref.last_modified,
                )
                events.append(
                    make_event(op, source, {"transfer_id": row["transfer_id"]}, None, row, trace_id)
                )
                n_rows += 1
            seen[ref.name] = ref.etag
            manifest.append(
                {
                    "file_name": ref.name,
                    "etag": ref.etag,
                    "size": ref.size,
                    "sha256": sha,
                    "rows": n_rows,
                    "rejected": n_rejected,
                    "trace_id": trace_id,
                }
            )
        return CaptureResult(events, {"files": seen}, rejects, manifest)

    def source_stats(self) -> dict[str, dict[str, Any]]:
        ids = set()
        for ref in self.share.list_files():
            if ref.name.endswith(self.suffix):
                text = self.share.read(ref.name).decode()
                ids |= {
                    r["transfer_id"] for r in csv.DictReader(io.StringIO(text)) if not _invalid(r)
                }
        return {TABLE: {"rows": len(ids)}}


def _invalid(row: dict[str, str]) -> str | None:
    for col in REQUIRED:
        if not row.get(col):
            return f"missing {col}"
    if parse_amount(row["amount"]) is None:
        return "invalid amount"
    for col in ("debtor_iban", "creditor_iban"):
        if not iban_is_valid(row[col]):
            return f"invalid {col}"
    return None
