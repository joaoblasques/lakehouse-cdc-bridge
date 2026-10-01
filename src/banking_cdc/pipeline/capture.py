"""Capture step: source → Kafka, with the Delta audit trail and watermark.

Order matters: produce → flush → audit → watermark. A crash before the watermark commit
replays the same range with the same event_ids, which Bronze deduplicates.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from banking_cdc.pipeline.producer import publish
from banking_cdc.pipeline.tables import append_rows, name
from banking_cdc.sources.base import SourceAdapter


def read_watermark(spark, ns: str, source_name: str) -> dict[str, Any] | None:
    rows = (
        spark.table(name(ns, "cdc_control"))
        .where(f"source_name = '{source_name}'")
        .select("watermark")
        .collect()
    )
    return json.loads(rows[0].watermark) if rows else None


def commit_watermark(spark, ns: str, row: dict[str, Any]) -> None:
    from delta.tables import DeltaTable

    staged = spark.createDataFrame([row]).selectExpr(
        "source_name",
        "watermark",
        "last_trace_id",
        "CAST(events_captured AS BIGINT) AS events_captured",
        "CAST(rejects AS BIGINT) AS rejects",
        "status",
        "CAST(updated_at AS TIMESTAMP) AS updated_at",
    )
    (
        DeltaTable.forName(spark, name(ns, "cdc_control"))
        .alias("t")
        .merge(staged.alias("s"), "t.source_name = s.source_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_capture(
    spark,
    ns: str,
    source_name: str,
    adapter: SourceAdapter,
    producer: Any,
    topics: dict[str, str],
    dlq_topic: str,
    trace_id: str | None = None,
) -> dict[str, Any]:
    trace_id = trace_id or str(uuid.uuid4())
    started = datetime.now(UTC)
    result = adapter.capture(read_watermark(spark, ns, source_name), trace_id)
    audit = publish(producer, result.events, topics, result.rejects, dlq_topic, source_name)
    append_rows(spark, name(ns, "cdc_event_audit"), audit)
    now = datetime.now(UTC).isoformat()
    append_rows(
        spark, name(ns, "cdc_file_manifest"), [m | {"processed_at": now} for m in result.manifest]
    )
    commit_watermark(
        spark,
        ns,
        {
            "source_name": source_name,
            "watermark": json.dumps(result.new_watermark, sort_keys=True),
            "last_trace_id": trace_id,
            "events_captured": len(result.events),
            "rejects": len(result.rejects),
            "status": "OK",
            "updated_at": now,
        },
    )
    ops: dict[str, int] = {}
    for e in result.events:
        ops[e.op] = ops.get(e.op, 0) + 1
    return {
        "source_name": source_name,
        "trace_id": trace_id,
        "events": len(result.events),
        "rejects": len(result.rejects),
        "ops": ops,
        "files": len(result.manifest),
        "seconds": (datetime.now(UTC) - started).total_seconds(),
    }
