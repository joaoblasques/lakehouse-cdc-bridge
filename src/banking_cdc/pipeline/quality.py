"""Reconciliation: prove nothing was lost or invented between source, Kafka and Delta."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from banking_cdc.pipeline.tables import append_rows, name


class ReconciliationError(RuntimeError):
    pass


def _check(run_id, source, table, check, src_val, tgt_val, skip=False) -> dict[str, Any]:
    status = "SKIP" if skip else "PASS" if str(src_val) == str(tgt_val) else "FAIL"
    return {
        "run_id": run_id,
        "checked_at": datetime.now(UTC).isoformat(),
        "source_name": source,
        "table_name": table,
        "check_name": check,
        "source_value": str(src_val),
        "target_value": str(tgt_val),
        "status": status,
    }


def snapshot_pending(spark, ns: str, source_name: str) -> bool:
    """True when a query-based source has not run its delete-finding snapshot pass since its
    last capture. Until it does, Silver legitimately still holds rows deleted at the source."""
    from banking_cdc.pipeline.capture import read_watermark

    watermark = read_watermark(spark, ns, source_name) or {}
    return watermark.get("runs_since_snapshot", 0) != 0


def run_reconciliation(
    spark,
    ns: str,
    cfg: dict[str, Any],
    stats_by_source: dict[str, dict[str, dict[str, Any]]],
    fail_on_mismatch: bool = True,
) -> list[dict[str, Any]]:
    """`stats_by_source` comes from each adapter's source_stats(), taken while sources are quiet.

    Checks:
      * row_count: live rows in the source == non-deleted rows in Silver
      * balance_sum: sum of account balances matches to the cent
      * audit_to_bronze: every event Kafka acknowledged is in Bronze (no loss in transit)

    For snapshot-based sources (DB2), row_count is SKIP between snapshot passes instead of a
    false FAIL: deletes there are only visible to the snapshot.
    """
    run_id = str(uuid.uuid4())
    checks = []
    for source_name, src in cfg["sources"].items():
        pending = "snapshot_every" in src and snapshot_pending(spark, ns, source_name)
        for t in src["tables"]:
            qualified = f"{t['schema']}.{t['table']}" if "schema" in t else t["table"]
            silver = spark.table(name(ns, t["silver_table"])).where("NOT _is_deleted")
            src_stats = stats_by_source[source_name][qualified]
            checks.append(
                _check(
                    run_id,
                    source_name,
                    qualified,
                    "row_count",
                    src_stats["rows"],
                    silver.count(),
                    skip=pending,
                )
            )
            if "balance_sum" in src_stats:
                tgt = silver.selectExpr("CAST(SUM(balance) AS STRING) AS s").first().s
                checks.append(
                    _check(
                        run_id,
                        source_name,
                        qualified,
                        "balance_sum",
                        Decimal(src_stats["balance_sum"]),
                        Decimal(tgt or "0"),
                    )
                )
    missing = (
        spark.sql(
            f"""SELECT COUNT(*) AS n FROM {name(ns, "cdc_event_audit")} a
            LEFT ANTI JOIN {name(ns, "bronze_change_events")} b ON a.event_id = b.event_id"""
        )
        .first()
        .n
    )
    checks.append(_check(run_id, "*", "*", "audit_to_bronze_missing", 0, missing))
    append_rows(spark, name(ns, "dq_reconciliation"), checks)
    failed = [c for c in checks if c["status"] == "FAIL"]
    if failed and fail_on_mismatch:
        raise ReconciliationError(f"{len(failed)} reconciliation checks failed: {failed}")
    return checks
