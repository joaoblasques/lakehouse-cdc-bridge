"""Delta tables of the CDC platform. `ns` is `catalog.schema` on Unity Catalog, a schema locally."""

from __future__ import annotations

from typing import Any

DDL = {
    "cdc_control": """
        source_name STRING, watermark STRING, last_trace_id STRING, events_captured BIGINT,
        rejects BIGINT, status STRING, updated_at TIMESTAMP""",
    "cdc_file_manifest": """
        file_name STRING, etag STRING, size BIGINT, sha256 STRING, rows BIGINT, rejected BIGINT,
        trace_id STRING, processed_at TIMESTAMP""",
    "cdc_event_audit": """
        event_id STRING, source_name STRING, source_table STRING, op STRING,
        source_position STRING, source_commit_ts TIMESTAMP, payload_hash STRING, topic STRING,
        kafka_partition INT, kafka_offset BIGINT, trace_id STRING, produced_at TIMESTAMP""",
    "bronze_change_events": """
        event_id STRING, op STRING, source_system STRING, source_database STRING,
        source_table STRING, source_position STRING, source_sequence STRING,
        source_commit_ts TIMESTAMP, key_json STRING, before_json STRING, after_json STRING,
        payload_hash STRING, captured_at TIMESTAMP, trace_id STRING, kafka_topic STRING,
        kafka_partition INT, kafka_offset BIGINT, kafka_ts TIMESTAMP, ingested_at TIMESTAMP""",
    "dq_reconciliation": """
        run_id STRING, checked_at TIMESTAMP, source_name STRING, table_name STRING,
        check_name STRING, source_value STRING, target_value STRING, status STRING""",
    "gold_fraud_alerts": """
        alert_id STRING, rule STRING, account_id INT, evidence_keys ARRAY<STRING>,
        first_evidence_ts TIMESTAMP, last_source_commit_ts TIMESTAMP,
        alert_created_at TIMESTAMP, latency_seconds DOUBLE,
        published_at TIMESTAMP""",  # outbox flag: NULL until Kafka confirmed the alert
}
SILVER_META = """_event_id STRING, _op STRING, _source_sequence STRING, _first_commit_ts TIMESTAMP,
    _last_commit_ts TIMESTAMP, _is_deleted BOOLEAN, _updated_at TIMESTAMP"""


def name(ns: str, table: str) -> str:
    return f"{ns}.{table}"


def ensure_tables(spark, ns: str, cfg: dict[str, Any]) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns}")
    for table, cols in DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name(ns, table)} ({cols}) USING DELTA")
    for src in cfg["sources"].values():
        for t in src["tables"]:
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {name(ns, t['silver_table'])} "
                f"({t['silver_schema']}, {SILVER_META}) USING DELTA"
            )


def append_rows(spark, table: str, rows: list[dict[str, Any]]) -> None:
    """Append python dicts, casting each value to the target column's type."""
    if not rows:
        return
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    target = spark.table(table).schema
    cols = [f.name for f in target.fields]
    raw = spark.createDataFrame(
        [tuple(None if r.get(c) is None else str(r[c]) for c in cols) for r in rows],
        StructType([StructField(c, StringType()) for c in cols]),
    )
    raw.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target.fields]).write.format(
        "delta"
    ).mode("append").saveAsTable(table)
