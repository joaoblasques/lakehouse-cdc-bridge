"""Kafka → Bronze (dedup on event_id) → Silver (latest state per key, soft deletes)."""

from __future__ import annotations

from typing import Any

from banking_cdc.pipeline.tables import name

ENVELOPE_SCHEMA = """
    event_id STRING, op STRING, key STRING, before STRING, after STRING,
    source STRUCT<system: STRING, database: STRING, table: STRING, position: STRING,
                  commit_ts: STRING, sequence: STRING>,
    captured_at STRING, trace_id STRING, schema_version INT, payload_hash STRING"""


def spark_kafka_options(conf: dict[str, str], shaded: bool = False) -> dict[str, str]:
    """confluent-kafka settings → Spark Kafka source options (Databricks shades the client)."""
    opts = {"kafka.bootstrap.servers": conf["bootstrap.servers"]}
    if conf.get("security.protocol") == "SASL_SSL":
        module = "org.apache.kafka.common.security.plain.PlainLoginModule"
        if shaded:
            module = "kafkashaded." + module
        opts |= {
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                f'{module} required username="{conf["sasl.username"]}" '
                f'password="{conf["sasl.password"]}";'
            ),
        }
    return opts


def run_bronze(spark, ns: str, kafka_options: dict[str, str], topics: list[str], checkpoint: str):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    bronze = name(ns, "bronze_change_events")

    def merge_batch(batch, _batch_id):
        rows = batch.dropDuplicates(["event_id"])
        (
            DeltaTable.forName(spark, bronze)
            .alias("t")
            .merge(rows.alias("s"), "t.event_id = s.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

    events = (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .option("subscribe", ",".join(topics))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("e"),
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
        .select(
            "e.event_id",
            "e.op",
            F.col("e.source.system").alias("source_system"),
            F.col("e.source.database").alias("source_database"),
            F.col("e.source.table").alias("source_table"),
            F.col("e.source.position").alias("source_position"),
            F.coalesce("e.source.sequence", "e.source.position").alias("source_sequence"),
            F.to_timestamp("e.source.commit_ts").alias("source_commit_ts"),
            F.col("e.key").alias("key_json"),
            F.col("e.before").alias("before_json"),
            F.col("e.after").alias("after_json"),
            "e.payload_hash",
            F.to_timestamp("e.captured_at").alias("captured_at"),
            "e.trace_id",
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
    query = (
        events.writeStream.foreachBatch(merge_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return query.lastProgress


def apply_silver_batch(spark, ns: str, batch, table_cfg: dict[str, Any], source_table: str) -> None:
    """Latest change per key wins; older or replayed changes never overwrite newer state.

    Deletes are soft. A delete may carry no row image at all (DB2's query-based CDC only knows
    the key of a row that disappeared), so a delete never overwrites column values: Silver
    keeps the last known version and flags it.
    """
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    changes = batch.where(F.col("source_table") == source_table)
    keys = table_cfg["primary_key"]
    data_cols = [f.name for f in StructType.fromDDL(table_cfg["silver_schema"]).fields]
    image = F.from_json(
        F.coalesce(
            F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json")),
            F.col("key_json"),
        ),
        table_cfg["silver_schema"],
    )
    parsed = changes.withColumn("img", image)
    by_key = Window.partitionBy(*[F.col(f"img.{k}") for k in keys])
    history = by_key.orderBy("source_sequence").rowsBetween(Window.unboundedPreceding, 0)
    # A key-only delete takes the last values seen earlier in this batch, if any.
    filled = [
        F.when(F.col("op") == "d", F.last(F.col(f"img.{c}"), ignorenulls=True).over(history))
        .otherwise(F.col(f"img.{c}"))
        .alias(c)
        for c in data_cols
    ]
    first_commit = (
        parsed.where(F.col("op").isin("c", "r"))
        .groupBy(*[F.col(f"img.{k}").alias(k) for k in keys])
        .agg(F.min("source_commit_ts").alias("_first_commit_ts"))
    )
    latest = (
        parsed.select(
            *filled,
            F.col("event_id").alias("_event_id"),
            F.col("op").alias("_op"),
            F.col("source_sequence").alias("_source_sequence"),
            F.col("source_commit_ts").alias("_last_commit_ts"),
            (F.col("op") == "d").alias("_is_deleted"),
            F.current_timestamp().alias("_updated_at"),
            F.row_number().over(by_key.orderBy(F.col("source_sequence").desc())).alias("_rn"),
        )
        .where("_rn = 1")
        .drop("_rn")
        .join(first_commit, keys, "left")
    )
    target = DeltaTable.forName(spark, name(ns, table_cfg["silver_table"]))
    meta = ["_event_id", "_op", "_source_sequence", "_last_commit_ts", "_is_deleted", "_updated_at"]
    newer = "s._source_sequence > t._source_sequence"
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        target.alias("t")
        .merge(latest.alias("s"), on)
        .whenMatchedUpdate(condition=f"{newer} AND s._op = 'd'", set={c: f"s.{c}" for c in meta})
        .whenMatchedUpdate(
            condition=newer,
            set={c: f"s.{c}" for c in data_cols + meta}
            | {"_first_commit_ts": "coalesce(t._first_commit_ts, s._first_commit_ts)"},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver(spark, ns: str, cfg: dict[str, Any], checkpoint: str):
    """Incremental: streams new Bronze rows (Bronze is insert-only) and MERGEs every entity."""
    entities = []
    for src in cfg["sources"].values():
        for t in src["tables"]:
            qualified = f"{t['schema']}.{t['table']}" if "schema" in t else t["table"]
            entities.append((t, qualified))

    def per_batch(batch, _batch_id):
        batch.persist()
        for table_cfg, qualified in entities:
            apply_silver_batch(spark, ns, batch, table_cfg, qualified)
        batch.unpersist()

    query = (
        spark.readStream.table(name(ns, "bronze_change_events"))
        .writeStream.foreachBatch(per_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return query.lastProgress
