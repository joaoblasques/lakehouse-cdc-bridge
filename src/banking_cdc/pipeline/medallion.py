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
    """Latest change per key wins; older or replayed changes never overwrite newer state."""
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    changes = batch.where(F.col("source_table") == source_table)
    keys = table_cfg["primary_key"]
    image = F.from_json(
        F.when(F.col("op") == "d", F.col("before_json")).otherwise(F.col("after_json")),
        table_cfg["silver_schema"],
    )
    parsed = changes.withColumn("img", image)
    first_commit = (
        parsed.where(F.col("op").isin("c", "r"))
        .groupBy(*[F.col(f"img.{k}").alias(k) for k in keys])
        .agg(F.min("source_commit_ts").alias("_first_commit_ts"))
    )
    latest = (
        parsed.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(*[F.col(f"img.{k}") for k in keys]).orderBy(
                    F.col("source_sequence").desc()
                )
            ),
        )
        .where("_rn = 1")
        .select(
            "img.*",
            F.col("event_id").alias("_event_id"),
            F.col("op").alias("_op"),
            F.col("source_sequence").alias("_source_sequence"),
            F.col("source_commit_ts").alias("_last_commit_ts"),
            (F.col("op") == "d").alias("_is_deleted"),
            F.current_timestamp().alias("_updated_at"),
        )
        .join(first_commit, keys, "left")
    )
    target = DeltaTable.forName(spark, name(ns, table_cfg["silver_table"]))
    cols = [c for c in latest.columns if c != "_first_commit_ts"]
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        target.alias("t")
        .merge(latest.alias("s"), on)
        .whenMatchedUpdate(
            condition="s._source_sequence > t._source_sequence",
            set={c: f"s.{c}" for c in cols}
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
