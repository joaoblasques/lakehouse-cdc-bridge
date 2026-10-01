"""Spark session: Databricks provides one; locally we build Delta + Kafka support."""

from __future__ import annotations

import os

DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def get_spark(app_name: str = "banking-cdc"):
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:  # on Databricks the notebook already has `spark`
        return active

    from delta import configure_spark_with_delta_pip

    warehouse = os.path.abspath(os.environ.get("LOCAL_WAREHOUSE", ".local/warehouse"))
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.warehouse.dir", warehouse)
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={warehouse}/metastore_db;create=true",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "2")  # tiny local tables
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
