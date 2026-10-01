# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 · CDC capture (one source per run)
# MAGIC Called by the job's **for-each** task with `source_name` from `conf/sources.yml`.
# MAGIC Reads the watermark from `cdc_control`, captures changes, produces them to Confluent,
# MAGIC writes `cdc_event_audit`, then commits the new watermark.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
from banking_cdc.pipeline.capture import run_capture
from banking_cdc.pipeline.config import build_adapter, topic_map
from banking_cdc.pipeline.producer import make_producer
from banking_cdc.pipeline.tables import ensure_tables

dbutils.widgets.text("source_name", "sqlserver_cards")
source_name = dbutils.widgets.get("source_name")
ensure_tables(spark, NS, CFG)

summary = run_capture(
    spark,
    NS,
    source_name,
    build_adapter(source_name, CFG, secret),
    make_producer(KAFKA),
    topic_map(CFG["sources"][source_name]),
    CFG["kafka"]["dlq_topic"],
)
print(json.dumps(summary, indent=2))
dbutils.jobs.taskValues.set("events", summary["events"])
