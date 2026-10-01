# Databricks notebook source
# MAGIC %md
# MAGIC ## 05 · Gold fraud alerts
# MAGIC Velocity, impossible travel and structuring rules over Silver. Each alert keeps its evidence
# MAGIC keys, so it traces back through Bronze and the audit table to the source LSN or file line.
# MAGIC New alerts are then published to Kafka (outbox: only rows whose `published_at` is empty).

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
from banking_cdc.pipeline.alerts import publish_pending_alerts
from banking_cdc.pipeline.gold import run_gold
from banking_cdc.pipeline.producer import make_producer

print(f"{run_gold(spark, NS)} alerts in gold_fraud_alerts")
sent = publish_pending_alerts(spark, NS, make_producer(KAFKA), CFG["kafka"]["alerts_topic"])
print(f"{sent} new alerts published to {CFG['kafka']['alerts_topic']}")
