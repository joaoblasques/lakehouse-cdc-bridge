# Databricks notebook source
# MAGIC %md
# MAGIC ## 05 · Gold fraud alerts
# MAGIC Velocity, impossible travel and structuring rules over Silver. Each alert keeps its evidence
# MAGIC keys, so it traces back through Bronze and the audit table to the source LSN or file line.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
from banking_cdc.pipeline.gold import run_gold

print(f"{run_gold(spark, NS)} alerts in gold_fraud_alerts")
