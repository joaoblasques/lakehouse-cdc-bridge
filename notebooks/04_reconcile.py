# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 · Reconcile
# MAGIC Source counts and balance sums vs Silver, and Kafka-acknowledged events vs Bronze.
# MAGIC Fails the task (and the job) on any mismatch; results land in `dq_reconciliation`.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
from banking_cdc.pipeline.config import build_adapter
from banking_cdc.pipeline.quality import run_reconciliation

stats = {name: build_adapter(name, CFG, secret).source_stats() for name in CFG["sources"]}
checks = run_reconciliation(spark, NS, CFG, stats)
display(spark.createDataFrame(checks))
