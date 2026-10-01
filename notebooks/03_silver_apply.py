# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 · Silver apply
# MAGIC Streams new Bronze rows and MERGEs the latest change per key into each `silver_*` table.
# MAGIC Out-of-order guard on the source sequence (LSN / file+line); deletes are soft.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
from banking_cdc.pipeline.medallion import run_silver

print(run_silver(spark, NS, CFG, f"{CHECKPOINTS}/silver"))
