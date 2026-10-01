# Databricks notebook source
# MAGIC %md
# MAGIC ## 02 · Bronze ingest
# MAGIC Structured Streaming from every CDC topic, `availableNow` trigger, MERGE on `event_id`
# MAGIC so replays and producer retries never create duplicates.

# COMMAND ----------
# MAGIC %run ./_common

# COMMAND ----------
from banking_cdc.pipeline.config import topic_map
from banking_cdc.pipeline.medallion import run_bronze, spark_kafka_options
from banking_cdc.pipeline.topics import ensure_topics

topics = sorted({t for s in CFG["sources"].values() for t in topic_map(s).values()})
ensure_topics(
    KAFKA, [*topics, CFG["kafka"]["dlq_topic"], CFG["kafka"]["alerts_topic"]]
)  # no-op once provisioned
progress = run_bronze(
    spark,
    NS,
    spark_kafka_options(KAFKA, shaded=True),
    topics,
    f"{CHECKPOINTS}/bronze",
)
print(progress)
