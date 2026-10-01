# Databricks notebook source
# Shared setup for every task: parameters, secrets, Kafka options. `%run ./_common` from each step.

from banking_cdc.pipeline.config import kafka_conf, load_config

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "banking_cdc")
dbutils.widgets.text("secret_scope", "banking-cdc")
dbutils.widgets.text("checkpoint_root", "/Volumes/main/banking_cdc/checkpoints")

NS = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
SCOPE = dbutils.widgets.get("secret_scope")
CHECKPOINTS = dbutils.widgets.get("checkpoint_root")
CFG = load_config()


def secret(scope_key: str, field: str) -> str:
    """conf/sources.yml names a key prefix; the field is appended: mssql-cards-host, ..."""
    try:
        return dbutils.secrets.get(SCOPE, f"{scope_key}-{field}".replace("_", "-"))
    except Exception as exc:  # secret missing → let callers fall back (e.g. local share dir)
        raise KeyError(f"{scope_key}-{field}") from exc


KAFKA = kafka_conf(secret)
