"""Turn conf/sources.yml into adapters and Kafka settings.

Credentials come from a `secret(scope_key, field)` getter: Databricks secrets on the platform,
environment variables locally. Nothing secret lives in the repo.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from banking_cdc.sources.base import SourceAdapter

CONF = Path(__file__).resolve().parents[3] / "conf" / "sources.yml"
Secret = Callable[[str, str], str]


def load_config(path: Path | str = CONF) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text())


def env_secret(scope_key: str, field: str) -> str:
    """Local stand-in for dbutils.secrets: MSSQL_CARDS_HOST, FILESHARE_PARTNER_DIR, ..."""
    name = f"{scope_key}_{field}".upper().replace("-", "_")
    value = os.environ.get(name)
    if value is None:
        raise KeyError(f"set environment variable {name}")
    return value


def topic_map(source_cfg: dict[str, Any]) -> dict[str, str]:
    """Qualified table name (as written in event.source.table) -> Kafka topic."""
    out = {}
    for t in source_cfg["tables"]:
        qualified = f"{t['schema']}.{t['table']}" if "schema" in t else t["table"]
        out[qualified] = t["topic"]
    return out


def build_adapter(name: str, cfg: dict[str, Any], secret: Secret = env_secret) -> SourceAdapter:
    src = cfg["sources"][name]
    key = src["connection_secret"]
    if src["type"] == "sqlserver":
        import pymssql

        from banking_cdc.sources.sqlserver import SqlServerCdcAdapter, TableSpec

        def connect():
            return pymssql.connect(
                server=secret(key, "host"),
                port=secret(key, "port"),
                user=secret(key, "user"),
                password=secret(key, "password"),
                database=src["database"],
            )

        specs = [TableSpec(t["schema"], t["table"], t["primary_key"]) for t in src["tables"]]
        return SqlServerCdcAdapter(connect, src["database"], specs)

    if src["type"] == "fileshare":
        from banking_cdc.sources.fileshare import (
            AzureFileShare,
            FileShareAdapter,
            LocalDirectoryShare,
        )

        try:
            share = LocalDirectoryShare(secret(key, "dir"))
        except KeyError:
            share = AzureFileShare(secret(key, "connection_string"), src["share"], src["directory"])
        return FileShareAdapter(share, share_name=src["share"])

    raise ValueError(f"no adapter for source type {src['type']!r}")


def kafka_conf(secret: Secret = env_secret) -> dict[str, str]:
    """Local broker by default; Confluent Cloud (SASL_SSL) when an API key is configured."""
    conf = {"bootstrap.servers": secret("kafka", "bootstrap")}
    try:
        conf |= {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": secret("kafka", "api_key"),
            "sasl.password": secret("kafka", "api_secret"),
        }
    except KeyError:
        pass
    return conf
