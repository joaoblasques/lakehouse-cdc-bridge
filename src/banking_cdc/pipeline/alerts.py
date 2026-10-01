"""Publish Gold alerts to Kafka with the transactional outbox pattern.

`gold_fraud_alerts.published_at` is the outbox flag. Each run sends the rows where it is empty,
waits for Kafka to confirm every message, and only then stamps them. A crash in between re-sends
the same alerts next run; consumers store by `alert_id`, so a repeat changes nothing.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from banking_cdc.pipeline.producer import DeliveryError
from banking_cdc.pipeline.tables import name

FIELDS = (
    "alert_id",
    "rule",
    "account_id",
    "evidence_keys",
    "first_evidence_ts",
    "last_source_commit_ts",
    "alert_created_at",
    "latency_seconds",
)


def alert_message(alert: dict[str, Any]) -> dict[str, Any]:
    msg: dict[str, Any] = {"schema_version": 1}
    for field in FIELDS:
        value = alert.get(field)
        if isinstance(value, datetime | date):
            value = value.isoformat()
        elif isinstance(value, tuple):
            value = list(value)
        msg[field] = value
    return msg


def publish_alerts(producer: Any, alerts: list[dict[str, Any]], topic: str) -> list[str]:
    """Produce and flush; raise unless every alert was acknowledged."""
    delivered: list[str] = []
    errors: list[str] = []

    def on_delivery(alert_id: str):
        def callback(err, _msg):
            (errors if err is not None else delivered).append(
                f"{alert_id}: {err}" if err is not None else alert_id
            )

        return callback

    for alert in alerts:
        msg = alert_message(alert)
        producer.produce(
            topic,
            key=str(msg["account_id"]).encode(),  # one account's alerts stay in order
            value=json.dumps(msg).encode(),
            headers={"alert_id": msg["alert_id"], "schema_version": "1"},
            on_delivery=on_delivery(msg["alert_id"]),
        )
        producer.poll(0)
    remaining = producer.flush(60)
    if errors or remaining:
        raise DeliveryError(f"{len(errors)} alerts failed, {remaining} unflushed: {errors[:3]}")
    return delivered


def publish_pending_alerts(spark, ns: str, producer: Any, topic: str) -> int:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    table = name(ns, "gold_fraud_alerts")
    pending = [r.asDict() for r in spark.table(table).where("published_at IS NULL").collect()]
    if not pending:
        return 0
    ids = publish_alerts(producer, pending, topic)
    sent = spark.createDataFrame([(i,) for i in ids], "alert_id STRING")
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(sent.alias("s"), "t.alert_id = s.alert_id")
        .whenMatchedUpdate(set={"published_at": F.current_timestamp()})
        .execute()
    )
    return len(ids)
