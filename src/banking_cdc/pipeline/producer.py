"""Publish change events to Kafka and return the audit trail of where each one landed."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from banking_cdc.envelope import ChangeEvent
from banking_cdc.sources.base import Reject

PRODUCER_DEFAULTS = {
    "enable.idempotence": "true",  # no duplicates from producer retries, per-partition order kept
    "acks": "all",
    "compression.type": "zstd",
    "linger.ms": "20",
}


class DeliveryError(RuntimeError):
    pass


def make_producer(conf: dict[str, str]):
    from confluent_kafka import Producer

    return Producer(PRODUCER_DEFAULTS | conf)


def publish(
    producer: Any,
    events: list[ChangeEvent],
    topics: dict[str, str],
    rejects: list[Reject],
    dlq_topic: str,
    source_name: str,
) -> list[dict[str, Any]]:
    """Produce, flush, and fail the run if any message was not acknowledged.

    The caller commits the watermark only after this returns, so a failure replays the same
    range with the same event_ids.
    """
    audit: list[dict[str, Any]] = []
    errors: list[str] = []

    def on_delivery(event: ChangeEvent):
        def callback(err, msg):
            if err is not None:
                errors.append(f"{event.event_id}: {err}")
                return
            audit.append(
                {
                    "event_id": event.event_id,
                    "source_name": source_name,
                    "source_table": event.source.table,
                    "op": event.op,
                    "source_position": event.source.position,
                    "source_commit_ts": event.source.commit_ts,
                    "payload_hash": event.payload_hash,
                    "topic": msg.topic(),
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset(),
                    "trace_id": event.trace_id,
                    "produced_at": datetime.now(UTC).isoformat(),
                }
            )

        return callback

    for event in events:
        producer.produce(
            topics[event.source.table],
            key=event.kafka_key.encode(),
            value=event.to_json().encode(),
            headers={"event_id": event.event_id, "schema_version": str(event.schema_version)},
            on_delivery=on_delivery(event),
        )
        producer.poll(0)
    for reject in rejects:
        producer.produce(
            dlq_topic,
            key=reject.position.encode(),
            value=json.dumps(
                {
                    "source_name": source_name,
                    "source_system": reject.source_system,
                    "position": reject.position,
                    "reason": reject.reason,
                    "raw": reject.raw,
                }
            ).encode(),
        )
    remaining = producer.flush(60)
    if errors or remaining:
        raise DeliveryError(f"{len(errors)} failed, {remaining} unflushed: {errors[:3]}")
    return audit
