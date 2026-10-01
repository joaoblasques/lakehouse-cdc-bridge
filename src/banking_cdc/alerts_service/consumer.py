"""Kafka consumer for `banking.fraud.alerts`.

Offsets are committed only after the alert is stored. A crash re-delivers at most the
uncommitted alerts, and the store's upsert makes a repeat harmless.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from banking_cdc.alerts_service.store import AlertStore

log = logging.getLogger(__name__)
REQUIRED = ("alert_id", "rule", "account_id", "evidence_keys")


def handle_message(msg: Any, store: AlertStore) -> str:
    """Store one message. Returns "stored", "duplicate" or "rejected"."""
    try:
        alert = json.loads(msg.value())
        missing = [f for f in REQUIRED if f not in alert]
        if missing:
            raise ValueError(f"missing {missing}")
    except (ValueError, TypeError) as exc:
        log.warning("rejected alert message: %s", exc)
        return "rejected"
    return "stored" if store.upsert(alert) else "duplicate"


class AlertConsumer:
    def __init__(self, conf: dict[str, str], topic: str, store: AlertStore):
        self.conf = conf | {
            "group.id": conf.get("group.id", "fraud-alerts-api"),
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        }
        self.topic = topic
        self.store = store
        self.counts = {"stored": 0, "duplicate": 0, "rejected": 0}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="alert-consumer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _run(self) -> None:
        from confluent_kafka import Consumer

        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])
        try:
            while not self._stop.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("kafka error: %s", msg.error())
                    continue
                self.counts[handle_message(msg, self.store)] += 1
                consumer.commit(message=msg, asynchronous=False)
        finally:
            consumer.close()
