"""Needs the local Kafka (`docker compose up -d`). Run with: uv run pytest -m kafka"""

import time
import uuid

import pytest

from banking_cdc.alerts_service.consumer import AlertConsumer
from banking_cdc.alerts_service.store import AlertStore
from banking_cdc.pipeline.alerts import publish_alerts
from banking_cdc.pipeline.producer import make_producer
from banking_cdc.pipeline.topics import ensure_topics

pytestmark = pytest.mark.kafka
CONF = {"bootstrap.servers": "localhost:9092"}


def _alert(i):
    return {
        "alert_id": f"a{i}",
        "rule": "card_velocity",
        "account_id": i,
        "evidence_keys": [str(i)],
        "alert_created_at": "2026-10-01T10:00:00",
    }


def _until(pred, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    return False


def test_alerts_flow_through_kafka_into_the_service_once(tmp_path):
    topic = f"test.fraud.alerts.{uuid.uuid4().hex[:8]}"
    ensure_topics(CONF, [topic])
    store = AlertStore(tmp_path / "a.db")
    group = {"group.id": f"it-{uuid.uuid4().hex[:8]}"}

    consumer = AlertConsumer(CONF | group, topic, store)
    consumer.start()
    try:
        publish_alerts(make_producer(CONF), [_alert(1), _alert(2)], topic)
        assert _until(lambda: store.stats()["total"] == 2)
        # The outbox may re-send after a crash: the service must not create a second alert.
        publish_alerts(make_producer(CONF), [_alert(1)], topic)
        assert _until(lambda: consumer.counts["duplicate"] == 1)
        assert store.stats()["total"] == 2
    finally:
        consumer.stop()

    # A restarted consumer in the same group resumes after the committed offsets.
    restarted = AlertConsumer(CONF | group, topic, store)
    restarted.start()
    try:
        publish_alerts(make_producer(CONF), [_alert(3)], topic)
        assert _until(lambda: store.stats()["total"] == 3)
        assert restarted.counts == {"stored": 1, "duplicate": 0, "rejected": 0}
    finally:
        restarted.stop()
