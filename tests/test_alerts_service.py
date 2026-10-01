import json

import pytest

from banking_cdc.alerts_service.consumer import handle_message
from banking_cdc.alerts_service.store import AlertStore
from banking_cdc.pipeline.alerts import alert_message, publish_alerts
from banking_cdc.pipeline.producer import DeliveryError

ALERT = {
    "alert_id": "a1",
    "rule": "card_velocity",
    "account_id": 42,
    "evidence_keys": ["101", "102", "103", "104", "105"],
    "first_evidence_ts": "2026-10-01T09:00:00",
    "last_source_commit_ts": "2026-10-01T10:00:00",
    "alert_created_at": "2026-10-01T10:00:25",
    "latency_seconds": 25.0,
}


# ---- message contract -------------------------------------------------------------------------


def test_alert_message_is_json_safe_and_versioned():
    from datetime import datetime

    msg = alert_message(ALERT | {"alert_created_at": datetime(2026, 10, 1, 10, 0, 25)})
    assert msg["schema_version"] == 1
    assert msg["alert_created_at"] == "2026-10-01T10:00:25"
    json.dumps(msg)


class FakeProducer:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def produce(self, topic, key, value, headers=None, on_delivery=None):
        self.sent.append((topic, key, json.loads(value)))
        on_delivery("boom" if self.fail else None, None)

    def poll(self, _):
        return 0

    def flush(self, _):
        return 0


def test_alerts_are_keyed_by_account_so_one_account_stays_in_order():
    p = FakeProducer()
    ids = publish_alerts(p, [ALERT], "banking.fraud.alerts")
    assert ids == ["a1"]
    assert p.sent[0][0] == "banking.fraud.alerts"
    assert p.sent[0][1] == b"42"


def test_unconfirmed_publish_raises_so_the_outbox_is_not_marked():
    with pytest.raises(DeliveryError):
        publish_alerts(FakeProducer(fail=True), [ALERT], "t")


# ---- store ------------------------------------------------------------------------------------


@pytest.fixture()
def store(tmp_path):
    return AlertStore(tmp_path / "alerts.db")


def test_redelivered_alert_is_stored_once_and_keeps_the_analyst_decision(store):
    assert store.upsert(ALERT) is True
    store.set_status("a1", "CONFIRMED", "called the customer")
    assert (
        store.upsert(ALERT | {"evidence_keys": ["101", "102", "103", "104", "105", "106"]}) is False
    )
    a = store.get("a1")
    assert a["status"] == "CONFIRMED"
    assert a["note"] == "called the customer"
    assert a["evidence_keys"][-1] == "106"  # newer evidence is kept
    assert store.stats()["total"] == 1


def test_list_filters_by_status_and_rule(store):
    store.upsert(ALERT)
    store.upsert(ALERT | {"alert_id": "a2", "rule": "structuring", "account_id": 7})
    store.set_status("a2", "DISMISSED", None)
    assert [a["alert_id"] for a in store.list(status="OPEN")] == ["a1"]
    assert [a["alert_id"] for a in store.list(rule="structuring")] == ["a2"]


def test_unknown_status_is_rejected(store):
    store.upsert(ALERT)
    with pytest.raises(ValueError):
        store.set_status("a1", "MAYBE", None)


# ---- consumer ---------------------------------------------------------------------------------


class FakeMsg:
    def __init__(self, value, error=None):
        self._v, self._e = value, error

    def value(self):
        return self._v

    def error(self):
        return self._e


def test_consumer_stores_valid_messages_and_counts_bad_ones(store):
    assert handle_message(FakeMsg(json.dumps(alert_message(ALERT)).encode()), store) == "stored"
    assert handle_message(FakeMsg(b"not json"), store) == "rejected"
    assert handle_message(FakeMsg(json.dumps({"rule": "x"}).encode()), store) == "rejected"
    assert store.stats()["total"] == 1


# ---- API --------------------------------------------------------------------------------------


@pytest.fixture()
def client(store):
    from fastapi.testclient import TestClient

    from banking_cdc.alerts_service.api import create_app

    store.upsert(ALERT)
    return TestClient(create_app(store))


def test_api_lists_and_gets_alerts(client):
    assert client.get("/health").json()["status"] == "ok"
    body = client.get("/alerts", params={"status": "OPEN"}).json()
    assert [a["alert_id"] for a in body] == ["a1"]
    assert client.get("/alerts/a1").json()["rule"] == "card_velocity"
    assert client.get("/alerts/nope").status_code == 404


def test_api_records_the_analyst_decision(client):
    r = client.patch("/alerts/a1", json={"status": "CONFIRMED", "note": "card blocked"})
    assert r.status_code == 200
    assert r.json()["status"] == "CONFIRMED"
    assert r.json()["decided_at"] is not None
    assert client.patch("/alerts/a1", json={"status": "MAYBE"}).status_code == 422
    assert client.patch("/alerts/nope", json={"status": "DISMISSED"}).status_code == 404
    stats = client.get("/stats").json()
    assert stats["by_status"] == {"CONFIRMED": 1}
