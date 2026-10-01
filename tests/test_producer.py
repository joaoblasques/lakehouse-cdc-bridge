import json

import pytest

from banking_cdc.envelope import SourceInfo, make_event
from banking_cdc.pipeline.producer import DeliveryError, publish
from banking_cdc.sources.base import Reject


class FakeMsg:
    def __init__(self, topic, partition, offset):
        self._t, self._p, self._o = topic, partition, offset

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeProducer:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def produce(self, topic, key, value, headers=None, on_delivery=None):
        self.sent.append((topic, key, value))
        if on_delivery:
            err = "broker down" if self.fail else None
            on_delivery(err, FakeMsg(topic, 0, len(self.sent) - 1))

    def poll(self, _):
        return 0

    def flush(self, _):
        return 0


def _event(pk):
    src = SourceInfo("sqlserver", "cards", "dbo.accounts", f"0x{pk:04x}:0x01")
    return make_event("c", src, {"account_id": pk}, None, {"account_id": pk}, "run")


TOPICS = {"dbo.accounts": "banking.cdc.cards.accounts"}


def test_events_go_to_their_table_topic_keyed_by_row():
    p = FakeProducer()
    audit = publish(p, [_event(1), _event(2)], TOPICS, [], "dlq", "sqlserver_cards")
    assert [s[0] for s in p.sent] == ["banking.cdc.cards.accounts"] * 2
    assert p.sent[0][1] == b"dbo.accounts:account_id=1"
    assert json.loads(p.sent[0][2])["op"] == "c"
    assert [a["kafka_offset"] for a in audit] == [0, 1]
    assert audit[0]["source_position"] == "0x0001:0x01"


def test_rejects_go_to_dlq_and_are_not_audited_as_events():
    p = FakeProducer()
    reject = Reject("azure_fileshare", "f.csv:3@abc", "invalid amount", {"amount": "x"})
    audit = publish(p, [], TOPICS, [reject], "banking.cdc.dlq", "partner_fileshare")
    assert audit == []
    assert p.sent[0][0] == "banking.cdc.dlq"
    assert json.loads(p.sent[0][2])["reason"] == "invalid amount"


def test_failed_delivery_fails_the_run():
    with pytest.raises(DeliveryError):
        publish(FakeProducer(fail=True), [_event(1)], TOPICS, [], "dlq", "s")
