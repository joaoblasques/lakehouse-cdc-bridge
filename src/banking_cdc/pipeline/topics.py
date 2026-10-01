"""Provision topics explicitly. Auto-create is off: topic names, partitions and retention are
governed decisions, not side effects of the first producer."""

from __future__ import annotations


def ensure_topics(conf: dict[str, str], topics: list[str], partitions: int = 3) -> list[str]:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(conf)
    existing = set(admin.list_topics(timeout=15).topics)
    missing = [t for t in topics if t not in existing]
    if not missing:
        return []
    # Confluent Cloud requires replication factor 3; a single local broker can only do 1.
    rf = 3 if conf.get("security.protocol") == "SASL_SSL" else 1
    futures = admin.create_topics([NewTopic(t, partitions, rf) for t in missing])
    for future in futures.values():
        future.result(timeout=30)
    return missing
