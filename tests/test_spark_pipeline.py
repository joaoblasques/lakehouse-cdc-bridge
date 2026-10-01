"""Spark + Delta behaviour of Silver and Gold, without Kafka (Bronze rows are built directly)."""

import json
from datetime import datetime, timedelta

import pytest

from banking_cdc.pipeline.config import load_config
from banking_cdc.pipeline.gold import run_gold
from banking_cdc.pipeline.medallion import apply_silver_batch
from banking_cdc.pipeline.tables import ensure_tables

pytestmark = pytest.mark.spark

CFG = load_config()
TABLES = {t["table"]: t for s in CFG["sources"].values() for t in s["tables"]}
T0 = datetime(2026, 10, 1, 9, 0, 0)


@pytest.fixture()
def ns(spark, request):
    ns = f"t_{request.node.name[:40]}"
    spark.sql(f"DROP SCHEMA IF EXISTS {ns} CASCADE")
    ensure_tables(spark, ns, CFG)
    return ns


def _bronze(spark, rows):
    cols = (
        "event_id op source_table source_sequence source_commit_ts before_json after_json key_json"
    )
    data = [
        (
            r["event_id"],
            r["op"],
            r["table"],
            r["seq"],
            r.get("commit_ts", T0),
            json.dumps(r.get("before")) if r.get("before") else None,
            json.dumps(r.get("after")) if r.get("after") else None,
            json.dumps(r["key"]) if r.get("key") else None,
        )
        for r in rows
    ]
    return spark.createDataFrame(
        data,
        "event_id STRING, op STRING, source_table STRING, source_sequence STRING, "
        "source_commit_ts TIMESTAMP, before_json STRING, after_json STRING, key_json STRING",
    ).toDF(*cols.split())


def _acct(balance, status="ACTIVE", account_id=1):
    return {
        "account_id": account_id,
        "customer_id": 1,
        "iban": f"PT50IBAN{account_id}",
        "account_type": "CURRENT",
        "balance": balance,
        "status": status,
        "updated_at": "2026-10-01T09:00:00",
    }


def _apply(spark, ns, rows, table="accounts", source_table="dbo.accounts"):
    apply_silver_batch(spark, ns, _bronze(spark, rows), TABLES[table], source_table)


def test_latest_change_per_key_wins_within_a_batch(spark, ns):
    _apply(
        spark,
        ns,
        [
            {
                "event_id": "e1",
                "op": "c",
                "table": "dbo.accounts",
                "seq": "0x01",
                "after": _acct("100.00"),
            },
            {
                "event_id": "e2",
                "op": "u",
                "table": "dbo.accounts",
                "seq": "0x02",
                "after": _acct("80.00"),
            },
        ],
    )
    row = spark.table(f"{ns}.silver_accounts").collect()
    assert len(row) == 1
    assert str(row[0].balance) == "80.00"
    assert row[0]._event_id == "e2"


def test_older_change_arriving_late_does_not_overwrite(spark, ns):
    _apply(
        spark,
        ns,
        [
            {
                "event_id": "e2",
                "op": "u",
                "table": "dbo.accounts",
                "seq": "0x02",
                "after": _acct("80.00"),
            }
        ],
    )
    _apply(
        spark,
        ns,
        [
            {
                "event_id": "e1",
                "op": "u",
                "table": "dbo.accounts",
                "seq": "0x01",
                "after": _acct("100.00"),
            }
        ],
    )
    assert str(spark.table(f"{ns}.silver_accounts").first().balance) == "80.00"


def test_delete_is_a_soft_delete_and_replay_is_harmless(spark, ns):
    create = {
        "event_id": "e1",
        "op": "c",
        "table": "dbo.accounts",
        "seq": "0x01",
        "after": _acct("5.00"),
    }
    delete = {
        "event_id": "e2",
        "op": "d",
        "table": "dbo.accounts",
        "seq": "0x02",
        "before": _acct("5.00"),
    }
    _apply(spark, ns, [create, delete])
    _apply(spark, ns, [create])  # replay of an old event after a crash
    row = spark.table(f"{ns}.silver_accounts").first()
    assert row._is_deleted is True
    assert row._event_id == "e2"


def test_first_commit_ts_survives_updates(spark, ns):
    _apply(
        spark,
        ns,
        [
            {
                "event_id": "e1",
                "op": "c",
                "table": "dbo.accounts",
                "seq": "0x01",
                "after": _acct("1.00"),
                "commit_ts": T0,
            }
        ],
    )
    _apply(
        spark,
        ns,
        [
            {
                "event_id": "e2",
                "op": "u",
                "table": "dbo.accounts",
                "seq": "0x02",
                "after": _acct("2.00"),
                "commit_ts": T0 + timedelta(hours=1),
            }
        ],
    )
    row = spark.table(f"{ns}.silver_accounts").first()
    assert row._first_commit_ts == T0
    assert row._last_commit_ts == T0 + timedelta(hours=1)


def _tx(tx_id, account_id, minutes, country="PT"):
    return {
        "event_id": f"tx{tx_id}",
        "op": "c",
        "table": "dbo.card_transactions",
        "seq": f"0x{tx_id:04x}",
        "after": {
            "tx_id": tx_id,
            "account_id": account_id,
            "amount": "10.00",
            "currency": "EUR",
            "merchant": "M",
            "mcc": "5411",
            "country": country,
            "status": "AUTHORISED",
            "tx_ts": (T0 + timedelta(minutes=minutes)).isoformat(),
        },
    }


def test_gold_rules_detect_velocity_and_travel_once(spark, ns):
    burst = [_tx(i, 1, i) for i in range(1, 6)]  # 5 tx in 4 minutes
    calm = [_tx(10 + i, 2, i * 30) for i in range(5)]  # 5 tx, 30 min apart
    travel = [_tx(20, 3, 0, "PT"), _tx(21, 3, 20, "BR")]
    _apply(spark, ns, burst + calm + travel, "card_transactions", "dbo.card_transactions")
    run_gold(spark, ns)
    run_gold(spark, ns)  # idempotent
    alerts = {(r.rule, r.account_id): r for r in spark.table(f"{ns}.gold_fraud_alerts").collect()}
    assert set(alerts) == {("card_velocity", 1), ("impossible_travel", 3)}
    assert alerts[("card_velocity", 1)].evidence_keys == ["1", "2", "3", "4", "5"]
    assert alerts[("impossible_travel", 3)].evidence_keys == ["20", "21"]


def _loan(status, outstanding):
    return {
        "LOAN_ID": 7,
        "CUSTOMER_ID": 3,
        "PRODUCT": "PERSONAL",
        "PRINCIPAL": "5000.00",
        "OUTSTANDING": outstanding,
        "RATE_PCT": "6.250",
        "STATUS": status,
        "OPENED_ON": "2026-10-01",
    }


def test_delete_without_before_image_keeps_last_values_and_flags_row(spark, ns):
    """DB2 query-based CDC can't see a deleted row, so its delete events carry only the key."""
    rows = [
        {
            "event_id": "l1",
            "op": "c",
            "table": "CORE.LOANS",
            "seq": "2026-10-01T09:00:00.000001",
            "after": _loan("APPLIED", "0.00"),
        },
        {
            "event_id": "l2",
            "op": "d",
            "table": "CORE.LOANS",
            "seq": "2026-10-01T09:05:00.000000|snapshot",
            "key": {"LOAN_ID": 7},
        },
    ]
    apply_silver_batch(spark, ns, _bronze(spark, rows), TABLES["LOANS"], "CORE.LOANS")
    row = spark.table(f"{ns}.silver_loans").collect()
    assert len(row) == 1
    assert row[0]._is_deleted is True
    assert row[0].STATUS == "APPLIED"  # last known values kept, not nulled
    assert str(row[0].PRINCIPAL) == "5000.00"


def test_key_only_delete_in_a_later_batch_keeps_values(spark, ns):
    create = {
        "event_id": "l1",
        "op": "c",
        "table": "CORE.LOANS",
        "seq": "2026-10-01T09:00:00.000001",
        "after": _loan("ACTIVE", "4000.00"),
    }
    delete = {
        "event_id": "l2",
        "op": "d",
        "table": "CORE.LOANS",
        "seq": "2026-10-01T09:05:00.000000|snapshot",
        "key": {"LOAN_ID": 7},
    }
    _apply(spark, ns, [create], "LOANS", "CORE.LOANS")
    _apply(spark, ns, [delete], "LOANS", "CORE.LOANS")
    row = spark.table(f"{ns}.silver_loans").first()
    assert row._is_deleted is True and row._event_id == "l2"
    assert str(row.OUTSTANDING) == "4000.00"


def test_reconciliation_skips_row_count_until_the_snapshot_pass(spark, ns):
    import pytest

    from banking_cdc.pipeline.capture import commit_watermark
    from banking_cdc.pipeline.quality import ReconciliationError, run_reconciliation

    cfg = {"sources": {"db2_core": CFG["sources"]["db2_core"]}}
    _apply(
        spark,
        ns,
        [
            {
                "event_id": "l1",
                "op": "c",
                "table": "CORE.LOANS",
                "seq": "2026-10-01T09:00:00.000001",
                "after": _loan("ACTIVE", "1.00"),
            }
        ],
        "LOANS",
        "CORE.LOANS",
    )
    stats = {"db2_core": {"CORE.LOANS": {"rows": 0}}}  # deleted at source, not yet seen

    def wm(runs):
        commit_watermark(
            spark,
            ns,
            {
                "source_name": "db2_core",
                "watermark": json.dumps({"runs_since_snapshot": runs}),
                "last_trace_id": "t",
                "events_captured": 0,
                "rejects": 0,
                "status": "OK",
                "updated_at": "2026-10-01T09:00:00",
            },
        )

    wm(1)
    checks = run_reconciliation(spark, ns, cfg, stats)
    assert [c["status"] for c in checks if c["check_name"] == "row_count"] == ["SKIP"]
    wm(0)  # a snapshot just ran: the mismatch is real now
    with pytest.raises(ReconciliationError):
        run_reconciliation(spark, ns, cfg, stats)
