"""End-to-end local run: simulator → SQL Server + file share → CDC → Kafka → Delta → alerts.

Runs the same package functions the Databricks notebooks call, against `docker compose`
(SQL Server + Confluent Kafka) and local Spark + Delta. Writes the measured results to
site/data/run_metrics.json, which the project website renders.

    docker compose up -d && uv run python scripts/run_local.py --fresh
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / ".local"
NS = "banking_cdc"

os.environ.setdefault("LOCAL_WAREHOUSE", str(LOCAL / "warehouse"))
os.environ.setdefault("MSSQL_CARDS_HOST", "localhost")
os.environ.setdefault("MSSQL_CARDS_PORT", "1433")
os.environ.setdefault("MSSQL_CARDS_USER", "sa")
os.environ.setdefault(
    "MSSQL_CARDS_PASSWORD", os.environ.get("MSSQL_SA_PASSWORD", "LocalDev!Passw0rd")
)
os.environ.setdefault("FILESHARE_PARTNER_DIR", str(LOCAL / "fileshare" / "sepa" / "inbound"))
os.environ.setdefault("KAFKA_BOOTSTRAP", "localhost:9092")
os.environ.setdefault("DB2_CORE_HOST", "localhost")
os.environ.setdefault("DB2_CORE_PORT", "50000")
os.environ.setdefault("DB2_CORE_USER", "db2inst1")
os.environ.setdefault("DB2_CORE_PASSWORD", os.environ.get("DB2_PASSWORD", "LocalDev!Passw0rd"))

from banking_cdc.evaluate import percentile, score  # noqa: E402
from banking_cdc.generator import BankSimulator  # noqa: E402
from banking_cdc.pipeline.capture import run_capture  # noqa: E402
from banking_cdc.pipeline.config import (  # noqa: E402
    build_adapter,
    kafka_conf,
    load_config,
    topic_map,
)
from banking_cdc.pipeline.gold import run_gold  # noqa: E402
from banking_cdc.pipeline.medallion import run_bronze, run_silver, spark_kafka_options  # noqa: E402
from banking_cdc.pipeline.producer import make_producer  # noqa: E402
from banking_cdc.pipeline.quality import run_reconciliation  # noqa: E402
from banking_cdc.pipeline.spark import get_spark  # noqa: E402
from banking_cdc.pipeline.tables import ensure_tables  # noqa: E402
from banking_cdc.pipeline.topics import ensure_topics  # noqa: E402
from banking_cdc.seed import (  # noqa: E402
    apply_batch,
    apply_db2_batch,
    apply_db2_schema,
    apply_schema,
    connect,
    db2_connect,
    wait_for_capture,
    write_files,
)


def fresh_stack() -> None:
    """Recreate SQL Server and Kafka. DB2 is left running (it takes minutes to initialise);
    its table is emptied instead."""
    services = ["sqlserver", "kafka"]
    subprocess.run(["docker", "compose", "rm", "-sfv", *services], cwd=ROOT, check=True)
    shutil.rmtree(LOCAL, ignore_errors=True)
    subprocess.run(["docker", "compose", "up", "-d", "--wait", *services], cwd=ROOT, check=True)
    time.sleep(5)  # broker listener settles after the container reports started


def db2_available() -> bool:
    try:
        db2_connect().close()
        return True
    except Exception:
        return False


def timed(fn, *args, **kwargs):
    t = time.monotonic()
    out = fn(*args, **kwargs)
    return out, round(time.monotonic() - t, 2)


def lineage(spark, alert) -> list[dict]:
    """Every source change behind one alert: Kafka coordinates and source position."""
    table, key = (
        ("partner_transfers", "transfer_id")
        if alert.rule == "structuring"
        else ("dbo.card_transactions", "tx_id")
    )
    keys = ",".join(f"'{k}'" for k in alert.evidence_keys)
    rows = spark.sql(
        f"""SELECT b.event_id, b.op, b.source_system, b.source_table, b.source_position,
                   CAST(b.source_commit_ts AS STRING) AS source_commit_ts,
                   b.kafka_topic, b.kafka_partition, b.kafka_offset,
                   get_json_object(b.key_json, '$.{key}') AS entity_key, b.after_json
            FROM {NS}.bronze_change_events b
            WHERE b.source_table = '{table}'
              AND CAST(get_json_object(b.key_json, '$.{key}') AS STRING) IN ({keys})
            ORDER BY b.source_sequence"""
    ).collect()
    return [r.asDict() for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--customers", type=int, default=150)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--fresh", action="store_true", help="recreate containers and Delta")
    parser.add_argument("--out", default=str(ROOT / "site" / "data" / "run_metrics.json"))
    args = parser.parse_args()

    if args.fresh:
        fresh_stack()
    cfg = load_config()
    apply_schema()
    with_db2 = db2_available()
    if with_db2:
        apply_db2_schema()
        if args.fresh:
            reset = db2_connect()
            reset.cursor().execute("DELETE FROM CORE.LOANS")
            reset.commit()
            reset.close()
    else:
        print("DB2 not reachable: running without the db2_core source", flush=True)
        cfg["sources"].pop("db2_core", None)
    db2_conn = db2_connect() if with_db2 else None
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    ensure_tables(spark, NS, cfg)

    adapters = {name: build_adapter(name, cfg) for name in cfg["sources"]}
    producer = make_producer(kafka_conf())
    topics = {n: topic_map(s) for n, s in cfg["sources"].items()}
    all_topics = sorted({t for m in topics.values() for t in m.values()})
    kopts = spark_kafka_options(kafka_conf())
    ensure_topics(kafka_conf(), [*all_topics, cfg["kafka"]["dlq_topic"]])
    share_dir = Path(os.environ["FILESHARE_PARTNER_DIR"])

    sim = BankSimulator(
        seed=args.seed, start=datetime(2026, 10, 1, 8, tzinfo=UTC), n_customers=args.customers
    )
    truth: list[dict] = []
    rounds = []
    with connect() as conn:
        for r in range(args.rounds + 1):
            batch = sim.initial_load() if r == 0 else sim.next_round()
            truth += batch.ground_truth
            apply_batch(conn, batch)
            write_files(share_dir, batch)
            if db2_conn:
                apply_db2_batch(db2_conn, batch)
            lag = wait_for_capture(conn, beat=int(time.time() * 1000) % 2_000_000_000)
            if db2_conn:
                time.sleep(2.5)  # DB2 adapter skips rows younger than its 2 s safety lag

            steps, captures = {}, []
            for name, adapter in adapters.items():
                summary, secs = timed(
                    run_capture,
                    spark,
                    NS,
                    name,
                    adapter,
                    producer,
                    topics[name],
                    cfg["kafka"]["dlq_topic"],
                )
                captures.append(summary)
                steps[f"capture:{name}"] = secs
            _, steps["bronze"] = timed(
                run_bronze, spark, NS, kopts, all_topics, str(LOCAL / "checkpoints" / "bronze")
            )
            _, steps["silver"] = timed(
                run_silver, spark, NS, cfg, str(LOCAL / "checkpoints" / "silver")
            )
            _, steps["gold"] = timed(run_gold, spark, NS)
            stats = {n: a.source_stats() for n, a in adapters.items()}
            checks, steps["reconcile"] = timed(run_reconciliation, spark, NS, cfg, stats)
            rounds.append(
                {
                    "round": r,
                    "label": "initial load" if r == 0 else f"hour {r}",
                    "source_ops": len(batch.sql_ops),
                    "files": len(batch.files),
                    "capture_job_lag_s": round(lag, 2),
                    "captures": captures,
                    "step_seconds": steps,
                    "reconciliation": [
                        {
                            k: c[k]
                            for k in (
                                "table_name",
                                "check_name",
                                "source_value",
                                "target_value",
                                "status",
                            )
                        }
                        for c in checks
                    ],
                }
            )
            print(
                f"round {r}: {sum(c['events'] for c in captures)} events, steps {steps}", flush=True
            )

    alerts = spark.table(f"{NS}.gold_fraud_alerts").collect()
    alert_dicts = [
        {"rule": a.rule, "account_id": a.account_id, "evidence_keys": list(a.evidence_keys)}
        for a in alerts
    ]
    latencies = [a.latency_seconds for a in alerts if a.latency_seconds is not None]
    by_rule = score(alert_dicts, truth)
    samples = []
    for rule in ("card_velocity", "impossible_travel", "structuring"):
        hit = next(
            (
                a
                for a in alerts
                if a.rule == rule
                and by_rule[rule]["true_positives"]
                and any(
                    set(a.evidence_keys)
                    & {str(x) for x in t.get("tx_ids", []) + t.get("transfer_ids", [])}
                    for t in truth
                    if t["rule"] == rule
                )
            ),
            None,
        )
        if hit:
            samples.append(
                {
                    "alert_id": hit.alert_id,
                    "rule": hit.rule,
                    "account_id": hit.account_id,
                    "alert_created_at": str(hit.alert_created_at),
                    "latency_seconds": round(hit.latency_seconds, 2),
                    "events": lineage(spark, hit),
                }
            )

    counts = {
        t: spark.table(f"{NS}.{t}").count()
        for t in (
            "cdc_event_audit",
            "bronze_change_events",
            "cdc_file_manifest",
            "silver_customers",
            "silver_accounts",
            "silver_card_transactions",
            "silver_partner_transfers",
            "gold_fraud_alerts",
        )
    }
    ops = spark.sql(
        f"SELECT source_name, source_table, op, COUNT(*) AS n FROM {NS}.cdc_event_audit "
        "GROUP BY ALL ORDER BY ALL"
    ).collect()
    metrics = {
        "generated_at": datetime.now(UTC).isoformat(),
        "environment": "local: docker compose (SQL Server 2022"
        + (", Db2 11.5" if with_db2 else "")
        + " + Confluent cp-kafka 7.7, KRaft) "
        f"+ Spark {spark.version} / Delta Lake, 4 cores",
        "params": vars(args) | {"out": None},
        "rounds": rounds,
        "table_counts": counts,
        "events_by_table_op": [r.asDict() for r in ops],
        "rejects_to_dlq": sum(c["rejects"] for r in rounds for c in r["captures"]),
        "detection": by_rule,
        "latency_seconds": {
            "p50": percentile(latencies, 0.5),
            "p95": percentile(latencies, 0.95),
            "max": round(max(latencies), 2) if latencies else None,
            "n": len(latencies),
            "by_alert": [
                {"rule": a.rule, "seconds": round(a.latency_seconds, 2)}
                for a in alerts
                if a.latency_seconds is not None
            ],
        },
        "lineage_samples": samples,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(metrics, indent=2, default=str))
    print(
        json.dumps(
            {
                k: metrics[k]
                for k in ("table_counts", "detection", "latency_seconds", "rejects_to_dlq")
            },
            indent=2,
        )
    )
    producer.flush()
    spark.stop()


if __name__ == "__main__":
    main()
