"""Gold: fraud/AML alerts over Silver, each traceable to the source changes that caused it.

Rules (thresholds are illustrative, not a bank's real policy):
  card_velocity      >= 5 card transactions on one account within 10 minutes
  impossible_travel  consecutive card transactions in two countries within 60 minutes
  structuring        >= 3 outgoing partner transfers of 9,000-9,999.99 EUR within 24 hours
"""

from __future__ import annotations

from banking_cdc.pipeline.tables import name


def _burst_sql(base: str, id_col: str, ts_col: str, min_count: int, window_s: int, rule: str):
    """Gaps-and-islands: one alert per burst of >= min_count events within window_s seconds."""
    return f"""
    WITH base AS ({base}),
    t AS (SELECT *, unix_timestamp({ts_col}) AS s FROM base),
    w AS (
        SELECT *, COUNT(*) OVER (PARTITION BY account_id ORDER BY s
                                 RANGE BETWEEN CURRENT ROW AND {window_s} FOLLOWING) AS n
        FROM t),
    starts AS (
        SELECT account_id, s,
               LAG(s) OVER (PARTITION BY account_id ORDER BY s) AS prev_s
        FROM w WHERE n >= {min_count}),
    bursts AS (SELECT account_id, s FROM starts WHERE prev_s IS NULL OR s - prev_s > {window_s})
    SELECT '{rule}' AS rule, b.account_id,
           array_sort(collect_list(CAST(t.{id_col} AS STRING))) AS evidence_keys,
           MIN(t.{ts_col}) AS first_evidence_ts,
           MAX(t._first_commit_ts) AS last_source_commit_ts
    FROM bursts b JOIN t ON t.account_id = b.account_id AND t.s BETWEEN b.s AND b.s + {window_s}
    GROUP BY b.account_id, b.s"""


def alert_queries(ns: str) -> dict[str, str]:
    # Soft-deleted card transactions stay in: an expired authorisation is still an attempt.
    cards = f"SELECT * FROM {name(ns, 'silver_card_transactions')}"
    outgoing = f"""
        SELECT p.*, a.account_id FROM {name(ns, "silver_partner_transfers")} p
        JOIN {name(ns, "silver_accounts")} a ON p.debtor_iban = a.iban
        WHERE NOT p._is_deleted AND p.amount >= 9000 AND p.amount < 10000"""
    travel = f"""
        SELECT 'impossible_travel' AS rule, account_id,
               array(CAST(prev_id AS STRING), CAST(tx_id AS STRING)) AS evidence_keys,
               prev_ts AS first_evidence_ts,
               greatest(prev_commit, _first_commit_ts) AS last_source_commit_ts
        FROM (
            SELECT *, LAG(country) OVER w AS prev_country, LAG(tx_ts) OVER w AS prev_ts,
                      LAG(tx_id) OVER w AS prev_id, LAG(_first_commit_ts) OVER w AS prev_commit
            FROM ({cards}) WINDOW w AS (PARTITION BY account_id ORDER BY tx_ts, tx_id))
        WHERE prev_country IS NOT NULL AND country <> prev_country
          AND unix_timestamp(tx_ts) - unix_timestamp(prev_ts) <= 3600"""
    return {
        "card_velocity": _burst_sql(cards, "tx_id", "tx_ts", 5, 600, "card_velocity"),
        "impossible_travel": travel,
        "structuring": _burst_sql(outgoing, "transfer_id", "booking_ts", 3, 86_400, "structuring"),
    }


def run_gold(spark, ns: str) -> int:
    """MERGE on a stable alert_id, so re-runs never duplicate alerts or reset their timestamps."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    frames = [spark.sql(q) for q in alert_queries(ns).values()]
    alerts = frames[0]
    for f in frames[1:]:
        alerts = alerts.unionByName(f)
    alerts = (
        alerts.withColumn(
            "alert_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    "rule",
                    F.col("account_id").cast("string"),
                    F.element_at("evidence_keys", 1),
                ),
                256,
            ),
        )
        .withColumn("alert_created_at", F.current_timestamp())
        .withColumn("published_at", F.lit(None).cast("timestamp"))
        .withColumn(
            "latency_seconds",
            F.unix_micros("alert_created_at").cast("double") / 1e6
            - F.unix_micros("last_source_commit_ts").cast("double") / 1e6,
        )
    )
    (
        DeltaTable.forName(spark, name(ns, "gold_fraud_alerts"))
        .alias("t")
        .merge(alerts.alias("s"), "t.alert_id = s.alert_id")
        .whenMatchedUpdate(set={"evidence_keys": "s.evidence_keys"})
        .whenNotMatchedInsertAll()
        .execute()
    )
    return spark.table(name(ns, "gold_fraud_alerts")).count()
