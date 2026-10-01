# Lakehouse CDC Bridge

Change data capture on Databricks for a bank's legacy sources. Changes are published to Kafka
(Confluent), stored in Delta Lake with a full audit trail, and turned into fraud alerts that
trace back to the exact source log position or file line.

**Website:** https://joaoblasques.github.io/lakehouse-cdc-bridge/ (business case, architecture, measured
results, alert traceability)

> Synthetic data only. "Banco Atlântico" is fictional.

## The problem

A bank's core data sits on several systems: a DB2 mainframe, a SQL Server cards platform,
SingleStore for instant payments, and partner files on an Azure File Share. Every downstream
team queries those systems directly, and fraud/AML checks run as next-day batch reports.

This project builds one capture layer that turns those sources into a single, governed event
stream, and measures what that is worth: latency from source commit to alert, precision and
recall against planted fraud, and reconciliation proving nothing was lost.

## How it works

```
SQL Server ──(native CDC, LSN watermark)────┐
Azure File Share ──(etag + sha256 manifest)─┤ 01 capture (Databricks for-each task)
DB2 ──(row-change timestamp + snapshot diff)┤
                                            ▼
                          Kafka: banking.cdc.<domain>.<entity>  (+ DLQ)
                                            │
   cdc_control · cdc_event_audit ◄──────────┤
                                            ▼
             02 Bronze (dedup on event_id) → 03 Silver (MERGE, newest wins, soft deletes)
                                            ├─ 04 reconcile (source ↔ Silver, audit ↔ Bronze)
                                            └─ 05 Gold fraud alerts (velocity, travel, structuring)
                                                    │ outbox → Kafka: banking.fraud.alerts
                                                    ▼
                                       fraud-alert service (FastAPI): analyst queue
```

- **CDC runs inside Databricks** and publishes to Kafka, the opposite of the Debezium-first
  pattern (see [ADR-001](site/decisions.html)).
- **At-least-once on the wire, exactly-once in Delta.** The order is produce → flush → audit →
  watermark, `event_id` is deterministic, Bronze deduplicates, and Silver ignores out-of-order
  changes.
- **Every source gets the CDC technique it can support**: log-based for SQL Server; a file
  manifest for Azure Files (which Auto Loader can't read); a row-change-timestamp watermark
  plus a periodic snapshot diff for DB2, which is how hard deletes are found without log
  access. SingleStore is next.
- **Config-driven.** `conf/sources.yml` defines tables, keys, topics and Silver schemas. The
  notebooks are thin wrappers over a tested Python package.

Details: [docs/design.md](docs/design.md).

## Run it locally

Full step-by-step procedure, with requirements and troubleshooting: the
[Setup guide](https://joaoblasques.github.io/lakehouse-cdc-bridge/setup.html). The short version:

Needs Docker, Java 17+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-extras
docker compose up -d --wait               # SQL Server 2022 (with Agent) + Confluent Kafka (KRaft)
docker compose --profile db2 up -d db2    # optional: Db2 11.5 (4 GB, a few minutes to initialise)
uv run pytest                             # unit + Spark/Delta tests
uv run pytest -m integration              # real SQL Server CDC, end to end
uv run pytest -m db2                      # real DB2: replayed events rebuild the table
uv run pytest -m kafka                    # real Kafka: alerts reach the service exactly once
uv run python scripts/run_local.py --fresh   # full pipeline → site/data/run_metrics.json
python -m http.server -d site 8000        # browse the results
docker compose --profile api up -d --build alerts-api   # analyst API on http://localhost:8080/docs
```

## Deploy to Databricks

```bash
databricks bundle deploy -t dev    # builds the wheel, deploys notebooks + Workflows job
databricks bundle run banking_cdc
```

Secrets (SQL Server, file share, Confluent API key) come from a Databricks secret scope; the
key names are listed on the [Deploy](https://joaoblasques.github.io/lakehouse-cdc-bridge/run.html) page.

## Repository

| Path | What |
|---|---|
| `src/banking_cdc/sources/` | CDC adapters: SQL Server (log), Azure File Share (manifest), DB2 (timestamp + snapshot) |
| `src/banking_cdc/pipeline/` | capture → Kafka, Bronze/Silver, reconciliation, Gold rules |
| `src/banking_cdc/generator.py` | synthetic bank with labelled fraud patterns |
| `src/banking_cdc/alerts_service/` | fraud-alert microservice: Kafka consumer + FastAPI analyst queue |
| `notebooks/` | Databricks tasks |
| `databricks.yml` | Asset Bundle: the Workflows job |
| `schemas/change_event.schema.json` | the event contract |
| `scripts/run_local.py` | end-to-end run that writes the website's numbers |
| `site/` | the website (static HTML/CSS/JS, deployed to GitHub Pages) |

## Roadmap

A SingleStore adapter, analyst decisions sent back as events, Schema Registry serialisation,
SCD2 history, an AI assistant that drafts `sources.yml` entries from DDL, and an MLflow fraud
model scored on the same harness as the rules.
