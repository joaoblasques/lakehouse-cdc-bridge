# Banking CDC

Change data capture on Databricks for a bank's legacy sources. Changes are published to Kafka
(Confluent), stored in Delta Lake with a full audit trail, and turned into fraud alerts that
trace back to the exact source log position or file line.

**Website:** https://joaoblasques.github.io/Banking_cdc/ (business case, architecture, measured
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
                                            ▼
                          Kafka: banking.cdc.<domain>.<entity>  (+ DLQ)
                                            │
   cdc_control · cdc_event_audit ◄──────────┤
                                            ▼
             02 Bronze (dedup on event_id) → 03 Silver (MERGE, newest wins, soft deletes)
                                            ├─ 04 reconcile (source ↔ Silver, audit ↔ Bronze)
                                            └─ 05 Gold fraud alerts (velocity, travel, structuring)
```

- **CDC runs inside Databricks** and publishes to Kafka, the opposite of the Debezium-first
  pattern (see [ADR-001](site/decisions.html)).
- **At-least-once on the wire, exactly-once in Delta.** The order is produce → flush → audit →
  watermark, `event_id` is deterministic, Bronze deduplicates, and Silver ignores out-of-order
  changes.
- **Every source gets the CDC technique it can support**: log-based for SQL Server; a file
  manifest for Azure Files (which Auto Loader can't read); watermark plus hash diff for DB2
  and SingleStore (phase 2).
- **Config-driven.** `conf/sources.yml` defines tables, keys, topics and Silver schemas. The
  notebooks are thin wrappers over a tested Python package.

Details: [docs/design.md](docs/design.md).

## Run it locally

Needs Docker, Java 17+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-extras
docker compose up -d --wait               # SQL Server 2022 (with Agent) + Confluent Kafka (KRaft)
uv run pytest                             # unit + Spark/Delta tests
uv run pytest -m integration              # real SQL Server CDC, end to end
uv run python scripts/run_local.py --fresh   # full pipeline → site/data/run_metrics.json
python -m http.server -d site 8000        # browse the results
```

## Deploy to Databricks

```bash
databricks bundle deploy -t dev    # builds the wheel, deploys notebooks + Workflows job
databricks bundle run banking_cdc
```

Secrets (SQL Server, file share, Confluent API key) come from a Databricks secret scope; the
key names are listed on the [Run it](site/run.html) page.

## Repository

| Path | What |
|---|---|
| `src/banking_cdc/sources/` | CDC adapters: SQL Server (log), Azure File Share (manifest) |
| `src/banking_cdc/pipeline/` | capture → Kafka, Bronze/Silver, reconciliation, Gold rules |
| `src/banking_cdc/generator.py` | synthetic bank with labelled fraud patterns |
| `notebooks/` | Databricks tasks |
| `databricks.yml` | Asset Bundle: the Workflows job |
| `schemas/change_event.schema.json` | the event contract |
| `scripts/run_local.py` | end-to-end run that writes the website's numbers |
| `site/` | the website (static HTML/CSS/JS, deployed to GitHub Pages) |

## Roadmap

DB2 and SingleStore adapters, a FastAPI fraud-alert microservice consuming from Kafka, Schema
Registry serialisation, SCD2 history, an AI assistant that drafts `sources.yml` entries from
DDL, and an MLflow fraud model scored on the same harness as the rules.
