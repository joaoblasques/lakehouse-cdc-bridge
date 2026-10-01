# Banking CDC: design

## Business case

**Banco Atlântico** (fictional) runs its core data on legacy systems: a mainframe (DB2), a cards
platform (SQL Server), a real-time payments store (SingleStore) and nightly partner files dropped
on an Azure File Share. Every downstream team queries those systems directly, and fraud/AML
checks run as T+1 batch reports.

This project builds one governed **change data capture layer on Databricks** that turns every
source into a single, standard event stream on Kafka (Confluent), keeps a Delta audit trail of
every event, and feeds near-real-time fraud alerts.

| Value lever | How the project measures it |
|---|---|
| Detection latency: T+1 → minutes | `alert_created_at − source_commit_ts`, per alert |
| Traceability (BCBS 239 accuracy/integrity) | every Gold row links to `event_id` → source position (LSN or file + line + sha256) |
| Source load: N extractions → 1 | consumers subscribe to topics instead of querying sources |
| Detection quality | generator plants labelled fraud; precision/recall reported per rule |

## Direction of CDC

The target team runs CDC *inside Databricks*: notebooks poll heterogeneous sources, detect
changes and **produce** events to Confluent. This project mirrors that direction. It does not
use Debezium.

```
 SQL Server ──(cdc.fn_cdc_get_all_changes, LSN watermark)──┐
 Azure File Share ──(file manifest: etag + sha256)─────────┤  01_cdc_capture (for-each source)
 DB2 / SingleStore ──(phase 2: watermark + hash diff)──────┘        │
                                                                    ▼
                                          Kafka (Confluent): banking.cdc.<domain>
                                                                    │
                   cdc_control (watermarks) ◄── capture ──► cdc_event_audit (event_id → offset)
                                                                    │
                                    02_bronze_ingest (availableNow, dedup on event_id)
                                                                    │
                                    03_silver_apply (MERGE, out-of-order guard)
                                                                    │
                          04_reconcile (source ↔ audit ↔ silver)   05_gold_fraud_alerts
```

## One CDC technique per source

| Source | Technique | MVP |
|---|---|---|
| SQL Server | Log-based: native CDC change tables, LSN watermark | yes |
| Azure File Share | File-arrival: SDK listing + Delta manifest (Auto Loader does not read Azure Files) | yes |
| DB2 | Query-based: `ROW CHANGE TIMESTAMP` watermark + periodic snapshot hash diff for deletes | phase 2 |
| SingleStore | Watermark + row hash (`OBSERVE` documented as the native option) | phase 2 |

All adapters implement one interface, `SourceAdapter.capture(watermark) -> CaptureResult`, so a
new source is a new adapter plus a `conf/sources.yml` entry, not a new pipeline.

## Event contract

`schemas/change_event.schema.json`. Key fields:

- `event_id`: sha256 of `source_system | table | primary key | source position`. Deterministic, so a
  replay after a crash produces the same id and downstream dedup makes delivery effectively
  exactly-once.
- `op`: `c` insert, `u` update, `d` delete, `r` snapshot read.
- `before` / `after`: row images (`before` is null for inserts, `after` is null for deletes).
- `source`: `{system, database, table, position, commit_ts}`. For SQL Server the position is the LSN
  plus sequence value; for files it is `path:line@sha256`.
- `captured_at`, `trace_id` (one per capture run).

Kafka key = `<table>:<primary key>`, so all changes to one row land in the same partition, in order.

## Delivery guarantees

Capture order per run: **produce → flush → write audit → commit watermark.**
A crash at any point before the watermark commit replays the same range with the same
`event_id`s. Bronze ingest deduplicates on `event_id` with a `MERGE`, and Silver applies a row only if
its source position is newer than the stored one. Result: at-least-once on the wire,
exactly-once in Delta.

## Delta tables

| Table | Purpose |
|---|---|
| `cdc_control` | one row per source/table: last watermark, last run, status |
| `cdc_file_manifest` | processed files: path, etag, size, sha256, rows, processed_at |
| `cdc_event_audit` | every produced event: event_id, topic, partition, offset, source position, payload hash |
| `bronze_change_events` | raw events from Kafka plus Kafka metadata, unique on `event_id` |
| `silver_<entity>` | current state per entity, with `_source_position`, `_event_id` |
| `dq_reconciliation` | per run: source count vs audit count vs silver count, status |
| `gold_fraud_alerts` | rule, account, evidence event_ids, alert time, latency |

## Orchestration

A Databricks Asset Bundle (`databricks.yml`) defines one Workflows job:
`capture` (a **for-each task** over `conf/sources.yml`) → `bronze` → `silver` → `reconcile` →
`gold`. The notebooks are thin: they read widgets/parameters and call the tested `banking_cdc`
package. Locally, `scripts/run_local.py` runs the same functions against Docker
(SQL Server + Confluent Kafka) and local Spark + Delta.

## Out of scope for the MVP (phase 2/3)

DB2 and SingleStore adapters (local Docker only, since neither has a usable free cloud tier for CDC),
the FastAPI fraud-alert microservice, Schema Registry serialization, SCD2 history, the AI
source-onboarding assistant, an MLflow fraud model.
