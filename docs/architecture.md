# Architecture — Azure Event Hub Streaming Ingest

## Overview

Two parallel streaming pipelines consume events from Azure Event Hubs and land them into a Delta Lake staging layer via a Parquet raw zone intermediate.

---

## End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCE SYSTEMS                              │
│   Transaction Service          Activity / Disposition Service   │
└────────────┬───────────────────────────────┬────────────────────┘
             │  publishes events              │  publishes events
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   AZURE EVENT HUBS                              │
│   Topic: transactions              Topic: activities            │
│   Capture: enabled (Avro)          Capture: enabled (Avro)      │
└────────────┬───────────────────────────────┬────────────────────┘
             │  .avro files written           │  .avro files written
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              ADLS Gen2 — Unity Catalog Volumes                  │
│   /eventhub/transactions/*.avro    /eventhub/activities/*.avro  │
└────────────┬───────────────────────────────┬────────────────────┘
             │  Auto Loader (cloudFiles)      │  Auto Loader (cloudFiles)
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│           AZURE DATABRICKS — Structured Streaming               │
│                                                                 │
│  transaction_ingest.py              activity_ingest.py          │
│  ┌──────────────────────┐           ┌──────────────────────┐   │
│  │ 1. Read Avro stream  │           │ 1. Read Avro stream  │   │
│  │ 2. Cast Body→string  │           │ 2. Cast Body→string  │   │
│  │ 3. Validate JSON UDF │           │ 3. Validate JSON UDF │   │
│  │ 4. Load dyn. schema  │           │ 4. Load dyn. schema  │   │
│  │ 5. Parse JSON fields │           │ 5. Parse + EXPLODE   │   │
│  │ 6. Null filter       │           │    salesrep array    │   │
│  │ 7. Write Parquet     │           │ 6. Null filter       │   │
│  │ 8. ROW_NUMBER dedup  │           │ 7. Write Parquet     │   │
│  │ 9. Delta MERGE       │           │ 8. ROW_NUMBER dedup  │   │
│  └──────────────────────┘           │ 9. Delta MERGE       │   │
│                                     └──────────────────────┘   │
└────────────┬───────────────────────────────┬────────────────────┘
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              ADLS Gen2 — Parquet Raw Zone (raw_ext)             │
│   transactions_raw_ext/             activities_raw_ext/         │
│   (external table over Parquet)     (external table over Parquet│
└────────────┬───────────────────────────────┬────────────────────┘
             │  ROW_NUMBER window dedup       │
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              DELTA LAKE — Staging Tables                        │
│   staging_db.transactions           staging_db.activities       │
│   (MERGE — upsert on natural key)   (MERGE — upsert on nat. key)│
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. Auto Loader (`cloudFiles`) over manual file listing
Auto Loader tracks which files have been processed using a checkpoint directory. No manual bookmarking needed. Supports schema evolution via `cloudFiles.schemaLocation`.

### 2. `trigger(availableNow=True)` + `processAllAvailable()`
Rather than a continuously running stream (which holds a cluster alive), each pipeline run drains all backlogged files then exits. This is cost-efficient and integrates cleanly with Databricks Workflows scheduling.

### 3. JSON Validation UDF before schema parsing
A malformed JSON message would crash `from_json` silently (null row) or cause downstream failures. The UDF explicitly filters invalid messages before parsing, making failure visible in record counts.

### 4. Dynamic schema from config path
The event schema is not hardcoded in the notebook. It's stored as a JSON schema file in a Unity Catalog Volume. Schema changes only require updating the schema file — no notebook edits or redeployments.

### 5. Two-stage write: Parquet raw zone → Delta MERGE
Writing first to a Parquet external table (`raw_ext`) and then merging into a Delta managed table gives:
- A full raw audit trail (immutable, append-only Parquet)
- An idempotent, deduplicated Delta table for downstream consumers
- Easy reprocessing — replay from raw_ext by clearing batch_id entries

### 6. ROW_NUMBER deduplication window
Event Hubs can deliver duplicate messages (at-least-once delivery). The ROW_NUMBER window over `(lead_id, customer_id, cksid, activity_date, pacode)` ordered by `activity_date DESC, batch_id DESC` keeps only the latest record per natural key per batch.

### 7. LPAD on site/dealer code
Dealer/site codes are stored as zero-padded 5-digit strings (e.g. `00123`). Incoming events may strip leading zeros. `LPAD(pacode, 5, '0')` normalises on insert to ensure join correctness with downstream tables.

### 8. `explode()` on salesrep array (activity pipeline only)
Activity events carry a salesrep array (0–3 reps per event). Exploding this array normalises the data into one row per rep, which is the correct relational form for the staging table schema.

---

## Workflow Orchestration

Both pipelines are deployed as Databricks Workflows (YAML-defined jobs). Parameters are injected at runtime — no hardcoded values in notebooks. Workflows can be triggered:
- On a schedule (e.g. every 15 minutes)
- On-demand via the Databricks UI or REST API
- Via Azure Data Factory pipeline activity

---

## Dependencies

| Package | Purpose |
|---|---|
| `azure-eventhub` | SDK (used in related utility functions) |
| `azure-storage-file-share` | File share logging utility |
| `paramiko` | SFTP utility (shared library) |
| `sendgrid` | Email alerting (shared library) |
| `shared_functions-0.8.whl` | Internal utility wheel (logging, secrets) |
