# Azure Event Hub Streaming Ingest — Databricks Auto Loader

![Databricks](https://img.shields.io/badge/Databricks-Spark-FF3621)
![Azure Event Hubs](https://img.shields.io/badge/Azure-Event%20Hubs-0078D4)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)
![Streaming](https://img.shields.io/badge/Processing-Structured%20Streaming-orange)
![Python](https://img.shields.io/badge/Python-3.9-blue)

## Overview
A production **real-time streaming ingestion framework** built on Azure Databricks, consuming Avro-encoded messages from Azure Event Hubs using Spark Structured Streaming and Auto Loader (`cloudFiles`). Ingests two distinct event streams — **transaction events** and **activity events** — into a Delta Lake staging layer with MERGE-based deduplication.

## Architecture

```
Azure Event Hubs (Avro capture → ADLS Gen2)
         │
         ▼
Auto Loader (cloudFiles) — incremental .avro discovery
         │
         ▼
Spark Structured Streaming
  ├── JSON Validation UDF        (filter bad messages before parse)
  ├── Dynamic schema loading     (from config Volume path)
  ├── Field extraction & casting
  ├── explode(salesrep[])        (activity pipeline only)
  └── Null filter on key fields
         │
         ▼
Parquet Raw Zone (ADLS Gen2) ←── External table (raw_ext)
         │
         ▼
Delta Lake Staging — MERGE / UPSERT
  ├── ROW_NUMBER deduplication   (handles Event Hub at-least-once)
  └── LPAD normalisation on site code
```

## Two Pipelines

| Pipeline | Event Stream | Target Table | Key Pattern |
|---|---|---|---|
| `transaction_ingest` | Transaction/lead events | `transactions_raw_ext` → `transactions` | Nested customer + vehicle + trade-in JSON |
| `activity_ingest` | Activity/disposition events | `activities_raw_ext` → `activities` | Exploded salesrep array (1-to-many) |

## Key Features
- **Auto Loader** — incremental file discovery with checkpoint, no manual file tracking
- **Avro + JSON** — Event Hub Capture writes Avro; Body field decoded as JSON per message
- **Dynamic schema** — schema loaded from a config Volume path, not hardcoded in notebook
- **JSON validation UDF** — filters malformed messages before schema parsing
- **Batch ID tracking** — monotonically increasing batch IDs for incremental load control
- **ROW_NUMBER dedup** — window function handles at-least-once delivery from Event Hubs
- **MERGE upsert** — idempotent writes handle duplicates and late-arriving events
- **`trigger(availableNow=True)`** — drains all backlog then stops; cost-efficient scheduling

## Project Structure
```
azure-eventhub-streaming-ingest/
├── notebooks/
│   ├── transaction_ingest.py          # Streaming ingest — transaction events
│   └── activity_ingest.py             # Streaming ingest — activity events (with explode)
├── workflows/
│   ├── wf_transaction_ingest.yml      # Databricks Workflow YAML
│   └── wf_activity_ingest.yml         # Databricks Workflow YAML
├── data/
│   ├── sample_events/
│   │   ├── sample_transaction_events.json   # 5 sample transaction event messages
│   │   └── sample_activity_events.json      # 10 sample activity event messages
│   └── schema/
│       ├── transaction_event_schema.json    # Schema file loaded via schema_path param
│       └── activity_event_schema.json       # Schema file loaded via schema_path param
├── docs/
│   └── architecture.md                # Full architecture + 8 design decisions
└── README.md
```

## Sample Data
The `data/` folder contains realistic sample JSON messages that match the exact field structure parsed by the notebooks:

- **`sample_transaction_events.json`** — 5 transaction events with nested customer info (3 emails, 3 phones, address), vehicle details, trade-in, and up to 3 salesreps
- **`sample_activity_events.json`** — 10 activity events across 5 leads, showing multiple activity types per lead (Appointment → Test Drive → Deal Closed), with the salesrep array that gets exploded
- **`schema/`** — JSON schema files uploaded to the Databricks Volume path configured in `schema_path` widget

## Tech Stack
| Component | Technology |
|---|---|
| Streaming engine | Spark Structured Streaming, Auto Loader (cloudFiles) |
| Message source | Azure Event Hubs (Avro capture files) |
| Storage | ADLS Gen2 (abfss), Unity Catalog Volumes |
| Table format | Delta Lake (MERGE, partitioning) |
| Orchestration | Databricks Workflows (YAML) |
| Secrets | Databricks Secret Scope |

## How to Deploy
1. Upload notebooks to Databricks workspace under `/Workspace/projects/`
2. Upload `data/schema/*.json` to the Unity Catalog Volume path set in `schema_path` widget
3. Update workflow YAML — replace `yourstorageaccount` and `<your-service-principal-id>`
4. Create the GitHub repo and push: `git init` → `git add .` → `git commit` → `git push`
5. In Databricks, deploy workflow and trigger: `databricks bundle run wf_transaction_ingest`

## Author
**Sanjeev Kumar Pandey** — Data Engineer | Azure | Spark | Databricks  
[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) | sanjeevpandey640@gmail.com
