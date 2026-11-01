# Architecture — Azure Event Hub Streaming Ingest

## Purpose

This document describes how the real-time streaming ingest framework works, why it is shaped the way it is, and where it can fail. It is written so a reviewer can check each claim against the two notebooks in `notebooks/` and the two workflow definitions in `workflows/`.

Two parallel pipelines consume events from Azure Event Hubs and land them into a Delta Lake staging layer, via a Parquet raw zone. The pipelines are not one generic stream: the transaction pipeline and the activity pipeline have different event shapes, different dedup keys, and one real behavioural difference in how they filter null keys (§7.1).

> **Status of this document.** This is an improved rewrite of the architecture document previously in this repository. The notebooks, workflow YAMLs, sample data, and schema files are unchanged. Where the previous document stated a behaviour that only one pipeline shows, or a key that belongs to the other pipeline, this version corrects it and says so (see §3 and §7).

---

## 1. System overview

```
SOURCE SYSTEMS
  Transaction service            Activity / disposition service
        │ publishes events               │ publishes events
        ▼                                ▼
AZURE EVENT HUBS (Avro Capture enabled)
  Topic: transactions            Topic: activities
        │ .avro files                    │ .avro files
        ▼                                ▼
ADLS Gen2 — Unity Catalog Volumes (capture files)
  /eventhub/transactions/*.avro  /eventhub/activities/*.avro
        │ Auto Loader (cloudFiles)       │ Auto Loader (cloudFiles)
        ▼                                ▼
AZURE DATABRICKS — Structured Streaming
  transaction_ingest.py          activity_ingest.py
  (see §2 for the shared steps    (same shared steps, plus
   and the per-pipeline           explode of the salesrep
   differences)                   array)
        │                                │
        ▼                                ▼
ADLS Gen2 — Parquet Raw Zone (external tables)
  transactions_raw_ext           activities_raw_ext
        │ ROW_NUMBER dedup + MERGE       │ ROW_NUMBER dedup + MERGE
        ▼                                ▼
DELTA LAKE — Staging tables
  staging_db.transactions        staging_db.activities
```

**Reading the diagram.** Event Hubs is not read live by the notebooks. Event Hub Capture writes Avro files to ADLS Gen2, and the notebooks read those files with Auto Loader. That distinction matters for latency and for replay: the system is "streaming" in its processing model, but its input is files that have already landed.

---

## 2. End-to-end flow

Both pipelines follow the same steps, with the differences noted at each step. The workflow for each pipeline runs its notebook once per scheduled run; the notebook drains the backlog and exits (see §4.2).

| Step | What happens | Transaction pipeline | Activity pipeline |
|---|---|---|---|
| 1. Read | Auto Loader (`cloudFiles`, format `avro`, recursive lookup) reads capture files from `eventhub_capture_path`, using the Event Hub Avro envelope schema. The event payload is the `Body` field, cast to string as `input_json`. Checkpoint/schema location is `checkpoint_directory`. | Same | Same |
| 2. Validate | A Python UDF, `validate_json`, tags each row as valid or invalid JSON; only valid rows continue. Invalid messages are dropped silently — they are not written to a dead-letter location (see §7). | Same | Same |
| 3. Batch ID | `batch_id` is read as `max(batch_id)` from the raw external table, plus 1. | From `transactions_raw_ext` | From `activities_raw_ext` |
| 4. Schema | The event schema is loaded from `schema_path` (a JSON file on a Unity Catalog Volume), read with `spark.read.json`, and used with `from_json` to parse `input_json`. The schema is not hardcoded in the notebook. | `transaction_event_schema.json` | `activity_event_schema.json` |
| 5. Parse & flatten | `from_json` output is flattened into named columns: identifiers, customer info, up to 3 emails, up to 3 phones, address, opportunity, vehicle, trade-in, up to 3 sales reps, and audit columns (`update_date`, `ingest_date`, `batch_id`, `partition_key` from the first 10 characters of `eventDate`). | As shown; sales reps stay as `rep1_*` / `rep2_*` / `rep3_*` columns | Salesrep array is `explode`d first, so one input event becomes one row per rep, with a single rep's fields per row |
| 6. Null filter | Rows are filtered on key identifiers. | **See §7.1:** the filter is computed as `filtered_df` but the stream write uses `final_df`, so in this notebook the filter has no effect on what is written. | Filter on `leadId` and `pacode` not null, applied via `filtered_df` in the stream write |
| 7. Write raw | `writeStream` in Parquet `append` mode, `trigger(availableNow=True)`, checkpointed at `checkpoint_directory`, writing to `raw_output_directory`. `processAllAvailable()` blocks until the backlog is drained. | To `transactions_raw_ext` path | To `activities_raw_ext` path |
| 8. Dedup | A SQL query reads only batches not yet present in the staging table (`batch_id NOT IN (SELECT batch_id FROM staging)`), then `ROW_NUMBER()` keeps the latest row per natural key. | Key: `transaction_id, tracking_id, site_code, event_date`, ordered by `event_date DESC, batch_id DESC` | Key: `customer_id, lead_id, cksid, activity_date, pacode`, ordered by `activity_date DESC, batch_id DESC` |
| 9. MERGE | `MERGE INTO` the Delta staging table: matched rows are updated, unmatched rows are inserted. Site/dealer codes are normalised to 5 digits with `LPAD(..., 5, '0')`. | `LPAD` is applied only in the `INSERT` values, not in the `UPDATE` set or the `ON` condition — see §7.2 | `LPAD` is applied in the `ON` condition (against the padded target value) and in the `INSERT` values |
| 10. Report | The notebook prints pipeline name, record count, duration, and `Status: SUCCESS`. | Same | Same |

**What the raw zone is for.** The Parquet external tables (`*_raw_ext`) are an append-only audit trail and the replay source: dedup reads from them, and a batch can be reprocessed by clearing its `batch_id` entries from the staging table and re-running, so the dedup query picks it up again. The raw zone is not itself deduplicated.

---

## 3. The two pipelines, side by side

| | Transaction ingest | Activity ingest |
|---|---|---|
| Notebook | `transaction_ingest.py` | `activity_ingest.py` |
| Workflow | `wf_transaction_ingest.yml` | `wf_activity_ingest.yml` |
| Event stream | Transaction / lead events | Activity / disposition events |
| Raw external table (default) | `transactions_raw_ext` | `activities_raw_ext` |
| Staging table (default) | `transactions` | `activities` |
| Dataset parameter (default) | `TRANSACTIONS` | `ACTIVITIES` |
| Distinctive transform | Nested customer + vehicle + trade-in JSON, flattened; up to 3 emails, phones, and reps as numbered columns | `explode(salesrep[])`: one row per sales rep |
| Dedup key | `transaction_id, tracking_id, site_code, event_date` | `customer_id, lead_id, cksid, activity_date, pacode` |
| Null-key filter effective? | No — see §7.1 | Yes |
| `LPAD` in MERGE match? | No — see §7.2 | Yes |

The previous architecture document described a single dedup key and a single null-filter behaviour for both pipelines. Neither is true of both notebooks as written; the table above is what the code actually does.

---

## 4. Design decisions

### 4.1 Auto Loader (`cloudFiles`) over manual file listing

**Decision.** Read capture files with Auto Loader, checkpointed at `checkpoint_directory`, rather than listing files and tracking progress manually.

**Why.** Auto Loader tracks which files have been processed, supports incremental discovery without manual bookmarking, and handles schema evolution through `cloudFiles.schemaLocation`.

**Trade-off.** Progress is checkpoint state. Deleting or moving the checkpoint directory reprocesses files from the start of the capture path, and the dedup/MERGE step — not Auto Loader — is what prevents those reprocessed files from duplicating staging rows. Protect the checkpoint location as part of the deployment.

### 4.2 `trigger(availableNow=True)` — drain, then stop

**Decision.** Each run drains all backlogged files and exits, instead of running a continuous stream.

**Why.** A continuous stream holds a cluster alive between events. Draining on a schedule (for example every 15 minutes, as suggested in the previous document — no schedule is defined in the YAMLs as supplied; see §6) is cheaper and fits Databricks Workflows scheduling cleanly.

**Trade-off.** Latency is bounded by the schedule interval, not by event arrival. If near-real-time latency is required, this pattern needs a schedule interval to match it, or a continuous stream instead. There is also a failure window: if a run fails part-way through the drain, the next run re-drains from the checkpoint — see §7.3.

### 4.3 JSON validation UDF before schema parsing

**Decision.** Validate each `Body` as JSON with a UDF and drop invalid rows before `from_json`.

**Why.** A malformed message would otherwise produce a null row from `from_json` or fail downstream parsing, and a single bad record could obscure a batch. Filtering first makes the failure visible in record counts — valid rows in, invalid rows out.

**Trade-off.** Invalid messages are dropped without a record. There is no dead-letter table or count of dropped messages in the notebooks, so a rise in malformed events shows up only as fewer rows written, not as an alert. If message quality matters, add a dead-letter write and a dropped-count log.

### 4.4 Dynamic schema from a config path

**Decision.** Load the event schema from a JSON file on a Unity Catalog Volume at `schema_path`, at run time.

**Why.** Schema changes require updating a file, not editing and redeploying a notebook. The same notebook shape serves both pipelines with different schema files.

**Trade-off.** The schema file is a deployment dependency: if it is missing, stale, or does not match the events in the capture files, `from_json` produces nulls for the mismatched fields. The notebooks do not validate the schema file against a sample event before parsing.

### 4.5 Two-stage write: Parquet raw zone, then Delta MERGE

**Decision.** Write first to a Parquet external table, then MERGE into a Delta staging table, instead of writing directly to Delta from the stream.

**Why.** The raw zone is a full, append-only audit trail; the Delta table is the deduplicated, queryable staging layer; and replay is possible from the raw zone without re-reading Event Hubs.

**Trade-off.** Every event is written twice, and the raw zone grows without bound unless retention is managed elsewhere. Storage lifecycle for the raw zone is not handled by these notebooks.

### 4.6 `ROW_NUMBER` deduplication for at-least-once delivery

**Decision.** Deduplicate with a window function over the natural key, keeping the latest row by date and `batch_id`, before MERGE.

**Why.** Event Hubs delivers at least once, so the same event can appear in more than one capture file or batch. A window dedup inside the batch, plus MERGE on the natural key, makes reprocessing idempotent at the staging layer.

**Trade-off.** The dedup only considers batches not yet in the staging table (`batch_id NOT IN …`). If a batch was partially merged and then re-run, its rows are excluded from reprocessing even though some may not have landed — see §7.3. The dedup key is also pipeline-specific (§3); using the wrong key for a pipeline would either under- or over-deduplicate.

### 4.7 `LPAD` normalisation of site/dealer codes

**Decision.** Normalise site/dealer codes to zero-padded 5-digit strings (for example `00123`) on insert.

**Why.** Incoming events may strip leading zeros. Without normalisation, the same dealer joins differently downstream depending on how its code arrived.

**Trade-off.** As written, the two pipelines apply `LPAD` inconsistently in the MERGE (§7.2). Normalisation that only happens on insert, and not in the match condition, can fail to match an existing padded row and insert a duplicate instead of updating it.

### 4.8 `explode()` on the salesrep array (activity only)

**Decision.** Explode the salesrep array into one row per rep, rather than keeping up to 3 reps as numbered columns (the transaction pipeline's approach).

**Why.** One row per rep is the correct relational form for the activity staging schema, where each row represents one rep's involvement in one activity.

**Trade-off.** Exploding multiplies rows: an event with 3 reps becomes 3 staging rows. Row counts in the activity pipeline are therefore rep-rows, not event-rows, and must not be compared directly with transaction-pipeline counts.

---

## 5. Data and schema

- **Input envelope.** Event Hub Capture Avro files, with the envelope schema defined in the notebooks (`SequenceNumber`, `Offset`, `EnqueuedTimeUtc`, `SystemProperties`, `Properties`, `Body`). The payload is `Body` as bytes, cast to a JSON string.
- **Event schemas.** The parsed event fields come from the JSON schema files in `data/schema/` (`transaction_event_schema.json`, `activity_event_schema.json`), uploaded to the Volume path in `schema_path`. The transaction schema is wide and deeply nested (customer, up to 3 emails/phones, address, opportunity, vehicle, trade-in, up to 3 reps); the activity schema centres on lead, activity, and the salesrep array.
- **Sample data.** `data/sample_events/` contains 5 sample transaction events and 10 sample activity events, with realistic field structure. They are test data for shape-checking the parse, not a volume or latency test.
- **Staging tables.** Delta tables in `staging_database` (default `staging_db`): `transactions` and `activities`, plus the Parquet-backed external tables `transactions_raw_ext` and `activities_raw_ext` that the raw zone writes feed. The external tables must already exist, or be creatable, at the paths in the workflow parameters — their creation is not part of either notebook.
- **Partitioning.** Parsed rows carry `partition_key`, the first 10 characters of the event date. Whether the staging tables are physically partitioned on it depends on their table definitions, which are not in this repository.

---

## 6. Configuration

| Parameter | Where | Default (transaction workflow) | Notes |
|---|---|---|---|
| `checkpoint_directory` | Workflow parameter / widget | `/Volumes/uc_dev/staging/transactions/ingest_checkpoint/` | Auto Loader checkpoint and schema location. Protect it; see §4.1. |
| `eventhub_capture_path` | Workflow parameter / widget | `/Volumes/uc_dev/default/eventhub/dev-events/transactions` | Capture files location for this pipeline. |
| `raw_output_directory` | Workflow parameter / widget | `abfss://landing@yourstorageaccount.dfs.core.windows.net/staging/transactions_raw_ext` | Placeholder storage account — replace before deploying. |
| `schema_path` | Workflow parameter / widget | `/Volumes/uc_dev/staging/transactions/schema` | Directory/file holding the event schema JSON. |
| `staging_database` | Workflow parameter / widget | `staging_db` | Delta staging database. |
| `env` | Workflow parameter / widget | `dev` | Environment label passed through; no database-name suffixing is done in these notebooks. |
| `table_transactions` / `table_transactions_raw_ext` | Workflow parameter / widget | `transactions` / `transactions_raw_ext` | Staging and raw-external table names. |
| `storage_name` | Workflow parameter | `yourstorageaccount` | Placeholder — replace. |
| `run_as` | Workflow YAML | `<your-service-principal-id>` | Placeholder — replace. |
| `status_database` / `status_tbl`, `ops_database` / `ops_metadata_table` | Workflow parameters | `ops` / `tbl_run_status`, `ops` / `ops_application_metadata` | Accepted but not written to by these notebooks. |
| Schedule | Neither workflow YAML defines one | — | No schedule is defined in the YAMLs as supplied. The "every 15 minutes" suggestion in the previous document is not a configured schedule. Add one deliberately when deploying. |
| Environment dependencies | Workflow YAML | `paramiko`, `sendgrid`, `azure-eventhub`, `azure-storage-file-share`, `shared_functions-0.8-py3-none-any.whl` | See §7.4 for the wheel note. |

The activity workflow mirrors this with activity paths, table names, and checkpoint locations. Check both YAMLs against your own workspace before deploying — the defaults are dev placeholders, not production values.

---

## 7. Failure modes, gaps, and limitations

Read this section before operating the pipelines. Each item is visible in the supplied files.

1. **Transaction null filter is dead code (§2, Step 6).** `transaction_ingest.py` builds `filtered_df` (transaction ID, tracking ID, and site code not null) but writes `final_df` to the raw zone. Null-key rows are therefore written to the raw zone and can reach the MERGE, where the `NVL(..., '')` match condition can match null keys to each other. `activity_ingest.py` does use its `filtered_df`. Fix by writing `filtered_df` in the transaction notebook, or remove the filter and document that null keys are allowed — do not leave the two notebooks inconsistent without a reason.
2. **`LPAD` inconsistency in the transaction MERGE (§2, Step 9).** In `transaction_ingest.py`, `LPAD` is applied only in the `INSERT` values. The `ON` condition compares the unpadded source site code with the target site code, and the `UPDATE` set writes the unpadded value. A re-delivered event whose site code arrived without leading zeros may therefore fail to match its padded target row and be inserted as a duplicate. The activity notebook applies `LPAD` in both the match and the insert. Align the transaction notebook with the activity one.
3. **Partial-batch reprocessing gap.** Dedup considers only batches whose `batch_id` is not already in the staging table. If a run writes the raw zone for a batch but fails during MERGE, the next run excludes that batch from dedup entirely, even though some of its rows may not have been merged. Reprocessing then requires manually clearing that `batch_id` from the staging table or re-running the MERGE step by hand.
4. **`shared_functions` wheel is referenced but not included.** Both workflow YAMLs list `/Volumes/uc_dev/default/jars/shared_functions-0.8-py3-none-any.whl`. The wheel is not in this repository and is not imported by either notebook. The previous document's dependency table also lists `azure-eventhub`, `azure-storage-file-share`, `paramiko`, and `sendgrid` as if used by the pipelines; none is imported by either notebook as supplied. Remove the entries you do not need, or point them at your own libraries — do not assume they are required.
5. **No dead-letter handling.** Invalid JSON is dropped by the validation UDF with no count and no dead-letter write. A malformed-event problem is invisible except as fewer rows than expected.
6. **Batch ID race.** `batch_id` is `max(batch_id) + 1` read from the raw external table at the start of a run. Two overlapping runs of the same pipeline can compute the same `batch_id`, after which the dedup query's `NOT IN` logic treats them as one batch. Do not run the same pipeline concurrently.
7. **No schedule in the YAMLs.** Neither workflow defines a schedule or a trigger. Until one is added, the pipelines run only on demand.
8. **External tables are a prerequisite, not created here.** The notebooks read and write through the `*_raw_ext` tables and the staging tables, but neither notebook creates them. Their definitions, locations, and permissions must exist before the first run.
9. **Record count reporting.** The notebooks print a record count from the result of the MERGE statement. Treat that printed figure as a run summary, not as an independently audited count of rows inserted versus updated — the MERGE result does not break the two apart.

---

## 8. Performance — what is known and what is not

**Known from the code.** Cost is controlled by the drain-and-stop pattern (§4.2): the cluster runs only while a backlog is being drained. Throughput depends on capture-file size, the number of files, cluster size, and the MERGE/dedup work per batch — none of which is stated in this repository.

**Not known from the supplied files.** No event volume, latency, file count, cluster size, or run duration was supplied. This document therefore claims no throughput, latency, or cost figure for either pipeline. Do not quote one from this repository without a measured run.

**How to measure it.** Run each pipeline against a known backlog (file count and total event count recorded), record the printed duration and the staging row counts before and after, and note the cluster configuration. Only then add a performance table, labelled with the backlog size, environment, and date.

---

## 9. Deployment and operations checklist

- [ ] Capture files are landing in the paths in `eventhub_capture_path`, in Avro, with the envelope schema the notebooks expect.
- [ ] Schema JSON files are uploaded to the Volume path in `schema_path`, and match the current event shape.
- [ ] Raw external tables and staging Delta tables exist, with the names in the workflow parameters.
- [ ] `yourstorageaccount` and `<your-service-principal-id>` placeholders are replaced in both YAMLs; checkpoint and raw-output paths point at real locations.
- [ ] `shared_functions` wheel entries are removed or corrected (§7.4).
- [ ] The transaction null-filter and `LPAD` inconsistencies (§7.1, §7.2) are fixed or explicitly accepted before the first production run.
- [ ] A schedule is added deliberately, or the pipelines are run on demand with that choice recorded.
- [ ] First run is against a small backlog, with raw-zone, dedup, and staging counts checked by hand before scheduling.
- [ ] Checkpoint directories are backed up or treated as protected state; deleting one reprocesses the capture path.
- [ ] A dead-letter or dropped-message count is added if message quality needs to be visible (§7.5).

---

## 10. File inventory

| Path | What it is | Changed in this repository? |
|---|---|---|
| `notebooks/transaction_ingest.py`, `notebooks/activity_ingest.py` | The two streaming notebooks | No — as supplied |
| `workflows/wf_transaction_ingest.yml`, `workflows/wf_activity_ingest.yml` | Databricks Workflow definitions | No — as supplied, including placeholders and no schedule |
| `data/sample_events/*.json`, `data/schema/*.json` | Sample events and event schema files | No — as supplied |
| `docs/architecture.md` | This document | **Yes — rewritten** at the author's request to be professional and checkable; see the status note at the top |
| `README.md` | Project README | No change in this step |

---

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
