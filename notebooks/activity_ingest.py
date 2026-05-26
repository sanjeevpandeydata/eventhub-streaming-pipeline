# Databricks notebook source
# ============================================================
# NOTEBOOK : activity_ingest.py
# PURPOSE  : Consume activity/disposition events from Azure
#            Event Hubs (Avro capture files on ADLS Gen2)
#            using Spark Structured Streaming + Auto Loader.
#            Explodes salesrep array, validates JSON body,
#            writes to Parquet raw zone, then merges into
#            Delta staging table.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, MapType,
    DoubleType, BinaryType, BooleanType
)
from pyspark.sql.functions import (
    from_json, col, lit,
    current_timestamp, current_date,
    max as spark_max, coalesce,
    substring, explode
)
import json
import time

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("appName",                      "")
appName                      = dbutils.widgets.get("appName")

dbutils.widgets.text("checkpoint_directory",         "")
checkpoint_directory         = dbutils.widgets.get("checkpoint_directory")

dbutils.widgets.text("eventhub_capture_path",        "")
eventhub_capture_path        = dbutils.widgets.get("eventhub_capture_path")

dbutils.widgets.text("raw_output_directory",         "")
raw_output_directory         = dbutils.widgets.get("raw_output_directory")

dbutils.widgets.text("dataset",                      "")
dataset                      = dbutils.widgets.get("dataset")

dbutils.widgets.text("staging_database",             "")
staging_database             = dbutils.widgets.get("staging_database")

dbutils.widgets.text("env",                          "")
env                          = dbutils.widgets.get("env")

dbutils.widgets.text("schema_path",                  "")
schema_path                  = dbutils.widgets.get("schema_path")

dbutils.widgets.text("table_activities",             "")
table_activities             = dbutils.widgets.get("table_activities")

dbutils.widgets.text("table_activities_raw_ext",     "")
table_activities_raw_ext     = dbutils.widgets.get("table_activities_raw_ext")

# COMMAND ----------
# ── Event Hub Avro Envelope Schema ───────────────────────────
# Azure Event Hubs Capture wraps each message in this Avro
# envelope. The actual event payload lives in the Body field
# as raw bytes which we cast to string (JSON).

event_hub_file_schema = StructType([
    StructField("SequenceNumber",   LongType(),   True),
    StructField("Offset",           StringType(), True),
    StructField("EnqueuedTimeUtc",  StringType(), True),
    StructField("SystemProperties", MapType(
        StringType(),
        StructType([
            StructField("member0", LongType(),   True),
            StructField("member1", DoubleType(), True),
            StructField("member2", StringType(), True),
            StructField("member3", BinaryType(), True),
        ]), True
    ), True),
    StructField("Properties", MapType(
        StringType(),
        StructType([
            StructField("member0", LongType(),   True),
            StructField("member1", DoubleType(), True),
            StructField("member2", StringType(), True),
            StructField("member3", BinaryType(), True),
        ]), True
    ), True),
    StructField("Body", BinaryType(), True),
])

# COMMAND ----------
# ── JSON Validation UDF ───────────────────────────────────────
# Filters malformed messages before schema parsing.
# Prevents a single bad record from crashing the stream.

def validate_json(json_string):
    try:
        if json.loads(json_string):
            return True
    except Exception:
        return False

validate_json_udf = spark.udf.register(
    name="validate_json",
    f=validate_json,
    returnType=BooleanType()
)

# COMMAND ----------
# ── Step 1-4: Stream Read + JSON Validation ───────────────────

# Read Avro capture files from Event Hub using Auto Loader.
# cloudFiles handles incremental file discovery automatically.
raw_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",         "avro")
        .option("cloudFiles.schemaLocation",  checkpoint_directory)
        .option("recursiveFileLookup",        "true")
        .schema(event_hub_file_schema)
        .load(path=eventhub_capture_path, pathGlobFilter="*.avro")
        .withColumn("input_json", col("Body").cast(StringType()))
        .select("input_json")
)

# Tag each row as valid/invalid JSON, then keep only valid ones
raw_stream_df   = raw_stream_df.withColumn("json_is_valid", validate_json_udf(col("input_json")))
valid_stream_df = raw_stream_df.where("json_is_valid = true").drop("json_is_valid")

# COMMAND ----------
# ── Step 5-6: Batch ID + Dynamic Schema ──────────────────────

# Monotonically increasing batch_id for incremental load tracking
batch_id = (
    spark.read
        .table(f"{staging_database}.{table_activities_raw_ext}")
        .select(coalesce(spark_max("batch_id"), lit(0)))
        .first()[0]
    + 1
)

# Schema loaded from config path — not hardcoded in notebook.
# Allows schema evolution without code changes.
raw_schema_df    = spark.read.option("multiLine", True).json(schema_path)
activity_schema  = raw_schema_df.schema

# COMMAND ----------
# ── Step 7-8: Parse JSON + Explode salesrep array + Select ───
# Activity events carry a salesrep array. We explode it so
# each salesrep gets its own row — one-to-many normalisation.

parsed_df = (
    valid_stream_df
        .select(from_json(col("input_json"), activity_schema).alias("evt"))
        .selectExpr("evt.*")
        .withColumn("salesrep", explode(col("salesrep")))    # explode array → rows
)

final_df = parsed_df.select(
    # ── Core identifiers ─────────────────────────────────────
    col("leadId"),
    lit(None).cast("string").alias("customer_id"),
    col("pacode"),
    lit("eventSource").alias("source"),
    col("cksid"),

    # ── Disposition flags (nullable — not in all events) ──────
    lit(None).alias("inshowroom").cast(BooleanType()),
    lit(None).alias("appraisal").cast(BooleanType()),
    lit(None).alias("writeup").cast(BooleanType()),

    # ── Opportunity ───────────────────────────────────────────
    parsed_df.opportunities.opportunityId.cast("int").alias("opportunity_id"),
    lit(None).cast(BooleanType()).alias("appointment_shown"),

    # ── Activity details ──────────────────────────────────────
    parsed_df.activity.activityName.alias("activity_name"),
    parsed_df.activity.comments.alias("comments"),
    parsed_df.activity.activityDate.alias("activity_date"),

    # ── Sales rep (one row per rep after explode) ─────────────
    parsed_df.salesrep.salesreptype.alias("salesrep_type"),
    parsed_df.salesrep.salesrepCdsId.alias("salesrep_id"),
    parsed_df.salesrep.salesrepName.alias("salesrep_name"),

    # ── Appointment ───────────────────────────────────────────
    parsed_df.appointments.appointmentDateTime.alias("appointment_datetime"),

    # ── Audit columns ─────────────────────────────────────────
    lit(current_date()).cast("string").alias("ingest_date"),
    lit(current_timestamp()).cast("string").alias("update_date"),
    lit(batch_id).alias("batch_id"),
    substring(parsed_df.activity.activityDate, 1, 10).alias("partition_key"),
)

# COMMAND ----------
# ── Step 9: Null filter on key identifiers ────────────────────

filtered_df = final_df.filter(
    final_df.leadId.isNotNull() &
    final_df.pacode.isNotNull()
)

# COMMAND ----------
# ── Step 10-11: Write stream to Parquet raw zone ──────────────
# trigger(availableNow=True) drains all backlogged events then
# stops — behaves like a batch job using the streaming engine.
# processAllAvailable() blocks until the drain is complete.

write_query = (
    filtered_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_directory)
        .option("path", raw_output_directory)
        .queryName("EventHub-ActivityIngest")
        .start()
)

write_query.processAllAvailable()

# COMMAND ----------
# ── Step 14-15: Deduplicate via ROW_NUMBER ────────────────────
# Fetch only new batches not yet in the staging table,
# then pick the latest record per natural key using ROW_NUMBER.

dedup_query = f"""
WITH new_data AS (
    SELECT DISTINCT ext.*
    FROM {staging_database}.{table_activities_raw_ext} ext
    WHERE batch_id NOT IN (
        SELECT batch_id FROM {staging_database}.{table_activities}
    )
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id, lead_id, cksid, activity_date, pacode
               ORDER BY activity_date DESC, batch_id DESC
           ) AS rn
    FROM new_data
)
SELECT
    lead_id,
    customer_id,
    pacode,
    disposition_date,
    source,
    cksid,
    inshowroom,
    appraisal,
    writeup,
    opportunity_id,
    appointment_shown,
    activity_name,
    comments,
    activity_date,
    salesrep_type,
    salesrep_id,
    salesrep_name,
    appointment_datetime,
    ingest_date,
    update_date,
    batch_id,
    partition_key
FROM ranked
WHERE rn = 1
"""

spark.sql(dedup_query).createOrReplaceTempView("view_activities_deduped")

# COMMAND ----------
# ── Step 16-17: MERGE into Delta staging table ────────────────
# Idempotent upsert — matched rows are updated, new rows inserted.
# LPAD on pacode normalises 5-digit dealer/site codes.

merge_query = f"""
MERGE INTO {staging_database}.{table_activities} AS target
USING (
    SELECT
        lead_id,
        customer_id,
        pacode,
        disposition_date,
        source,
        cksid,
        inshowroom,
        appraisal,
        writeup,
        opportunity_id,
        appointment_shown,
        activity_name,
        comments,
        activity_date,
        salesrep_type,
        salesrep_id,
        salesrep_name,
        appointment_datetime,
        ingest_date,
        update_date,
        batch_id,
        partition_key
    FROM view_activities_deduped
) AS src
ON  NVL(target.customer_id,    '') = NVL(src.customer_id,    '')
AND NVL(target.lead_id,        '') = NVL(src.lead_id,        '')
AND NVL(target.cksid,          '') = NVL(src.cksid,          '')
AND NVL(target.activity_date,  '') = NVL(src.activity_date,  '')
AND NVL(target.pacode,         '') = NVL(LPAD(src.pacode, 5, '0'), '')

WHEN MATCHED THEN UPDATE SET
    disposition_date      = src.disposition_date,
    source                = src.source,
    inshowroom            = src.inshowroom,
    appraisal             = src.appraisal,
    writeup               = src.writeup,
    opportunity_id        = src.opportunity_id,
    appointment_shown     = src.appointment_shown,
    activity_name         = src.activity_name,
    comments              = src.comments,
    salesrep_type         = src.salesrep_type,
    salesrep_id           = src.salesrep_id,
    salesrep_name         = src.salesrep_name,
    appointment_datetime  = src.appointment_datetime,
    ingest_date           = src.ingest_date,
    update_date           = src.update_date,
    batch_id              = src.batch_id,
    partition_key         = src.partition_key

WHEN NOT MATCHED THEN INSERT (
    lead_id, customer_id, pacode, disposition_date, source, cksid,
    inshowroom, appraisal, writeup,
    opportunity_id, appointment_shown,
    activity_name, comments, activity_date,
    salesrep_type, salesrep_id, salesrep_name,
    appointment_datetime,
    ingest_date, update_date, batch_id, partition_key
) VALUES (
    src.lead_id,
    src.customer_id,
    LPAD(src.pacode, 5, '0'),
    src.disposition_date,
    src.source,
    src.cksid,
    src.inshowroom,
    src.appraisal,
    src.writeup,
    src.opportunity_id,
    src.appointment_shown,
    src.activity_name,
    src.comments,
    src.activity_date,
    src.salesrep_type,
    src.salesrep_id,
    src.salesrep_name,
    src.appointment_datetime,
    src.ingest_date,
    src.update_date,
    src.batch_id,
    src.partition_key
)
"""

merge_result         = spark.sql(merge_query)
total_recs_processed = merge_result.count()

# COMMAND ----------
end          = time.time()
run_duration = end - start
print(f"Pipeline : {appName}")
print(f"Records  : {total_recs_processed:,}")
print(f"Duration : {run_duration:.2f}s")
print("Status   : SUCCESS")
