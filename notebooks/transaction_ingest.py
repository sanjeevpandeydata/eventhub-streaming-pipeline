# Databricks notebook source
# ============================================================
# NOTEBOOK : transaction_ingest.py
# PURPOSE  : Consume transaction events from Azure Event Hubs
#            (Avro capture files on ADLS Gen2) using Spark
#            Structured Streaming + Auto Loader.
#            Validates JSON body, extracts fields, writes to
#            Parquet raw zone, then merges into Delta staging.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, MapType,
    DoubleType, BinaryType, BooleanType, IntegerType
)
from pyspark.sql.functions import (
    from_json, col, lit,
    current_timestamp, current_date,
    max as spark_max, coalesce,
    substring
)
import json
import time

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("appName",                    "")
appName                    = dbutils.widgets.get("appName")

dbutils.widgets.text("checkpoint_directory",       "")
checkpoint_directory       = dbutils.widgets.get("checkpoint_directory")

dbutils.widgets.text("eventhub_capture_path",      "")
eventhub_capture_path      = dbutils.widgets.get("eventhub_capture_path")

dbutils.widgets.text("raw_output_directory",       "")
raw_output_directory       = dbutils.widgets.get("raw_output_directory")

dbutils.widgets.text("dataset",                    "")
dataset                    = dbutils.widgets.get("dataset")

dbutils.widgets.text("staging_database",           "")
staging_database           = dbutils.widgets.get("staging_database")

dbutils.widgets.text("env",                        "")
env                        = dbutils.widgets.get("env")

dbutils.widgets.text("schema_path",                "")
schema_path                = dbutils.widgets.get("schema_path")

dbutils.widgets.text("table_transactions",         "")
table_transactions         = dbutils.widgets.get("table_transactions")

dbutils.widgets.text("table_transactions_raw_ext", "")
table_transactions_raw_ext = dbutils.widgets.get("table_transactions_raw_ext")

# COMMAND ----------
# ── Event Hub Avro Envelope Schema ───────────────────────────
# Azure Event Hubs Capture wraps each message in this Avro
# envelope. The actual event payload lives in the Body field
# as raw bytes which we cast to string (JSON).

event_hub_file_schema = StructType([
    StructField("SequenceNumber",  LongType(),   True),
    StructField("Offset",          StringType(), True),
    StructField("EnqueuedTimeUtc", StringType(), True),
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
# cloudFiles handles incremental discovery automatically —
# no need to track which files have been processed.
raw_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",        "avro")
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .option("recursiveFileLookup",       "true")
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
        .table(f"{staging_database}.{table_transactions_raw_ext}")
        .select(coalesce(spark_max("batch_id"), lit(0)))
        .first()[0]
    + 1
)

# Schema loaded from a config path — not hardcoded in notebook.
# This allows schema evolution without code changes.
raw_schema_df = spark.read.option("multiLine", True).json(schema_path)
event_schema  = raw_schema_df.schema

# COMMAND ----------
# ── Step 7-8: Parse JSON + Select Fields ─────────────────────

parsed_df = (
    valid_stream_df
        .select(from_json(col("input_json"), event_schema).alias("evt"))
        .selectExpr("evt.*")
)

final_df = parsed_df.select(
    # ── Core identifiers ─────────────────────────────────────
    parsed_df.transactionId.cast("int").alias("transaction_id"),
    col("siteCode"),
    col("eventDate"),
    col("sourceSystem"),
    col("trackingId"),

    # ── Customer info ─────────────────────────────────────────
    parsed_df.customerInfo.customer.type.alias("customer_type"),
    parsed_df.customerInfo.customer.firstName.alias("first_name"),
    parsed_df.customerInfo.customer.lastName.alias("last_name"),
    parsed_df.customerInfo.customer.middleName.alias("middle_name"),
    parsed_df.customerInfo.customer.title.alias("title"),

    # ── Emails (up to 3) ──────────────────────────────────────
    parsed_df.customerInfo.customer.emails[0].address.alias("emailaddress1"),
    parsed_df.customerInfo.customer.emails[0].emailType.alias("email1_type"),
    parsed_df.customerInfo.customer.emails[0].primary.cast(BooleanType()).alias("email1_primary"),
    parsed_df.customerInfo.customer.emails[0].optIn.cast(BooleanType()).alias("email1_optin"),

    parsed_df.customerInfo.customer.emails[1].address.alias("emailaddress2"),
    parsed_df.customerInfo.customer.emails[1].emailType.alias("email2_type"),
    parsed_df.customerInfo.customer.emails[1].primary.cast(BooleanType()).alias("email2_primary"),
    parsed_df.customerInfo.customer.emails[1].optIn.cast(BooleanType()).alias("email2_optin"),

    parsed_df.customerInfo.customer.emails[2].address.alias("emailaddress3"),
    parsed_df.customerInfo.customer.emails[2].emailType.alias("email3_type"),
    parsed_df.customerInfo.customer.emails[2].primary.cast(BooleanType()).alias("email3_primary"),
    parsed_df.customerInfo.customer.emails[2].optIn.cast(BooleanType()).alias("email3_optin"),

    # ── Phones (up to 3) ──────────────────────────────────────
    parsed_df.customerInfo.customer.phones[0].number.alias("phonenumber1"),
    parsed_df.customerInfo.customer.phones[0].phoneType.alias("phone1_type"),
    parsed_df.customerInfo.customer.phones[0].primary.cast(BooleanType()).alias("phone1_primary"),
    parsed_df.customerInfo.customer.phones[0].phoneOptIn.cast(BooleanType()).alias("phone1_optin"),
    parsed_df.customerInfo.customer.phones[0].textOptIn.cast(BooleanType()).alias("text1_optin"),

    parsed_df.customerInfo.customer.phones[1].number.alias("phonenumber2"),
    parsed_df.customerInfo.customer.phones[1].phoneType.alias("phone2_type"),
    parsed_df.customerInfo.customer.phones[1].primary.cast(BooleanType()).alias("phone2_primary"),
    parsed_df.customerInfo.customer.phones[1].phoneOptIn.cast(BooleanType()).alias("phone2_optin"),
    parsed_df.customerInfo.customer.phones[1].textOptIn.cast(BooleanType()).alias("text2_optin"),

    parsed_df.customerInfo.customer.phones[2].number.alias("phonenumber3"),
    parsed_df.customerInfo.customer.phones[2].phoneType.alias("phone3_type"),
    parsed_df.customerInfo.customer.phones[2].primary.cast(BooleanType()).alias("phone3_primary"),
    parsed_df.customerInfo.customer.phones[2].phoneOptIn.cast(BooleanType()).alias("phone3_optin"),
    parsed_df.customerInfo.customer.phones[2].textOptIn.cast(BooleanType()).alias("text3_optin"),

    # ── Address ───────────────────────────────────────────────
    "customerInfo.customer.address.*",
    lit(None).cast("string").alias("address_ref_id"),

    # ── Opportunity ───────────────────────────────────────────
    parsed_df.opportunity.opportunityId.cast("int").alias("opportunity_id"),
    parsed_df.opportunity.appointmentShown.alias("appointment_shown"),

    # ── Vehicle details ───────────────────────────────────────
    parsed_df.vehicleDetails.inventoryType.alias("inventory_type"),
    parsed_df.vehicleDetails.year.cast("int").alias("vehicle_year"),
    parsed_df.vehicleDetails.make.alias("vehicle_make"),
    parsed_df.vehicleDetails.model.alias("vehicle_model"),
    parsed_df.vehicleDetails.trim.alias("vehicle_trim"),
    parsed_df.vehicleDetails.vin.alias("vin"),
    parsed_df.vehicleDetails.stockId.alias("stock_id"),

    # ── Trade-in ──────────────────────────────────────────────
    parsed_df.tradeIn.tradeInVin.alias("tradein_vin"),
    parsed_df.tradeIn.tradeInAmount.alias("tradein_amount"),
    parsed_df.tradeIn.payOffAmount.alias("payoff_amount"),

    # ── Sales reps (up to 3) ──────────────────────────────────
    parsed_df.salesReps[0].repType.alias("rep1_type"),
    parsed_df.salesReps[0].repId.alias("rep1_id"),
    parsed_df.salesReps[0].repName.alias("rep1_name"),
    parsed_df.salesReps[1].repType.alias("rep2_type"),
    parsed_df.salesReps[1].repId.alias("rep2_id"),
    parsed_df.salesReps[1].repName.alias("rep2_name"),
    parsed_df.salesReps[2].repType.alias("rep3_type"),
    parsed_df.salesReps[2].repId.alias("rep3_id"),
    parsed_df.salesReps[2].repName.alias("rep3_name"),

    # ── Audit columns ─────────────────────────────────────────
    lit(current_timestamp()).cast("string").alias("update_date"),
    lit(current_date()).cast("string").alias("ingest_date"),
    lit(batch_id).alias("batch_id"),
    substring("eventDate", 1, 10).alias("partition_key"),
)

# COMMAND ----------
# ── Step 9: Null filter on key identifiers ────────────────────

filtered_df = final_df.filter(
    final_df.transaction_id.isNotNull() &
    final_df.trackingId.isNotNull() &
    final_df.siteCode.isNotNull()
)

# COMMAND ----------
# ── Step 10-11: Write stream to Parquet raw zone ──────────────
# trigger(availableNow=True) — processes all backlogged events
# then stops (like a batch job but using the streaming engine).
# processAllAvailable() blocks until the drain is complete.

write_query = (
    final_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_directory)
        .option("path", raw_output_directory)
        .queryName("EventHub-TransactionIngest")
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
    FROM {staging_database}.{table_transactions_raw_ext} ext
    WHERE batch_id NOT IN (
        SELECT batch_id FROM {staging_database}.{table_transactions}
    )
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction_id, tracking_id, site_code, event_date
               ORDER BY event_date DESC, batch_id DESC
           ) AS rn
    FROM new_data
)
SELECT
    transaction_id, site_code, event_date, source_system, tracking_id,
    customer_type, first_name, last_name, middle_name, title,
    emailaddress1, email1_type, email1_primary, email1_optin,
    emailaddress2, email2_type, email2_primary, email2_optin,
    emailaddress3, email3_type, email3_primary, email3_optin,
    phonenumber1, phone1_type, phone1_primary, phone1_optin, text1_optin,
    phonenumber2, phone2_type, phone2_primary, phone2_optin, text2_optin,
    phonenumber3, phone3_type, phone3_primary, phone3_optin, text3_optin,
    address_ref_id,
    opportunity_id, appointment_shown,
    inventory_type, vehicle_year, vehicle_make, vehicle_model,
    vehicle_trim, vin, stock_id,
    tradein_vin, tradein_amount, payoff_amount,
    rep1_type, rep1_id, rep1_name,
    rep2_type, rep2_id, rep2_name,
    rep3_type, rep3_id, rep3_name,
    update_date, ingest_date, batch_id, partition_key
FROM ranked
WHERE rn = 1
"""

spark.sql(dedup_query).createOrReplaceTempView("view_transactions_deduped")

# COMMAND ----------
# ── Step 16-17: MERGE into Delta staging table ────────────────
# Idempotent upsert — matched rows are updated, new rows inserted.
# LPAD on site_code normalises 5-digit codes.

merge_query = f"""
MERGE INTO {staging_database}.{table_transactions} AS target
USING (
    SELECT
        transaction_id, site_code, event_date, source_system, tracking_id,
        customer_type, first_name, last_name, middle_name, title,
        emailaddress1, email1_type, email1_primary, email1_optin,
        emailaddress2, email2_type, email2_primary, email2_optin,
        emailaddress3, email3_type, email3_primary, email3_optin,
        phonenumber1, phone1_type, phone1_primary, phone1_optin, text1_optin,
        phonenumber2, phone2_type, phone2_primary, phone2_optin, text2_optin,
        phonenumber3, phone3_type, phone3_primary, phone3_optin, text3_optin,
        address_ref_id,
        opportunity_id, appointment_shown,
        inventory_type, vehicle_year, vehicle_make, vehicle_model,
        vehicle_trim, vin, stock_id,
        tradein_vin, tradein_amount, payoff_amount,
        rep1_type, rep1_id, rep1_name,
        rep2_type, rep2_id, rep2_name,
        rep3_type, rep3_id, rep3_name,
        update_date, ingest_date, batch_id, partition_key
    FROM view_transactions_deduped
) AS src
ON  NVL(target.transaction_id, '') = NVL(src.transaction_id, '')
AND NVL(target.tracking_id,    '') = NVL(src.tracking_id,    '')
AND NVL(target.site_code,      '') = NVL(src.site_code,      '')
AND NVL(target.event_date,     '') = NVL(src.event_date,     '')

WHEN MATCHED THEN UPDATE SET
    customer_type     = src.customer_type,
    first_name        = src.first_name,
    last_name         = src.last_name,
    middle_name       = src.middle_name,
    title             = src.title,
    emailaddress1     = src.emailaddress1,
    email1_type       = src.email1_type,
    email1_primary    = src.email1_primary,
    email1_optin      = src.email1_optin,
    emailaddress2     = src.emailaddress2,
    email2_type       = src.email2_type,
    email2_primary    = src.email2_primary,
    email2_optin      = src.email2_optin,
    emailaddress3     = src.emailaddress3,
    email3_type       = src.email3_type,
    email3_primary    = src.email3_primary,
    email3_optin      = src.email3_optin,
    phonenumber1      = src.phonenumber1,
    phone1_type       = src.phone1_type,
    phone1_primary    = src.phone1_primary,
    phone1_optin      = src.phone1_optin,
    text1_optin       = src.text1_optin,
    phonenumber2      = src.phonenumber2,
    phone2_type       = src.phone2_type,
    phone2_primary    = src.phone2_primary,
    phone2_optin      = src.phone2_optin,
    text2_optin       = src.text2_optin,
    phonenumber3      = src.phonenumber3,
    phone3_type       = src.phone3_type,
    phone3_primary    = src.phone3_primary,
    phone3_optin      = src.phone3_optin,
    text3_optin       = src.text3_optin,
    address_ref_id    = src.address_ref_id,
    opportunity_id    = src.opportunity_id,
    appointment_shown = src.appointment_shown,
    inventory_type    = src.inventory_type,
    vehicle_year      = src.vehicle_year,
    vehicle_make      = src.vehicle_make,
    vehicle_model     = src.vehicle_model,
    vehicle_trim      = src.vehicle_trim,
    vin               = src.vin,
    stock_id          = src.stock_id,
    tradein_vin       = src.tradein_vin,
    tradein_amount    = src.tradein_amount,
    payoff_amount     = src.payoff_amount,
    rep1_type         = src.rep1_type,
    rep1_id           = src.rep1_id,
    rep1_name         = src.rep1_name,
    rep2_type         = src.rep2_type,
    rep2_id           = src.rep2_id,
    rep2_name         = src.rep2_name,
    rep3_type         = src.rep3_type,
    rep3_id           = src.rep3_id,
    rep3_name         = src.rep3_name,
    update_date       = src.update_date,
    batch_id          = src.batch_id,
    partition_key     = src.partition_key

WHEN NOT MATCHED THEN INSERT (
    transaction_id, site_code, event_date, source_system, tracking_id,
    customer_type, first_name, last_name, middle_name, title,
    emailaddress1, email1_type, email1_primary, email1_optin,
    emailaddress2, email2_type, email2_primary, email2_optin,
    emailaddress3, email3_type, email3_primary, email3_optin,
    phonenumber1, phone1_type, phone1_primary, phone1_optin, text1_optin,
    phonenumber2, phone2_type, phone2_primary, phone2_optin, text2_optin,
    phonenumber3, phone3_type, phone3_primary, phone3_optin, text3_optin,
    address_ref_id,
    opportunity_id, appointment_shown,
    inventory_type, vehicle_year, vehicle_make, vehicle_model,
    vehicle_trim, vin, stock_id,
    tradein_vin, tradein_amount, payoff_amount,
    rep1_type, rep1_id, rep1_name,
    rep2_type, rep2_id, rep2_name,
    rep3_type, rep3_id, rep3_name,
    update_date, ingest_date, batch_id, partition_key
) VALUES (
    src.transaction_id, LPAD(src.site_code, 5, '0'), src.event_date, src.source_system, src.tracking_id,
    src.customer_type, src.first_name, src.last_name, src.middle_name, src.title,
    src.emailaddress1, src.email1_type, src.email1_primary, src.email1_optin,
    src.emailaddress2, src.email2_type, src.email2_primary, src.email2_optin,
    src.emailaddress3, src.email3_type, src.email3_primary, src.email3_optin,
    src.phonenumber1, src.phone1_type, src.phone1_primary, src.phone1_optin, src.text1_optin,
    src.phonenumber2, src.phone2_type, src.phone2_primary, src.phone2_optin, src.text2_optin,
    src.phonenumber3, src.phone3_type, src.phone3_primary, src.phone3_optin, src.text3_optin,
    src.address_ref_id,
    src.opportunity_id, src.appointment_shown,
    src.inventory_type, src.vehicle_year, src.vehicle_make, src.vehicle_model,
    src.vehicle_trim, src.vin, src.stock_id,
    src.tradein_vin, src.tradein_amount, src.payoff_amount,
    src.rep1_type, src.rep1_id, src.rep1_name,
    src.rep2_type, src.rep2_id, src.rep2_name,
    src.rep3_type, src.rep3_id, src.rep3_name,
    src.update_date, src.ingest_date, src.batch_id, src.partition_key
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
