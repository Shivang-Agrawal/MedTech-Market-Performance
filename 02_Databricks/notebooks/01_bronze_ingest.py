# Databricks notebook source
# =====================================================================================
# 01_bronze_ingest
# Ingests the vendor sales extract (and the two small reference files) into Bronze using
# Auto Loader: incremental (only new files are processed), exactly-once (checkpointed),
# schema-validated (DQ-01), with a rescued-data column so no delivered value is silently lost.
#
# Trigger: file-notification (production) / Trigger.AvailableNow (batch schedule, used here).
# Idempotent re-run: Auto Loader's checkpoint means re-running this notebook on the same
# files ingests nothing new - safe to re-trigger after a failure.
# =====================================================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/medtech/src")   # adjust to your repo path, or use a Databricks Asset Bundle 'sync' root
from medtech_pipeline import transforms as tr

dbutils.widgets.text("landing_path", "/Volumes/medtech/landing/vendor_sales", "Landing volume (vendor drops files here)")
dbutils.widgets.text("checkpoint_path", "/Volumes/medtech/checkpoints/bronze_vendor_sales", "Auto Loader checkpoint")
dbutils.widgets.text("batch_run_id", "", "Run id (defaults to job run id)")
landing_path = dbutils.widgets.get("landing_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
batch_run_id = dbutils.widgets.get("batch_run_id") or dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().getOrElse("local")

# COMMAND ----------
from pyspark.sql import functions as F

raw_stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")                 # vendor delivers CSV; xlsx handled by a separate one-shot loader below for this case study
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .option("header", "true")
    .load(landing_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_row", F.monotonically_increasing_id())
    .withColumn("_batch_run_id", F.lit(batch_run_id))
    .withColumn("_ingest_ts", F.current_timestamp()))

# DQ-01: schema validation before anything is trusted downstream
schema_report = tr.validate_schema(raw_stream, tr.VENDOR_SCHEMA)
if not schema_report["ok"]:
    # BLOCK: stop the run rather than silently drop/miscast vendor columns
    raise Exception(f"Bronze schema validation failed: {schema_report}")

conformed = tr.conform(raw_stream, tr.VENDOR_SCHEMA)

query = (conformed.writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)                          # batch-like, but built on the streaming engine -> exactly-once + incremental for free
    .toTable("medtech.bronze.vendor_sales_raw"))
query.awaitTermination()

print(f"Bronze ingest complete. Rows staged this run visible via: "
      f"SELECT count(*) FROM medtech.bronze.vendor_sales_raw WHERE _batch_run_id = '{batch_run_id}'")

# COMMAND ----------
# Reference data (Product_Scope, Facility_Access): small, slowly-changing -> full refresh each run, not streamed.
# In this case study both sheets live in the same workbook as the vendor extract; in production they would be
# separate governed feeds (e.g. from the product master-data system and the HR/identity system).
import pandas as pd

XLSX_PATH = dbutils.widgets.get("landing_path").replace("/vendor_sales", "") + "/MedTech_Case_Study_Data.xlsx"

def load_reference_sheet(sheet, schema):
    pdf = pd.read_excel(XLSX_PATH, sheet_name=sheet)
    sdf = spark.createDataFrame(pdf)
    return tr.conform(sdf.withColumn("_source_file", F.lit(XLSX_PATH)).withColumn("_ingest_ts", F.current_timestamp()), schema)

load_reference_sheet("Product_Scope", tr.SCOPE_SCHEMA).write.mode("overwrite").saveAsTable("medtech.bronze.product_scope_raw")
load_reference_sheet("Facility_Access", tr.ACCESS_SCHEMA).write.mode("overwrite").saveAsTable("medtech.bronze.facility_access_raw")
print("Reference data refreshed: product_scope_raw, facility_access_raw")
