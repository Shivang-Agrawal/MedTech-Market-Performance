# Databricks notebook source
# =====================================================================================
# 03_gold_publish
# Aggregates Silver into the Gold tables Power BI and the AI/Genie prompt layer read.
# Only runs if 04_dq_checks (upstream task in the job) passed all BLOCK-severity checks -
# see resources/job_workflow.json: this task's dependency is conditioned on that outcome,
# so a blocking DQ failure leaves Gold on its last good snapshot (Delta table = automatic
# "last good version"; add `RESTORE TABLE ... TO VERSION AS OF n` for a manual rollback).
# =====================================================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/medtech/src")
from medtech_pipeline import transforms as tr
from pyspark.sql import functions as F

vendor_sales = spark.table("medtech.silver.vendor_sales")
mapping = spark.table("medtech.silver.dq_product_mapping").select("ProductCode", "Brand", "Category", "Subcategory")
fact = vendor_sales.join(mapping, "ProductCode")

gold = tr.market_share_monthly(fact, our_company="Our Company")

# full overwrite of the partition set actually affected keeps this both correct and cheap;
# at 30M rows/yr this becomes a dynamic-partition-overwrite by MonthStart:
(gold.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
     .partitionBy("MonthStart").saveAsTable("medtech.gold.market_share_monthly"))

# publish the reconciliation view alongside it (also feeds the Power BI ControlTotals check)
audit = spark.table("medtech.silver.dq_row_audit")
control = tr.control_summary(audit)
run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().getOrElse("local")
rows = [(run_id, k, float(v)) for k, v in control.items()]
(spark.createDataFrame(rows, ["run_id", "metric", "model_value"])
      .withColumn("checked_at", F.current_timestamp())
      .write.mode("append").saveAsTable("medtech.gold.control_summary"))

print("Gold published:", gold.count(), "aggregate rows.",
      "CleanRows =", control["CleanRows"], "CleanSales =", control["CleanSales"])
