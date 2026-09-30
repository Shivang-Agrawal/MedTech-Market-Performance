# Databricks notebook source
# =====================================================================================
# 04_dq_checks
# Runs the 10-check DQ rule engine (src/medtech_pipeline/dq_checks.py) against the
# just-built Silver tables, logs every result to medtech.gold.dq_check_results, and fails
# this task (raises) if any BLOCK-severity check did not pass - which stops the Databricks
# Workflow before 03_gold_publish runs, per the job DAG in resources/job_workflow.json.
# QUARANTINE/WARN failures do not stop the run; they are surfaced on the Data Quality page.
# =====================================================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/medtech/src")
from medtech_pipeline import transforms as tr
from medtech_pipeline import dq_checks as dq
from pyspark.sql import functions as F
import pandas as pd

XLSX_PATH = dbutils.widgets.get("xlsx_path") if "xlsx_path" in [w for w in dbutils.widgets.getAll()] else \
            "/Volumes/medtech/landing/vendor_sales/MedTech_Case_Study_Data.xlsx"
try:
    control_expected = {r.Metric: float(r.Value) for r in pd.read_excel(XLSX_PATH, sheet_name="_Control_Totals").itertuples()}
except Exception:
    control_expected = None  # production: vendor does not supply control totals; DQ-10 falls back to internal balance only

audit = spark.table("medtech.silver.dq_row_audit")
mapping = spark.table("medtech.silver.dq_product_mapping")
completeness = spark.table("medtech.silver.dq_period_completeness")
facilities = spark.table("medtech.bronze.facility_access_raw").select("FacilityID").distinct()
bronze = spark.table("medtech.bronze.vendor_sales_raw")
schema_report = tr.validate_schema(bronze, tr.VENDOR_SCHEMA)

results = dq.run_checks(audit, mapping, completeness, facilities, control_expected, schema_report)

run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().getOrElse("local")
dq.results_df(spark, results, run_id).write.mode("append").saveAsTable("medtech.gold.dq_check_results")

for r in results:
    flag = "PASS" if r.passed else f"FAIL ({r.severity})"
    print(f"[{r.check_id}] {r.check_name}: {flag} - observed={r.observed}")

blocking_failures = [r for r in results if r.severity == "BLOCK" and not r.passed]
if blocking_failures:
    details = "; ".join(f"{r.check_id} {r.check_name}: {r.observed}" for r in blocking_failures)
    dbutils.notebook.exit(f"BLOCKED: {details}")  # non-zero-equivalent exit -> Workflow marks task failed, downstream Gold task does not run
    raise Exception(f"Blocking DQ check(s) failed: {details}")

print(f"All BLOCK-severity checks passed ({len(results)} checks total). Proceeding to Gold publish.")
