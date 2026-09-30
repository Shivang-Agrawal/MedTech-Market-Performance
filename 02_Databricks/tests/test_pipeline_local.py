"""
Runs the real Bronze -> Silver -> Gold transform functions (src/medtech_pipeline) with local
PySpark against the case-study workbook, and asserts the results equal the vendor's own
_Control_Totals sheet. This is the same test that would run in CI / as a Databricks job task
before every deploy; here it doubles as evidence the pipeline design is correct, independent of
the Power BI model (built separately in Python/pandas for the report).

Run:  cd 02_Databricks && python -m pytest tests/test_pipeline_local.py -q
      (needs: pip install pyspark openpyxl pandas --break-system-packages)
"""
import sys, os, glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import pandas as pd
from pyspark.sql import SparkSession
from medtech_pipeline import transforms as tr
from medtech_pipeline import dq_checks as dq

SRC = os.environ.get("MEDTECH_XLSX", "/mnt/user-data/uploads/MedTech_Case_Study_Data.xlsx")


def get_spark():
    return (SparkSession.builder.master("local[*]").appName("medtech-local-test")
            .config("spark.sql.shuffle.partitions", "4").config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def load_expected_controls():
    wb = pd.read_excel(SRC, sheet_name="_Control_Totals", header=0)
    return {r.Metric: float(r.Value) for r in wb.itertuples()}


def build_frames(spark):
    vendor_pd = pd.read_excel(SRC, sheet_name="Vendor_Market_Data")
    vendor_pd["_source_row"] = range(1, len(vendor_pd) + 1)
    scope_pd = pd.read_excel(SRC, sheet_name="Product_Scope")
    access_pd = pd.read_excel(SRC, sheet_name="Facility_Access")

    vendor = spark.createDataFrame(vendor_pd)
    for c in ["TransactionDate"]:
        vendor = vendor.withColumn(c, vendor[c].cast("date"))
    for c in ["IngestedAt"]:
        vendor = vendor.withColumn(c, vendor[c].cast("timestamp"))

    scope = spark.createDataFrame(scope_pd)
    access = spark.createDataFrame(access_pd)
    facilities = vendor.select("FacilityID").distinct()  # stand-in for a facility master in this local test
    return vendor, scope, access, facilities


def test_pipeline_reproduces_control_totals():
    spark = get_spark()
    expected = load_expected_controls()
    vendor, scope, access, _ = build_frames(spark)

    schema_report = tr.validate_schema(vendor, tr.VENDOR_SCHEMA)
    assert schema_report["ok"], schema_report

    cleaned = tr.clean_vendor(vendor)
    deduped = tr.flag_duplicates(cleaned, order_col="_source_row")
    mapping = tr.build_product_mapping(scope)
    audit = tr.classify_rows(deduped, mapping)
    completeness = tr.period_completeness(audit)
    facilities = audit.select("FacilityID").distinct()

    got = tr.control_summary(audit)
    mismatches = {k: (got.get(k), v) for k, v in expected.items() if abs(got.get(k, 0) - v) > 0.01}
    assert not mismatches, f"Control total mismatches: {mismatches}"

    checks = dq.run_checks(audit, mapping, completeness, facilities, expected, schema_report)
    blocking_failures = [c for c in checks if c.severity == "BLOCK" and not c.passed]
    assert not blocking_failures, blocking_failures

    # sanity on the two disclosed data-quality stories
    amb = mapping.filter("MappingStatus = 'Ambiguous'").select("ProductCode").collect()
    assert {r.ProductCode for r in amb} == {"P007"}
    inactive = mapping.filter("MappingStatus = 'Inactive'").select("ProductCode").collect()
    assert {r.ProductCode for r in inactive} == {"P028", "P029", "P030"}

    print("ALL CONTROL TOTALS RECONCILE:", got)
    spark.stop()


if __name__ == "__main__":
    test_pipeline_reproduces_control_totals()
    print("OK")
