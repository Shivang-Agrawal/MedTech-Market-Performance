# Databricks notebook source
# =====================================================================================
# 02_silver_transform
# Cleanses the Bronze extract, de-duplicates invoice lines, applies the product-scope
# mapping (with ambiguity detection), classifies every row into exactly one RowStatus,
# quarantines everything that is not Accepted, and assesses monthly period completeness.
#
# Idempotent re-run: vendor_sales and dq_row_audit are written with Delta MERGE keyed on
# the natural key (InvoiceNumber, InvoiceLineNumber) [+ RawRowId for the audit table], so
# re-running this notebook on the same Bronze batch updates rows in place rather than
# duplicating them.
# Incremental: only Bronze rows with _batch_run_id in the newly-ingested set are processed;
# reference tables (dq_product_mapping) are small and rebuilt in full each run.
# =====================================================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/medtech/src")
from medtech_pipeline import transforms as tr
from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("batch_run_id", "", "Run id to process (blank = all unprocessed Bronze rows)")
batch_run_id = dbutils.widgets.get("batch_run_id")

# COMMAND ----------
bronze = spark.table("medtech.bronze.vendor_sales_raw")
if batch_run_id:
    bronze = bronze.filter(F.col("_batch_run_id") == batch_run_id)
scope = spark.table("medtech.bronze.product_scope_raw")

cleaned = tr.clean_vendor(bronze)
# RawRowId: stable surrogate key derived from the immutable _source_file + _source_row, not from row order
cleaned = cleaned.withColumn("RawRowId", F.crc32(F.concat_ws("::", "_source_file", F.col("_source_row").cast("string"))))
deduped = tr.flag_duplicates(cleaned, order_col="_source_row")

mapping = tr.build_product_mapping(scope)
audit = tr.classify_rows(deduped, mapping)

# COMMAND ----------
# ---- dq_row_audit: MERGE keyed on RawRowId (idempotent re-run of the same Bronze rows) ----
audit_out = audit.select(
    "RawRowId", "InvoiceNumber", "InvoiceLineNumber", "TransactionDate", "MonthStart", "FacilityID",
    "ProductCodeRaw", "ProductCode", "CodeWasNormalised", "ProductDescription", "Manufacturer", "Units",
    "SalesAmount", "BatchID", "IngestedAt", "InvoiceMonthMatchesDate", "IsDuplicate", "DuplicateType",
    "MappingStatus", "RowStatus", "IsAccepted", "_batch_run_id")

if spark.catalog.tableExists("medtech.silver.dq_row_audit") and spark.table("medtech.silver.dq_row_audit").count() > 0:
    tgt = DeltaTable.forName(spark, "medtech.silver.dq_row_audit")
    (tgt.alias("t").merge(audit_out.alias("s"), "t.RawRowId = s.RawRowId")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
else:
    audit_out.write.mode("overwrite").partitionBy("MonthStart").saveAsTable("medtech.silver.dq_row_audit")

# ---- vendor_sales: accepted rows only, MERGE keyed on the natural key ----
accepted = audit.filter("IsAccepted").select(
    "RawRowId", "InvoiceNumber", "InvoiceLineNumber", "TransactionDate", "MonthStart", "FacilityID",
    "ProductCode", "Manufacturer", "Units", "SalesAmount", "BatchID", "IngestedAt")

if spark.catalog.tableExists("medtech.silver.vendor_sales") and spark.table("medtech.silver.vendor_sales").count() > 0:
    tgt = DeltaTable.forName(spark, "medtech.silver.vendor_sales")
    # A row previously Accepted can later become Rejected (e.g. a newer, different-valued delivery for the
    # same key supersedes it, or its product mapping changes from Active to Ambiguous). Delete such rows from
    # vendor_sales first so the accepted table never holds a row that dq_row_audit no longer marks Accepted.
    now_rejected = (spark.table("medtech.silver.dq_row_audit").filter("NOT IsAccepted")
                    .select("InvoiceNumber", "InvoiceLineNumber").distinct())
    (tgt.alias("t").merge(now_rejected.alias("r"), "t.InvoiceNumber = r.InvoiceNumber AND t.InvoiceLineNumber = r.InvoiceLineNumber")
        .whenMatchedDelete().execute())
    (tgt.alias("t").merge(accepted.alias("s"), "t.InvoiceNumber = s.InvoiceNumber AND t.InvoiceLineNumber = s.InvoiceLineNumber")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
else:
    accepted.write.mode("overwrite").partitionBy("MonthStart").saveAsTable("medtech.silver.vendor_sales")

# ---- quarantine: everything not accepted, with the reason, for steward review ----
quarantine = audit.filter("NOT IsAccepted").select(
    "RawRowId", "InvoiceNumber", "InvoiceLineNumber", "ProductCode", "RowStatus", "MappingStatus",
    "SalesAmount", "MonthStart", "_batch_run_id")
quarantine.write.mode("append").saveAsTable("medtech.silver.dq_row_audit_quarantine") \
    if spark.catalog.tableExists("medtech.silver.dq_row_audit_quarantine") \
    else quarantine.write.mode("overwrite").saveAsTable("medtech.silver.dq_row_audit_quarantine")

# ---- dq_product_mapping: small reference, full overwrite ----
canon = tr.canonical_descriptions(deduped)
mapping_out = (mapping.join(canon, "ProductCode", "left")
               .select("ProductCode", "MappingStatus", "Brand", "Category", "Subcategory", "ActiveMappings",
                        "ScopeRowsRaw", "EffectiveFrom", "EffectiveTo", "MappingDetail", "ProductName", "VendorDescriptionCount"))
mapping_out.write.mode("overwrite").saveAsTable("medtech.silver.dq_product_mapping")

# ---- dq_period_completeness ----
full_audit = spark.table("medtech.silver.dq_row_audit")
completeness = tr.period_completeness(full_audit)
completeness.write.mode("overwrite").saveAsTable("medtech.silver.dq_period_completeness")

print("Silver complete:",
      "vendor_sales rows =", spark.table("medtech.silver.vendor_sales").count(),
      "| quarantined this run =", quarantine.count(),
      "| ambiguous codes =", mapping.filter("MappingStatus='Ambiguous'").count())
