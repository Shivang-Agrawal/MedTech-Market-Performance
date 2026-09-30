"""
Pure PySpark transformation logic for the MedTech market-share pipeline.

Every function takes and returns DataFrames (no I/O), so the same code runs
  * inside Databricks notebooks / jobs (Bronze -> Silver -> Gold), and
  * locally in unit tests (tests/test_pipeline_local.py) against the case-study workbook.

Business rules implemented here are identical to the Power BI Power Query layer, and the local
test proves they reproduce the vendor control totals exactly.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --------------------------------------------------------------------------------------------
# Contract: expected source schemas (used for schema validation in Bronze)
# --------------------------------------------------------------------------------------------
VENDOR_SCHEMA = T.StructType([
    T.StructField("TransactionDate", T.DateType()),
    T.StructField("InvoiceMonth", T.StringType()),
    T.StructField("InvoiceNumber", T.StringType()),
    T.StructField("InvoiceLineNumber", T.IntegerType()),
    T.StructField("FacilityID", T.StringType()),
    T.StructField("FacilityName", T.StringType()),
    T.StructField("State", T.StringType()),
    T.StructField("Region", T.StringType()),
    T.StructField("ProductCode", T.StringType()),
    T.StructField("ProductDescription", T.StringType()),
    T.StructField("Manufacturer", T.StringType()),
    T.StructField("VendorBrand", T.StringType()),
    T.StructField("Units", T.IntegerType()),
    T.StructField("SalesAmount", T.DecimalType(18, 2)),
    T.StructField("IngestedAt", T.TimestampType()),
    T.StructField("BatchID", T.StringType()),
])
SCOPE_SCHEMA = T.StructType([
    T.StructField("ProductCode", T.StringType()),
    T.StructField("InternalBrand", T.StringType()),
    T.StructField("Category", T.StringType()),
    T.StructField("Subcategory", T.StringType()),
    T.StructField("ActiveFlag", T.StringType()),
    T.StructField("EffectiveFrom", T.DateType()),
    T.StructField("EffectiveTo", T.DateType()),
])
ACCESS_SCHEMA = T.StructType([
    T.StructField("UserEmail", T.StringType()),
    T.StructField("Region", T.StringType()),
    T.StructField("State", T.StringType()),
    T.StructField("FacilityID", T.StringType()),
    T.StructField("AccessLevel", T.StringType()),
])
NATURAL_KEY = ["InvoiceNumber", "InvoiceLineNumber"]


def validate_schema(df: DataFrame, expected: T.StructType) -> dict:
    """Return missing / unexpected columns and columns whose values cannot be cast to the contract type."""
    actual = set(df.columns)
    required = {f.name for f in expected.fields}
    missing = sorted(required - actual)
    extra = sorted(actual - required - {"_rescued_data", "_source_file", "_ingest_ts", "_source_row", "_batch_run_id"})
    uncastable = {}
    if not missing:
        for f in expected.fields:
            if isinstance(f.dataType, T.StringType):
                continue
            n = df.filter(F.col(f.name).isNotNull() & F.col(f.name).cast(f.dataType).isNull()).limit(1).count()
            if n:
                uncastable[f.name] = str(f.dataType)
    return {"missing": missing, "extra": extra, "uncastable": uncastable, "ok": not missing and not uncastable}


def conform(df: DataFrame, expected: T.StructType) -> DataFrame:
    """Select + cast to the contract; keep lineage columns if present."""
    lineage = [c for c in ["_source_file", "_ingest_ts", "_source_row", "_batch_run_id", "_rescued_data"] if c in df.columns]
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in expected.fields], *lineage)


# --------------------------------------------------------------------------------------------
# Silver: vendor hygiene, de-duplication
# --------------------------------------------------------------------------------------------
def clean_vendor(df: DataFrame) -> DataFrame:
    """Trim text, normalise product/facility/state codes, keep the raw code for audit, add consistency flags."""
    for c in ["InvoiceNumber", "FacilityName", "Region", "ProductDescription", "Manufacturer", "VendorBrand", "BatchID"]:
        df = df.withColumn(c, F.trim(F.col(c)))
    return (df
            .filter(F.col("InvoiceNumber").isNotNull() & (F.col("InvoiceNumber") != ""))
            .withColumnRenamed("ProductCode", "ProductCodeRaw")
            .withColumn("ProductCode", F.upper(F.trim(F.col("ProductCodeRaw"))))
            .withColumn("CodeWasNormalised", F.col("ProductCodeRaw") != F.col("ProductCode"))
            .withColumn("FacilityID", F.upper(F.trim("FacilityID")))
            .withColumn("State", F.upper(F.trim("State")))
            .withColumn("MonthStart", F.trunc("TransactionDate", "month"))
            .withColumn("InvoiceMonthMatchesDate", F.col("InvoiceMonth") == F.date_format("TransactionDate", "yyyy-MM"))
            .withColumn("RecordHash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in
                        ["InvoiceNumber", "InvoiceLineNumber", "TransactionDate", "FacilityID", "ProductCode", "Manufacturer", "Units", "SalesAmount"]]), 256)))


def flag_duplicates(df: DataFrame, order_col: str = "_source_row") -> DataFrame:
    """One surviving row per invoice line: latest IngestedAt wins, ties -> earliest source row.
    DuplicateType distinguishes exact re-deliveries from superseded (changed) versions."""
    w = Window.partitionBy(*NATURAL_KEY).orderBy(F.col("IngestedAt").desc(), F.col(order_col).asc())
    versions = Window.partitionBy(*NATURAL_KEY)
    return (df.withColumn("_rn", F.row_number().over(w))
              .withColumn("_versions", F.size(F.collect_set("RecordHash").over(versions)))
              .withColumn("IsDuplicate", F.col("_rn") > 1)
              .withColumn("DuplicateType", F.when(F.col("_rn") == 1, F.lit(None))
                                           .when(F.col("_versions") == 1, F.lit("Exact duplicate"))
                                           .otherwise(F.lit("Superseded version")))
              .drop("_rn", "_versions"))


# --------------------------------------------------------------------------------------------
# Silver: product scope mapping with ambiguity detection
# --------------------------------------------------------------------------------------------
def build_product_mapping(scope: DataFrame) -> DataFrame:
    """One row per normalised code with MappingStatus Active / Ambiguous / Inactive.
    Exact duplicate rows are collapsed first (they are not conflicts); >1 DISTINCT active mapping = Ambiguous."""
    s = (scope.filter(F.col("ProductCode").isNotNull() & (F.trim("ProductCode") != ""))
              .withColumn("ProductCode", F.upper(F.trim("ProductCode")))
              .withColumn("ActiveFlag", F.upper(F.trim("ActiveFlag")))
              .withColumn("InternalBrand", F.trim("InternalBrand"))
              .withColumn("Category", F.trim("Category"))
              .withColumn("Subcategory", F.trim("Subcategory"))
              .withColumn("EffectiveFrom", F.col("EffectiveFrom").cast("string").cast("date"))
              .withColumn("EffectiveTo", F.col("EffectiveTo").cast("string").cast("date")))
    raw_counts = s.groupBy("ProductCode").agg(F.count("*").alias("ScopeRowsRaw"),
                                              F.sum(F.when(F.col("ActiveFlag") == "Y", 1).otherwise(0)).alias("ActiveRowsRaw"))
    d = s.dropDuplicates()
    active = d.filter(F.col("ActiveFlag") == "Y")
    act = (active.groupBy("ProductCode")
                 .agg(F.countDistinct("InternalBrand", "Category", "Subcategory").alias("ActiveMappings"),
                      F.first("InternalBrand", ignorenulls=True).alias("Brand"), F.first("Category", ignorenulls=True).alias("Category"),
                      F.first("Subcategory", ignorenulls=True).alias("Subcategory"),
                      F.min("EffectiveFrom").alias("EffectiveFrom"), F.max(F.coalesce("EffectiveTo", F.lit("9999-12-31").cast("date"))).alias("EffectiveTo"),
                      F.concat_ws(" | ", F.sort_array(F.collect_set(F.concat_ws(" / ", "InternalBrand", "Category", "Subcategory",
                                  F.concat(F.lit("from "), F.col("EffectiveFrom").cast("string")))))).alias("MappingDetail")))
    codes = d.select("ProductCode").distinct().join(raw_counts, "ProductCode").join(act, "ProductCode", "left") \
             .withColumn("ActiveMappings", F.coalesce("ActiveMappings", F.lit(0)))
    return codes.withColumn("MappingStatus", F.when(F.col("ActiveMappings") == 1, "Active")
                                              .when(F.col("ActiveMappings") > 1, "Ambiguous").otherwise("Inactive"))


def classify_rows(vendor: DataFrame, mapping: DataFrame) -> DataFrame:
    """Assign exactly one RowStatus to every vendor row. Precedence: duplicate > unmapped > inactive > ambiguous > outside window > accepted."""
    m = mapping.select("ProductCode", "MappingStatus", "Brand", "Category", "Subcategory", "EffectiveFrom", "EffectiveTo")
    j = vendor.join(F.broadcast(m), "ProductCode", "left")   # mapping is tiny -> broadcast join, no shuffle of the fact
    status = (F.when(F.col("IsDuplicate"), "Rejected - Duplicate")
               .when(F.col("MappingStatus").isNull(), "Rejected - Unmapped product")
               .when(F.col("MappingStatus") == "Inactive", "Rejected - Inactive product")
               .when(F.col("MappingStatus") == "Ambiguous", "Rejected - Ambiguous mapping")
               .when((F.col("TransactionDate") < F.col("EffectiveFrom")) | (F.col("TransactionDate") > F.col("EffectiveTo")),
                     "Rejected - Outside effective window")
               .otherwise("Accepted"))
    return j.withColumn("RowStatus", status).withColumn("IsAccepted", F.col("RowStatus") == "Accepted")


def canonical_descriptions(vendor_dedup: DataFrame) -> DataFrame:
    """Most frequent description per code (ties -> alphabetical) = governed product name."""
    c = vendor_dedup.groupBy("ProductCode", "ProductDescription").count()
    w = Window.partitionBy("ProductCode").orderBy(F.col("count").desc(), F.col("ProductDescription").asc())
    return (c.withColumn("_r", F.row_number().over(w)).groupBy("ProductCode")
             .agg(F.max(F.when(F.col("_r") == 1, F.col("ProductDescription"))).alias("ProductName"),
                  F.count("*").alias("VendorDescriptionCount")))


# --------------------------------------------------------------------------------------------
# Period completeness
# --------------------------------------------------------------------------------------------
def period_completeness(audit: DataFrame, volume_threshold: float = 0.8, coverage_days: int = 3) -> DataFrame:
    """A month is complete when its last transaction is within `coverage_days` of month end AND its delivered
    rows are >= `volume_threshold` x the trailing-3-complete-month average."""
    m = (audit.filter(~F.col("IsDuplicate")).groupBy("MonthStart")
              .agg(F.count("*").alias("DeliveredRows"), F.sum(F.col("IsAccepted").cast("int")).alias("AcceptedRows"),
                   F.max("TransactionDate").alias("LastTransactionDate"), F.countDistinct("BatchID").alias("BatchCount")))
    w = Window.orderBy("MonthStart").rowsBetween(-3, -1)
    return (m.withColumn("MonthEnd", F.last_day("MonthStart"))
             .withColumn("Trailing3MonthAvgRows", F.avg("DeliveredRows").over(w))
             .withColumn("VolumeRatio", F.col("DeliveredRows") / F.col("Trailing3MonthAvgRows"))
             .withColumn("DaysBeforeMonthEnd", F.datediff("MonthEnd", "LastTransactionDate"))
             .withColumn("CoverageCheckPassed", F.col("DaysBeforeMonthEnd") <= coverage_days)
             .withColumn("VolumeCheckPassed", F.col("Trailing3MonthAvgRows").isNull() | (F.col("VolumeRatio") >= volume_threshold))
             .withColumn("IsComplete", F.col("CoverageCheckPassed") & F.col("VolumeCheckPassed")))


# --------------------------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------------------------
def market_share_monthly(fact: DataFrame, our_company: str = "Our Company") -> DataFrame:
    """Gold aggregate at month x product x facility grain - the table Power BI / Genie / the AI prompt read."""
    return (fact.groupBy("MonthStart", "ProductCode", "Brand", "Category", "Subcategory", "FacilityID", "State", "Region")
                .agg(F.sum("SalesAmount").alias("MarketSales"),
                     F.sum(F.when(F.col("Manufacturer") == our_company, F.col("SalesAmount")).otherwise(0)).alias("CompanySales"),
                     F.sum("Units").alias("MarketUnits"), F.count("*").alias("InvoiceLines")))


def control_summary(audit: DataFrame) -> dict:
    """Row/value totals used for reconciliation against the vendor control totals."""
    r = audit.groupBy("RowStatus").agg(F.count("*").alias("n")).collect()
    counts = {x["RowStatus"]: x["n"] for x in r}
    acc = audit.filter("IsAccepted").agg(F.sum("SalesAmount").alias("s"), F.sum("Units").alias("u")).collect()[0]
    monthly = {row["m"]: float(row["s"]) for row in audit.filter("IsAccepted").groupBy(F.date_format("MonthStart", "yyyy-MM").alias("m"))
               .agg(F.sum("SalesAmount").alias("s")).collect()}
    out = {"RawRows": sum(counts.values()), "CleanRows": counts.get("Accepted", 0),
           "DuplicateTransactionsRemoved": counts.get("Rejected - Duplicate", 0),
           "InactiveOrUnmappedRejected": counts.get("Rejected - Inactive product", 0) + counts.get("Rejected - Unmapped product", 0)
                                         + counts.get("Rejected - Outside effective window", 0),
           "AmbiguousMappingRejected": counts.get("Rejected - Ambiguous mapping", 0),
           "CleanSales": float(acc["s"] or 0), "CleanUnits": int(acc["u"] or 0)}
    out.update({f"CleanSales_{k}": v for k, v in monthly.items()})
    return out
