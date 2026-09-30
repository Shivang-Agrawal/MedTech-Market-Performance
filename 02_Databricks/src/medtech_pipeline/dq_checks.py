"""
Data-quality rule engine. Each check returns a result row; the orchestrator decides the action:
  BLOCK      -> fail the job before Gold is published (Gold keeps the last good version)
  QUARANTINE -> offending rows written to silver.vendor_sales_quarantine, pipeline continues, steward notified
  WARN       -> pipeline continues, alert raised and shown on the Power BI Data Quality page
"""
from dataclasses import dataclass, asdict
from pyspark.sql import DataFrame, functions as F


@dataclass
class CheckResult:
    check_id: str
    check_name: str
    severity: str        # BLOCK / QUARANTINE / WARN
    passed: bool
    observed: str
    threshold: str
    action: str


def run_checks(audit: DataFrame, mapping: DataFrame, completeness: DataFrame, facilities: DataFrame,
               control_expected: dict, schema_report: dict, dup_rate_threshold: float = 0.02,
               tolerance: float = 0.005):
    res = []
    raw = audit.count()

    # DQ-01 schema contract
    res.append(CheckResult("DQ-01", "Schema contract (required columns present & castable)", "BLOCK", schema_report["ok"],
                           "missing=%s uncastable=%s" % (schema_report['missing'], list(schema_report['uncastable'])),
                           "no missing/uncastable columns",
                           "Stop run, move file to /landing/_rejected, alert data engineering"))

    # DQ-02 uniqueness of natural key among accepted rows
    dup_keys = audit.filter("IsAccepted").groupBy("InvoiceNumber", "InvoiceLineNumber").count().filter("count > 1").count()
    res.append(CheckResult("DQ-02", "Accepted rows unique on InvoiceNumber + InvoiceLineNumber", "BLOCK", dup_keys == 0,
                           "%d duplicated keys" % dup_keys, "0", "Stop Gold publish; investigate de-duplication logic"))

    # DQ-03 duplicate delivery rate
    dups = audit.filter("IsDuplicate").count(); rate = dups / raw if raw else 0
    res.append(CheckResult("DQ-03", "Duplicate invoice-line rate", "WARN", rate <= dup_rate_threshold,
                           "%d rows (%.2f%%)" % (dups, rate * 100), "<= %.0f%%" % (dup_rate_threshold * 100),
                           "Duplicates removed automatically; alert vendor manager if above threshold"))

    # DQ-04 ambiguous product mappings
    amb = mapping.filter("MappingStatus = 'Ambiguous'")
    amb_codes = [r.ProductCode for r in amb.select("ProductCode").collect()]
    amb_rows = audit.filter(F.col("RowStatus") == "Rejected - Ambiguous mapping").count()
    res.append(CheckResult("DQ-04", "Product codes with conflicting active mappings", "QUARANTINE", not amb_codes,
                           "codes=%s rows=%d" % (amb_codes, amb_rows), "0 codes",
                           "Rows quarantined; ticket to product master-data steward; auto-released next run once resolved"))

    # DQ-05 unmapped / inactive products
    unm = audit.filter(F.col("RowStatus").isin("Rejected - Unmapped product", "Rejected - Inactive product")).groupBy("RowStatus").count().collect()
    unm_d = {r.RowStatus: r["count"] for r in unm}
    unmapped = unm_d.get("Rejected - Unmapped product", 0)
    res.append(CheckResult("DQ-05", "Vendor codes not found in Product_Scope (unmapped)", "QUARANTINE", unmapped == 0,
                           "unmapped=%d inactive=%d (inactive = expected exclusion)" % (unmapped, unm_d.get('Rejected - Inactive product', 0)),
                           "0 unmapped", "Unmapped rows quarantined and listed for steward; inactive rows excluded by design"))

    # DQ-06 mandatory values & ranges
    bad = audit.filter(F.col("SalesAmount").isNull() | (F.col("SalesAmount") < 0) | F.col("Units").isNull() | (F.col("Units") <= 0)
                       | F.col("TransactionDate").isNull() | F.col("FacilityID").isNull() | F.col("ProductCode").isNull()).count()
    res.append(CheckResult("DQ-06", "Mandatory fields present, SalesAmount >= 0, Units > 0", "QUARANTINE", bad == 0,
                           "%d rows" % bad, "0", "Rows quarantined with reason; vendor notified"))

    # DQ-07 InvoiceMonth vs TransactionDate consistency
    mism = audit.filter(~F.col("InvoiceMonthMatchesDate")).count()
    res.append(CheckResult("DQ-07", "InvoiceMonth consistent with TransactionDate", "WARN", mism == 0, "%d rows" % mism, "0",
                           "TransactionDate is authoritative; mismatches reported to vendor"))

    # DQ-08 referential integrity: facilities known to the security model
    orphan = audit.select("FacilityID").distinct().join(facilities.select("FacilityID"), "FacilityID", "left_anti").count()
    res.append(CheckResult("DQ-08", "Every FacilityID exists in the facility / access master", "WARN", orphan == 0,
                           "%d unknown facilities" % orphan, "0",
                           "Rows kept but invisible under RLS until access mapping is updated; alert security admin"))

    # DQ-09 period completeness
    inc = [str(r.MonthStart)[:7] for r in completeness.filter("NOT IsComplete").select("MonthStart").collect()]
    res.append(CheckResult("DQ-09", "Latest delivered month complete (coverage + volume)", "WARN", not inc,
                           "incomplete=%s" % inc, "none",
                           "Month flagged is_complete = false in Gold; excluded from KPIs/YoY; banner shown in report"))

    # DQ-10 reconciliation: raw = accepted + every rejection bucket, and vs vendor control totals
    from .transforms import control_summary
    got = control_summary(audit)
    ok_internal = (got["CleanRows"] + got["DuplicateTransactionsRemoved"] + got["InactiveOrUnmappedRejected"]
                   + got["AmbiguousMappingRejected"]) == got["RawRows"]
    diffs = {}
    if control_expected:
        for k, v in control_expected.items():
            if abs(got.get(k, 0) - v) > tolerance:
                diffs[k] = (got.get(k), v)
    res.append(CheckResult("DQ-10", "Reconciliation: raw = accepted + rejected, and = vendor control totals", "BLOCK",
                           ok_internal and not diffs,
                           "internal_balanced=%s control_diffs=%s" % (ok_internal, diffs), "|diff| <= %s" % tolerance,
                           "Stop Gold publish (last good snapshot stays live); alert data engineering + vendor"))
    return res


def results_df(spark, results, run_id: str):
    return spark.createDataFrame([dict(asdict(r), run_id=run_id) for r in results]).withColumn("checked_at", F.current_timestamp())
