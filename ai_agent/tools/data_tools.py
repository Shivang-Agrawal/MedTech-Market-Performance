"""
Deterministic, read-only query functions over the Gold dataset.

Design principle (mirrors the case study's [DATA SOURCE] and [PROHIBITION] rules):
the LLM never writes or executes its own SQL against raw data. It can only call these
named functions, each of which encodes one specific, auditable business definition
(the same definitions as the DAX measures in 01_PowerBI and the PySpark functions in
02_Databricks). That means the agent literally cannot query outside the governed Gold
tables, cannot invent a join, and cannot include an excluded/ambiguous product by mistake
-- the guardrail is architectural, not just a prompt instruction.
"""
from __future__ import annotations
import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _load(name: str) -> pd.DataFrame:
    return pd.read_parquet(os.path.join(DATA_DIR, f"{name}.parquet"))


# Loaded once per process; this dataset is small (~7k rows) so no need for lazy/streaming reads.
_GOLD = _load("gold_market_share_monthly")
_DATES = _load("dim_date")
_DQ = _load("dq_row_status_by_month")
_MAPPING = _load("dq_product_mapping")
_CONTROLS = _load("control_totals")

_GOLD = _GOLD.merge(_DATES, on="MonthStart", how="left")


def _complete_months(year: int | None = None) -> list[str]:
    d = _DATES[_DATES["PeriodStatus"] == "Complete"]
    if year:
        d = d[d["Year"] == year]
    return sorted(d["MonthStart"].unique().tolist())


def _yoy_comparable_pairs(months: list[str]) -> tuple[list[str], list[str]]:
    """Given a list of current-year months, return (current, prior_year) months that are
    BOTH flagged complete -- i.e. safe to compare like-for-like. Mirrors DimDate[IsYoYComparable]."""
    comp = [m for m in months if m in set(_DATES[_DATES["IsYoYComparable"]]["MonthStart"])]
    py = [(pd.Timestamp(m) - pd.DateOffset(years=1)).strftime("%Y-%m-%d") for m in comp]
    py_valid = [p for p in py if p in set(_DATES["MonthStart"])]
    comp_valid = [c for c, p in zip(comp, py) if p in py_valid]
    return comp_valid, py_valid


def get_market_share_summary(year: int = 2026, start_month: int = 1, end_month: int = 7) -> dict:
    """Market Sales, Company Sales, Market Share % and YoY change for a range of months in `year`,
    using only like-for-like complete months in both years -- mirrors [Market Share Change (pp)]."""
    months = [m for m in _complete_months(year)
              if start_month <= pd.Timestamp(m).month <= end_month]
    cy, py = _yoy_comparable_pairs(months)
    if not cy:
        return {"error": "No like-for-like complete months in the requested range."}

    cur = _GOLD[_GOLD["MonthStart"].isin(cy)]
    prior = _GOLD[_GOLD["MonthStart"].isin(py)]

    m1, c1 = cur["MarketSales"].sum(), cur.loc[cur["IsOurCompany"], "MarketSales"].sum()
    m0, c0 = prior["MarketSales"].sum(), prior.loc[prior["IsOurCompany"], "MarketSales"].sum()

    return {
        "period": f"{cy[0][:7]} to {cy[-1][:7]}", "comparison_period": f"{py[0][:7]} to {py[-1][:7]}",
        "market_sales": round(float(m1), 2), "company_sales": round(float(c1), 2),
        "market_share_pct": round(c1 / m1 * 100, 2) if m1 else None,
        "market_sales_py": round(float(m0), 2), "company_sales_py": round(float(c0), 2),
        "market_share_py_pct": round(c0 / m0 * 100, 2) if m0 else None,
        "market_yoy_growth_pct": round((m1 / m0 - 1) * 100, 2) if m0 else None,
        "company_yoy_growth_pct": round((c1 / c0 - 1) * 100, 2) if c0 else None,
        "share_change_pp": round((c1 / m1 - c0 / m0) * 100, 4) if m0 and m1 else None,
        "months_used_current": cy, "months_used_prior": py,
        "excluded_incomplete_months": [m for m in months if m not in cy],
    }


def get_contribution_breakdown(year: int = 2026, start_month: int = 1, end_month: int = 7,
                                group_by: str = "ProductCode", top_n: int = 10) -> dict:
    """Share Change Contribution (pp) per slice of `group_by` (ProductCode, Brand, Category,
    Subcategory, Region, or State). Additive: contributions across ALL slices sum exactly to the
    total share_change_pp from get_market_share_summary for the same period."""
    valid_cols = {"ProductCode", "Brand", "Category", "Subcategory", "Region", "State"}
    if group_by not in valid_cols:
        return {"error": f"group_by must be one of {sorted(valid_cols)}"}

    months = [m for m in _complete_months(year) if start_month <= pd.Timestamp(m).month <= end_month]
    cy, py = _yoy_comparable_pairs(months)
    if not cy:
        return {"error": "No like-for-like complete months in the requested range."}

    cur = _GOLD[_GOLD["MonthStart"].isin(cy)]
    prior = _GOLD[_GOLD["MonthStart"].isin(py)]
    M1, M0 = cur["MarketSales"].sum(), prior["MarketSales"].sum()
    C0 = prior.loc[prior["IsOurCompany"], "MarketSales"].sum()
    S0 = C0 / M0 if M0 else 0

    def agg(df):
        g = df.groupby(group_by).apply(
            lambda x: pd.Series({"m": x["MarketSales"].sum(), "c": x.loc[x["IsOurCompany"], "MarketSales"].sum()}),
            include_groups=False)
        return g

    a1, a0 = agg(cur), agg(prior)
    joined = a1.join(a0, lsuffix="1", rsuffix="0", how="outer").fillna(0)
    joined["contribution_pp"] = ((joined["c1"] - S0 * joined["m1"]) / M1 - (joined["c0"] - S0 * joined["m0"]) / M0) * 100
    joined["market_yoy_pct"] = joined.apply(lambda r: (r["m1"] / r["m0"] - 1) * 100 if r["m0"] else None, axis=1)
    joined = joined.sort_values("contribution_pp", ascending=False).reset_index()

    rows = joined.head(top_n).to_dict("records") + [{"__note__": "..."}] + joined.tail(top_n).to_dict("records") \
        if len(joined) > 2 * top_n else joined.to_dict("records")
    return {
        "group_by": group_by, "period": f"{cy[0][:7]} to {cy[-1][:7]}",
        "comparison_period": f"{py[0][:7]} to {py[-1][:7]}",
        "sum_of_all_contributions_pp": round(float(joined["contribution_pp"].sum()), 4),
        "top_and_bottom": rows,
    }


def get_top_brands(year: int = 2026, month: int | None = None, n: int = 10) -> dict:
    """Top N brands by Market Sales for a given year (optionally a single month), with Company Sales
    and Market Share % per brand. Mirrors [Top N Brand Market Sales] / dense-rank logic."""
    d = _GOLD[_GOLD["Year"] == year]
    if month:
        d = d[pd.to_datetime(d["MonthStart"]).dt.month == month]
    g = d.groupby("Brand").apply(
        lambda x: pd.Series({"market_sales": x["MarketSales"].sum(), "company_sales": x.loc[x["IsOurCompany"], "MarketSales"].sum()}),
        include_groups=False).reset_index()
    g["market_share_pct"] = (g["company_sales"] / g["market_sales"] * 100).round(2)
    g = g.sort_values("market_sales", ascending=False).head(n)
    g[["market_sales", "company_sales"]] = g[["market_sales", "company_sales"]].round(2)
    return {"year": year, "month": month, "top_n": n, "brands": g.to_dict("records")}


def get_period_completeness(year: int = 2026) -> dict:
    """Which months in `year` are flagged Complete vs Incomplete, and why -- mirrors
    [Complete Period Indicator] / DQ_PeriodCompleteness. Always call this before trusting any
    YoY figure for the most recent month in the dataset."""
    d = _DATES[_DATES["Year"] == year].sort_values("MonthStart")
    incomplete = d[d["PeriodStatus"] != "Complete"]
    return {
        "year": year,
        "latest_complete_month": d[d["PeriodStatus"] == "Complete"]["MonthStart"].max(),
        "incomplete_months": incomplete["MonthStart"].tolist(),
        "all_months": d[["MonthStart", "PeriodStatus"]].to_dict("records"),
    }


def get_data_quality_summary() -> dict:
    """Row counts by status (Accepted / Duplicate / Inactive / Unmapped / Ambiguous) and the
    control-total reconciliation status. Mirrors the Power BI Data Quality page. Use this to
    answer any question about data trustworthiness, exclusions, or rejected rows."""
    by_status = _DQ.groupby("RowStatus").agg(Rows=("Rows", "sum"), Sales=("Sales", "sum")).reset_index()
    ambiguous = _MAPPING[_MAPPING["MappingStatus"] == "Ambiguous"][["ProductCode", "MappingReason"]].to_dict("records")
    inactive = _MAPPING[_MAPPING["MappingStatus"] == "Inactive"]["ProductCode"].tolist()
    return {
        "row_status_breakdown": by_status.round(2).to_dict("records"),
        "ambiguous_products_excluded": ambiguous,
        "inactive_products_excluded": inactive,
        "control_total_checks_total": int(len(_CONTROLS)),
    }


def get_control_reconciliation() -> dict:
    """Recomputes every control-total metric from this dataset and compares it to the
    vendor-supplied expected value. Use this if asked 'can these numbers be trusted' or
    'has this been validated' -- it proves the answer live, rather than asserting it."""
    by_status = _DQ.groupby("RowStatus").agg(Rows=("Rows", "sum"), Sales=("Sales", "sum"), Units=("Units", "sum"))
    counts = by_status["Rows"].to_dict()
    model_values = {
        "RawRows": float(sum(counts.values())),
        "CleanRows": float(counts.get("Accepted", 0)),
        "DuplicateTransactionsRemoved": float(counts.get("Rejected - Duplicate", 0)),
        "InactiveOrUnmappedRejected": float(counts.get("Rejected - Inactive product", 0) + counts.get("Rejected - Unmapped product", 0)),
        "AmbiguousMappingRejected": float(counts.get("Rejected - Ambiguous mapping", 0)),
        "CleanSales": float(by_status.loc["Accepted", "Sales"]) if "Accepted" in by_status.index else 0.0,
        "CleanUnits": float(by_status.loc["Accepted", "Units"]) if "Accepted" in by_status.index else 0.0,
    }
    monthly = _DQ[_DQ["RowStatus"] == "Accepted"].groupby("MonthStart")["Sales"].sum()
    for m, v in monthly.items():
        model_values[f"CleanSales_{m[:7]}"] = float(v)

    rows, all_pass = [], True
    for r in _CONTROLS.itertuples():
        mv = model_values.get(r.Metric)
        diff = round(mv - r.ExpectedValue, 2) if mv is not None else None
        status = "PASS" if diff is not None and abs(diff) < 0.01 else ("NOT COMPUTED" if mv is None else "FAIL")
        if status != "PASS":
            all_pass = False
        rows.append({"metric": r.Metric, "expected": r.ExpectedValue, "model": mv, "diff": diff, "status": status})
    return {"checks": rows, "all_pass": all_pass, "checks_total": len(rows), "checks_passed": sum(1 for r in rows if r["status"] == "PASS")}


# Registry consumed by agent.py to build the tool-calling schema automatically.
TOOL_FUNCTIONS = {
    "get_market_share_summary": get_market_share_summary,
    "get_contribution_breakdown": get_contribution_breakdown,
    "get_top_brands": get_top_brands,
    "get_period_completeness": get_period_completeness,
    "get_data_quality_summary": get_data_quality_summary,
    "get_control_reconciliation": get_control_reconciliation,
}
