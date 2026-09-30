"""
Proves the AI agent's tools are grounded correctly -- not just that the agent's prose sounds
right. This extends the same validation pattern used in the Power BI model (control-total
reconciliation) and the Databricks pipeline (test_pipeline_local.py) to this project's third
and final layer: the AI agent itself.

Run:  cd ai_agent && python -m pytest tests/test_tools_reconcile.py -v
      (no API key needed -- this only tests the deterministic tool functions, not the LLM)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.data_tools import (
    get_market_share_summary, get_contribution_breakdown, get_period_completeness,
    get_data_quality_summary, get_control_reconciliation,
)


def test_control_reconciliation_all_pass():
    r = get_control_reconciliation()
    failing = [c for c in r["checks"] if c["status"] != "PASS"]
    assert not failing, f"Control checks failing: {failing}"
    assert r["checks_passed"] == r["checks_total"]


def test_market_share_matches_known_figures():
    """These are the same figures independently verified in the Power BI model and the
    Databricks PySpark pipeline (Validation_Workbook.xlsx, tab 2) -- the agent's tool must
    agree with them to the cent/pp, not approximately."""
    r = get_market_share_summary(2026, 1, 7)
    assert abs(r["market_sales"] - 6460624.33) < 0.01
    assert abs(r["company_sales"] - 1556673.94) < 0.01
    assert abs(r["share_change_pp"] - 0.6852) < 0.001
    assert r["excluded_incomplete_months"] == []


def test_contribution_sums_to_total_share_change():
    """The additive property that makes 'which products contributed most' answerable at all:
    every product's contribution must sum EXACTLY to the total share change for the period."""
    total = get_market_share_summary(2026, 1, 7)["share_change_pp"]
    breakdown = get_contribution_breakdown(2026, 1, 7, group_by="ProductCode", top_n=100)
    assert abs(breakdown["sum_of_all_contributions_pp"] - total) < 0.001


def test_period_completeness_flags_incomplete_month():
    r = get_period_completeness(2026)
    assert r["latest_complete_month"] == "2026-07-01"
    assert "2026-08-01" in r["incomplete_months"]


def test_ambiguous_and_inactive_products_are_named():
    r = get_data_quality_summary()
    codes = {row["ProductCode"] for row in r["ambiguous_products_excluded"]}
    assert codes == {"P007"}
    assert set(r["inactive_products_excluded"]) == {"P028", "P029", "P030"}


if __name__ == "__main__":
    test_control_reconciliation_all_pass()
    test_market_share_matches_known_figures()
    test_contribution_sums_to_total_share_change()
    test_period_completeness_flags_incomplete_month()
    test_ambiguous_and_inactive_products_are_named()
    print("All agent tool-validation tests passed.")
