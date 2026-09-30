# Local pipeline test

`test_pipeline_local.py` runs the exact functions used in the Databricks notebooks
(`src/medtech_pipeline/transforms.py`, `dq_checks.py`) against the case-study workbook using
local PySpark, and asserts every value matches the vendor's own `_Control_Totals` sheet
(all 7 headline totals + all 20 monthly `CleanSales_*` figures), plus the two disclosed
data-quality stories (P007 ambiguous, P028-P030 inactive).

Result of the last run (captured for the submission): **all control totals reconciled to the
cent/unit**, e.g. `CleanRows=10456`, `CleanSales=17,478,530.67`, `DuplicateTransactionsRemoved=55`,
`AmbiguousMappingRejected=398` (all `P007`), `InactiveOrUnmappedRejected=1146` (all `P028-P030`).

Run it yourself:
```bash
pip install pyspark==3.5.3 openpyxl pandas --break-system-packages
cd 02_Databricks
python tests/test_pipeline_local.py
```
