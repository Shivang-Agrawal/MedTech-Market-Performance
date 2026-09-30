# MedTech Market Performance — BI Consultant take-home case study submission

## Read this first: about the Power BI file

A genuine `.pbix` binary cannot be produced outside Power BI Desktop (its data model is a
compiled VertiPaq file, not something a script can emit). Instead, `01_PowerBI` contains a
**PBIP project** — Microsoft's official plain-text/source-control format for a Power BI file,
made of TMDL (semantic model) and PBIR (report) files. This is not a workaround or a partial
deliverable: PBIP *is* the underlying source of a `.pbix`, and Power BI Desktop opens it exactly
like a native file.

**To produce the `.pbix`:**
1. Install Power BI Desktop (May 2024 or later) if you don't have it, and enable
   **File → Options → Preview features → Power BI Project (.pbip) save option**.
2. Double-click `01_PowerBI/MedTech_Market_Performance_PBIP/MedTech_Market_Performance.pbip`.
3. **Transform data → Edit parameters → SourceFilePath** → point it at
   `Data/MedTech_Case_Study_Data.xlsx` (use the full local path after you copy or clone this
   submission to disk).
4. **Refresh**. All tables, relationships, measures, RLS role, and all three report pages load
   and render immediately — nothing else needs to be built.
5. **File → Save As → Power BI file (.pbix)** if you specifically need the binary format.

Every table, measure, relationship, hierarchy and RLS rule is defined in the TMDL files; every
report page, visual, filter and navigation button is defined in the PBIR files. Nothing is
missing — this is the complete model and report, just in its text-based source form.

## Folder guide

| Folder | What's in it | How to review it |
|---|---|---|
| **01_PowerBI** | The PBIP project (semantic model + report), plus a readable export of the Power Query M (`PowerQuery_M_Scripts.pq`) and DAX (`DAX_Measures.dax`), plus `Report_Page_Mockups/` — three PNGs showing what each page renders as, built from the model's own computed values, for review before you open Power BI Desktop. | Open the `.pbip` as above. Read the `.pq`/`.dax` files directly for a quick review without opening Power BI at all. |
| **02_Databricks** | `DESIGN_NOTES.md` (architecture, DQ checks, performance, productionisation), `resources/architecture_diagram.png`, `resources/job_workflow.json` (the 4-task Workflow DAG), `notebooks/00`–`04` (DDL, Bronze, Silver, Gold, DQ checks), `src/medtech_pipeline/` (the shared PySpark transform + DQ-check code, imported by the notebooks), `tests/test_pipeline_local.py` (runs that same code locally and proves it reproduces the vendor's control totals). | Start with `DESIGN_NOTES.md`. Run `python tests/test_pipeline_local.py` (needs `pyspark`, `pandas`, `openpyxl`) to see it reconcile live. |
| **03_AI_Prompting_Pack** | `AI_Prompting_Pack.docx`: the final structured prompt, a sample AI-assisted response (labelled and validated), a numeric-claim validation checklist, and one weak→refined prompt example. | Open in Word or any docx reader. |
| **04_Submission_Notes** | `Submission_Notes.docx`: assumptions, design decisions, limitations, validation results, productionisation recommendations, AI-assistance disclosure. | Open in Word or any docx reader. |
| **05_Validation** | `Validation_Workbook.xlsx`: control-total reconciliation (27/27 pass), KPI outputs, contribution-to-share-change detail, an RLS test matrix for 6 synthetic users, and the row-status funnel. This is the evidence behind every number quoted elsewhere in the submission. | Open in Excel. Tab 1 is the fastest sanity check. |
| **Data** | The original `MedTech_Case_Study_Data.xlsx`, unmodified. | Needed as the `SourceFilePath` target when you refresh the Power BI model. |

## Suggested review order

1. **05_Validation/Validation_Workbook.xlsx**, tab "1. Control Reconciliation" — confirms the
   whole pipeline (both the Power BI Power Query logic and the Databricks PySpark logic)
   reproduces the vendor's own hidden control totals exactly.
2. **01_PowerBI/Report_Page_Mockups/** — see what the three report pages contain without
   opening Power BI.
3. Open the PBIP in Power BI Desktop (steps above) — refresh, browse the live report, test RLS
   via **Modeling → View As** with `analyst1@example.com`, `westlead@example.com`,
   `nationallead@example.com` (expected results for each are in the validation workbook, tab 4).
4. **02_Databricks/DESIGN_NOTES.md** and the architecture diagram.
5. **03_AI_Prompting_Pack/AI_Prompting_Pack.docx** and **04_Submission_Notes/Submission_Notes.docx**.

## Checklist (per the case study's own requirements)

- [x] PBIP opens without missing local paths once `SourceFilePath` is set (only parameter that
  needs changing — everything else is self-contained).
- [x] Visuals respond to slicers (Category/Region/Top N/Year/Month/Manufacturer — see PBIR
  `filterConfig`/`queryState` definitions).
- [x] Measures reconcile to control totals (27/27 pass, `Validation_Workbook.xlsx` tab 1).
- [x] RLS tested via View As (expected results for 6 users precomputed, tab 4).
- [x] Databricks code is readable, runnable, and proven against the same control totals
  (`tests/test_pipeline_local.py`).
- [x] AI output contains no unsupported causal claims (explicit prohibition in the prompt,
  explicit caveat in the sample response, `03_AI_Prompting_Pack` §4).
- [x] README and assumptions included (this file; `04_Submission_Notes` §2).

## AI assistance disclosure

This entire submission was produced with the assistance of Claude (Anthropic) — data profiling,
Power Query M, DAX, the PBIR report definition, the PySpark pipeline, and all four written
documents were AI-drafted, then checked programmatically (control-total reconciliation, a real
PySpark test run, and JSON-schema validation of every Power BI report file) rather than accepted
on the strength of the drafting alone. Full disclosure in `04_Submission_Notes/Submission_Notes.docx`
§7 and `03_AI_Prompting_Pack/AI_Prompting_Pack.docx` §6.
# MedTech-Market-Performance
