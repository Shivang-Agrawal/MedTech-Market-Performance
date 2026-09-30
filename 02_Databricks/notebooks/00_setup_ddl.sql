-- Databricks notebook source
-- =====================================================================================
-- 00_setup_ddl
-- Creates the catalog/schema structure and Delta table DDL for the MedTech pipeline.
-- Run once per environment (dev/test/prod). Idempotent: CREATE ... IF NOT EXISTS throughout.
-- =====================================================================================

CREATE CATALOG IF NOT EXISTS medtech;

CREATE SCHEMA IF NOT EXISTS medtech.bronze COMMENT 'Raw, schema-validated vendor extracts. Append-only, one row per source line.';
CREATE SCHEMA IF NOT EXISTS medtech.silver COMMENT 'Cleansed, de-duplicated, mapped facts and dimensions. Business rules applied here.';
CREATE SCHEMA IF NOT EXISTS medtech.gold   COMMENT 'Published, aggregated tables consumed by Power BI and the AI/Genie prompt layer.';

-- -------------------------------------------------------------------------------------
-- BRONZE
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS medtech.bronze.vendor_sales_raw (
  TransactionDate     DATE,
  InvoiceMonth        STRING,
  InvoiceNumber       STRING,
  InvoiceLineNumber   INT,
  FacilityID          STRING,
  FacilityName        STRING,
  State               STRING,
  Region              STRING,
  ProductCode         STRING,
  ProductDescription  STRING,
  Manufacturer        STRING,
  VendorBrand         STRING,
  Units               INT,
  SalesAmount         DECIMAL(18,2),
  IngestedAt          TIMESTAMP,
  BatchID             STRING,
  _rescued_data       STRING,          -- Auto Loader: anything that didn't fit the schema
  _source_file        STRING,
  _source_row         BIGINT,
  _batch_run_id       STRING,
  _ingest_ts          TIMESTAMP
)
USING DELTA
PARTITIONED BY (BatchID)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')
COMMENT 'Raw vendor rows exactly as delivered, after schema validation (DQ-01). Never updated, only appended.';

CREATE TABLE IF NOT EXISTS medtech.bronze.product_scope_raw (
  ProductCode    STRING, InternalBrand STRING, Category STRING, Subcategory STRING,
  ActiveFlag     STRING, EffectiveFrom DATE, EffectiveTo DATE,
  _source_file   STRING, _ingest_ts TIMESTAMP
) USING DELTA COMMENT 'Raw Product_Scope reference data, refreshed in full on every run (small dimension).';

CREATE TABLE IF NOT EXISTS medtech.bronze.facility_access_raw (
  UserEmail STRING, Region STRING, State STRING, FacilityID STRING, AccessLevel STRING,
  _source_file STRING, _ingest_ts TIMESTAMP
) USING DELTA COMMENT 'Raw Facility_Access security mapping, refreshed in full on every run.';

-- -------------------------------------------------------------------------------------
-- SILVER
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS medtech.silver.dq_row_audit (
  RawRowId BIGINT, InvoiceNumber STRING, InvoiceLineNumber INT, TransactionDate DATE, MonthStart DATE,
  FacilityID STRING, ProductCodeRaw STRING, ProductCode STRING, CodeWasNormalised BOOLEAN,
  ProductDescription STRING, Manufacturer STRING, Units INT, SalesAmount DECIMAL(18,2),
  BatchID STRING, IngestedAt TIMESTAMP, InvoiceMonthMatchesDate BOOLEAN,
  IsDuplicate BOOLEAN, DuplicateType STRING, MappingStatus STRING, RowStatus STRING, IsAccepted BOOLEAN,
  _batch_run_id STRING
)
USING DELTA
PARTITIONED BY (MonthStart)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Every Bronze row with its de-duplication and mapping outcome. Drives the Data Quality report page. RowStatus in: Accepted, Rejected - Duplicate, Rejected - Unmapped product, Rejected - Inactive product, Rejected - Ambiguous mapping, Rejected - Outside effective window.';

CREATE TABLE IF NOT EXISTS medtech.silver.vendor_sales (
  RawRowId BIGINT, InvoiceNumber STRING, InvoiceLineNumber INT, TransactionDate DATE, MonthStart DATE,
  FacilityID STRING, ProductCode STRING, Manufacturer STRING, Units INT, SalesAmount DECIMAL(18,2),
  BatchID STRING, IngestedAt TIMESTAMP
)
USING DELTA
PARTITIONED BY (MonthStart)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
COMMENT 'Accepted invoice lines only: de-duplicated, active, unambiguous, in-window. Grain = one invoice line. MERGE target keyed on (InvoiceNumber, InvoiceLineNumber).';

CREATE TABLE IF NOT EXISTS medtech.silver.dq_product_mapping (
  ProductCode STRING, MappingStatus STRING, Brand STRING, Category STRING, Subcategory STRING,
  ActiveMappings INT, ScopeRowsRaw INT, EffectiveFrom DATE, EffectiveTo DATE, MappingDetail STRING,
  ProductName STRING, VendorDescriptionCount INT
) USING DELTA COMMENT 'One row per normalised product code with its governance decision (Active / Ambiguous / Inactive) and lineage detail.';

CREATE TABLE IF NOT EXISTS medtech.silver.dq_period_completeness (
  MonthStart DATE, DeliveredRows BIGINT, AcceptedRows BIGINT, LastTransactionDate DATE,
  Trailing3MonthAvgRows DOUBLE, VolumeRatio DOUBLE, MonthEnd DATE, DaysBeforeMonthEnd INT,
  CoverageCheckPassed BOOLEAN, VolumeCheckPassed BOOLEAN, IsComplete BOOLEAN
) USING DELTA COMMENT 'Monthly delivery completeness assessment; IsComplete gates YoY / share-change comparisons downstream.';

-- -------------------------------------------------------------------------------------
-- GOLD
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS medtech.gold.market_share_monthly (
  MonthStart DATE, ProductCode STRING, Brand STRING, Category STRING, Subcategory STRING,
  FacilityID STRING, State STRING, Region STRING,
  MarketSales DECIMAL(18,2), CompanySales DECIMAL(18,2), MarketUnits BIGINT, InvoiceLines BIGINT
)
USING DELTA
PARTITIONED BY (MonthStart)
CLUSTER BY (ProductCode, FacilityID)                              -- Delta liquid clustering: replaces Z-order at production scale
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
COMMENT 'Published Gold aggregate, month x product x facility grain. Consumed by Power BI (Import) and the AI/Genie prompt layer.';

CREATE TABLE IF NOT EXISTS medtech.gold.dq_check_results (
  run_id STRING, checked_at TIMESTAMP, check_id STRING, check_name STRING, severity STRING,
  passed BOOLEAN, observed STRING, threshold STRING, action STRING
) USING DELTA COMMENT 'Append-only log of every DQ check result for every run - the audit trail behind the Data Quality report page.';

CREATE TABLE IF NOT EXISTS medtech.gold.control_summary (
  run_id STRING, checked_at TIMESTAMP, metric STRING, model_value DOUBLE
) USING DELTA COMMENT 'Model-computed values for each control-total metric, for reconciliation against the vendor-supplied expected values.';

-- Grants (illustrative - align group names to your workspace's identity provider)
-- GRANT SELECT ON TABLE medtech.gold.market_share_monthly TO `analysts`;
-- GRANT SELECT ON TABLE medtech.gold.market_share_monthly TO `region_leads`;
-- GRANT SELECT ON TABLE medtech.gold.market_share_monthly TO `national_leads`;
-- Row-level security in production: apply a ROW FILTER function on FacilityID driven by
-- medtech.silver.facility_access (mirrors the Power BI RLS role) so the same grant model
-- protects both the lakehouse tables and the semantic model.
