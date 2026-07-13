-- ============================================================
-- 00_schemas.sql  –  Create all schemas / databases
-- ============================================================
-- Replace {catalog_name} with your Unity Catalog catalog name.
-- Leave blank for Databricks Community Edition (hive_metastore).
-- Run via ddl/run_all.py (Databricks notebook) which performs
-- token substitution automatically.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze
COMMENT 'Raw data snapshots from all landing sources. Overwrite-per-batch.';

CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver
COMMENT 'Standardised, append-only Delta tables. One row per event across all batches.';

CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold
COMMENT 'Analytics-ready star schema. SCD2 dimensions + idempotent facts.';

CREATE SCHEMA IF NOT EXISTS {catalog_name}.audit
COMMENT 'Pipeline observability: run logs, DQ check results, and alert records.';
