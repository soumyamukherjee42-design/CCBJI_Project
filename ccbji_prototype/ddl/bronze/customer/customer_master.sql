-- ============================================================
-- bronze/customer/customer_master.sql
-- ============================================================
-- Overwrite mode – each batch replaces the previous snapshot.
-- ============================================================

CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.customer_master
(
    customer_id     STRING        NOT NULL  COMMENT 'Customer master key (SAP/Azure SQL)',
    customer_name   STRING                  COMMENT 'Full legal entity name',
    region          STRING                  COMMENT 'Sales region code',
    channel         STRING                  COMMENT 'Trade channel (GT / MT / EC / ...)',
    last_modified   TIMESTAMP               COMMENT 'Source system last-modified timestamp',

    batch_id        STRING        NOT NULL  COMMENT 'ADF pipeline run ID',
    processed_date  TIMESTAMP               COMMENT 'UTC timestamp when written to Bronze'
)
USING DELTA
PARTITIONED BY (batch_id)
COMMENT 'Raw customer master data from Azure SQL (via Parquet export). Overwritten each batch.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
