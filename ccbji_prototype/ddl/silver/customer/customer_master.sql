-- ============================================================
-- silver/customer/customer_master.sql
-- ============================================================
-- Append mode – full history of all batches is retained.
-- Error table captures DQ-rejected rows (permanent audit trail).
-- ============================================================

CREATE TABLE IF NOT EXISTS {catalog_name}.silver.customer_master
(
    customer_id     STRING        NOT NULL  COMMENT 'Customer master key (trimmed)',
    customer_name   STRING                  COMMENT 'Full legal entity name (trimmed)',
    region          STRING                  COMMENT 'Sales region code (trimmed)',
    channel         STRING                  COMMENT 'Trade channel (trimmed)',
    last_modified   TIMESTAMP               COMMENT 'Source last-modified timestamp',
    batch_id        STRING        NOT NULL  COMMENT 'ADF pipeline run ID',
    processed_date  TIMESTAMP               COMMENT 'UTC timestamp from Bronze write'
)
USING DELTA
PARTITIONED BY (batch_id)
COMMENT 'Standardised customer master. Append-only; full change history across all batches.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


CREATE TABLE IF NOT EXISTS {catalog_name}.silver.customer_master_error
(
    customer_id     STRING,
    customer_name   STRING,
    region          STRING,
    channel         STRING,
    last_modified   TIMESTAMP,
    batch_id        STRING,
    processed_date  TIMESTAMP,

    error_reason    STRING        NOT NULL,
    error_timestamp TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Rows rejected during customer master DQ checks.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
