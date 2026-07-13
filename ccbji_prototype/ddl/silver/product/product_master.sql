-- ============================================================
-- silver/product/product_master.sql
-- ============================================================
-- Append mode – full history of all batches is retained.
-- Error table captures DQ-rejected rows (permanent audit trail).
-- ============================================================

CREATE TABLE IF NOT EXISTS {catalog_name}.silver.product_master
(
    product_id      STRING        NOT NULL  COMMENT 'SAP material number (trimmed)',
    product_name    STRING                  COMMENT 'Material description (trimmed)',
    brand           STRING                  COMMENT 'Brand name (trimmed)',
    category        STRING                  COMMENT 'Product category (trimmed)',
    status          STRING                  COMMENT 'Material status (trimmed + uppercased)',
    effective_date  DATE                    COMMENT 'Status effective date',
    batch_id        STRING        NOT NULL  COMMENT 'ADF pipeline run ID',
    processed_date  TIMESTAMP               COMMENT 'UTC timestamp from Bronze write'
)
USING DELTA
PARTITIONED BY (batch_id)
COMMENT 'Standardised product master. Append-only; full change history across all batches.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


CREATE TABLE IF NOT EXISTS {catalog_name}.silver.product_master_error
(
    product_id      STRING,
    product_name    STRING,
    brand           STRING,
    category        STRING,
    status          STRING,
    effective_date  DATE,
    batch_id        STRING,
    processed_date  TIMESTAMP,

    error_reason    STRING        NOT NULL,
    error_timestamp TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Rows rejected during product master DQ checks.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
