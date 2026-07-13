-- ============================================================
-- bronze/product/product_master.sql
-- ============================================================
-- Overwrite mode – each batch replaces the previous snapshot.
-- ============================================================

CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.product_master
(
    product_id      STRING        NOT NULL  COMMENT 'SAP material number',
    product_name    STRING                  COMMENT 'Material description',
    brand           STRING                  COMMENT 'Brand name',
    category        STRING                  COMMENT 'Product category',
    status          STRING                  COMMENT 'Material status (ACTIVE / DELISTED)',
    effective_date  DATE                    COMMENT 'Date the status/category became effective',

    batch_id        STRING        NOT NULL  COMMENT 'ADF pipeline run ID',
    processed_date  TIMESTAMP               COMMENT 'UTC timestamp when written to Bronze'
)
USING DELTA
PARTITIONED BY (batch_id)
COMMENT 'Raw product master data from REST API (JSON). Overwritten each batch.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
