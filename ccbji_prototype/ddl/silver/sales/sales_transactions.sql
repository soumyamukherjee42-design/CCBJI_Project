-- ============================================================
-- silver/sales/sales_transactions.sql
-- ============================================================
-- Append mode – full history of all batches is retained.
-- Error table captures DQ-rejected rows (permanent audit trail).
-- ============================================================

CREATE TABLE IF NOT EXISTS {catalog_name}.silver.sales_transactions
(
    sales_id        STRING        NOT NULL  COMMENT 'SAP sales document line ID (trimmed)',
    order_date      DATE                    COMMENT 'SAP order creation date',
    customer_id     STRING                  COMMENT 'Customer master key (trimmed)',
    product_id      STRING                  COMMENT 'SAP material number (trimmed)',
    quantity        INT                     COMMENT 'Order quantity',
    net_amount      DECIMAL(18, 2)          COMMENT 'Net sales value (JPY)',
    currency        STRING                  COMMENT 'Currency code (trimmed + uppercased)',
    source_system   STRING                  COMMENT 'Source system identifier (trimmed + uppercased)',
    batch_id        STRING        NOT NULL  COMMENT 'ADF pipeline run ID',
    processed_date  TIMESTAMP               COMMENT 'UTC timestamp from Bronze write'
)
USING DELTA
PARTITIONED BY (batch_id)
COMMENT 'Standardised sales transactions. Append-only; full history across all batches.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.dataSkippingNumIndexedCols' = '4'
);


CREATE TABLE IF NOT EXISTS {catalog_name}.silver.sales_transactions_error
(
    sales_id        STRING                  COMMENT 'Source value (may be null – that is the error)',
    order_date      DATE,
    customer_id     STRING,
    product_id      STRING,
    quantity        INT,
    net_amount      DECIMAL(18, 2),
    currency        STRING,
    source_system   STRING,
    batch_id        STRING,
    processed_date  TIMESTAMP,

    error_reason    STRING        NOT NULL  COMMENT 'Human-readable description of the DQ failure',
    error_timestamp TIMESTAMP     NOT NULL  COMMENT 'UTC timestamp when the error was captured'
)
USING DELTA
COMMENT 'Rows rejected during SD DQ checks. Never deleted – permanent audit trail.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
