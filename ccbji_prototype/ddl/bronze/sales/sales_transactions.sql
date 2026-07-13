-- ============================================================
-- bronze/sales/sales_transactions.sql
-- ============================================================
-- Overwrite mode – each batch replaces the previous snapshot.
-- batch_id + processed_date added by SD_Standardization_Engine.
-- ============================================================

CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.sales_transactions
(
    sales_id        STRING        NOT NULL  COMMENT 'SAP sales document line ID',
    order_date      DATE                    COMMENT 'SAP order creation date',
    customer_id     STRING                  COMMENT 'SAP customer master key',
    product_id      STRING                  COMMENT 'SAP material number',
    quantity        INT                     COMMENT 'Order quantity',
    net_amount      DECIMAL(18, 2)          COMMENT 'Net sales value (JPY)',
    currency        STRING                  COMMENT 'Transaction currency code',
    source_system   STRING                  COMMENT 'Originating source system identifier',

    batch_id        STRING        NOT NULL  COMMENT 'ADF pipeline run ID or auto-generated timestamp token',
    processed_date  TIMESTAMP               COMMENT 'UTC timestamp when this row was written to Bronze'
)
USING DELTA
PARTITIONED BY (batch_id)
COMMENT 'Raw SAP sales transaction CSV data. One partition per ADF trigger run. Overwritten each batch.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
