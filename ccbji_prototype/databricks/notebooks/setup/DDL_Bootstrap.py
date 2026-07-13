# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # DDL Bootstrap – Create All Schemas and Tables
# MAGIC
# MAGIC **Run once per environment** to initialise the full CCBJI medallion schema.
# MAGIC Safe to re-run – every statement uses `CREATE TABLE IF NOT EXISTS`.
# MAGIC
# MAGIC | Widget | Description | Example |
# MAGIC |---|---|---|
# MAGIC | `catalog_name` | Unity Catalog name. **Leave blank for CE** | `ccbji_dev` |
# MAGIC | `bronze_schema` | Bronze schema name | `bronze` |
# MAGIC | `silver_schema` | Silver schema name | `silver` |
# MAGIC | `gold_schema` | Gold schema name | `gold` |
# MAGIC | `audit_schema` | Audit / observability schema name | `audit` |

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",  "",       "Catalog name (blank for CE)")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema",   "gold",   "Gold schema")
dbutils.widgets.text("audit_schema",  "audit",  "Audit schema")

# COMMAND ----------

# MAGIC %md ## 0  Resolve schema prefixes

# COMMAND ----------

catalog_name  = dbutils.widgets.get("catalog_name").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema   = dbutils.widgets.get("gold_schema").strip()
audit_schema  = dbutils.widgets.get("audit_schema").strip()

pfx = f"{catalog_name}." if catalog_name else ""
B   = f"{pfx}{bronze_schema}"
S   = f"{pfx}{silver_schema}"
G   = f"{pfx}{gold_schema}"
A   = f"{pfx}{audit_schema}"

print(f"Catalog : '{catalog_name}' (blank = CE / hive_metastore)")
print(f"Bronze  : {B}")
print(f"Silver  : {S}")
print(f"Gold    : {G}")
print(f"Audit   : {A}")

# COMMAND ----------

# MAGIC %md ## 1  Schemas

# COMMAND ----------

for schema_fqn, desc in [
    (B, "Raw landing snapshots"),
    (S, "Standardised append-only tables"),
    (G, "Analytics star schema (Gold)"),
    (A, "Pipeline observability and alerting"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn} COMMENT '{desc}'")
    print(f"  ✓  schema: {schema_fqn}")

# COMMAND ----------

# MAGIC %md ## 2  Bronze Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.sales_transactions (
    sales_id        STRING,
    order_date      DATE,
    customer_id     STRING,
    product_id      STRING,
    quantity        INT,
    net_amount      DECIMAL(18,2),
    currency        STRING,
    source_system   STRING,
    batch_id        STRING,
    processed_date  TIMESTAMP
) USING DELTA
  PARTITIONED BY (batch_id)
  COMMENT 'Raw SAP sales transactions (overwrite per batch)'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.customer_master (
    customer_id     STRING,
    customer_name   STRING,
    region          STRING,
    channel         STRING,
    last_modified   TIMESTAMP,
    batch_id        STRING,
    processed_date  TIMESTAMP
) USING DELTA
  PARTITIONED BY (batch_id)
  COMMENT 'Raw customer master from Azure SQL (overwrite per batch)'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {B}.product_master (
    product_id      STRING,
    product_name    STRING,
    brand           STRING,
    category        STRING,
    status          STRING,
    effective_date  DATE,
    batch_id        STRING,
    processed_date  TIMESTAMP
) USING DELTA
  PARTITIONED BY (batch_id)
  COMMENT 'Raw product master from REST API (overwrite per batch)'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

print("  ✓  bronze tables created")

# COMMAND ----------

# MAGIC %md ## 3  Silver Tables (main + error)

# COMMAND ----------

# ── Main silver tables ─────────────────────────────────────────────────────
for tbl, src, comment in [
    ("sales_transactions",  B + ".sales_transactions",  "Standardised sales transactions"),
    ("customer_master",     B + ".customer_master",      "Standardised customer master"),
    ("product_master",      B + ".product_master",       "Standardised product master"),
]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {S}.{tbl}
    USING DELTA
    PARTITIONED BY (batch_id)
    COMMENT '{comment}'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
    AS SELECT * FROM {src} WHERE 1=0
    """)
    print(f"  ✓  {S}.{tbl}")

# ── Error tables (mirror + error context columns) ──────────────────────────
for tbl in ["sales_transactions_error", "customer_master_error", "product_master_error"]:
    base    = tbl.replace("_error", "")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {S}.{tbl} (
        error_reason    STRING    NOT NULL,
        error_timestamp TIMESTAMP NOT NULL
    ) USING DELTA
      COMMENT 'DQ error rows for {base}'
      TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
    """)
    print(f"  ✓  {S}.{tbl}")

# COMMAND ----------

# MAGIC %md ## 4  Gold Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.dim_date (
    date_key        INT       COMMENT 'yyyyMMdd surrogate key',
    calendar_date   DATE      COMMENT 'Calendar date',
    year            INT,
    quarter         INT,
    month           INT,
    month_name      STRING,
    week_of_year    INT,
    day_of_week     INT,
    day_name        STRING,
    is_weekend      BOOLEAN
) USING DELTA
  COMMENT 'Calendar dimension (generated 2024–2030, full overwrite)'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.dim_customer (
    customer_key    BIGINT    COMMENT 'Surrogate key (-1 = UNKNOWN)',
    customer_id     STRING    COMMENT 'Natural key',
    customer_name   STRING,
    region          STRING,
    channel         STRING,
    attribute_hash  STRING    COMMENT 'SHA-256 of tracked columns',
    effective_from  DATE      NOT NULL,
    effective_to    DATE      NOT NULL,
    is_current      BOOLEAN   NOT NULL,
    batch_id        STRING
) USING DELTA
  COMMENT 'SCD Type 2 customer dimension'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.dim_product (
    product_key     BIGINT    COMMENT 'Surrogate key (-1 = UNKNOWN)',
    product_id      STRING    COMMENT 'Natural key',
    product_name    STRING,
    brand           STRING,
    category        STRING,
    status          STRING,
    attribute_hash  STRING    COMMENT 'SHA-256 of tracked columns',
    effective_from  DATE      NOT NULL,
    effective_to    DATE      NOT NULL,
    is_current      BOOLEAN   NOT NULL,
    batch_id        STRING
) USING DELTA
  COMMENT 'SCD Type 2 product dimension'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.fact_sales (
    transaction_hash    STRING         COMMENT 'SHA-256 idempotency key',
    sales_id            STRING,
    date_key            INT            COMMENT 'FK → dim_date',
    customer_key        BIGINT         COMMENT 'FK → dim_customer (-1 if late-arriving)',
    product_key         BIGINT         COMMENT 'FK → dim_product (-1 if late-arriving)',
    quantity            INT,
    net_amount          DECIMAL(18,2),
    currency            STRING,
    source_system       STRING,
    batch_id            STRING,
    needs_resolution    BOOLEAN        COMMENT 'True when any SK = -1',
    fact_batch_id       STRING,
    fact_loaded_at      TIMESTAMP
) USING DELTA
  PARTITIONED BY (fact_batch_id)
  COMMENT 'Central fact table – upserted on transaction_hash'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {G}.late_arriving_dimension_bridge (
    resolution_hash         STRING     COMMENT 'SHA-256 idempotency key for bridge entries',
    transaction_hash        STRING     COMMENT 'FK → fact_sales',
    transaction_id          STRING,
    dimension_name          STRING,
    natural_key             STRING,
    transaction_date        DATE,
    fact_key_column         STRING,
    fact_table_name         STRING,
    batch_id                STRING,
    resolution_status       STRING     COMMENT 'PENDING | RESOLVED',
    resolved_surrogate_key  BIGINT,
    first_seen_timestamp    TIMESTAMP,
    resolved_timestamp      TIMESTAMP
) USING DELTA
  COMMENT 'Bridge table for late-arriving dimensions'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

print("  ✓  gold tables created")

# COMMAND ----------

# MAGIC %md ## 5  Audit Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {A}.pipeline_run_log (
    run_id              STRING    NOT NULL  COMMENT 'UUID per notebook invocation',
    pipeline_name       STRING    NOT NULL,
    batch_id            STRING,
    layer               STRING             COMMENT 'silver | gold | resolution',
    source_object       STRING,
    target_object       STRING,
    status              STRING    NOT NULL  COMMENT 'SUCCESS | FAILED | SKIPPED | ROW_COUNT_MISMATCH | ...',
    rows_read           BIGINT,
    rows_written        BIGINT,
    rows_rejected       BIGINT,
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP,
    duration_seconds    DOUBLE,
    error_message       STRING,
    error_stacktrace    STRING,
    catalog_name        STRING,
    environment         STRING,
    run_metadata        STRING             COMMENT 'JSON blob for extra context'
) USING DELTA
  PARTITIONED BY (layer, DATE(started_at))
  COMMENT 'Central pipeline run log – one row per notebook invocation'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {A}.dq_check_log (
    check_id        STRING    NOT NULL  COMMENT 'UUID per check evaluation',
    run_id          STRING    NOT NULL  COMMENT 'FK → pipeline_run_log',
    batch_id        STRING,
    check_name      STRING    NOT NULL,
    target_table    STRING    NOT NULL,
    column_name     STRING,
    check_status    STRING    NOT NULL  COMMENT 'PASSED | FAILED | WARNING',
    expected_value  STRING,
    actual_value    STRING,
    row_count       BIGINT,
    checked_at      TIMESTAMP NOT NULL
) USING DELTA
  PARTITIONED BY (DATE(checked_at))
  COMMENT 'DQ rule evaluation results – written by SD_Standardization_Engine'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {A}.alert_log (
    alert_id                STRING    NOT NULL  COMMENT 'UUID per alert event',
    run_id                  STRING             COMMENT 'FK → pipeline_run_log (NULL for SLA alerts)',
    alert_type              STRING    NOT NULL  COMMENT 'PIPELINE_FAILURE | SLA_BREACH | DQ_FAILURE | ROW_COUNT_ANOMALY',
    severity                STRING    NOT NULL  COMMENT 'CRITICAL | HIGH | MEDIUM | LOW',
    pipeline_name           STRING,
    batch_id                STRING,
    message                 STRING    NOT NULL,
    notify_email            STRING,
    notification_status     STRING             COMMENT 'SENT | FAILED | SUPPRESSED',
    sla_target              STRING,
    actual_time             STRING,
    created_at              TIMESTAMP NOT NULL,
    environment             STRING
) USING DELTA
  PARTITIONED BY (DATE(created_at))
  COMMENT 'Alert and notification log – written by Azure Function and ADF failure pipelines'
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true','delta.autoOptimize.autoCompact'='true')
""")

print("  ✓  audit tables created")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

summary = {}
for layer, schema in [("bronze", B), ("silver", S), ("gold", G), ("audit", A)]:
    tbls = [r.tableName for r in spark.sql(f"SHOW TABLES IN {schema}").collect()]
    summary[layer] = tbls
    print(f"  {layer:6s} ({schema}): {tbls}")

dbutils.notebook.exit(f"Bootstrap complete. Tables: {sum(len(v) for v in summary.values())} across 4 schemas.")
