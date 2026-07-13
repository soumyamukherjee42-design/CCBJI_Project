-- ============================================================
-- 04_audit_tables.sql  –  Audit / observability schema
-- ============================================================
-- Three tables support full pipeline observability:
--
--   pipeline_run_log  – one row per notebook invocation
--                       Written by SD_Standardization_Engine,
--                       TF_Gold_Load_Engine, TF_Resolve_Late_Dimensions.
--
--   dq_check_log      – one row per DQ rule evaluation
--                       Written by SD_Standardization_Engine after
--                       each batch (row count, null, negative checks).
--
--   alert_log         – one row per alert notification sent
--                       Written by the Azure Function (config resolver)
--                       and ADF failure handlers. NOT written by notebooks.
--
-- All three tables are:
--   • Append-only (never updated, never deleted)
--   • Partitioned by date for efficient time-range queries
-- ============================================================


-- ── Pipeline Run Log ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {catalog_name}.audit.pipeline_run_log
(
    run_id              STRING      NOT NULL  COMMENT 'UUID generated at notebook startup. Unique per invocation.',
    pipeline_name       STRING      NOT NULL  COMMENT 'Notebook name: SD_Standardization_Engine | TF_Gold_Load_Engine | TF_Resolve_Late_Dimensions',
    batch_id            STRING                COMMENT 'ADF batch / pipeline run ID passed as widget',
    layer               STRING                COMMENT 'Pipeline layer: silver | gold | resolution',
    source_object       STRING                COMMENT 'Source table or file path read by this run',
    target_object       STRING                COMMENT 'Target table written by this run',
    status              STRING      NOT NULL  COMMENT 'STARTED | SUCCESS | FAILED | SKIPPED | ROW_COUNT_MISMATCH | PARTIAL_RESOLUTION | ...',
    rows_read           BIGINT                COMMENT 'Rows read from source (bronze table or silver batch)',
    rows_written        BIGINT                COMMENT 'Rows written to the target table',
    rows_rejected       BIGINT                COMMENT 'Rows sent to the _error table (null primary key, etc.)',
    started_at          TIMESTAMP   NOT NULL  COMMENT 'UTC timestamp at notebook start',
    completed_at        TIMESTAMP             COMMENT 'UTC timestamp when the audit record was written (≈ notebook end)',
    duration_seconds    DOUBLE                COMMENT 'Wall-clock seconds from started_at to completed_at',
    error_message       STRING                COMMENT 'Exception message if status = FAILED',
    error_stacktrace    STRING                COMMENT 'Full Python traceback if status = FAILED',
    catalog_name        STRING                COMMENT 'Unity Catalog name (blank = CE / hive_metastore)',
    environment         STRING                COMMENT 'dev | test | prod (from env YAML, if injected)',
    run_metadata        STRING                COMMENT 'JSON blob for ad-hoc key-value context (ADF run URL, etc.)'
)
USING DELTA
PARTITIONED BY (layer, DATE(started_at))
COMMENT 'Central pipeline run log. One row per notebook invocation. Query: SELECT * FROM audit.pipeline_run_log WHERE DATE(started_at) = current_date().'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


-- ── DQ Check Log ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {catalog_name}.audit.dq_check_log
(
    check_id        STRING      NOT NULL  COMMENT 'UUID per individual DQ rule evaluation',
    run_id          STRING      NOT NULL  COMMENT 'FK → audit.pipeline_run_log.run_id',
    batch_id        STRING                COMMENT 'ADF batch ID',
    check_name      STRING      NOT NULL  COMMENT 'Rule identifier: row_count_bronze_vs_source | row_count_silver_vs_source | null_primary_key | negative_value | ...',
    target_table    STRING      NOT NULL  COMMENT 'Table evaluated by this check',
    column_name     STRING                COMMENT 'Column evaluated (NULL for table-level checks)',
    check_status    STRING      NOT NULL  COMMENT 'PASSED | FAILED | WARNING',
    expected_value  STRING                COMMENT 'Expected value or threshold (stored as string for flexibility)',
    actual_value    STRING                COMMENT 'Actual observed value',
    row_count       BIGINT                COMMENT 'Number of rows that failed this check (NULL for aggregate checks)',
    checked_at      TIMESTAMP   NOT NULL  COMMENT 'UTC timestamp when this check was evaluated'
)
USING DELTA
PARTITIONED BY (DATE(checked_at))
COMMENT 'Row-level DQ check results. Written by SD_Standardization_Engine after each batch.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


-- ── Alert Log ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS {catalog_name}.audit.alert_log
(
    alert_id                STRING      NOT NULL  COMMENT 'UUID per alert event',
    run_id                  STRING                COMMENT 'FK → audit.pipeline_run_log.run_id (NULL for SLA alerts that have no single run)',
    alert_type              STRING      NOT NULL  COMMENT 'PIPELINE_FAILURE | SLA_BREACH | DQ_FAILURE | ROW_COUNT_ANOMALY',
    severity                STRING      NOT NULL  COMMENT 'CRITICAL | HIGH | MEDIUM | LOW',
    pipeline_name           STRING                COMMENT 'Notebook or ADF pipeline that raised the alert',
    batch_id                STRING                COMMENT 'Batch ID in scope when alert was raised',
    message                 STRING      NOT NULL  COMMENT 'Human-readable alert description sent to recipients',
    notify_email            STRING                COMMENT 'Recipient email addresses (comma-separated)',
    notification_status     STRING                COMMENT 'SENT | FAILED | SUPPRESSED',
    sla_target              STRING                COMMENT 'SLA completion time (HH:MM+TZ) – populated for SLA_BREACH alerts',
    actual_time             STRING                COMMENT 'Actual completion time – populated for SLA_BREACH alerts',
    created_at              TIMESTAMP   NOT NULL  COMMENT 'UTC timestamp when the alert was created',
    environment             STRING                COMMENT 'dev | test | prod'
)
USING DELTA
PARTITIONED BY (DATE(created_at))
COMMENT 'Alert and notification log. Written by the Azure Function (config resolver) and ADF failure pipelines. NOT written by notebooks.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
