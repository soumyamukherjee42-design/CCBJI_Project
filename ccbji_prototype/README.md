# CCBJI Azure Data Engineering Platform

**Configuration-driven Medallion Architecture on Azure Data Factory + Databricks Delta Lake**

> Submitted as a Senior Data Engineer assignment prototype for CCBJI.  
> All pipeline logic is controlled by YAML configuration files — adding a new data source, dimension, or fact table requires **zero notebook changes**.

---

## Table of Contents

1. [Solution Overview](#1-solution-overview)
2. [Architecture](#2-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Data Model](#4-data-model)
5. [Key Design Decisions](#5-key-design-decisions)
6. [Pipeline Walkthrough](#6-pipeline-walkthrough)
7. [Configuration Reference](#7-configuration-reference)
8. [Data Quality](#8-data-quality)
9. [Security & PII](#9-security--pii)
10. [Audit & Observability](#10-audit--observability)
11. [CI/CD Pipeline](#11-cicd-pipeline)
12. [Quick Start (Databricks Community Edition)](#12-quick-start-databricks-community-edition)
13. [Extending the Platform](#13-extending-the-platform)

---

## 1. Solution Overview

This platform ingests data from three source systems into a Databricks Delta Lake medallion architecture and serves a Gold-layer star schema to downstream analytics.

| Source | Type | Ingestion Method |
|---|---|---|
| SAP (sales transactions) | CSV files | ADF event trigger on ADLS `BlobCreated` |
| Azure SQL (customer master) | Relational DB | ADF scheduled incremental extract with watermark |
| Product REST API (product master) | JSON API | ADF scheduled pull with pagination |

The three notebooks that drive all data movement are **fully generic** — they read a YAML config at runtime to know which tables, schemas, transforms, business rules, DQ checks, and security policies to apply. This means:

- A new source dataset = a new YAML file, no code change.
- A new Gold dimension or fact = a new YAML file, no code change.
- Adding a DQ rule or a column mask = a YAML change, no notebook change.
- Schema evolution is automatic via `mergeSchema=true` on Silver writes.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  SOURCES                  │  INGEST (ADF)          │  DATABRICKS MEDALLION       │
│                           │                        │                             │
│  SAP ──── CSV ────────────┼── BlobCreated event ───┼──▶ Bronze  (overwrite)      │
│  Azure SQL ───────────────┼── Schedule + watermark ┼──▶ Bronze ──▶ Silver(append)│
│  REST API ────────────────┼── Schedule + paginate  ┼──▶ Bronze                  │
│                           │                        │         │                   │
│  [ADLS Landing encrypted  │                        │         ▼  DQ checks run    │
│   with CMK via Key Vault] │  ADF Schedule (03:00)  │      Gold Star Schema ★     │
│                           │  ─────────────────── ─ │      dim_date               │
│                           │                        │      dim_customer  (SCD2)   │
│                           │                        │      dim_product   (SCD2)   │
│                           │                        │      fact_sales             │
│                           │                        │      late_arriving_bridge   │
│                           │                        │                             │
│                           │                        │  Unity Catalog              │
│                           │                        │  Column Mask policies       │
│                           │                        │  (customer_name → PII role) │
│                           │                        │                             │
│                           │                        │  Audit / Observability      │
│                           │                        │  pipeline_run_log           │
│                           │                        │  dq_check_log               │
│                           │                        │  alert_log                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

| Step | Trigger | Notebook | Config |
|---|---|---|---|
| Landing → Bronze → Silver (sales) | `BlobCreated` event on ADLS | `SD_Standardization_Engine` | `standardization/sales/sd_sales_transactions.yaml` |
| Landing → Bronze → Silver (masterdata) | Daily schedule 01:00 JST | `SD_Standardization_Engine` | `standardization/product/` · `standardization/customer/` |
| Silver → Gold (DimDate) | Daily schedule 03:00 JST (Step 1) | `TF_Gold_Load_Engine` | `transformation/dimensions/tf_dim_date.yaml` |
| Silver → Gold (DimCustomer, DimProduct) | Step 2 — parallel after Step 1 | `TF_Gold_Load_Engine` | `transformation/dimensions/tf_dim_*.yaml` |
| Silver → Gold (FactSales) | Step 3 — after all dims succeed | `TF_Gold_Load_Engine` | `transformation/facts/tf_fact_sales.yaml` |
| Late FK resolution | Step 4 — after fact load | `TF_Gold_Load_Engine` | `transformation/facts/tf_resolve_fact_sales.yaml` |

**SLA target:** all Gold tables available by **06:00 JST** daily.

---

## 3. Repository Structure

```
ccbji_prototype/
│
├── .github/workflows/
│   ├── ci.yml                  PR gate: pytest + YAML validation + notebook syntax
│   └── cd.yml                  Deploy on push: dev (auto) / test (auto) / prod (approval)
│
├── config/env/                 Non-secret per-environment values
│   ├── dev.yaml
│   ├── test.yaml
│   └── prod.yaml
│
├── databricks/notebooks/
│   ├── setup/
│   │   ├── 00_CE_Setup.py      Community Edition bootstrap (sample data + YAML → DBFS)
│   │   └── DDL_Bootstrap.py    Idempotent CREATE TABLE IF NOT EXISTS (run once per env)
│   ├── standardization/
│   │   └── SD_Standardization_Engine.py     Generic: Landing → Bronze → Silver + DQ
│   └── transformation/
│       ├── TF_Gold_Load_Engine.py            Generic: Silver → Gold (dim + fact) + DQ
│       └── TF_Resolve_Late_Dimensions.py     Late-arriving FK resolution
│
├── adf/
│   ├── ingestion/              ADF pipelines that land raw files into ADLS
│   │   ├── PL_FILE_To_Landing_Ingestion.json
│   │   ├── PL_REST_To_Landing_Ingestion.json
│   │   └── PL_SQL_To_Landing_Ingestion.json
│   └── orchestration/          ADF pipelines that call Databricks notebooks
│       ├── PL_SD_Standardisation.json
│       └── PL_TF_Gold_Load.json
│
├── azure_function/
│   └── function_app.py         Config-resolver: reads YAML from DBFS, returns JSON to ADF
│
├── ddl/                        Delta Lake DDL — organised by layer then source
│   ├── 00_schemas.sql
│   ├── bronze/
│   │   ├── sales/sales_transactions.sql
│   │   ├── product/product_master.sql
│   │   └── customer/customer_master.sql
│   ├── silver/                 Each source has a main table + _error table
│   │   ├── sales/sales_transactions.sql
│   │   ├── product/product_master.sql
│   │   └── customer/customer_master.sql
│   ├── gold/03_gold_tables.sql
│   ├── audit/04_audit_tables.sql
│   └── security/
│       └── mask_functions.sql  Unity Catalog PII masking UDFs
│
├── docs/
│   ├── data_model.md           Full ER diagram + per-layer table reference
│   └── CCBJI_End_to_End_Prototype.py   Reference CE prototype notebook
│
├── ingestion/config/           Per-source ingestion connectivity + security configs
│   ├── file/sales_transactions.yaml
│   ├── rest/product_master.yaml
│   └── sql/customer_master.yaml
│
├── standardization/            SD YAML — one file per source dataset
│   ├── sales/sd_sales_transactions.yaml
│   ├── product/sd_product_master.yaml
│   └── customer/sd_customer_master.yaml
│
├── transformation/             TF YAML — one file per Gold object
│   ├── dimensions/
│   │   ├── tf_dim_date.yaml
│   │   ├── tf_dim_customer.yaml
│   │   └── tf_dim_product.yaml
│   └── facts/
│       ├── tf_fact_sales.yaml
│       └── tf_resolve_fact_sales.yaml
│
├── trigger/                    ADF trigger definitions — declarative YAML
│   ├── sales/trg_event_sales_landing_to_silver.yaml
│   ├── masterdata/trg_schedule_masterdata_to_silver.yaml
│   └── gold/trg_schedule_gold_refresh.yaml
│
├── scripts/
│   └── validate_yamls.py       CI YAML schema validator (exit 0 = all valid)
│
├── tests/
│   └── test_validate_yamls.py  87 pytest unit tests for the validator
│
├── sample_data/                Day-1 CSV/JSON files for local CE testing
├── .gitignore
├── pyproject.toml              pytest + ruff config
└── requirements.txt
```

---

## 4. Data Model

### Gold Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (date_key) │
                    └──────┬──────┘
                           │
┌──────────────────┐       │       ┌──────────────────┐
│  dim_customer    │       │       │  dim_product     │
│  (customer_key)  │       │       │  (product_key)   │
│  SCD Type 2      │       │       │  SCD Type 2      │
│  [PII masked]    │       │       └────────┬─────────┘
└────────┬─────────┘       │                │
         │                 │                │
         └────────────┬────┘────────────────┘
                      │
               ┌──────▼──────┐          ┌──────────────────────────┐
               │  fact_sales │──────────▶ late_arriving_dimension   │
               │             │          │        _bridge            │
               └─────────────┘          └──────────────────────────┘
```

### Table Summary

| Table | Layer | Type | Key |
|---|---|---|---|
| `bronze.sales_transactions` | Bronze | Snapshot (overwrite) | `batch_id` partition |
| `bronze.customer_master` | Bronze | Snapshot (overwrite) | `batch_id` partition |
| `bronze.product_master` | Bronze | Snapshot (overwrite) | `batch_id` partition |
| `silver.sales_transactions` | Silver | Append-only | `batch_id` partition |
| `silver.customer_master` | Silver | Append-only | `batch_id` partition |
| `silver.product_master` | Silver | Append-only | `batch_id` partition |
| `silver.*_error` | Silver | Append-only (DQ rejects) | none |
| `gold.dim_date` | Gold | Full overwrite | `date_key` INT (yyyyMMdd) |
| `gold.dim_customer` | Gold | SCD Type 2 merge | `customer_key` BIGINT |
| `gold.dim_product` | Gold | SCD Type 2 merge | `product_key` BIGINT |
| `gold.fact_sales` | Gold | Idempotent upsert | `transaction_hash` SHA-256 |
| `gold.late_arriving_dimension_bridge` | Gold | Append + update | `resolution_hash` SHA-256 |
| `audit.pipeline_run_log` | Audit | Append-only | `run_id` UUID |
| `audit.dq_check_log` | Audit | Append-only | per check |
| `audit.alert_log` | Audit | Append-only | per alert |

For the full DDL and ER diagram with all columns, see [docs/data_model.md](docs/data_model.md).

---

## 5. Key Design Decisions

### 5.1 Config-Driven Architecture (Zero-Code Extension)

Every engine notebook reads a YAML file at startup to understand what to do. The same `SD_Standardization_Engine.py` notebook handles all three source datasets. The same `TF_Gold_Load_Engine.py` handles date dimensions, SCD2 dimensions, and facts — it routes by `meta.object_type` in the YAML.

This means:
- **No notebook modification** to add a new source, Gold object, DQ rule, or column mask.
- **CI validates every YAML** on every PR before it can merge.
- **Onboarding a new dataset** is a YAML PR, not a code change.

### 5.2 SCD Type 2 with Attribute Hashing

Customer and product dimensions use SCD Type 2: each change to a tracked column creates a new row with updated `effective_from` / `effective_to` dates, and the old row is closed.

Change detection uses a **SHA-256 hash over all tracked columns** (`attribute_hash`). This reduces a multi-column comparison to a single string equality check and eliminates bugs from column-list drift.

```
New hash ≠ Stored hash  →  CLOSE old row, INSERT new row
New hash = Stored hash  →  No action (idempotent re-run)
```

### 5.3 Transaction Hash for Fact Idempotency

`fact_sales` uses a SHA-256 hash of `(source_system, sales_id, order_date)` as its primary key (`transaction_hash`). The Gold load uses a Delta **merge on this hash** — re-running the same batch is safe with no duplicate rows.

This eliminates the need for a separate deduplication step and makes ADF retries completely safe.

### 5.4 Temporal FK Resolution at Load Time

When loading `fact_sales`, each row is joined to the dimension version **active on the transaction date**:

```sql
ON fact.customer_id = dim.customer_id
   AND fact.order_date BETWEEN dim.effective_from AND dim.effective_to
```

The resolved surrogate key is stored directly in the fact row. **Analytical queries never need to join to dimension history** — the correct key is already embedded. This gives query-time performance equal to a non-SCD2 design while preserving full change history.

### 5.5 Late-Arriving Dimension Bridge

If a dimension row for a natural key does not exist when a fact arrives (e.g. a new customer first appears in sales before the customer master extract runs), the fact receives `customer_key = -1` and `needs_resolution = true`. A `PENDING` entry is written to `late_arriving_dimension_bridge`.

A dedicated resolution run executes after all dimensions and facts load each day. It:
1. Joins pending bridge entries to the dimension by natural key and date.
2. Backfills the correct surrogate key into `fact_sales`.
3. Marks the bridge entry `RESOLVED`.

This design means facts are never blocked on dimension availability — they load immediately and self-correct asynchronously.

### 5.6 UNKNOWN Sentinel Key (-1)

Every SCD2 dimension has a pre-loaded UNKNOWN member with surrogate key `-1`. This ensures `fact_sales` always has a valid FK, maintaining referential integrity in BI tools (Power BI, Tableau) even before resolution runs.

### 5.7 `batch_id` as Partition Key

Bronze and Silver tables are partitioned by `batch_id` (the ADF pipeline run ID or a timestamp token). This enables:
- **Partition pruning** on incremental reads.
- **Point-in-time audit** — read exactly one batch.
- **Rollback** — drop a partition to undo a bad load without affecting history.

### 5.8 Error Tables with Permanent Audit Trail

Every Silver table has a corresponding `*_error` table. Rows that fail DQ checks with `severity: error` are written here instead of being silently dropped. Error tables are append-only and never truncated — they form a permanent record of every data quality failure.

### 5.9 Config-Driven Data Quality

DQ rules are declared in YAML, not hardcoded in notebooks. The engines read the rules at runtime and execute them against the written data. This makes DQ changes a YAML PR, not a code change.

**Silver-layer (SD) checks** — declared in `dataset.dq.rules`:

| Check | What it validates | Requires |
|---|---|---|
| `row_count` | Bronze count and Silver count both match the source file row count | — |
| `not_null` | No NULL values in the specified column | `column` |
| `not_negative` | No negative values in a numeric column | `column` |
| `allowed_values` | Column values are restricted to a declared list | `column`, `values` |
| `regex` | Column values match a regular expression pattern | `column`, `pattern` |

**Gold-layer (TF) checks** — declared in top-level `dq.rules`, run post-load against the written Gold table:

| Check | What it validates | Requires |
|---|---|---|
| `row_count_min` | Table has at least `min` rows after load | `min` |
| `not_null` | No NULL values in the specified column | `column` |
| `not_negative` | No negative values in a numeric column | `column` |
| `zero_check` | No zero values in a numeric column (e.g. quantity) | `column` |
| `allowed_values` | Column values restricted to a declared list | `column`, `values` |
| `duplicate_key` | No duplicate combinations across `columns` (list) or single `column` | `columns` or `column` |
| `referential_integrity` | FK column values all exist in the reference dimension table (UNKNOWN key −1 is excluded) | `column`, `ref_table`, `ref_column` |

**Severity semantics** (same for both layers):

| `severity` | Silver behaviour | Gold behaviour |
|---|---|---|
| `error` | Failing rows routed to `*_error` table; status = `DQ_FAILED` | Check result logged; error count reported in result |
| `warning` | Violation logged to `audit.dq_check_log` only; rows still written | Check result logged; pipeline continues |

### 5.10 PII Protection — Unity Catalog Column Masking + ADLS CMK

Customer PII (`customer_name`) is protected at two levels:

**Landing → Bronze (encryption at rest):**  
ADLS landing zone is encrypted with a **Customer-Managed Key (CMK)** stored in Azure Key Vault. Only the ADF Managed Identity and the Databricks cluster service principal are granted Key Vault `Key Get/Wrap/Unwrap` access. The key reference is declared in the ingestion config YAML, not hardcoded.

**Bronze → Gold (column-level access control):**  
Unity Catalog **column mask functions** are applied after each table write. The mask function is resolved from the SD or TF YAML `security.column_masks` block and applied via:

```sql
ALTER TABLE <table> ALTER COLUMN customer_name SET MASK <catalog>.security.mask_pii_name
```

The mask function returns the plaintext value to `pii_reader` and `data_engineer` roles; all other principals see a redacted string (`J*** D**`). On Databricks Community Edition (hive_metastore), the `ALTER TABLE ... SET MASK` call is silently skipped — the notebook does not fail.

---

## 6. Pipeline Walkthrough

### 6.1 Standardization Engine (`SD_Standardization_Engine.py`)

Reads `sd_*.yaml` at runtime. For each run:

1. **Read** — loads raw files from ADLS landing using schema declared in `source.columns`.
2. **Bronze write** — overwrites the Bronze table partition for this `batch_id`.
3. **Column mask** — applies Unity Catalog column mask policies from `dataset.security.column_masks`.
4. **Transform** — applies column-level transforms (`trim`, `upper`, `lower`) from `target.columns`.
5. **DQ checks** — executes all rules in `dataset.dq.rules` against the Silver data.
6. **Error routing** — rows failing an `error`-severity check → `*_error` table; clean rows → Silver table (append).
7. **Audit write** — one row to `audit.pipeline_run_log`, one row per DQ check to `audit.dq_check_log`.
8. **Exit** — `dbutils.notebook.exit(json.dumps(result))` returns status + counts to ADF.

### 6.2 Gold Load Engine (`TF_Gold_Load_Engine.py`)

Reads `tf_*.yaml` at runtime. Routes by `meta.object_type`:

| `object_type` | What happens |
|---|---|
| `dimension` (no `scd_type`) | Full overwrite — used for DimDate (static range) |
| `dimension` + `scd_type: 2` | SCD2 merge: hash-based change detection, row insert + close |
| `fact` | Temporal FK lookup per dimension; Delta merge on `transaction_hash`; bridge writes for unresolved FKs |
| `resolution` | Joins pending bridge entries to dimensions; backfills surrogate keys in fact table |

After the load:
- **Column masks** — applies Unity Catalog column mask policies from `security.column_masks`.
- **DQ checks** — executes all rules in `dq.rules` against the written Gold table. Results logged to `audit.dq_check_log`.

### 6.3 Token Substitution

All YAML string values support `{token}` placeholders. The engines resolve tokens at startup:

| Token | Resolves to |
|---|---|
| `{catalog_name}` | Unity Catalog catalog name (blank = CE 2-part names) |
| `{bronze_schema}` | Bronze schema/database |
| `{silver_schema}` | Silver schema/database |
| `{gold_schema}` | Gold schema/database |
| `{datalake_name}` | ADLS Gen2 storage account name |
| `{key_vault_name}` | Azure Key Vault name (for CMK config) |

---

## 7. Configuration Reference

### 7.1 SD YAML (`standardization/<source>/sd_*.yaml`)

Controls the standardization engine for one source dataset.

```yaml
version: 1.0.0
meta:
  dataset_owner: {name: Sales}
  function: sales

dataset:
  identifier:
    datasetname:        sales_transactions
    ingestion_src_path: abfss://inbound@{datalake_name}.dfs.core.windows.net/sales/
    bronze_table:       '{catalog_name}.{bronze_schema}.sales_transactions'
    silver_table:       '{catalog_name}.{silver_schema}.sales_transactions'
    badrecordtable:     '{catalog_name}.{silver_schema}.sales_transactions_error'
    fileformat:         csv
    bronze_mode:        overwrite
    silver_mode:        append

  source:
    options:
      header: 'true'
      sep: ','
      dateFormat: yyyy-MM-dd
    columns:
      - {name: sales_id,   dtype: StringType}
      - {name: net_amount, dtype: 'DecimalType(18,2)'}

  target:
    columns:
      - {name: sales_id,  standardname: sales_id,  transforms: [trim]}
      - {name: currency,  standardname: currency,  transforms: [trim, upper]}

  # ── Data Quality ─────────────────────────────────────────
  dq:
    rules:
      - check: row_count
        severity: error
      - check: not_null
        column: sales_id
        severity: error
      - check: not_negative
        column: net_amount
        severity: warning

  # ── Security (optional) ───────────────────────────────────
  security:
    column_masks:
      - column: customer_name
        mask_function: "{catalog_name}.security.mask_pii_name"
        exempt_roles: [pii_reader, data_engineer]
    landing_encryption:
      type: cmk
      key_vault_name: "{key_vault_name}"
      key_name: ccbji-adls-cmk
```

**Allowed transforms:** `trim` · `upper` · `lower`

### 7.2 TF YAML (`transformation/<dimensions|facts>/tf_*.yaml`)

Controls the Gold load engine for one Gold object.

**SCD2 Dimension:**
```yaml
version: 1.0.0
meta:
  object_type: dimension
  scd_type: 2

transformation:
  source_table:          '{catalog_name}.{silver_schema}.customer_master'
  target_table:          '{catalog_name}.{gold_schema}.dim_customer'
  natural_key:           customer_id
  surrogate_key:         customer_key
  tracked_columns:       [customer_name, region, channel]
  effective_from_column: last_modified

# ── Data Quality ─────────────────────────────────────────────
dq:
  rules:
    - check: not_null
      column: customer_key
      severity: error
    - check: duplicate_key
      columns: [customer_id, effective_from]
      severity: error

# ── Security (optional) ──────────────────────────────────────
security:
  column_masks:
    - column: customer_name
      mask_function: "{catalog_name}.security.mask_pii_name"
      exempt_roles: [pii_reader, data_engineer]
```

**Fact Table:**
```yaml
version: 1.0.0
meta:
  object_type: fact

transformation:
  source_table:             '{catalog_name}.{silver_schema}.sales_transactions'
  target_table:             '{catalog_name}.{gold_schema}.fact_sales'
  late_bridge_table:        '{catalog_name}.{gold_schema}.late_arriving_dimension_bridge'
  transaction_id_column:    sales_id
  transaction_date_column:  order_date
  transaction_hash_columns: [source_system, sales_id, order_date]
  dimensions:
    - name:                  customer
      table:                 '{catalog_name}.{gold_schema}.dim_customer'
      natural_key_source:    customer_id
      natural_key_dimension: customer_id
      surrogate_key:         customer_key
      effective_from:        effective_from
      effective_to:          effective_to
      unknown_key:           -1
  measures:           [quantity, net_amount]
  degenerate_columns: [sales_id, currency, source_system, batch_id]

# ── Data Quality ─────────────────────────────────────────────
dq:
  rules:
    - check: not_null
      column: transaction_hash
      severity: error
    - check: zero_check
      column: quantity
      severity: warning
    - check: referential_integrity
      column: customer_key
      ref_table: '{catalog_name}.{gold_schema}.dim_customer'
      ref_column: customer_key
      unknown_key: -1
      severity: warning
```

### 7.3 Ingestion Config (`ingestion/config/<type>/<source>.yaml`)

Controls ADF source connectivity. The `security` block declares landing-zone encryption.

```yaml
security:
  landing_encryption:
    type: cmk
    key_vault_name: "{key_vault_name}"
    key_name: ccbji-adls-cmk
    secret_scope: ccbji-kv-scope
    access_identity: adf_managed_identity
```

### 7.4 Trigger YAML (`trigger/<domain>/trg_*.yaml`)

Declarative definition of ADF triggers. Schedule triggers support a `sequence` block where steps with the same step number run in parallel and each step waits for the previous to complete.

```yaml
trigger:
  type: schedule
  timezone: Asia/Tokyo
  start_time: '2026-01-01T03:00:00+09:00'
  sequence:
    - {step: 1, name: Refresh_DimDate,    tf_yml: tf_dim_date.yaml}
    - {step: 2, name: Refresh_DimProduct, tf_yml: tf_dim_product.yaml}  # parallel with step 2
    - {step: 2, name: Refresh_DimCustomer,tf_yml: tf_dim_customer.yaml} # parallel with step 2
    - {step: 3, name: Load_FactSales,     tf_yml: tf_fact_sales.yaml}
    - {step: 4, name: Resolve_LateFKs,   tf_yml: tf_resolve_fact_sales.yaml}
```

### 7.5 Environment Config (`config/env/<env>.yaml`)

Non-secret per-environment values. Secrets are stored in GitHub / Key Vault — never in YAML.

```yaml
environment:        dev
catalog_name:       ccbji_dev
bronze_schema:      bronze
silver_schema:      silver
gold_schema:        gold
datalake_name:      stccbjidev
adf_name:           adf-ccbji-dev
adf_resource_group: rg-ccbji-dev
```

---

## 8. Data Quality

### 8.1 Silver Layer (SD Engine)

DQ rules are evaluated after Silver write. Rules are read from `dataset.dq.rules` in the SD YAML.

```
SD Notebook flow:
  ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────────┐
  │  Bronze  │───▶│  Silver  │───▶│  DQ checks   │───▶│ audit.dq_   │
  │  write   │    │  write   │    │  (all rules) │    │ check_log    │
  └──────────┘    └──────────┘    └──────┬───────┘    └──────────────┘
                                         │
                              error severity + rows failed
                                         │
                                  ┌──────▼──────┐
                                  │ *_error     │
                                  │ table       │
                                  └─────────────┘
```

**SD DQ checks available:**

| `check` | Requires | Description |
|---|---|---|
| `row_count` | — | Bronze count and Silver count must both equal the source file count |
| `not_null` | `column` | Zero NULLs allowed in the column |
| `not_negative` | `column` | All numeric values must be ≥ 0 |
| `allowed_values` | `column`, `values` | Column restricted to the declared list |
| `regex` | `column`, `pattern` | All values must match the regex pattern |

### 8.2 Gold Layer (TF Engine)

DQ rules run **after** the Gold table is written. Rules are read from top-level `dq.rules` in the TF YAML. Results are logged; rows are never removed from Gold (post-load validation, not filtering).

**TF DQ checks available:**

| `check` | Requires | Description |
|---|---|---|
| `row_count_min` | `min` | Table must have at least `min` rows after load |
| `not_null` | `column` | Zero NULLs in the column |
| `not_negative` | `column` | All numeric values ≥ 0 |
| `zero_check` | `column` | No zero values (catches quantity = 0 rows) |
| `allowed_values` | `column`, `values` | Column restricted to declared list (e.g. ACTIVE/INACTIVE/UNKNOWN) |
| `duplicate_key` | `columns` or `column` | No duplicate key combinations (natural key uniqueness) |
| `referential_integrity` | `column`, `ref_table`, `ref_column` | FK values exist in the reference table; UNKNOWN key (−1) excluded from check |

### 8.3 Severity Semantics

```yaml
severity: error    # SD: bad rows → *_error table; TF: failure logged, result.status = DQ_FAILED
severity: warning  # both layers: violation logged to audit.dq_check_log; pipeline continues
```

### 8.4 Sample DQ Query

```sql
-- All DQ failures in the last 7 days
SELECT  pipeline_name, table_name, check_name, column_name,
        rows_failed, severity, message, checked_at
FROM    audit.dq_check_log
WHERE   status   = 'FAILED'
  AND   checked_at >= current_date() - INTERVAL 7 DAYS
ORDER   BY checked_at DESC;
```

---

## 9. Security & PII

### 9.1 Landing Zone Encryption (ADLS CMK)

Raw files landing in ADLS are encrypted with a **Customer-Managed Key** in Azure Key Vault. The platform never touches the key directly — Azure Storage uses it transparently via the Key Vault REST API.

**Access model:**
- Only the ADF Managed Identity and the Databricks cluster service principal are granted `Key Get`, `Key Wrap`, `Key Unwrap` on the Key Vault key.
- No human identity has key access; all access is audited by Key Vault diagnostic logs.

The key reference is declared in each ingestion config YAML under `security.landing_encryption` — no credentials are embedded in code or config.

### 9.2 Unity Catalog Column Masking

`customer_name` and similar PII columns are protected by Unity Catalog **column mask functions** defined in `ddl/security/mask_functions.sql`. Three functions are provided:

| Function | Masking behaviour |
|---|---|
| `security.mask_pii_name` | `J*** D**` — first char + stars + last char |
| `security.mask_pii_email` | `j***@***.com` — domain preserved, local-part masked |
| `security.mask_pii_partial` | Last 4 chars visible, rest masked |

Each function uses `is_member()` to check the calling principal's role. Members of `pii_reader` or `data_engineer` see plaintext; all others see masked values.

**YAML declaration (SD and TF):**

```yaml
security:
  column_masks:
    - column: customer_name
      mask_function: "{catalog_name}.security.mask_pii_name"
      exempt_roles: [pii_reader, data_engineer]
```

The engine applies masks via `ALTER TABLE ... ALTER COLUMN ... SET MASK` after each write. On Community Edition (hive_metastore), the call is silently skipped — CE does not support Unity Catalog column masking, and the notebook does not fail.

### 9.3 Masking Coverage by Layer

| Column | Bronze | Silver | Gold |
|---|---|---|---|
| `customer_name` | Mask applied | Mask applied | Mask applied (`dim_customer`) |

Masking is applied at the table level in Unity Catalog, not at read time in the notebook — it is enforced for every consumer regardless of which tool or query they use.

### 9.4 Secrets Management

| Secret type | Storage |
|---|---|
| ADLS CMK key | Azure Key Vault |
| Databricks token | GitHub Actions secret / Key Vault |
| Azure SP credentials | GitHub Actions secret |
| DB password (SQL source) | Azure Key Vault, referenced via Databricks Secret Scope `ccbji-kv-scope` |

No secrets appear in any YAML, notebook, or JSON pipeline file in this repository.

---

## 10. Audit & Observability

### `audit.pipeline_run_log`

One row per notebook execution. Written by all engine notebooks.

```sql
SELECT  pipeline_name, batch_id, status,
        rows_read, rows_written, rows_rejected, duration_seconds
FROM    audit.pipeline_run_log
WHERE   DATE(started_at) = current_date()
ORDER   BY started_at;
```

### `audit.dq_check_log`

One row per DQ rule per batch — for both SD (Silver) and TF (Gold) engines.

| Column | Description |
|---|---|
| `run_id` | Links to `pipeline_run_log` |
| `pipeline_name` | `SD_Standardization_Engine` or `TF_Gold_Load_Engine` |
| `table_name` | Target table checked |
| `check_name` | Rule type (e.g. `not_null`, `referential_integrity`) |
| `column_name` | Column checked (blank for `row_count*`) |
| `status` | `PASSED` / `FAILED` / `ERROR` / `SKIPPED` |
| `rows_checked` | Total rows in table |
| `rows_failed` | Number of rows violating the rule |
| `severity` | `error` or `warning` |
| `message` | Human-readable failure detail |

### `audit.alert_log`

| `alert_type` | When raised |
|---|---|
| `PIPELINE_FAILURE` | Notebook activity returned non-zero exit |
| `SLA_BREACH` | Gold pipeline completed after 06:00 JST |
| `DQ_FAILURE` | Any `error`-severity DQ check failed |
| `ROW_COUNT_ANOMALY` | Row count deviates > threshold from rolling average |

---

## 11. CI/CD Pipeline

### CI (`ci.yml`) — runs on every PR

```
PR opened / updated
        │
        ├─ pytest tests/ -v               (87 unit tests for the YAML validator)
        ├─ python scripts/validate_yamls.py    (SD + TF + trigger + env schema checks)
        └─ py_compile databricks/notebooks/**/*.py   (notebook syntax check)

All three must pass → PR can merge.
```

The YAML validator checks:
- All required keys present for each YAML type.
- All SD DQ `check` values from the supported set; required sub-keys present.
- All TF DQ `check` values from the supported set; `referential_integrity` has `ref_table`/`ref_column`; `duplicate_key` has `columns` or `column`; `row_count_min` has `min`.
- `security.column_masks` entries have `column`, `mask_function`, `exempt_roles`.
- `security.landing_encryption` has `type`, `key_vault_name`, `key_name`.
- `severity` is `error` or `warning`.

### CD (`cd.yml`) — runs on branch push

| Branch push | Environment | Approval |
|---|---|---|
| `develop` | dev | Auto-deploy |
| `test` | test | Auto-deploy |
| `main` | prod | **Manual approval gate required** |

**What the deploy does:**
1. Re-runs all CI checks as a gate.
2. Reads non-secret config from `config/env/<env>.yaml`.
3. Uploads notebooks to Databricks workspace via `databricks workspace import-dir`.
4. Uploads SD + TF YAML configs to DBFS via `databricks fs cp`.
5. Deploys ADF pipeline JSONs via `az datafactory pipeline create`.

**Required GitHub Secrets** (per environment):

| Secret | Purpose |
|---|---|
| `DATABRICKS_HOST` | Databricks workspace URL |
| `DATABRICKS_TOKEN` | PAT or service principal token |
| `AZURE_CREDENTIALS` | Service principal JSON for ADF + Key Vault deployment |

---

## 12. Quick Start (Databricks Community Edition)

No ADF or ADLS required — the CE setup notebook writes sample data directly to DBFS.

**Step 1 — Import and run `00_CE_Setup.py`**

```
databricks/notebooks/setup/00_CE_Setup.py
```

This creates the three schemas, writes Day-1 and Day-2 sample CSV files to DBFS, and copies SD + TF YAML configs to DBFS. It prints the exact widget values to use in each subsequent notebook.

**Step 2 — Import the engine notebooks**

```
databricks/notebooks/standardization/SD_Standardization_Engine.py
databricks/notebooks/transformation/TF_Gold_Load_Engine.py
databricks/notebooks/transformation/TF_Resolve_Late_Dimensions.py
```

**Step 3 — Run in sequence (Day 1, `batch_id = BATCH_20260316_001`)**

| Order | Notebook | `sd_yml_path` / `tf_yml_path` |
|---|---|---|
| 1 | SD_Standardization_Engine | `.../sd_customer_master.yaml` |
| 2 | SD_Standardization_Engine | `.../sd_product_master.yaml` |
| 3 | SD_Standardization_Engine | `.../sd_sales_transactions.yaml` |
| 4 | TF_Gold_Load_Engine | `.../tf_dim_date.yaml` |
| 5 | TF_Gold_Load_Engine | `.../tf_dim_product.yaml` |
| 6 | TF_Gold_Load_Engine | `.../tf_dim_customer.yaml` |
| 7 | TF_Gold_Load_Engine | `.../tf_fact_sales.yaml` |
| 8 | TF_Gold_Load_Engine | `.../tf_resolve_fact_sales.yaml` |

Repeat with `batch_id = BATCH_20260317_001` and Day-2 sample files. The Day-2 run demonstrates:
- SCD2 attribute changes (C101 channel: Wholesale → Direct, P1003 status: INACTIVE → ACTIVE).
- Late-arriving dimension resolution (C103 appears in Day-2 customer master, resolving the S0006 bridge entry from Day 1).

**Step 4 — Bootstrap DDL for a fresh environment**

```
databricks/notebooks/setup/DDL_Bootstrap.py
```

Widgets: `catalog_name`, `bronze_schema`, `silver_schema`, `gold_schema`, `audit_schema`.  
Creates all schemas and tables idempotently (`CREATE TABLE IF NOT EXISTS`). Safe to re-run.  
Run `ddl/security/mask_functions.sql` separately to deploy PII masking UDFs (requires Unity Catalog).

---

## 13. Extending the Platform

### Adding a New Source Dataset

1. Create `standardization/<source>/sd_<source>.yaml` — copy an existing YAML as a template.
2. Add `dataset.dq.rules` for the business rules that apply to this source.
3. If the source contains PII, add `dataset.security.column_masks` and `security.landing_encryption`.
4. Create `ingestion/config/<type>/<source>.yaml` — specify connectivity and schedule.
5. Add or extend a trigger YAML in `trigger/<domain>/`.
6. Add Bronze + Silver DDL in `ddl/bronze/<source>/` and `ddl/silver/<source>/`.
7. Open a PR → CI validates the YAML schema automatically. No notebook changes required.

### Adding a New Gold Dimension

1. Create `transformation/dimensions/tf_dim_<name>.yaml`.
2. Set `meta.object_type: dimension` and `meta.scd_type: 2` (or omit `scd_type` for a static overwrite dimension).
3. Add `dq.rules` — at minimum `not_null` on the surrogate key and `duplicate_key` on the natural key + `effective_from`.
4. Add an entry to the Gold trigger's `sequence` block.
5. Add DDL to `ddl/gold/03_gold_tables.sql`.
6. Open a PR. No notebook changes required.

### Adding a New Fact Table

1. Create `transformation/facts/tf_fact_<name>.yaml` with `meta.object_type: fact`.
2. Define the `dimensions` block with temporal join keys and sentinel values.
3. Add `dq.rules` — at minimum `not_null` on `transaction_hash` and `referential_integrity` for each FK.
4. Create `transformation/facts/tf_resolve_fact_<name>.yaml` for late-arriving resolution.
5. Add both to the Gold trigger sequence.
6. Open a PR. No notebook changes required.

### Adding a New DQ Rule

To add a DQ rule to an existing dataset, edit only the YAML file — no notebook change:

```yaml
# In standardization/sales/sd_sales_transactions.yaml:
dq:
  rules:
    - check: regex
      column: currency
      pattern: '^[A-Z]{3}$'   # ISO 4217 currency code
      severity: warning
```

The CI validator will confirm the rule is valid before the PR can merge.

---

## Validation

Run locally at any time:

```bash
# YAML schema validation (SD + TF + trigger + env)
python scripts/validate_yamls.py

# Unit tests (87 tests)
pytest tests/ -v
```

Both must exit `0` before any PR can merge.
