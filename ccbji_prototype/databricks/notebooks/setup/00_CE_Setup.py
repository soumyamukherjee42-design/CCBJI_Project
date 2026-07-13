# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 · Community Edition Setup
# MAGIC **Run this once** before running SD and TF engine notebooks.
# MAGIC
# MAGIC What it does:
# MAGIC 1. Creates the three schemas (`ccbji_bronze`, `ccbji_silver`, `ccbji_gold`)
# MAGIC 2. Writes Day-1 and Day-2 sample files to DBFS landing area
# MAGIC 3. Writes CE-compatible SD and TF YAML configs to DBFS
# MAGIC 4. Prints the exact widget values to use when running the engine notebooks

# COMMAND ----------

# MAGIC %md ## Step 1 – Schemas

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name",   "",              "Catalog (blank = hive_metastore default / CE)")
dbutils.widgets.text("bronze_schema",  "ccbji_bronze",  "Bronze schema")
dbutils.widgets.text("silver_schema",  "ccbji_silver",  "Silver schema")
dbutils.widgets.text("gold_schema",    "ccbji_gold",    "Gold schema")
dbutils.widgets.text("reset",          "true",          "Drop & recreate schemas (true/false)")

cat    = dbutils.widgets.get("catalog_name").strip()
bronze = dbutils.widgets.get("bronze_schema")
silver = dbutils.widgets.get("silver_schema")
gold   = dbutils.widgets.get("gold_schema")
reset  = dbutils.widgets.get("reset").lower() == "true"

# Helper: qualify table names (supports empty catalog → 2-part names for CE)
def _tbl(schema, table):
    if cat:
        return f"`{cat}`.`{schema}`.`{table}`"
    return f"`{schema}`.`{table}`"

def _db(schema):
    if cat:
        return f"`{cat}`.`{schema}`"
    return f"`{schema}`"

if reset:
    for s in [bronze, silver, gold]:
        spark.sql(f"DROP DATABASE IF EXISTS {_db(s)} CASCADE")
    print("Dropped existing schemas.")

for s in [bronze, silver, gold]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_db(s)}")
    print(f"  Schema ready: {_db(s)}")

# COMMAND ----------

# MAGIC %md ## Step 2 – Write Sample CSV / JSON files to DBFS Landing

# COMMAND ----------

BASE = "dbfs:/FileStore/ccbji"
LANDING = f"{BASE}/landing"
CONFIGS = f"{BASE}/configs"

# ── Day-1 Sales CSV ─────────────────────────────────────────────────────────
SALES_D1 = """\
sales_id,order_date,customer_id,product_id,quantity,net_amount,currency,source_system
S0001,2026-03-15,C100,P1001,24,43200.50,JPY,SAP
S0002,2026-03-15,C101,P1002,12,19800.00,JPY,SAP
S0003,2026-03-16,C102,P1001,6,10800.25,JPY,SAP
S0004,2026-03-16,C101,P1003,30,51000.00,JPY,SAP
S0005,2026-03-16,C999,P9999,2,-200.00,JPY,SAP
S0006,2026-03-16,C103,P1001,8,14400.00,JPY,SAP
"""

# ── Day-1 Product CSV (REST API payload – written as CSV for CE simplicity) ──
PRODUCT_D1 = """\
product_id,product_name,brand,category,status,effective_date
P1001,Coca-Cola 500ml,Coca-Cola,Beverages,ACTIVE,2024-01-01
P1002,Georgia Coffee 185g,Georgia,Beverages,ACTIVE,2024-01-01
P1003,Aquarius 500ml,Aquarius,Beverages,INACTIVE,2025-07-01
"""

# ── Day-1 Customer CSV (Azure SQL incremental extract) ───────────────────────
CUSTOMER_D1 = """\
customer_id,customer_name,region,channel,last_modified
C100,Tokyo Mart,Kanto,Retail,2026-03-14 10:15:00
C101,Osaka Wholesale,Kansai,Wholesale,2026-03-15 08:05:00
C102,Sapporo Shop,Hokkaido,Retail,2026-03-16 12:30:00
"""

# ── Day-2 Sales CSV ─────────────────────────────────────────────────────────
SALES_D2 = """\
sales_id,order_date,customer_id,product_id,quantity,net_amount,currency,source_system
S0007,2026-03-17,C100,P1001,10,18000.00,JPY,SAP
S0008,2026-03-17,C101,P1004,15,27000.00,JPY,SAP
S0009,2026-03-17,C103,P1003,20,36000.00,JPY,SAP
"""

# ── Day-2 Product CSV (full refresh – P1003 now ACTIVE, new P1004, P9999 resolves S0005)
PRODUCT_D2 = """\
product_id,product_name,brand,category,status,effective_date
P1001,Coca-Cola 500ml,Coca-Cola,Beverages,ACTIVE,2024-01-01
P1002,Georgia Coffee 185g,Georgia,Beverages,ACTIVE,2024-01-01
P1003,Aquarius 500ml,Aquarius,Beverages,ACTIVE,2026-03-17
P1004,Monster Energy 500ml,Monster,Beverages,ACTIVE,2026-03-17
P9999,Test Product (Return),Unknown,Other,INACTIVE,2026-03-16
"""

# ── Day-2 Customer CSV (incremental – C101 channel changed, C103 is new) ────
# C103 last_modified=2026-03-14 so effective_from < S0006 order_date=2026-03-16 → will resolve bridge
CUSTOMER_D2 = """\
customer_id,customer_name,region,channel,last_modified
C101,Osaka Wholesale,Kansai,Direct,2026-03-17 09:00:00
C103,Fukuoka Store,Kyushu,Retail,2026-03-14 08:00:00
"""

files = {
    f"{LANDING}/sales/sales_d1.csv":      SALES_D1,
    f"{LANDING}/sales/sales_d2.csv":      SALES_D2,
    f"{LANDING}/product/product_d1.csv":  PRODUCT_D1,
    f"{LANDING}/product/product_d2.csv":  PRODUCT_D2,
    f"{LANDING}/customer/customer_d1.csv":CUSTOMER_D1,
    f"{LANDING}/customer/customer_d2.csv":CUSTOMER_D2,
}

for path, content in files.items():
    dbutils.fs.put(path, content, overwrite=True)
    print(f"  Written: {path}")

print("\nSample data files ready.")

# COMMAND ----------

# MAGIC %md ## Step 3 – Write SD YAML Configs to DBFS
# MAGIC
# MAGIC These are CE-adapted copies of `Standardization/src/config/*.yaml`:
# MAGIC - `ingestion_src_path` points to DBFS landing (instead of ADLS)
# MAGIC - `fileformat: csv` for all sources (simplest for CE)
# MAGIC - All table-name tokens unchanged – resolved at notebook runtime

# COMMAND ----------

SD_SALES = f"""\
version: 1.0.0
meta:
  dataset_owner: {{name: Sales}}
  function: sales
dataset:
  identifier:
    datasetname: sales_transactions
    loadtype: batch
    sourcesystem: SAP
    sourcelayer: landing
    ingestion_src_path: {LANDING}/sales/
    bronze_table: '{{catalog_name}}.{{bronze_schema}}.sales_transactions'
    silver_table: '{{catalog_name}}.{{silver_schema}}.sales_transactions'
    badrecordtable: '{{catalog_name}}.{{silver_schema}}.sales_transactions_error'
    bronze_mode: overwrite
    silver_mode: append
    batchIdentifier: batch_id
    fileformat: csv
  source:
    options:
      header: 'true'
      sep: ','
      dateFormat: yyyy-MM-dd
    columns:
      - {{name: sales_id,      dtype: StringType}}
      - {{name: order_date,    dtype: DateType}}
      - {{name: customer_id,   dtype: StringType}}
      - {{name: product_id,    dtype: StringType}}
      - {{name: quantity,      dtype: IntegerType}}
      - {{name: net_amount,    dtype: 'DecimalType(18,2)'}}
      - {{name: currency,      dtype: StringType}}
      - {{name: source_system, dtype: StringType}}
  target:
    columns:
      - {{name: sales_id,      standardname: sales_id,      transforms: [trim]}}
      - {{name: order_date,    standardname: order_date}}
      - {{name: customer_id,   standardname: customer_id,   transforms: [trim]}}
      - {{name: product_id,    standardname: product_id,    transforms: [trim]}}
      - {{name: quantity,      standardname: quantity}}
      - {{name: net_amount,    standardname: net_amount}}
      - {{name: currency,      standardname: currency,      transforms: [trim, upper]}}
      - {{name: source_system, standardname: source_system, transforms: [trim, upper]}}
"""

SD_PRODUCT = f"""\
version: 1.0.0
meta:
  dataset_owner: {{name: Product Master}}
  function: product
dataset:
  identifier:
    datasetname: product_master
    loadtype: batch
    sourcesystem: ProductAPI
    sourcelayer: landing
    ingestion_src_path: {LANDING}/product/
    bronze_table: '{{catalog_name}}.{{bronze_schema}}.product_master'
    silver_table: '{{catalog_name}}.{{silver_schema}}.product_master'
    badrecordtable: '{{catalog_name}}.{{silver_schema}}.product_master_error'
    bronze_mode: overwrite
    silver_mode: append
    batchIdentifier: batch_id
    fileformat: csv
  source:
    options:
      header: 'true'
      sep: ','
      dateFormat: yyyy-MM-dd
    columns:
      - {{name: product_id,    dtype: StringType}}
      - {{name: product_name,  dtype: StringType}}
      - {{name: brand,         dtype: StringType}}
      - {{name: category,      dtype: StringType}}
      - {{name: status,        dtype: StringType}}
      - {{name: effective_date,dtype: DateType}}
  target:
    columns:
      - {{name: product_id,    standardname: product_id,    transforms: [trim]}}
      - {{name: product_name,  standardname: product_name,  transforms: [trim]}}
      - {{name: brand,         standardname: brand,         transforms: [trim]}}
      - {{name: category,      standardname: category,      transforms: [trim]}}
      - {{name: status,        standardname: status,        transforms: [trim, upper]}}
      - {{name: effective_date,standardname: effective_date}}
"""

SD_CUSTOMER = f"""\
version: 1.0.0
meta:
  dataset_owner: {{name: Customer Master}}
  function: customer
dataset:
  identifier:
    datasetname: customer_master
    loadtype: batch
    sourcesystem: AzureSQL
    sourcelayer: landing
    ingestion_src_path: {LANDING}/customer/
    bronze_table: '{{catalog_name}}.{{bronze_schema}}.customer_master'
    silver_table: '{{catalog_name}}.{{silver_schema}}.customer_master'
    badrecordtable: '{{catalog_name}}.{{silver_schema}}.customer_master_error'
    bronze_mode: overwrite
    silver_mode: append
    batchIdentifier: batch_id
    fileformat: csv
  source:
    options:
      header: 'true'
      sep: ','
    columns:
      - {{name: customer_id,   dtype: StringType}}
      - {{name: customer_name, dtype: StringType}}
      - {{name: region,        dtype: StringType}}
      - {{name: channel,       dtype: StringType}}
      - {{name: last_modified, dtype: TimestampType}}
  target:
    columns:
      - {{name: customer_id,   standardname: customer_id,   transforms: [trim]}}
      - {{name: customer_name, standardname: customer_name, transforms: [trim]}}
      - {{name: region,        standardname: region,        transforms: [trim]}}
      - {{name: channel,       standardname: channel,       transforms: [trim]}}
      - {{name: last_modified, standardname: last_modified}}
"""

sd_files = {
    f"{CONFIGS}/sd/sd_sales_transactions.yaml": SD_SALES,
    f"{CONFIGS}/sd/sd_product_master.yaml":     SD_PRODUCT,
    f"{CONFIGS}/sd/sd_customer_master.yaml":    SD_CUSTOMER,
}
for path, content in sd_files.items():
    dbutils.fs.put(path, content, overwrite=True)
    print(f"  Written: {path}")

# COMMAND ----------

# MAGIC %md ## Step 4 – Write TF YAML Configs to DBFS
# MAGIC TF YAMLs only reference table names (tokens), no file paths – identical to repo.

# COMMAND ----------

TF_DIM_DATE = """\
version: 1.0.0
meta:
  object_type: dimension
  target_name: dim_date
transformation:
  target_table: '{catalog_name}.{gold_schema}.dim_date'
  start_date: '2024-01-01'
  end_date: '2030-12-31'
"""

TF_DIM_PRODUCT = """\
version: 1.0.0
meta:
  object_type: dimension
  target_name: dim_product
  scd_type: 2
transformation:
  source_table: '{catalog_name}.{silver_schema}.product_master'
  natural_key: product_id
  tracked_columns: [product_name, brand, category, status]
  effective_from_column: effective_date
  target_table: '{catalog_name}.{gold_schema}.dim_product'
  surrogate_key: product_key
  unknown_member_key: -1
"""

TF_DIM_CUSTOMER = """\
version: 1.0.0
meta:
  object_type: dimension
  target_name: dim_customer
  scd_type: 2
transformation:
  source_table: '{catalog_name}.{silver_schema}.customer_master'
  natural_key: customer_id
  tracked_columns: [customer_name, region, channel]
  effective_from_column: last_modified
  target_table: '{catalog_name}.{gold_schema}.dim_customer'
  surrogate_key: customer_key
  unknown_member_key: -1
"""

TF_FACT_SALES = """\
version: 1.0.0
meta:
  object_type: fact
  target_name: fact_sales
transformation:
  source_table: '{catalog_name}.{silver_schema}.sales_transactions'
  target_table: '{catalog_name}.{gold_schema}.fact_sales'
  late_bridge_table: '{catalog_name}.{gold_schema}.late_arriving_dimension_bridge'
  transaction_id_column: sales_id
  transaction_date_column: order_date
  transaction_hash_columns: [source_system, sales_id, order_date]
  dimensions:
    - name: customer
      table: '{catalog_name}.{gold_schema}.dim_customer'
      natural_key_source: customer_id
      natural_key_dimension: customer_id
      surrogate_key: customer_key
      effective_from: effective_from
      effective_to: effective_to
      unknown_key: -1
    - name: product
      table: '{catalog_name}.{gold_schema}.dim_product'
      natural_key_source: product_id
      natural_key_dimension: product_id
      surrogate_key: product_key
      effective_from: effective_from
      effective_to: effective_to
      unknown_key: -1
  measures: [quantity, net_amount]
  degenerate_columns: [sales_id, currency, source_system, batch_id]
"""

TF_RESOLVE_FACT_SALES = """\
version: 1.0.0
meta:
  object_type: resolution
  target_fact: fact_sales
transformation:
  fact_table: '{catalog_name}.{gold_schema}.fact_sales'
  bridge_table: '{catalog_name}.{gold_schema}.late_arriving_dimension_bridge'
  dimensions:
    - name: customer
      table: '{catalog_name}.{gold_schema}.dim_customer'
      natural_key_dimension: customer_id
      surrogate_key: customer_key
      effective_from: effective_from
      effective_to: effective_to
    - name: product
      table: '{catalog_name}.{gold_schema}.dim_product'
      natural_key_dimension: product_id
      surrogate_key: product_key
      effective_from: effective_from
      effective_to: effective_to
"""

tf_files = {
    f"{CONFIGS}/tf/tf_dim_date.yaml":             TF_DIM_DATE,
    f"{CONFIGS}/tf/tf_dim_product.yaml":          TF_DIM_PRODUCT,
    f"{CONFIGS}/tf/tf_dim_customer.yaml":         TF_DIM_CUSTOMER,
    f"{CONFIGS}/tf/tf_fact_sales.yaml":           TF_FACT_SALES,
    f"{CONFIGS}/tf/tf_resolve_fact_sales.yaml":   TF_RESOLVE_FACT_SALES,
}
for path, content in tf_files.items():
    dbutils.fs.put(path, content, overwrite=True)
    print(f"  Written: {path}")

# COMMAND ----------

# MAGIC %md ## Step 5 – Run Sequence
# MAGIC
# MAGIC Use the widget values below when running each notebook.
# MAGIC In production, ADF calls these notebooks automatically with these parameters.

# COMMAND ----------

CAT    = cat or ""
B_SCH  = bronze
S_SCH  = silver
G_SCH  = gold

print("=" * 70)
print("RUN SEQUENCE – copy these widget values into each notebook")
print("=" * 70)

print(f"""
Common widgets (set on every notebook):
  catalog_name  = "{CAT}"
  bronze_schema = "{B_SCH}"
  silver_schema = "{S_SCH}"
  gold_schema   = "{G_SCH}"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DAY 1  (batch_id = BATCH_20260316_001)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[1] SD_Standardization_Engine  →  sd_yml_path = {CONFIGS}/sd/sd_customer_master.yaml
                                   batch_id    = BATCH_20260316_001
                                   file_filter = customer_d1.csv

[2] SD_Standardization_Engine  →  sd_yml_path = {CONFIGS}/sd/sd_product_master.yaml
                                   batch_id    = BATCH_20260316_001
                                   file_filter = product_d1.csv

[3] SD_Standardization_Engine  →  sd_yml_path = {CONFIGS}/sd/sd_sales_transactions.yaml
                                   batch_id    = BATCH_20260316_001
                                   file_filter = sales_d1.csv

[4] TF_Gold_Load_Engine        →  tf_yml_path = {CONFIGS}/tf/tf_dim_date.yaml
                                   batch_id    = BATCH_20260316_001

[5] TF_Gold_Load_Engine        →  tf_yml_path = {CONFIGS}/tf/tf_dim_product.yaml
                                   batch_id    = BATCH_20260316_001

[6] TF_Gold_Load_Engine        →  tf_yml_path = {CONFIGS}/tf/tf_dim_customer.yaml
                                   batch_id    = BATCH_20260316_001

[7] TF_Gold_Load_Engine        →  tf_yml_path = {CONFIGS}/tf/tf_fact_sales.yaml
                                   batch_id    = BATCH_20260316_001

[8] TF_Resolve_Late_Dimensions →  tf_resolve_yml_path = {CONFIGS}/tf/tf_resolve_fact_sales.yaml

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DAY 2  (batch_id = BATCH_20260317_001)  – repeat steps 1-8 with:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  batch_id    = BATCH_20260317_001
  file_filter = *_d2.csv   (in each respective SD widget)
""")
