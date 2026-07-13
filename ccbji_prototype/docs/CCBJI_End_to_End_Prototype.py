# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # CCBJI Analytics Platform – End-to-End Working Prototype
# MAGIC **Compatible with Databricks Community Edition** – no Azure dependencies, no Unity Catalog.
# MAGIC
# MAGIC | Layer | Storage | Mode | Purpose |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze** | Delta | overwrite per batch | Raw, schema-enforced snapshot |
# MAGIC | **Silver** | Delta | append + batch_id | Standardised, full history, replayable |
# MAGIC | **Gold** | Delta | merge / SCD2 | Star schema: DimDate, DimProduct, DimCustomer, FactSales |
# MAGIC
# MAGIC **Two-day simulation** demonstrates:
# MAGIC - SCD Type 2 (product status change, customer channel change)
# MAGIC - Late-arriving dimension resolution via bridge table
# MAGIC - Temporal join correctness (fact attributes captured at transaction time)
# MAGIC - Data quality reconciliation and anomaly detection

# COMMAND ----------

# MAGIC %md ## 0  Setup

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# ── Namespace ────────────────────────────────────────────────────────────────
BRONZE = "ccbji_bronze"
SILVER = "ccbji_silver"
GOLD   = "ccbji_gold"

# ── Batch IDs (replaces ADF pipeline_run_id) ─────────────────────────────────
BATCH_D1 = "BATCH_20260316_001"
BATCH_D2 = "BATCH_20260317_001"

# ── Set RESET=True for a clean slate run, False to skip re-initialisation ────
RESET = True

if RESET:
    for db in [BRONZE, SILVER, GOLD]:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
    print("Dropped existing databases.")

for db in [BRONZE, SILVER, GOLD]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

print(f"Databases ready: {BRONZE} | {SILVER} | {GOLD}")

# COMMAND ----------

# MAGIC %md ## 1  Source Data – Day 1 (2026-03-16)
# MAGIC > In production: ADF copies CSV/JSON/Parquet from inbound storage to ADLS Landing.
# MAGIC > Here we create DataFrames directly from the assignment sample payloads.

# COMMAND ----------

# ── Schemas ──────────────────────────────────────────────────────────────────
SALES_SCHEMA = T.StructType([
    T.StructField("sales_id",      T.StringType(),       True),
    T.StructField("order_date",    T.DateType(),         True),
    T.StructField("customer_id",   T.StringType(),       True),
    T.StructField("product_id",    T.StringType(),       True),
    T.StructField("quantity",      T.IntegerType(),      True),
    T.StructField("net_amount",    T.DecimalType(18, 2), True),
    T.StructField("currency",      T.StringType(),       True),
    T.StructField("source_system", T.StringType(),       True),
])

PRODUCT_SCHEMA = T.StructType([
    T.StructField("product_id",    T.StringType(), True),
    T.StructField("product_name",  T.StringType(), True),
    T.StructField("brand",         T.StringType(), True),
    T.StructField("category",      T.StringType(), True),
    T.StructField("status",        T.StringType(), True),
    T.StructField("effective_date",T.DateType(),   True),
])

CUSTOMER_COLS = ["customer_id", "customer_name", "region", "channel", "last_modified_str"]

# ── Day 1 Sales (CSV file from SAP) ─────────────────────────────────────────
# S0005: truly unknown customer+product (stays in bridge permanently)
# S0006: C103 not yet in master data on Day 1 (late-arriving → resolves on Day 2)
sales_d1 = spark.createDataFrame([
    ("S0001", date(2026, 3, 15), "C100", "P1001", 24, 43200.50, "JPY", "SAP"),
    ("S0002", date(2026, 3, 15), "C101", "P1002", 12, 19800.00, "JPY", "SAP"),
    ("S0003", date(2026, 3, 16), "C102", "P1001",  6, 10800.25, "JPY", "SAP"),
    ("S0004", date(2026, 3, 16), "C101", "P1003", 30, 51000.00, "JPY", "SAP"),
    ("S0005", date(2026, 3, 16), "C999", "P9999",  2,  -200.00, "JPY", "SAP"),
    ("S0006", date(2026, 3, 16), "C103", "P1001",  8, 14400.00, "JPY", "SAP"),
], schema=SALES_SCHEMA)

# ── Day 1 Customer Master (Azure SQL – incremental watermark extract) ────────
customer_d1 = (spark.createDataFrame([
    ("C100", "Tokyo Mart",      "Kanto",    "Retail",    "2026-03-14 10:15:00"),
    ("C101", "Osaka Wholesale", "Kansai",   "Wholesale", "2026-03-15 08:05:00"),
    ("C102", "Sapporo Shop",    "Hokkaido", "Retail",    "2026-03-16 12:30:00"),
], CUSTOMER_COLS)
    .withColumn("last_modified", F.to_timestamp("last_modified_str"))
    .drop("last_modified_str"))

# ── Day 1 Product Master (REST API – full extract) ───────────────────────────
product_d1 = spark.createDataFrame([
    ("P1001", "Coca-Cola 500ml",     "Coca-Cola", "Beverages", "ACTIVE",   date(2024, 1, 1)),
    ("P1002", "Georgia Coffee 185g", "Georgia",   "Beverages", "ACTIVE",   date(2024, 1, 1)),
    ("P1003", "Aquarius 500ml",      "Aquarius",  "Beverages", "INACTIVE", date(2025, 7, 1)),
], schema=PRODUCT_SCHEMA)

print("Day 1 source data ready.")
display(sales_d1)

# COMMAND ----------

# MAGIC %md ## 2  Bronze Layer – Raw Snapshot (overwrite per batch)

# COMMAND ----------

def write_bronze(df, table: str, batch_id: str):
    """Schema-enforced raw copy. Overwrite keeps exactly one batch (point-in-time)."""
    enriched = (df
        .withColumn("batch_id",       F.lit(batch_id))
        .withColumn("processed_date", F.current_timestamp()))
    (enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table))
    print(f"  Bronze {table}: {enriched.count()} rows")
    return enriched

print("=== Bronze Day 1 ===")
bronze_sales_d1    = write_bronze(sales_d1,    f"{BRONZE}.sales_transactions", BATCH_D1)
bronze_customer_d1 = write_bronze(customer_d1, f"{BRONZE}.customer_master",    BATCH_D1)
bronze_product_d1  = write_bronze(product_d1,  f"{BRONZE}.product_master",     BATCH_D1)

# COMMAND ----------

# MAGIC %md ## 3  Silver Layer – Standardised, Append-Only
# MAGIC Equivalent to `run_standardization.py` driven by SD YAML config.

# COMMAND ----------

# SD transform config (mirrors Standardization/src/config/*.yaml)
SD_CONFIG = {
    "sales_transactions": {
        "sales_id":      ["trim"],
        "customer_id":   ["trim"],
        "product_id":    ["trim"],
        "currency":      ["trim", "upper"],
        "source_system": ["trim", "upper"],
    },
    "customer_master": {
        "customer_id":   ["trim"],
        "customer_name": ["trim"],
        "region":        ["trim"],
        "channel":       ["trim"],
    },
    "product_master": {
        "product_id":    ["trim"],
        "product_name":  ["trim"],
        "brand":         ["trim"],
        "category":      ["trim"],
        "status":        ["trim", "upper"],
    },
}

def _apply_transforms(df, transforms: dict):
    result = df
    for col_name, ops in transforms.items():
        if col_name not in df.columns:
            continue
        expr = F.col(col_name)
        for op in ops:
            if op == "trim":
                expr = F.trim(expr)
            elif op == "upper":
                expr = F.upper(expr)
            elif op == "lower":
                expr = F.lower(expr)
        result = result.withColumn(col_name, expr)
    return result

def write_silver(bronze_df, entity: str):
    """Append standardised rows. Keeps full history across batches via batch_id."""
    silver_df = _apply_transforms(bronze_df, SD_CONFIG[entity])
    target = f"{SILVER}.{entity}"
    (silver_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target))
    print(f"  Silver {target}: {silver_df.count()} rows appended")

print("=== Silver Day 1 ===")
write_silver(bronze_sales_d1,    "sales_transactions")
write_silver(bronze_customer_d1, "customer_master")
write_silver(bronze_product_d1,  "product_master")

# COMMAND ----------

# MAGIC %md ## 4  Gold Layer – DimDate

# COMMAND ----------

def load_dim_date(target_table: str, start: str = "2024-01-01", end: str = "2030-12-31"):
    df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS calendar_date
    """)
    dim = (df
        .withColumn("date_key",     F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("year",         F.year("calendar_date"))
        .withColumn("quarter",      F.quarter("calendar_date"))
        .withColumn("month",        F.month("calendar_date"))
        .withColumn("month_name",   F.date_format("calendar_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("calendar_date"))
        .withColumn("day_of_week",  F.dayofweek("calendar_date"))
        .withColumn("day_name",     F.date_format("calendar_date", "EEEE"))
        .withColumn("is_weekend",   F.dayofweek("calendar_date").isin(1, 7)))
    (dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))
    print(f"DimDate: {dim.count()} rows → {target_table}")

load_dim_date(f"{GOLD}.dim_date")
spark.table(f"{GOLD}.dim_date").filter("year = 2026 AND month = 3").show(5)

# COMMAND ----------

# MAGIC %md ## 5  Gold Layer – SCD Type 2 Dimensions
# MAGIC Equivalent to `scd2_dimension_loader.py` driven by TF YAML config.

# COMMAND ----------

def _ensure_unknown_member(table: str, sk: str, nk: str, tracked: list):
    """Insert surrogate key = -1 UNKNOWN row if absent."""
    if spark.table(table).filter(F.col(sk) == -1).limit(1).count() > 0:
        return
    cols = spark.table(table).columns
    exprs = []
    for c in cols:
        if c == sk:
            exprs.append(f"CAST(-1 AS BIGINT) AS {c}")
        elif c in [nk] + tracked + ["attribute_hash"]:
            exprs.append(f"'UNKNOWN' AS {c}")
        elif c == "effective_from":
            exprs.append(f"TO_DATE('1900-01-01') AS {c}")
        elif c == "effective_to":
            exprs.append(f"TO_DATE('9999-12-31') AS {c}")
        elif c == "is_current":
            exprs.append(f"TRUE AS {c}")
        elif c == "batch_id":
            exprs.append(f"'SYSTEM' AS {c}")
        else:
            exprs.append(f"NULL AS {c}")
    spark.sql(f"INSERT INTO {table} SELECT {', '.join(exprs)}")
    print(f"  Added UNKNOWN member (key=-1) to {table}")


def load_scd2_dimension(
    source_table: str,
    target_table: str,
    nk: str,
    sk: str,
    tracked: list,
    effective_from_col: str,
    batch_id: str,
):
    """Generic SCD2 loader. New rows expire the old is_current=true row."""
    src = spark.table(source_table).filter(F.col("batch_id") == batch_id)
    if src.limit(1).count() == 0:
        print(f"  {target_table}: no rows for batch {batch_id}, skipping.")
        return

    hash_expr = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked]),
        256,
    )
    staged = (src
        .withColumn("attribute_hash", hash_expr)
        .withColumn("effective_from",
            F.coalesce(F.to_date(F.col(effective_from_col).cast("string")), F.current_date()))
        .withColumn("effective_to",  F.to_date(F.lit("9999-12-31")))
        .withColumn("is_current",    F.lit(True))
        .withColumn("batch_id",      F.lit(batch_id)))

    # ── Initial load ─────────────────────────────────────────────────────────
    if not spark.catalog.tableExists(target_table):
        w = Window.orderBy(nk)
        initial = staged.withColumn(sk, F.row_number().over(w).cast("long"))
        initial.write.format("delta").mode("overwrite").saveAsTable(target_table)
        _ensure_unknown_member(target_table, sk, nk, tracked)
        print(f"  Created {target_table}: {spark.table(target_table).count()} rows (incl. UNKNOWN)")
        return

    # ── Incremental SCD2 ─────────────────────────────────────────────────────
    dim = DeltaTable.forName(spark, target_table)
    current_hashes = (spark.table(target_table)
        .filter("is_current = true")
        .select(nk, "attribute_hash"))

    changed = (staged.alias("s")
        .join(current_hashes.alias("d"), nk, "left")
        .filter(F.col("d.attribute_hash").isNull() | (F.col("s.attribute_hash") != F.col("d.attribute_hash")))
        .select("s.*"))

    n_changed = changed.count()
    if n_changed == 0:
        print(f"  {target_table}: no attribute changes detected.")
        return

    # Expire old current rows
    dim.alias("d").merge(
        changed.alias("s"),
        f"d.{nk} = s.{nk} AND d.is_current = true",
    ).whenMatchedUpdate(set={
        "effective_to": "date_sub(s.effective_from, 1)",
        "is_current":   "false",
    }).execute()

    # Insert new rows with new surrogate keys
    max_key = spark.table(target_table).agg(F.max(sk)).first()[0] or 0
    w = Window.orderBy(nk)
    inserts = changed.withColumn(sk, (F.row_number().over(w) + F.lit(max_key)).cast("long"))
    inserts.write.format("delta").mode("append").saveAsTable(target_table)
    _ensure_unknown_member(target_table, sk, nk, tracked)
    print(f"  Updated {target_table}: {n_changed} new/changed rows inserted.")

# COMMAND ----------

# MAGIC %md ### 5a  DimProduct – Day 1

# COMMAND ----------

print("=== DimProduct Day 1 ===")
load_scd2_dimension(
    source_table       = f"{SILVER}.product_master",
    target_table       = f"{GOLD}.dim_product",
    nk                 = "product_id",
    sk                 = "product_key",
    tracked            = ["product_name", "brand", "category", "status"],
    effective_from_col = "effective_date",
    batch_id           = BATCH_D1,
)
display(spark.table(f"{GOLD}.dim_product").orderBy("product_key"))

# COMMAND ----------

# MAGIC %md ### 5b  DimCustomer – Day 1

# COMMAND ----------

print("=== DimCustomer Day 1 ===")
load_scd2_dimension(
    source_table       = f"{SILVER}.customer_master",
    target_table       = f"{GOLD}.dim_customer",
    nk                 = "customer_id",
    sk                 = "customer_key",
    tracked            = ["customer_name", "region", "channel"],
    effective_from_col = "last_modified",
    batch_id           = BATCH_D1,
)
display(spark.table(f"{GOLD}.dim_customer").orderBy("customer_key"))

# COMMAND ----------

# MAGIC %md ## 6  Gold Layer – FactSales
# MAGIC Equivalent to `fact_sales_loader.py` driven by TF YAML config.

# COMMAND ----------

# TF config (mirrors Transformation/src/config/facts/tf_fact_sales.yaml)
DIM_CONFIGS = [
    {
        "name":                 "customer",
        "table":                f"{GOLD}.dim_customer",
        "natural_key_source":   "customer_id",
        "natural_key_dimension":"customer_id",
        "surrogate_key":        "customer_key",
        "effective_from":       "effective_from",
        "effective_to":         "effective_to",
    },
    {
        "name":                 "product",
        "table":                f"{GOLD}.dim_product",
        "natural_key_source":   "product_id",
        "natural_key_dimension":"product_id",
        "surrogate_key":        "product_key",
        "effective_from":       "effective_from",
        "effective_to":         "effective_to",
    },
]


def _temporal_join(fact_df, dim_cfg: dict, date_col: str):
    """Left-join fact to dimension on natural key + transaction-date within effective range."""
    dim = spark.table(dim_cfg["table"]).filter("is_current = false OR is_current = true")  # all rows
    sk   = dim_cfg["surrogate_key"]
    nk_s = dim_cfg["natural_key_source"]
    nk_d = dim_cfg["natural_key_dimension"]
    eff_f = dim_cfg["effective_from"]
    eff_t = dim_cfg["effective_to"]

    join_cond = (
        (fact_df[nk_s] == dim[nk_d]) &
        (fact_df[date_col] >= dim[eff_f]) &
        (fact_df[date_col] <= dim[eff_t])
    )
    joined = fact_df.join(
        dim.select(F.col(nk_d), F.col(sk), F.col(eff_f), F.col(eff_t)),
        join_cond,
        "left",
    )
    return joined.withColumn(sk, F.coalesce(F.col(sk), F.lit(-1).cast("long")))


def load_fact_sales(batch_id: str):
    src = spark.table(f"{SILVER}.sales_transactions").filter(F.col("batch_id") == batch_id)
    if src.limit(1).count() == 0:
        print(f"FactSales: no rows for batch {batch_id}, skipping.")
        return

    hash_cols = ["source_system", "sales_id", "order_date"]
    fact = src.withColumn(
        "transaction_hash",
        F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256),
    )

    for dim_cfg in DIM_CONFIGS:
        fact = _temporal_join(fact, dim_cfg, "order_date")

    sk_cols = [d["surrogate_key"] for d in DIM_CONFIGS]
    unresolved_flag = F.lit(False)
    for sk in sk_cols:
        unresolved_flag = unresolved_flag | (F.col(sk) == -1)

    fact = (fact
        .withColumn("date_key",        F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("needs_resolution", unresolved_flag)
        .withColumn("fact_batch_id",   F.lit(batch_id))
        .withColumn("fact_loaded_at",  F.current_timestamp()))

    selected = (
        ["transaction_hash", "sales_id", "date_key"]
        + sk_cols
        + ["quantity", "net_amount", "currency", "source_system", "batch_id", "order_date"]
        + ["needs_resolution", "fact_batch_id", "fact_loaded_at"]
    )
    fact_out = fact.select(*list(dict.fromkeys(selected)))

    target = f"{GOLD}.fact_sales"
    if spark.catalog.tableExists(target):
        DeltaTable.forName(spark, target).alias("t").merge(
            fact_out.alias("s"), "t.transaction_hash = s.transaction_hash"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        fact_out.write.format("delta").mode("overwrite").saveAsTable(target)

    total = spark.table(target).count()
    unresolved = spark.table(target).filter("needs_resolution = true").count()
    print(f"FactSales: {total} total rows, {unresolved} with unresolved FK(s)")

    # ── Late-arriving dimension bridge ───────────────────────────────────────
    bridge_target = f"{GOLD}.late_arriving_dimension_bridge"
    bridge_frames = []
    for dim_cfg in DIM_CONFIGS:
        sk  = dim_cfg["surrogate_key"]
        nk  = dim_cfg["natural_key_source"]
        pending = (fact.filter(F.col(sk) == -1)
            .select(
                F.sha2(F.concat_ws("|", "transaction_hash", F.lit(dim_cfg["name"]), F.col(nk)), 256).alias("resolution_hash"),
                "transaction_hash",
                F.col("sales_id").alias("transaction_id"),
                F.lit(dim_cfg["name"]).alias("dimension_name"),
                F.col(nk).cast("string").alias("natural_key"),
                F.col("order_date").alias("transaction_date"),
                F.lit(sk).alias("fact_key_column"),
                F.lit(target).alias("fact_table_name"),
                F.lit(batch_id).alias("batch_id"),
                F.lit("PENDING").alias("resolution_status"),
                F.lit(None).cast("long").alias("resolved_surrogate_key"),
                F.current_timestamp().alias("first_seen_timestamp"),
                F.lit(None).cast("timestamp").alias("resolved_timestamp"),
            ))
        bridge_frames.append(pending)

    from functools import reduce
    bridge = reduce(lambda a, b: a.unionByName(b), bridge_frames)
    n_bridge = bridge.count()
    if n_bridge > 0:
        if spark.catalog.tableExists(bridge_target):
            DeltaTable.forName(spark, bridge_target).alias("t").merge(
                bridge.alias("s"), "t.resolution_hash = s.resolution_hash"
            ).whenNotMatchedInsertAll().execute()
        else:
            bridge.write.format("delta").mode("overwrite").saveAsTable(bridge_target)
        print(f"Bridge: {spark.table(bridge_target).count()} pending resolutions")

# COMMAND ----------

print("=== FactSales Day 1 ===")
load_fact_sales(BATCH_D1)
display(spark.table(f"{GOLD}.fact_sales").orderBy("sales_id"))

# COMMAND ----------

# MAGIC %md ## 7  Data Quality & Reconciliation

# COMMAND ----------

print("=" * 65)
print("DATA QUALITY REPORT – Day 1")
print("=" * 65)

# ── Row count reconciliation ──────────────────────────────────────────────────
src_cnt    = sales_d1.count()
bronze_cnt = spark.table(f"{BRONZE}.sales_transactions").count()
silver_cnt = spark.table(f"{SILVER}.sales_transactions").filter(f"batch_id = '{BATCH_D1}'").count()
fact_cnt   = spark.table(f"{GOLD}.fact_sales").count()

print(f"\n1. Row-count reconciliation (sales_transactions):")
print(f"   Source  : {src_cnt:>4}")
print(f"   Bronze  : {bronze_cnt:>4}  {'OK' if bronze_cnt == src_cnt else '*** MISMATCH ***'}")
print(f"   Silver  : {silver_cnt:>4}  {'OK' if silver_cnt == src_cnt else '*** MISMATCH ***'}")
print(f"   Fact    : {fact_cnt:>4}  (all rows loaded; unresolved FKs use key=-1)")

# ── Negative net_amount anomaly ───────────────────────────────────────────────
neg = spark.table(f"{SILVER}.sales_transactions").filter("net_amount < 0")
print(f"\n2. Negative net_amount rows (data anomaly / credit note): {neg.count()}")
neg.select("sales_id", "customer_id", "product_id", "net_amount").show()

# ── Unknown dimension references ─────────────────────────────────────────────
unresolved = spark.table(f"{GOLD}.fact_sales").filter("needs_resolution = true")
print(f"3. Rows with unresolved dimension FK (customer_key=-1 or product_key=-1): {unresolved.count()}")
unresolved.select("sales_id", "customer_key", "product_key", "needs_resolution").show()

# ── Currency standardisation ─────────────────────────────────────────────────
print("4. Currency values after UPPER standardisation:")
spark.table(f"{SILVER}.sales_transactions").groupBy("currency").agg(F.count("*").alias("cnt")).show()

# ── Null checks on key columns ───────────────────────────────────────────────
print("5. Null checks on critical columns:")
for col in ["sales_id", "order_date", "customer_id", "product_id", "net_amount"]:
    n = spark.table(f"{SILVER}.sales_transactions").filter(F.col(col).isNull()).count()
    status = "OK" if n == 0 else f"*** {n} NULLS ***"
    print(f"   {col:<20}: {status}")

# ── Bridge summary ────────────────────────────────────────────────────────────
bridge = spark.table(f"{GOLD}.late_arriving_dimension_bridge")
print(f"\n6. Late-arriving dimension bridge:")
bridge.select("dimension_name", "natural_key", "transaction_id", "resolution_status").show()

# COMMAND ----------

# MAGIC %md ## 8  Day 2 Source Data (2026-03-17)
# MAGIC
# MAGIC | Change | Impact |
# MAGIC |---|---|
# MAGIC | P1003 Aquarius status: INACTIVE → ACTIVE | SCD2 new row in DimProduct |
# MAGIC | P1004 Monster Energy: new product | New row in DimProduct |
# MAGIC | P9999 Test Product: arrives (was unknown) | Resolves S0005 product_key via bridge |
# MAGIC | C101 Osaka Wholesale channel: Wholesale → Direct | SCD2 new row in DimCustomer |
# MAGIC | C103 Fukuoka Store: arrives (was late for S0006) | Resolves S0006 customer_key via bridge |
# MAGIC | S0007-S0009: new transactions | Appended to FactSales |

# COMMAND ----------

# ── Day 2 Product Master (full REST API extract) ──────────────────────────────
# P9999 effective_date = 2026-03-16 → temporal join will resolve S0005 (order_date=2026-03-16)
product_d2 = spark.createDataFrame([
    ("P1001", "Coca-Cola 500ml",      "Coca-Cola", "Beverages", "ACTIVE",   date(2024,  1,  1)),
    ("P1002", "Georgia Coffee 185g",  "Georgia",   "Beverages", "ACTIVE",   date(2024,  1,  1)),
    ("P1003", "Aquarius 500ml",       "Aquarius",  "Beverages", "ACTIVE",   date(2026,  3, 17)),  # status changed
    ("P1004", "Monster Energy 500ml", "Monster",   "Beverages", "ACTIVE",   date(2026,  3, 17)),  # new product
    ("P9999", "Test Product (Return)","Unknown",   "Other",     "INACTIVE", date(2026,  3, 16)),  # resolves S0005
], schema=PRODUCT_SCHEMA)

# ── Day 2 Customer Master (incremental – only changed/new rows) ───────────────
# C103 last_modified = 2026-03-14 → effective_from=2026-03-14 ≤ S0006 order_date=2026-03-16 → will resolve
customer_d2 = (spark.createDataFrame([
    ("C101", "Osaka Wholesale", "Kansai", "Direct", "2026-03-17 09:00:00"),  # channel changed
    ("C103", "Fukuoka Store",   "Kyushu", "Retail", "2026-03-14 08:00:00"),  # was active since 3/14
], CUSTOMER_COLS)
    .withColumn("last_modified", F.to_timestamp("last_modified_str"))
    .drop("last_modified_str"))

# ── Day 2 Sales ───────────────────────────────────────────────────────────────
sales_d2 = spark.createDataFrame([
    ("S0007", date(2026, 3, 17), "C100", "P1001", 10, 18000.00, "JPY", "SAP"),
    ("S0008", date(2026, 3, 17), "C101", "P1004", 15, 27000.00, "JPY", "SAP"),  # C101 now Direct, P1004 new
    ("S0009", date(2026, 3, 17), "C103", "P1003", 20, 36000.00, "JPY", "SAP"),  # C103 arrives, P1003 now ACTIVE
], schema=SALES_SCHEMA)

print("Day 2 source data ready.")

# COMMAND ----------

# MAGIC %md ## 9  Day 2 – Bronze & Silver

# COMMAND ----------

print("=== Bronze Day 2 ===")
bronze_product_d2  = write_bronze(product_d2,  f"{BRONZE}.product_master",     BATCH_D2)
bronze_customer_d2 = write_bronze(customer_d2, f"{BRONZE}.customer_master",    BATCH_D2)
bronze_sales_d2    = write_bronze(sales_d2,    f"{BRONZE}.sales_transactions",  BATCH_D2)

print("\n=== Silver Day 2 ===")
write_silver(bronze_product_d2,  "product_master")
write_silver(bronze_customer_d2, "customer_master")
write_silver(bronze_sales_d2,    "sales_transactions")

# Confirm silver has cumulative rows
total_silver = spark.table(f"{SILVER}.sales_transactions").count()
print(f"\nSilver sales_transactions cumulative rows: {total_silver} (D1=6, D2=3)")

# COMMAND ----------

# MAGIC %md ## 10  Day 2 – SCD2 Dimension Updates

# COMMAND ----------

print("=== DimProduct Day 2 (SCD2 update) ===")
load_scd2_dimension(
    source_table       = f"{SILVER}.product_master",
    target_table       = f"{GOLD}.dim_product",
    nk                 = "product_id",
    sk                 = "product_key",
    tracked            = ["product_name", "brand", "category", "status"],
    effective_from_col = "effective_date",
    batch_id           = BATCH_D2,
)

print("\n=== DimCustomer Day 2 (SCD2 update) ===")
load_scd2_dimension(
    source_table       = f"{SILVER}.customer_master",
    target_table       = f"{GOLD}.dim_customer",
    nk                 = "customer_id",
    sk                 = "customer_key",
    tracked            = ["customer_name", "region", "channel"],
    effective_from_col = "last_modified",
    batch_id           = BATCH_D2,
)

# COMMAND ----------

# MAGIC %md ### SCD2 History Inspection

# COMMAND ----------

print("── P1003 (Aquarius 500ml) SCD2 history ────────────────────────")
spark.table(f"{GOLD}.dim_product") \
    .filter("product_id = 'P1003'") \
    .select("product_key", "product_id", "status", "effective_from", "effective_to", "is_current") \
    .orderBy("effective_from") \
    .show()

print("── C101 (Osaka Wholesale) SCD2 history ─────────────────────────")
spark.table(f"{GOLD}.dim_customer") \
    .filter("customer_id = 'C101'") \
    .select("customer_key", "customer_id", "channel", "effective_from", "effective_to", "is_current") \
    .orderBy("effective_from") \
    .show()

# Full dim snapshots
print("── Full DimProduct ───────────────────────────────────────────────")
display(spark.table(f"{GOLD}.dim_product").orderBy("product_id", "effective_from"))

print("── Full DimCustomer ─────────────────────────────────────────────")
display(spark.table(f"{GOLD}.dim_customer").orderBy("customer_id", "effective_from"))

# COMMAND ----------

# MAGIC %md ## 11  Day 2 – FactSales Load

# COMMAND ----------

print("=== FactSales Day 2 ===")
load_fact_sales(BATCH_D2)

print("\nAll fact rows after Day 2 load:")
display(spark.table(f"{GOLD}.fact_sales").orderBy("sales_id"))

# COMMAND ----------

# MAGIC %md ## 12  Late-Arriving Dimension Resolution
# MAGIC Equivalent to `resolve_late_dimensions.py`.
# MAGIC
# MAGIC **Expected outcomes:**
# MAGIC - S0006 / C103: `customer_key` updated from -1 → C103's surrogate key (effective_from=2026-03-14 ≤ order_date=2026-03-16 ✓)
# MAGIC - S0005 / P9999: `product_key` updated from -1 → P9999's surrogate key (effective_from=2026-03-16 = order_date ✓)
# MAGIC - S0005 / C999: remains PENDING (C999 never arrives)

# COMMAND ----------

print("Bridge BEFORE resolution:")
display(spark.table(f"{GOLD}.late_arriving_dimension_bridge")
    .select("dimension_name", "natural_key", "transaction_id", "transaction_date", "resolution_status"))

# COMMAND ----------

def resolve_late_dimensions():
    bridge_table = f"{GOLD}.late_arriving_dimension_bridge"
    fact_table   = f"{GOLD}.fact_sales"

    for dim_cfg in DIM_CONFIGS:
        dim_name  = dim_cfg["name"]
        dim_table = dim_cfg["table"]
        nk_dim    = dim_cfg["natural_key_dimension"]
        sk        = dim_cfg["surrogate_key"]

        pending = spark.table(bridge_table).filter(
            (F.col("dimension_name") == dim_name) & (F.col("resolution_status") == "PENDING")
        )
        if pending.count() == 0:
            print(f"  {dim_name}: no pending records.")
            continue

        dim = spark.table(dim_table)

        # Temporal join: transaction_date must fall within dim's effective range
        resolved = (pending.alias("b")
            .join(dim.alias("d"),
                (F.col("b.natural_key") == F.col(f"d.{nk_dim}")) &
                (F.col("b.transaction_date") >= F.col("d.effective_from")) &
                (F.col("b.transaction_date") <= F.col("d.effective_to")),
                "inner")
            .select(
                "b.resolution_hash",
                "b.transaction_hash",
                F.col(f"d.{sk}").alias("resolved_key"),
            ))

        n_resolved = resolved.count()
        n_pending  = pending.count()
        if n_resolved == 0:
            print(f"  {dim_name}: {n_pending} records still unresolvable (dimension not yet available).")
            continue

        # Patch fact table FK
        DeltaTable.forName(spark, fact_table).alias("f").merge(
            resolved.alias("r"), "f.transaction_hash = r.transaction_hash"
        ).whenMatchedUpdate(
            condition=f"f.{sk} = -1",
            set={sk: "r.resolved_key"},
        ).execute()

        # Mark bridge entries RESOLVED
        DeltaTable.forName(spark, bridge_table).alias("b").merge(
            resolved.alias("r"), "b.resolution_hash = r.resolution_hash"
        ).whenMatchedUpdate(set={
            "resolved_surrogate_key": "r.resolved_key",
            "resolution_status":      "'RESOLVED'",
            "resolved_timestamp":     "current_timestamp()",
        }).execute()

        still_unresolved = n_pending - n_resolved
        print(f"  {dim_name}: {n_resolved} resolved, {still_unresolved} still pending.")

    # Clear needs_resolution flag where ALL FKs are now filled
    sk_cols   = [d["surrogate_key"] for d in DIM_CONFIGS]
    clear_cond = " AND ".join([f"{sk} <> -1" for sk in sk_cols])
    DeltaTable.forName(spark, fact_table).update(
        condition=f"needs_resolution = true AND {clear_cond}",
        set={"needs_resolution": "false"},
    )
    print("\n  needs_resolution flag cleared where all FKs resolved.")

resolve_late_dimensions()

# COMMAND ----------

print("\nBridge AFTER resolution:")
display(spark.table(f"{GOLD}.late_arriving_dimension_bridge")
    .select("dimension_name", "natural_key", "transaction_id", "resolution_status",
            "resolved_surrogate_key", "resolved_timestamp"))

print("\nFact rows with remaining unresolved FKs:")
spark.table(f"{GOLD}.fact_sales") \
    .filter("needs_resolution = true") \
    .select("sales_id", "customer_key", "product_key", "needs_resolution") \
    .show()

# COMMAND ----------

# MAGIC %md ## 13  Analytical Queries (Gold Layer)
# MAGIC Using only `needs_resolution = false` rows for clean reporting.

# COMMAND ----------

# MAGIC %md ### Q1 – Total sales by customer (current attributes)

# COMMAND ----------

spark.sql(f"""
SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    c.channel,
    COUNT(*)                        AS transactions,
    SUM(f.quantity)                 AS total_units,
    ROUND(SUM(f.net_amount), 2)     AS revenue_jpy
FROM {GOLD}.fact_sales      f
JOIN {GOLD}.dim_customer    c ON f.customer_key = c.customer_key
WHERE c.is_current = true
  AND f.needs_resolution = false
GROUP BY c.customer_id, c.customer_name, c.region, c.channel
ORDER BY revenue_jpy DESC
""").show()

# COMMAND ----------

# MAGIC %md ### Q2 – Sales by product and category (current attributes)

# COMMAND ----------

spark.sql(f"""
SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.status,
    COUNT(*)                        AS transactions,
    SUM(f.quantity)                 AS total_units,
    ROUND(SUM(f.net_amount), 2)     AS revenue_jpy
FROM {GOLD}.fact_sales      f
JOIN {GOLD}.dim_product     p ON f.product_key = p.product_key
WHERE p.is_current = true
  AND f.needs_resolution = false
GROUP BY p.product_id, p.product_name, p.brand, p.status
ORDER BY revenue_jpy DESC
""").show()

# COMMAND ----------

# MAGIC %md ### Q3 – Daily sales trend

# COMMAND ----------

spark.sql(f"""
SELECT
    d.calendar_date,
    d.day_name,
    COUNT(*)                        AS transactions,
    SUM(f.quantity)                 AS units_sold,
    ROUND(SUM(f.net_amount), 2)     AS revenue_jpy
FROM {GOLD}.fact_sales   f
JOIN {GOLD}.dim_date     d ON f.date_key = d.date_key
WHERE f.needs_resolution = false
GROUP BY d.calendar_date, d.day_name
ORDER BY d.calendar_date
""").show()

# COMMAND ----------

# MAGIC %md ### Q4 – SCD2 temporal correctness (C101 Osaka Wholesale → Direct)
# MAGIC
# MAGIC The fact table stores the surrogate key valid **at transaction time**.
# MAGIC No temporal join is needed at query time – just join on `customer_key`.

# COMMAND ----------

spark.sql(f"""
SELECT
    f.sales_id,
    d.calendar_date  AS order_date,
    c.customer_id,
    c.customer_name,
    c.channel,          -- channel AS OF transaction date (SCD2 temporal correctness)
    c.effective_from,
    c.effective_to,
    c.is_current,
    f.net_amount
FROM {GOLD}.fact_sales      f
JOIN {GOLD}.dim_customer    c ON f.customer_key = c.customer_key
JOIN {GOLD}.dim_date        d ON f.date_key = d.date_key
WHERE c.customer_id = 'C101'
ORDER BY d.calendar_date
""").show()

# COMMAND ----------

# MAGIC %md ### Q5 – Unresolved rows (C999 permanently unknown)

# COMMAND ----------

spark.sql(f"""
SELECT
    f.sales_id,
    f.customer_key,
    f.product_key,
    f.needs_resolution,
    f.net_amount,
    b_c.natural_key  AS unresolved_customer_nk,
    b_p.natural_key  AS unresolved_product_nk
FROM {GOLD}.fact_sales f
LEFT JOIN {GOLD}.late_arriving_dimension_bridge b_c
    ON f.transaction_hash = b_c.transaction_hash
   AND b_c.dimension_name = 'customer'
   AND b_c.resolution_status = 'PENDING'
LEFT JOIN {GOLD}.late_arriving_dimension_bridge b_p
    ON f.transaction_hash = b_p.transaction_hash
   AND b_p.dimension_name = 'product'
   AND b_p.resolution_status = 'PENDING'
WHERE f.needs_resolution = true
""").show()

# COMMAND ----------

# MAGIC %md ## 14  Final Summary

# COMMAND ----------

print("=" * 70)
print("PROTOTYPE COMPLETE – TABLE INVENTORY")
print("=" * 70)

layer_tables = {
    "Bronze (raw snapshot)": [
        f"{BRONZE}.sales_transactions",
        f"{BRONZE}.customer_master",
        f"{BRONZE}.product_master",
    ],
    "Silver (standardised, append-only)": [
        f"{SILVER}.sales_transactions",
        f"{SILVER}.customer_master",
        f"{SILVER}.product_master",
    ],
    "Gold (star schema)": [
        f"{GOLD}.dim_date",
        f"{GOLD}.dim_product",
        f"{GOLD}.dim_customer",
        f"{GOLD}.fact_sales",
        f"{GOLD}.late_arriving_dimension_bridge",
    ],
}

for layer, tables in layer_tables.items():
    print(f"\n{layer}:")
    for t in tables:
        cnt = spark.table(t).count()
        print(f"  {t:<55}: {cnt:>4} rows")

print("""
Key patterns demonstrated
─────────────────────────
 1. Medallion architecture  Bronze(overwrite) → Silver(append) → Gold(merge/SCD2)
 2. Schema enforcement      YAML-driven SD config enforces types at Bronze
 3. SCD Type 2              Surrogate key + effective_from/to + is_current flag
 4. Temporal join           Fact stores SK valid at transaction time (no re-join needed)
 5. Late-arriving bridge    FK=-1 sentinel, bridge table, async resolution run
 6. Transaction hash        SHA-256 for idempotent fact upserts
 7. Unknown member          product_key=-1 / customer_key=-1 for referential integrity
 8. Data quality            Row-count reconciliation, null checks, negative amount alerts
 9. Incremental SQL load    Watermark pattern (Day 2 customer: only changed rows)
10. CI/CD pattern           YAML configs are environment-agnostic; tokens resolve at runtime
""")
