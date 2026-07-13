# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # TF Gold Load Engine
# MAGIC **Generic utility notebook** – fully driven by the TF YAML config.
# MAGIC Routes to the correct loader based on `meta.object_type`:
# MAGIC
# MAGIC | `meta.object_type` | `meta.scd_type` | Loader invoked |
# MAGIC |---|---|---|
# MAGIC | `dimension` | *(absent)* | Date dimension generator (no source table) |
# MAGIC | `dimension` | `2` | SCD Type 2 dimension loader |
# MAGIC | `fact` | – | Fact loader with temporal joins + late-arriving bridge |
# MAGIC
# MAGIC To add a new dimension or fact table, upload a TF YAML and point `tf_yml_path` at it.
# MAGIC No code changes required.
# MAGIC
# MAGIC | Widget | Description |
# MAGIC |---|---|
# MAGIC | `tf_yml_path` | Full path to TF YAML on Volume or DBFS |
# MAGIC | `batch_id` | Unique run ID – must match the SD batch that loaded Silver |
# MAGIC | `catalog_name` | UC catalog name. **Leave blank for CE** |
# MAGIC | `silver_schema` | Silver schema/database |
# MAGIC | `gold_schema` | Gold schema/database |

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("tf_yml_path",   "/dbfs/FileStore/ccbji/configs/tf/tf_dim_customer.yaml", "TF YAML path on Volume/DBFS")
dbutils.widgets.text("batch_id",      "",                                                        "Batch ID (must match SD batch)")
dbutils.widgets.text("catalog_name",  "",                                                        "Catalog name (blank for CE)")
dbutils.widgets.text("silver_schema", "ccbji_silver",                                            "Silver schema")
dbutils.widgets.text("gold_schema",   "ccbji_gold",                                              "Gold schema")

# COMMAND ----------

# MAGIC %md ## 0  Initialise

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce
from datetime import datetime
import yaml, json, uuid as _uuid

tf_yml_path   = dbutils.widgets.get("tf_yml_path")
batch_id      = dbutils.widgets.get("batch_id").strip() or f"BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}"
catalog_name  = dbutils.widgets.get("catalog_name").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema   = dbutils.widgets.get("gold_schema").strip()

print(f"YAML         : {tf_yml_path}")
print(f"Batch ID     : {batch_id}")
print(f"Catalog      : '{catalog_name}' (blank = CE 2-part names)")
print(f"Silver       : {silver_schema} | Gold: {gold_schema}")

_RUN_ID  = str(_uuid.uuid4())
_STARTED = datetime.utcnow()


def _audit_log(status, source_object="", target_object="",
               rows_read=None, rows_written=None, rows_rejected=0,
               error_msg=None):
    """Append one run record to audit.pipeline_run_log. Never raises."""
    try:
        pfx = f"{catalog_name}." if catalog_name else ""
        now = datetime.utcnow()
        row = [(_RUN_ID, "TF_Gold_Load_Engine", batch_id, "gold",
                source_object, target_object, status,
                int(rows_read)     if rows_read     is not None else None,
                int(rows_written)  if rows_written  is not None else None,
                int(rows_rejected) if rows_rejected is not None else 0,
                _STARTED, now, (now - _STARTED).total_seconds(),
                error_msg, None, catalog_name, None, None)]
        _schema = T.StructType([
            T.StructField("run_id",           T.StringType(),    False),
            T.StructField("pipeline_name",    T.StringType(),    False),
            T.StructField("batch_id",         T.StringType(),    True),
            T.StructField("layer",            T.StringType(),    True),
            T.StructField("source_object",    T.StringType(),    True),
            T.StructField("target_object",    T.StringType(),    True),
            T.StructField("status",           T.StringType(),    False),
            T.StructField("rows_read",        T.LongType(),      True),
            T.StructField("rows_written",     T.LongType(),      True),
            T.StructField("rows_rejected",    T.LongType(),      True),
            T.StructField("started_at",       T.TimestampType(), False),
            T.StructField("completed_at",     T.TimestampType(), True),
            T.StructField("duration_seconds", T.DoubleType(),    True),
            T.StructField("error_message",    T.StringType(),    True),
            T.StructField("error_stacktrace", T.StringType(),    True),
            T.StructField("catalog_name",     T.StringType(),    True),
            T.StructField("environment",      T.StringType(),    True),
            T.StructField("run_metadata",     T.StringType(),    True),
        ])
        (spark.createDataFrame(row, _schema)
             .write.format("delta").mode("append")
             .saveAsTable(f"{pfx}audit.pipeline_run_log"))
    except Exception as _e:
        print(f"  [audit] write skipped: {_e}")


def _apply_column_masks(table_name: str, security_cfg: dict) -> None:
    """Apply Unity Catalog column mask policies declared in the YAML security block.
    Silently skipped on CE / hive_metastore — column masking requires Unity Catalog."""
    if not security_cfg or "column_masks" not in security_cfg:
        return
    for mask in security_cfg["column_masks"]:
        col = mask["column"]
        fn  = mask["mask_function"]
        try:
            spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN `{col}` SET MASK {fn}")
            print(f"  [security] mask applied  : {table_name}.{col} → {fn}")
        except Exception as _e:
            print(f"  [security] mask skipped  : {_e}")


def _dq_log(target_table: str, check_name: str, column: str, status: str,
            rows_checked, rows_failed, severity: str, message: str = "") -> None:
    """Append one DQ check result to audit.dq_check_log. Never raises."""
    try:
        pfx = f"{catalog_name}." if catalog_name else ""
        now = datetime.utcnow()
        row = [(_RUN_ID, "TF_Gold_Load_Engine", batch_id, target_table,
                check_name, column or "", status,
                int(rows_checked) if rows_checked is not None else 0,
                int(rows_failed)  if rows_failed  is not None else 0,
                severity, message, now)]
        _schema = T.StructType([
            T.StructField("run_id",        T.StringType(),    False),
            T.StructField("pipeline_name", T.StringType(),    False),
            T.StructField("batch_id",      T.StringType(),    True),
            T.StructField("table_name",    T.StringType(),    True),
            T.StructField("check_name",    T.StringType(),    False),
            T.StructField("column_name",   T.StringType(),    True),
            T.StructField("status",        T.StringType(),    False),
            T.StructField("rows_checked",  T.LongType(),      True),
            T.StructField("rows_failed",   T.LongType(),      True),
            T.StructField("severity",      T.StringType(),    True),
            T.StructField("message",       T.StringType(),    True),
            T.StructField("checked_at",    T.TimestampType(), True),
        ])
        (spark.createDataFrame(row, _schema)
             .write.format("delta").mode("append")
             .saveAsTable(f"{pfx}audit.dq_check_log"))
    except Exception as _e:
        print(f"  [dq_log] write skipped: {_e}")


def _run_all_tf_dq(rules: list, target_table: str) -> list:
    """Run Gold-layer DQ checks against a written Delta table.

    Supported checks:
      row_count_min       – table has at least `min` rows
      not_null            – no NULLs in `column`
      not_negative        – no negative values in numeric `column`
      zero_check          – no zero values in numeric `column`
      allowed_values      – `column` only contains values from `values` list
      duplicate_key       – no duplicate combinations in `columns` (list) or `column`
      referential_integrity – FK `column` matches `ref_column` in `ref_table`
                              (rows with value == `unknown_key` are excluded)
    """
    if not rules:
        return []
    df         = spark.table(target_table)
    total_rows = df.count()
    results    = []
    for rule in rules:
        check    = rule["check"]
        col      = rule.get("column")
        severity = rule.get("severity", "warning")
        status   = "PASSED"
        n_failed = 0
        msg      = ""
        try:
            if check == "row_count_min":
                min_rows = int(rule.get("min", 1))
                if total_rows < min_rows:
                    status = "FAILED"
                    msg    = f"Expected ≥{min_rows} rows, got {total_rows}"
                else:
                    msg = f"{total_rows} rows (min {min_rows})"

            elif check == "not_null":
                n_failed = df.filter(F.col(col).isNull()).count()
                if n_failed:
                    status = "FAILED"
                    msg    = f"{n_failed} NULL values in '{col}'"

            elif check == "not_negative":
                n_failed = df.filter(F.col(col) < 0).count()
                if n_failed:
                    status = "FAILED"
                    msg    = f"{n_failed} negative values in '{col}'"

            elif check == "zero_check":
                n_failed = df.filter(F.col(col) == 0).count()
                if n_failed:
                    status = "FAILED"
                    msg    = f"{n_failed} zero values in '{col}'"

            elif check == "allowed_values":
                vals     = rule.get("values", [])
                n_failed = df.filter(~F.col(col).isin(vals)).count()
                if n_failed:
                    status = "FAILED"
                    msg    = f"{n_failed} values in '{col}' not in {vals}"

            elif check == "duplicate_key":
                key_cols = rule.get("columns") or [col]
                n_failed = (df.groupBy(key_cols)
                              .agg(F.count("*").alias("_cnt"))
                              .filter(F.col("_cnt") > 1)
                              .count())
                if n_failed:
                    status = "FAILED"
                    msg    = f"{n_failed} duplicate key combinations on {key_cols}"

            elif check == "referential_integrity":
                ref_table   = rule["ref_table"]
                ref_col     = rule["ref_column"]
                unknown_key = int(rule.get("unknown_key", -1))
                ref_df = (spark.table(ref_table)
                               .select(F.col(ref_col).alias("_ref"))
                               .distinct())
                n_failed = (df.filter(F.col(col) != unknown_key)
                              .join(ref_df, df[col] == ref_df["_ref"], "left_anti")
                              .count())
                if n_failed:
                    status = "FAILED"
                    msg    = f"{n_failed} unresolved FKs in '{col}' → {ref_table}.{ref_col}"

            else:
                status = "SKIPPED"
                msg    = f"Unknown check '{check}'"

        except Exception as _ex:
            status = "ERROR"
            msg    = str(_ex)

        icon = "✓" if status == "PASSED" else ("✗" if status in ("FAILED", "ERROR") else "⚠")
        print(f"  [{icon}] DQ [{severity.upper():7}] {check}"
              + (f" on '{col}'" if col else "")
              + f" → {status}"
              + (f"  {msg}" if msg else ""))
        _dq_log(target_table, check, col, status, total_rows, n_failed, severity, msg)
        results.append({
            "check": check, "column": col, "status": status,
            "rows_checked": total_rows, "rows_failed": n_failed,
            "severity": severity, "message": msg,
        })
    return results

# COMMAND ----------

# MAGIC %md ## 1  Read & Resolve YAML Config

# COMMAND ----------

def _read_yaml(path: str) -> dict:
    local = path.replace("dbfs:/", "/dbfs/")
    with open(local, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _apply_tokens(value, tokens: dict):
    if isinstance(value, str):
        for k, v in tokens.items():
            if k == "catalog_name" and not v:
                value = value.replace("{catalog_name}.", "")
                value = value.replace("{catalog_name}", "")
            else:
                value = value.replace("{" + k + "}", v)
        return value
    if isinstance(value, list):
        return [_apply_tokens(i, tokens) for i in value]
    if isinstance(value, dict):
        return {k: _apply_tokens(v, tokens) for k, v in value.items()}
    return value


tokens = {
    "catalog_name":  catalog_name,
    "silver_schema": silver_schema,
    "gold_schema":   gold_schema,
}
cfg = _apply_tokens(_read_yaml(tf_yml_path), tokens)

meta        = cfg["meta"]
tf_cfg      = cfg["transformation"]
object_type = meta["object_type"]          # "dimension" | "fact"
scd_type    = meta.get("scd_type")         # 2 | None

print(f"\nobject_type  : {object_type}")
print(f"scd_type     : {scd_type if scd_type else '(date dimension)'}")
print(f"target_table : {tf_cfg.get('target_table', 'N/A')}")

# COMMAND ----------

# MAGIC %md ## 2  Loader Implementations

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# LOADER A – Date Dimension
#   TF YAML keys used: target_table, start_date, end_date
# ─────────────────────────────────────────────────────────────────────────────
def load_date_dimension(cfg: dict) -> dict:
    target     = cfg["target_table"]
    start_date = cfg["start_date"]
    end_date   = cfg["end_date"]

    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS calendar_date"
    )
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

    (dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target))

    cnt = dim.count()
    print(f"DimDate: {cnt} rows ({start_date} → {end_date}) → {target}")
    return {"object_type": "date_dimension", "target_table": target, "rows": cnt, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# LOADER B – SCD Type 2 Dimension
#   TF YAML keys used: source_table, target_table, natural_key, surrogate_key,
#                      tracked_columns, effective_from_column, unknown_member_key
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_unknown_member(table: str, sk: str, nk: str, tracked: list) -> None:
    """Insert the surrogate_key = -1 UNKNOWN sentinel row if absent."""
    if spark.table(table).filter(F.col(sk) == -1).limit(1).count() > 0:
        return
    cols  = spark.table(table).columns
    exprs = []
    for c in cols:
        if c == sk:
            exprs.append(f"CAST(-1 AS BIGINT) AS `{c}`")
        elif c in [nk] + tracked + ["attribute_hash"]:
            exprs.append(f"'UNKNOWN' AS `{c}`")
        elif c == "effective_from":
            exprs.append(f"TO_DATE('1900-01-01') AS `{c}`")
        elif c == "effective_to":
            exprs.append(f"TO_DATE('9999-12-31') AS `{c}`")
        elif c == "is_current":
            exprs.append(f"TRUE AS `{c}`")
        elif c == "batch_id":
            exprs.append(f"'SYSTEM' AS `{c}`")
        else:
            exprs.append(f"NULL AS `{c}`")
    spark.sql(f"INSERT INTO {table} SELECT {', '.join(exprs)}")
    print(f"  ↳ Added UNKNOWN member (key=-1) to {table}")


def load_scd2_dimension(cfg: dict, batch_id: str) -> dict:
    src_table = cfg["source_table"]
    tgt_table = cfg["target_table"]
    nk        = cfg["natural_key"]
    sk        = cfg["surrogate_key"]
    tracked   = cfg["tracked_columns"]
    eff_col   = cfg["effective_from_column"]

    # Read only rows belonging to this batch
    src = spark.table(src_table).filter(F.col("batch_id") == batch_id)
    n_src = src.count()
    if n_src == 0:
        msg = f"No rows for batch_id='{batch_id}' in {src_table}. Nothing to do."
        print(f"  ⚠ {msg}")
        return {"object_type": "scd2_dimension", "target_table": tgt_table,
                "rows_in_batch": 0, "status": "SKIPPED", "reason": msg}

    print(f"  Source rows in batch: {n_src}")

    # Compute attribute hash over tracked columns (drives change detection)
    hash_expr = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked]),
        256,
    )
    staged = (src
        .withColumn("attribute_hash", hash_expr)
        .withColumn("effective_from",
            F.coalesce(F.to_date(F.col(eff_col).cast("string")), F.current_date()))
        .withColumn("effective_to",  F.to_date(F.lit("9999-12-31")))
        .withColumn("is_current",    F.lit(True))
        .withColumn("batch_id",      F.lit(batch_id)))

    # ── Initial load ──────────────────────────────────────────────────────────
    if not spark.catalog.tableExists(tgt_table):
        w = Window.orderBy(nk)
        initial = staged.withColumn(sk, F.row_number().over(w).cast("long"))
        initial.write.format("delta").mode("overwrite").saveAsTable(tgt_table)
        _ensure_unknown_member(tgt_table, sk, nk, tracked)
        total = spark.table(tgt_table).count()
        print(f"  ✓ Initial load → {tgt_table}: {total} rows (incl. UNKNOWN member)")
        return {"object_type": "scd2_dimension", "target_table": tgt_table,
                "rows_inserted": total, "status": "INITIAL_LOAD"}

    # ── Incremental SCD2 ──────────────────────────────────────────────────────
    dim     = DeltaTable.forName(spark, tgt_table)
    current = (spark.table(tgt_table)
        .filter("is_current = true")
        .select(nk, "attribute_hash"))

    # Rows that are new (no match) or have changed attributes
    changed = (staged.alias("s")
        .join(current.alias("d"), nk, "left")
        .filter(
            F.col("d.attribute_hash").isNull() |
            (F.col("s.attribute_hash") != F.col("d.attribute_hash"))
        )
        .select("s.*"))

    n_changed = changed.count()
    if n_changed == 0:
        print(f"  ✓ {tgt_table}: no attribute changes detected in this batch.")
        return {"object_type": "scd2_dimension", "target_table": tgt_table,
                "rows_inserted": 0, "status": "NO_CHANGES"}

    # Expire current rows for changed natural keys
    dim.alias("d").merge(
        changed.alias("s"),
        f"d.`{nk}` = s.`{nk}` AND d.is_current = true",
    ).whenMatchedUpdate(set={
        "effective_to": "date_sub(s.effective_from, 1)",
        "is_current":   "false",
    }).execute()

    # Insert new rows with fresh surrogate keys (max_key + row_number)
    max_key = spark.table(tgt_table).agg(F.max(sk)).first()[0] or 0
    w = Window.orderBy(nk)
    inserts = changed.withColumn(sk, (F.row_number().over(w) + F.lit(max_key)).cast("long"))
    inserts.write.format("delta").mode("append").saveAsTable(tgt_table)
    _ensure_unknown_member(tgt_table, sk, nk, tracked)

    total = spark.table(tgt_table).count()
    print(f"  ✓ {tgt_table}: {n_changed} new/changed rows | {total} total rows")
    return {"object_type": "scd2_dimension", "target_table": tgt_table,
            "rows_inserted": n_changed, "rows_total": total, "status": "OK"}


# ─────────────────────────────────────────────────────────────────────────────
# LOADER C – Fact Table with Late-Arriving Dimension Bridge
#   TF YAML keys used: source_table, target_table, late_bridge_table,
#                      transaction_id_column, transaction_date_column,
#                      transaction_hash_columns, dimensions[], measures[],
#                      degenerate_columns[]
# ─────────────────────────────────────────────────────────────────────────────
def _temporal_join(fact_df, dim_cfg: dict, date_col: str):
    """
    Left-join fact to a dimension on natural key AND transaction date
    falling within [effective_from, effective_to].
    Unresolved rows get unknown_key (typically -1).
    """
    dim_df = spark.table(dim_cfg["table"])
    sk     = dim_cfg["surrogate_key"]
    nk_s   = dim_cfg["natural_key_source"]
    nk_d   = dim_cfg["natural_key_dimension"]
    eff_f  = dim_cfg["effective_from"]
    eff_t  = dim_cfg["effective_to"]
    unk    = dim_cfg["unknown_key"]

    joined = fact_df.join(
        dim_df.select(F.col(nk_d), F.col(sk), F.col(eff_f), F.col(eff_t)),
        (fact_df[nk_s] == dim_df[nk_d]) &
        (fact_df[date_col] >= dim_df[eff_f]) &
        (fact_df[date_col] <= dim_df[eff_t]),
        "left",
    )
    return joined.withColumn(sk, F.coalesce(F.col(sk), F.lit(unk).cast("long")))


def _build_bridge_entry(fact_df, dim_cfg: dict, tx_id_col: str,
                        tx_date_col: str, tgt_table: str, batch_id: str):
    """Return a DataFrame of PENDING bridge rows for unresolved FKs."""
    sk  = dim_cfg["surrogate_key"]
    nk  = dim_cfg["natural_key_source"]
    unk = dim_cfg["unknown_key"]
    return (fact_df.filter(F.col(sk) == unk)
        .select(
            F.sha2(F.concat_ws("|", "transaction_hash",
                               F.lit(dim_cfg["name"]), F.col(nk)), 256).alias("resolution_hash"),
            "transaction_hash",
            F.col(tx_id_col).alias("transaction_id"),
            F.lit(dim_cfg["name"]).alias("dimension_name"),
            F.col(nk).cast("string").alias("natural_key"),
            F.col(tx_date_col).alias("transaction_date"),
            F.lit(sk).alias("fact_key_column"),
            F.lit(tgt_table).alias("fact_table_name"),
            F.lit(batch_id).alias("batch_id"),
            F.lit("PENDING").alias("resolution_status"),
            F.lit(None).cast("long").alias("resolved_surrogate_key"),
            F.current_timestamp().alias("first_seen_timestamp"),
            F.lit(None).cast("timestamp").alias("resolved_timestamp"),
        ))


def load_fact(cfg: dict, batch_id: str) -> dict:
    src_table    = cfg["source_table"]
    tgt_table    = cfg["target_table"]
    bridge_table = cfg["late_bridge_table"]
    tx_id_col    = cfg["transaction_id_column"]
    tx_date_col  = cfg["transaction_date_column"]
    hash_cols    = cfg["transaction_hash_columns"]
    dim_configs  = cfg["dimensions"]
    measures     = cfg["measures"]
    degen_cols   = cfg["degenerate_columns"]

    src = spark.table(src_table).filter(F.col("batch_id") == batch_id)
    n_src = src.count()
    if n_src == 0:
        msg = f"No rows for batch_id='{batch_id}' in {src_table}. Nothing to do."
        print(f"  ⚠ {msg}")
        return {"object_type": "fact", "target_table": tgt_table,
                "rows_in_batch": 0, "status": "SKIPPED", "reason": msg}

    print(f"  Source rows in batch: {n_src}")

    # Transaction hash: deterministic surrogate for idempotent upserts
    fact = src.withColumn(
        "transaction_hash",
        F.sha2(
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]),
            256,
        ),
    )

    # Temporal join to each dimension
    for dim_cfg in dim_configs:
        fact = _temporal_join(fact, dim_cfg, tx_date_col)

    # Flag rows with at least one unresolved FK
    unresolved_expr = F.lit(False)
    for dim_cfg in dim_configs:
        unresolved_expr = unresolved_expr | (F.col(dim_cfg["surrogate_key"]) == dim_cfg["unknown_key"])

    sk_cols = [d["surrogate_key"] for d in dim_configs]
    fact = (fact
        .withColumn("date_key",         F.date_format(F.col(tx_date_col), "yyyyMMdd").cast("int"))
        .withColumn("needs_resolution", unresolved_expr)
        .withColumn("fact_batch_id",    F.lit(batch_id))
        .withColumn("fact_loaded_at",   F.current_timestamp()))

    # Select final column set (deduplicate to handle join duplicates)
    selected = (
        ["transaction_hash", tx_id_col, "date_key"]
        + sk_cols + measures + degen_cols
        + ["needs_resolution", "fact_batch_id", "fact_loaded_at"]
    )
    fact_out = fact.select(*list(dict.fromkeys(selected)))

    # Upsert into Gold fact table on transaction_hash
    if spark.catalog.tableExists(tgt_table):
        DeltaTable.forName(spark, tgt_table).alias("t").merge(
            fact_out.alias("s"),
            "t.transaction_hash = s.transaction_hash",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        fact_out.write.format("delta").mode("overwrite").saveAsTable(tgt_table)

    total      = spark.table(tgt_table).count()
    unresolved = spark.table(tgt_table).filter("needs_resolution = true").count()
    print(f"  ✓ {tgt_table}: {total} total rows | {unresolved} with unresolved FK(s)")

    # Build and merge late-arriving bridge entries
    bridge_frames = [
        _build_bridge_entry(fact, d, tx_id_col, tx_date_col, tgt_table, batch_id)
        for d in dim_configs
    ]
    bridge = reduce(lambda a, b: a.unionByName(b), bridge_frames)
    n_bridge = bridge.count()
    if n_bridge > 0:
        if spark.catalog.tableExists(bridge_table):
            DeltaTable.forName(spark, bridge_table).alias("t").merge(
                bridge.alias("s"),
                "t.resolution_hash = s.resolution_hash",
            ).whenNotMatchedInsertAll().execute()
        else:
            bridge.write.format("delta").mode("overwrite").saveAsTable(bridge_table)
        bridge_total = spark.table(bridge_table).count()
        print(f"  ↳ Bridge {bridge_table}: {bridge_total} pending resolutions (cumulative)")

    return {"object_type": "fact", "target_table": tgt_table,
            "rows_total": total, "rows_unresolved": unresolved, "status": "OK"}

# COMMAND ----------

# MAGIC %md ## 3  Route & Execute

# COMMAND ----------

print(f"\n{'─'*60}")
print(f"Routing  object_type='{object_type}'  scd_type={scd_type}")
print(f"{'─'*60}")

if object_type == "dimension" and scd_type == 2:
    result = load_scd2_dimension(tf_cfg, batch_id)

elif object_type == "dimension" and not scd_type:
    result = load_date_dimension(tf_cfg)

elif object_type == "fact":
    result = load_fact(tf_cfg, batch_id)

else:
    raise ValueError(
        f"Cannot route: object_type='{object_type}' scd_type='{scd_type}'. "
        "Expected: (dimension + scd_type=2), (dimension, no scd_type), or (fact)."
    )

# Apply column masks declared in the TF YAML security block (Unity Catalog only)
_apply_column_masks(tf_cfg.get("target_table", ""), cfg.get("security"))

# Run Gold-layer DQ checks declared in the TF YAML dq block
_dq_rules   = (cfg.get("dq") or {}).get("rules") or []
_dq_results = _run_all_tf_dq(_dq_rules, tf_cfg.get("target_table", ""))
_dq_failed  = [r for r in _dq_results if r["status"] == "FAILED" and r["severity"] == "error"]
if _dq_failed:
    print(f"\n  [!] {len(_dq_failed)} error-severity DQ check(s) FAILED — see audit.dq_check_log")

# COMMAND ----------

# MAGIC %md ## 4  Preview Result

# COMMAND ----------

display(spark.table(tf_cfg["target_table"]))

# For SCD2 dims show full history sorted by natural key and effective date
if object_type == "dimension" and scd_type == 2:
    nk = tf_cfg["natural_key"]
    print(f"\nSCD2 history (all rows, ordered by natural key + effective_from):")
    display(
        spark.table(tf_cfg["target_table"])
        .orderBy(nk, "effective_from")
    )

# COMMAND ----------

_audit_log(
    result.get("status", "UNKNOWN"),
    source_object=tf_cfg.get("source_table", ""),
    target_object=tf_cfg.get("target_table", ""),
    rows_read=result.get("rows_in_batch") or result.get("rows"),
    rows_written=result.get("rows_total") or result.get("rows_inserted") or result.get("rows"),
)
print(f"\nResult: {json.dumps(result, indent=2)}")
dbutils.notebook.exit(json.dumps(result))
