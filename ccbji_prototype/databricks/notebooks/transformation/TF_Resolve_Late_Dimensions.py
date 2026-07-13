# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # TF Resolve Late-Arriving Dimensions
# MAGIC **Generic utility notebook** – driven by a dedicated Resolution YAML (`tf_resolve_*.yaml`).
# MAGIC
# MAGIC The resolution YAML is intentionally **separate** from the fact-load YAML so that:
# MAGIC - Resolution can be scheduled independently (e.g. nightly catch-up run)
# MAGIC - New dimensions can be added to resolution without touching the fact-load config
# MAGIC - A future fact table gets its own `tf_resolve_<fact>.yaml` with no shared config
# MAGIC
# MAGIC Resolution YAML structure (`meta.object_type = resolution`):
# MAGIC ```yaml
# MAGIC transformation:
# MAGIC   fact_table: ...
# MAGIC   bridge_table: ...
# MAGIC   dimensions:
# MAGIC     - name, table, natural_key_dimension, surrogate_key, effective_from, effective_to
# MAGIC ```
# MAGIC
# MAGIC Run **after** all TF_Gold_Load_Engine notebooks for the current batch.
# MAGIC Safe to run multiple times – RESOLVED entries are skipped.
# MAGIC
# MAGIC | Widget | Description |
# MAGIC |---|---|
# MAGIC | `tf_resolve_yml_path` | Path to Resolution YAML on Volume/DBFS |
# MAGIC | `catalog_name` | UC catalog name. **Leave blank for CE** |
# MAGIC | `silver_schema` | Silver schema (token substitution only) |
# MAGIC | `gold_schema` | Gold schema/database |

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("tf_resolve_yml_path", "/dbfs/FileStore/ccbji/configs/tf/tf_resolve_fact_sales.yaml", "Resolution YAML path on Volume/DBFS")
dbutils.widgets.text("catalog_name",        "",                                                              "Catalog name (blank for CE)")
dbutils.widgets.text("silver_schema",       "ccbji_silver",                                                 "Silver schema (token only)")
dbutils.widgets.text("gold_schema",         "ccbji_gold",                                                   "Gold schema")

# COMMAND ----------

# MAGIC %md ## 0  Initialise

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable
from datetime import datetime
import yaml, json, uuid as _uuid

tf_resolve_yml_path = dbutils.widgets.get("tf_resolve_yml_path")
catalog_name        = dbutils.widgets.get("catalog_name").strip()
silver_schema       = dbutils.widgets.get("silver_schema").strip()
gold_schema         = dbutils.widgets.get("gold_schema").strip()

print(f"Resolution YAML : {tf_resolve_yml_path}")
print(f"Catalog         : '{catalog_name}'")
print(f"Gold schema     : {gold_schema}")

_RUN_ID  = str(_uuid.uuid4())
_STARTED = datetime.utcnow()


def _audit_log(status, rows_read=None, rows_written=None, error_msg=None):
    """Append one run record to audit.pipeline_run_log. Never raises."""
    try:
        pfx  = f"{catalog_name}." if catalog_name else ""
        _tf  = globals().get("tf_cfg", {})
        now  = datetime.utcnow()
        row  = [(_RUN_ID, "TF_Resolve_Late_Dimensions", None, "gold",
                 _tf.get("bridge_table", ""), _tf.get("fact_table", ""), status,
                 int(rows_read)    if rows_read    is not None else None,
                 int(rows_written) if rows_written is not None else None,
                 0,
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

# COMMAND ----------

# MAGIC %md ## 1  Read & Resolve YAML

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
cfg    = _apply_tokens(_read_yaml(tf_resolve_yml_path), tokens)
meta   = cfg["meta"]
tf_cfg = cfg["transformation"]

assert meta["object_type"] == "resolution", (
    f"Expected meta.object_type='resolution', got '{meta['object_type']}'. "
    "Point tf_resolve_yml_path at a tf_resolve_*.yaml file, not a tf_fact_*.yaml."
)

fact_table   = tf_cfg["fact_table"]
bridge_table = tf_cfg["bridge_table"]
dim_configs  = tf_cfg["dimensions"]
sk_cols      = [d["surrogate_key"] for d in dim_configs]

print(f"Fact table      : {fact_table}")
print(f"Bridge table    : {bridge_table}")
print(f"Dimensions      : {[d['name'] for d in dim_configs]}")
print(f"Surrogate keys  : {sk_cols}")

# COMMAND ----------

# MAGIC %md ## 2  Bridge Status – Before Resolution

# COMMAND ----------

print("Bridge status BEFORE resolution:\n")
(spark.table(bridge_table)
    .groupBy("dimension_name", "resolution_status")
    .agg(F.count("*").alias("count"))
    .orderBy("dimension_name", "resolution_status")
    .show())

display(
    spark.table(bridge_table)
    .select("dimension_name", "natural_key", "transaction_id",
            "transaction_date", "resolution_status", "first_seen_timestamp")
    .orderBy("dimension_name", "natural_key")
)

# COMMAND ----------

# MAGIC %md ## 3  Resolution Logic

# COMMAND ----------

def resolve_dimension(dim_cfg: dict, fact_table: str, bridge_table: str) -> dict:
    """
    For a single dimension:
    1. Fetch PENDING bridge entries for this dimension.
    2. Temporal-join to the dimension table (transaction_date within [effective_from, effective_to]).
    3. Patch the fact table FK where the join succeeded.
    4. Mark bridge entries RESOLVED.
    Returns a summary dict.
    """
    dim_name  = dim_cfg["name"]
    dim_table = dim_cfg["table"]
    nk_dim    = dim_cfg["natural_key_dimension"]
    sk        = dim_cfg["surrogate_key"]
    eff_f     = dim_cfg["effective_from"]
    eff_t     = dim_cfg["effective_to"]

    pending = (spark.table(bridge_table)
        .filter(
            (F.col("dimension_name")    == dim_name) &
            (F.col("resolution_status") == "PENDING")
        ))
    n_pending = pending.count()

    if n_pending == 0:
        print(f"  [{dim_name}] No PENDING entries – skipping.")
        return {"dimension": dim_name, "pending": 0, "resolved": 0,
                "still_pending": 0, "status": "NOTHING_TO_DO"}

    print(f"  [{dim_name}] {n_pending} PENDING → attempting temporal join …")

    dim_df = spark.table(dim_table)

    resolved = (pending.alias("b")
        .join(
            dim_df.alias("d"),
            (F.col("b.natural_key")    == F.col(f"d.{nk_dim}")) &
            (F.col("b.transaction_date") >= F.col(f"d.{eff_f}")) &
            (F.col("b.transaction_date") <= F.col(f"d.{eff_t}")),
            "inner",
        )
        .select(
            "b.resolution_hash",
            "b.transaction_hash",
            F.col(f"d.{sk}").alias("resolved_key"),
        ))

    n_resolved    = resolved.count()
    n_still       = n_pending - n_resolved

    if n_resolved == 0:
        print(f"    → 0 resolved | {n_still} still unresolvable "
              "(dimension row absent or transaction_date outside effective range)")
        return {"dimension": dim_name, "pending": n_pending,
                "resolved": 0, "still_pending": n_still, "status": "UNRESOLVABLE"}

    # Patch fact FK (only rows still holding -1 for this key)
    DeltaTable.forName(spark, fact_table).alias("f").merge(
        resolved.alias("r"),
        "f.transaction_hash = r.transaction_hash",
    ).whenMatchedUpdate(
        condition=f"f.`{sk}` = -1",
        set={f"`{sk}`": "r.resolved_key"},
    ).execute()

    # Mark bridge entries RESOLVED
    DeltaTable.forName(spark, bridge_table).alias("b").merge(
        resolved.alias("r"),
        "b.resolution_hash = r.resolution_hash",
    ).whenMatchedUpdate(set={
        "resolved_surrogate_key": "r.resolved_key",
        "resolution_status":      "'RESOLVED'",
        "resolved_timestamp":     "current_timestamp()",
    }).execute()

    print(f"    → {n_resolved} resolved | {n_still} still unresolvable")
    return {"dimension": dim_name, "pending": n_pending,
            "resolved": n_resolved, "still_pending": n_still, "status": "OK"}

# COMMAND ----------

# MAGIC %md ## 4  Run Resolution for All Dimensions in YAML

# COMMAND ----------

results = []
for dim_cfg in dim_configs:
    r = resolve_dimension(dim_cfg, fact_table, bridge_table)
    results.append(r)

# Clear needs_resolution flag where ALL surrogate keys are now filled
clear_cond = " AND ".join([f"`{sk}` <> -1" for sk in sk_cols])
DeltaTable.forName(spark, fact_table).update(
    condition=f"needs_resolution = true AND {clear_cond}",
    set={"needs_resolution": "false"},
)
print(f"\n  needs_resolution cleared for rows where every FK is resolved.")

# COMMAND ----------

# MAGIC %md ## 5  Bridge Status – After Resolution

# COMMAND ----------

print("Bridge status AFTER resolution:\n")
(spark.table(bridge_table)
    .groupBy("dimension_name", "resolution_status")
    .agg(F.count("*").alias("count"))
    .orderBy("dimension_name", "resolution_status")
    .show())

display(
    spark.table(bridge_table)
    .select("dimension_name", "natural_key", "transaction_id",
            "resolution_status", "resolved_surrogate_key", "resolved_timestamp")
    .orderBy("dimension_name", "natural_key")
)

# COMMAND ----------

# MAGIC %md ## 6  Fact – Remaining Unresolved Rows

# COMMAND ----------

unresolved   = spark.table(fact_table).filter("needs_resolution = true")
n_unresolved = unresolved.count()

if n_unresolved == 0:
    print("✓ All fact rows have resolved dimension FKs.")
else:
    print(f"⚠  {n_unresolved} fact rows still have unresolved FKs:")
    display(unresolved.select(["transaction_hash"] + sk_cols + ["needs_resolution", "fact_batch_id"]))

# COMMAND ----------

summary = {
    "fact_table":                 fact_table,
    "bridge_table":               bridge_table,
    "resolution_results":         results,
    "fact_rows_still_unresolved": n_unresolved,
    "status": "OK" if n_unresolved == 0 else "PARTIAL_RESOLUTION",
}
_audit_log(
    summary["status"],
    rows_read=sum(r.get("pending", 0) for r in results),
    rows_written=sum(r.get("resolved", 0) for r in results),
)
print(f"\nSummary:\n{json.dumps(summary, indent=2)}")
dbutils.notebook.exit(json.dumps(summary))
