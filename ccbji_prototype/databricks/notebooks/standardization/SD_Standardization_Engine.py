# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # SD Standardization Engine
# MAGIC **Generic utility notebook** – fully driven by the SD YAML config.
# MAGIC To process any new source, upload its SD YAML to the Volume/DBFS and point `sd_yml_path` at it.
# MAGIC No code changes required.
# MAGIC
# MAGIC | Widget | Description | Example |
# MAGIC |---|---|---|
# MAGIC | `sd_yml_path` | Full path to SD YAML on Volume or DBFS | `/Volumes/ccbji_dev/configs/sd/sd_sales_transactions.yaml` |
# MAGIC | `batch_id` | Unique run ID (ADF `pipeline().RunId`) | `BATCH_20260316_001` |
# MAGIC | `catalog_name` | UC catalog name. **Leave blank for CE** (uses default hive_metastore) | `ccbji_dev` |
# MAGIC | `bronze_schema` | Bronze schema/database | `ccbji_bronze` |
# MAGIC | `silver_schema` | Silver schema/database | `ccbji_silver` |
# MAGIC | `datalake_name` | ADLS Gen2 account name (token substitution) | `stccbjidev` |
# MAGIC | `file_filter` | Glob sub-pattern to restrict which file in the landing folder is read (blank = read all) | `sales_d1.csv` |

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("sd_yml_path",   "/dbfs/FileStore/ccbji/configs/sd/sd_sales_transactions.yaml", "SD YAML path on Volume/DBFS")
dbutils.widgets.text("batch_id",      "",                                                              "Batch ID (blank = auto-generated)")
dbutils.widgets.text("catalog_name",  "",                                                              "Catalog name (blank for CE)")
dbutils.widgets.text("bronze_schema", "ccbji_bronze",                                                  "Bronze schema")
dbutils.widgets.text("silver_schema", "ccbji_silver",                                                  "Silver schema")
dbutils.widgets.text("datalake_name", "stccbjidev",                                                    "ADLS account name (token)")
dbutils.widgets.text("file_filter",   "",                                                              "File name glob filter (optional)")

# COMMAND ----------

# MAGIC %md ## 0  Initialise

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from datetime import datetime
import yaml, json, uuid as _uuid

sd_yml_path   = dbutils.widgets.get("sd_yml_path")
batch_id      = dbutils.widgets.get("batch_id").strip() or f"BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}"
catalog_name  = dbutils.widgets.get("catalog_name").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
datalake_name = dbutils.widgets.get("datalake_name").strip()
file_filter   = dbutils.widgets.get("file_filter").strip()

print(f"YAML       : {sd_yml_path}")
print(f"Batch ID   : {batch_id}")
print(f"Catalog    : '{catalog_name}' (blank = CE 2-part table names)")
print(f"Bronze     : {bronze_schema} | Silver: {silver_schema}")
print(f"File filter: '{file_filter}' (blank = load entire folder)")

_RUN_ID  = str(_uuid.uuid4())
_STARTED = datetime.utcnow()


def _audit_log(status, rows_read=None, rows_written=None,
               rows_rejected=0, error_msg=None):
    """Append one run record to audit.pipeline_run_log. Never raises – audit failure must not block ETL."""
    try:
        pfx  = f"{catalog_name}." if catalog_name else ""
        _id  = globals().get("identifier", {})
        now  = datetime.utcnow()
        row  = [(_RUN_ID, "SD_Standardization_Engine", batch_id, "silver",
                 _id.get("bronze_table", ""), _id.get("silver_table", ""), status,
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


def _dq_log(check_name, target_table, check_status,
            expected=None, actual=None, column_name=None, row_count=None):
    """Append one DQ check result to audit.dq_check_log. Never raises."""
    try:
        pfx = f"{catalog_name}." if catalog_name else ""
        row = [(str(_uuid.uuid4()), _RUN_ID, batch_id,
                check_name, target_table, column_name, check_status,
                str(expected) if expected is not None else None,
                str(actual)   if actual   is not None else None,
                int(row_count) if row_count is not None else None,
                datetime.utcnow())]
        _schema = T.StructType([
            T.StructField("check_id",       T.StringType(),    False),
            T.StructField("run_id",         T.StringType(),    False),
            T.StructField("batch_id",       T.StringType(),    True),
            T.StructField("check_name",     T.StringType(),    False),
            T.StructField("target_table",   T.StringType(),    False),
            T.StructField("column_name",    T.StringType(),    True),
            T.StructField("check_status",   T.StringType(),    False),
            T.StructField("expected_value", T.StringType(),    True),
            T.StructField("actual_value",   T.StringType(),    True),
            T.StructField("row_count",      T.LongType(),      True),
            T.StructField("checked_at",     T.TimestampType(), False),
        ])
        (spark.createDataFrame(row, _schema)
             .write.format("delta").mode("append")
             .saveAsTable(f"{pfx}audit.dq_check_log"))
    except Exception as _e:
        print(f"  [dq_log] write skipped: {_e}")


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

# COMMAND ----------

# MAGIC %md ## 1  Read & Resolve YAML Config

# COMMAND ----------

def _read_yaml(path: str) -> dict:
    """
    Read YAML from:
      - Databricks Volume : /Volumes/catalog/schema/volume/file.yaml
      - DBFS via /dbfs/   : /dbfs/FileStore/.../file.yaml
      - DBFS spark path   : dbfs:/FileStore/.../file.yaml  (auto-converted)
    """
    local = path.replace("dbfs:/", "/dbfs/")
    with open(local, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _apply_tokens(value, tokens: dict):
    """
    Recursively replace {token} placeholders.
    If catalog_name is blank, '{catalog_name}.' is removed so table refs
    become 2-part (schema.table) instead of .schema.table.
    """
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
    "bronze_schema": bronze_schema,
    "silver_schema": silver_schema,
    "datalake_name": datalake_name,
}
cfg = _apply_tokens(_read_yaml(sd_yml_path), tokens)

identifier = cfg["dataset"]["identifier"]
source_cfg  = cfg["dataset"]["source"]
target_cfg  = cfg["dataset"]["target"]

# If a file_filter is provided, append it to the source path so only that file is read
src_path = identifier["ingestion_src_path"]
if file_filter:
    src_path = src_path.rstrip("/") + "/" + file_filter

print(f"\nDataset      : {identifier['datasetname']}")
print(f"Source system: {identifier.get('sourcesystem', 'N/A')}")
print(f"Source path  : {src_path}")
print(f"File format  : {identifier['fileformat']}")
print(f"Bronze table : {identifier['bronze_table']}")
print(f"Silver table : {identifier['silver_table']}")

# COMMAND ----------

# MAGIC %md ## 2  Read Source (Landing)

# COMMAND ----------

TYPE_MAP = {
    "StringType":    T.StringType(),
    "IntegerType":   T.IntegerType(),
    "LongType":      T.LongType(),
    "DoubleType":    T.DoubleType(),
    "FloatType":     T.FloatType(),
    "DateType":      T.DateType(),
    "TimestampType": T.TimestampType(),
    "BooleanType":   T.BooleanType(),
}

def _parse_dtype(name: str) -> T.DataType:
    if name.startswith("DecimalType("):
        inner = name[len("DecimalType("):-1]
        precision, scale = inner.split(",")
        return T.DecimalType(int(precision.strip()), int(scale.strip()))
    if name not in TYPE_MAP:
        raise ValueError(f"Unsupported dtype '{name}' in YAML. "
                         f"Supported: {list(TYPE_MAP)} + DecimalType(p,s)")
    return TYPE_MAP[name]


def _build_schema(columns: list) -> T.StructType:
    return T.StructType([
        T.StructField(c["name"], _parse_dtype(c["dtype"]), True)
        for c in columns
    ])


schema = _build_schema(source_cfg["columns"])
reader = spark.read.format(identifier["fileformat"]).schema(schema)
for k, v in source_cfg.get("options", {}).items():
    reader = reader.option(k, str(v))  # all options passed as strings

source_df    = reader.load(src_path)
source_count = source_df.count()

print(f"Rows loaded from landing: {source_count}")
display(source_df)

# COMMAND ----------

# MAGIC %md ## 3  Bronze – Raw Snapshot (overwrite per batch)
# MAGIC Schema is enforced at read time. Bronze always holds exactly one batch (latest).

# COMMAND ----------

bronze_df = (source_df
    .withColumn("batch_id",       F.lit(batch_id))
    .withColumn("processed_date", F.current_timestamp()))

bronze_mode = identifier.get("bronze_mode", "overwrite")
(bronze_df.write
    .format("delta")
    .mode(bronze_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(identifier["bronze_table"]))

bronze_count = spark.table(identifier["bronze_table"]).count()
print(f"Bronze  {identifier['bronze_table']}")
print(f"  mode = {bronze_mode} | rows = {bronze_count}")
_apply_column_masks(identifier["bronze_table"], cfg["dataset"].get("security"))

# COMMAND ----------

# MAGIC %md ## 4  Silver – Standardised, Append-Only
# MAGIC Transforms are applied per column as declared in the SD YAML `target.columns` section.
# MAGIC Supported transforms: `trim`, `upper`, `lower`.

# COMMAND ----------

def _apply_standardization(df, columns: list):
    """Apply per-column transforms from SD YAML target.columns."""
    result = df
    for item in columns:
        src  = item["name"]
        tgt  = item.get("standardname", src)
        expr = F.col(src)
        for transform in item.get("transforms", []):
            if transform == "trim":
                expr = F.trim(expr)
            elif transform == "upper":
                expr = F.upper(expr)
            elif transform == "lower":
                expr = F.lower(expr)
            else:
                raise ValueError(
                    f"Unknown transform '{transform}' for column '{src}'. "
                    "Supported: trim, upper, lower"
                )
        result = result.withColumn(tgt, expr)
        if tgt != src:
            result = result.drop(src)
    return result


silver_df   = _apply_standardization(bronze_df, target_cfg["columns"])
silver_mode = identifier.get("silver_mode", "append")

(silver_df.write
    .format("delta")
    .mode(silver_mode)
    .option("mergeSchema", "true")
    .saveAsTable(identifier["silver_table"]))

silver_count = (spark.table(identifier["silver_table"])
    .filter(F.col("batch_id") == batch_id)
    .count())

print(f"Silver  {identifier['silver_table']}")
print(f"  mode = {silver_mode} | rows this batch = {silver_count}")
_apply_column_masks(identifier["silver_table"], cfg["dataset"].get("security"))

display(silver_df)

# COMMAND ----------

# MAGIC %md ## 5  Data Quality Checks
# MAGIC Rules are declared in the SD YAML `dataset.dq.rules` block.
# MAGIC Supported checks: `row_count` · `not_null` · `not_negative` · `allowed_values` · `regex`
# MAGIC `severity: error`   → failing rows routed to error table + logged FAILED
# MAGIC `severity: warning` → logged as WARNING, rows kept in Silver

# COMMAND ----------

_DQ_RULES = (cfg["dataset"].get("dq") or {}).get("rules") or [
    {"check": "row_count", "severity": "error"},
    {"check": "not_null",  "column": target_cfg["columns"][0]["name"], "severity": "error"},
]


def _run_all_dq(rules, df, s_count, b_count, src_count, identifier):
    """Execute all DQ rules from the YAML. Returns (result_list, error_frame_list)."""
    results, error_frames = [], []

    for rule in rules:
        check    = rule["check"]
        col      = rule.get("column")
        severity = rule.get("severity", "warning")

        if check == "row_count":
            for tbl, actual, label in [
                (identifier["bronze_table"], b_count, "row_count_bronze_vs_source"),
                (identifier["silver_table"], s_count, "row_count_silver_vs_source"),
            ]:
                ok = actual == src_count
                results.append({"check_name": label, "target": tbl,
                                 "status": "PASSED" if ok else "FAILED",
                                 "expected": str(src_count), "actual": str(actual),
                                 "col": None, "n": 0})

        elif check == "not_null":
            bad   = df.filter(F.col(col).isNull())
            n_bad = bad.count()
            results.append({"check_name": f"not_null.{col}",
                             "target": identifier["silver_table"],
                             "status": "PASSED" if n_bad == 0 else "FAILED",
                             "expected": "0", "actual": str(n_bad), "col": col, "n": n_bad})
            if n_bad > 0 and severity == "error":
                error_frames.append(bad.withColumn("error_reason",
                                    F.lit(f"NULL in column '{col}'")))

        elif check == "not_negative":
            try:
                bad   = df.filter(F.col(col) < 0)
                n_bad = bad.count()
                results.append({"check_name": f"not_negative.{col}",
                                 "target": identifier["silver_table"],
                                 "status": "PASSED" if n_bad == 0 else "WARNING",
                                 "expected": "0", "actual": str(n_bad), "col": col, "n": n_bad})
                if n_bad > 0 and severity == "error":
                    error_frames.append(bad.withColumn("error_reason",
                                        F.lit(f"Negative value in column '{col}'")))
            except Exception:
                pass

        elif check == "allowed_values":
            allowed = rule["values"]
            bad     = df.filter(~F.col(col).isin(allowed) & F.col(col).isNotNull())
            n_bad   = bad.count()
            results.append({"check_name": f"allowed_values.{col}",
                             "target": identifier["silver_table"],
                             "status": "PASSED" if n_bad == 0 else ("FAILED" if severity == "error" else "WARNING"),
                             "expected": str(allowed), "actual": f"{n_bad} violations",
                             "col": col, "n": n_bad})
            if n_bad > 0 and severity == "error":
                error_frames.append(bad.withColumn("error_reason",
                                    F.lit(f"Value not in allowed list for '{col}': expected {allowed}")))

        elif check == "regex":
            pattern = rule["pattern"]
            bad     = df.filter(F.col(col).isNotNull() & ~F.col(col).rlike(pattern))
            n_bad   = bad.count()
            results.append({"check_name": f"regex.{col}",
                             "target": identifier["silver_table"],
                             "status": "PASSED" if n_bad == 0 else ("FAILED" if severity == "error" else "WARNING"),
                             "expected": f"matches {pattern}", "actual": f"{n_bad} violations",
                             "col": col, "n": n_bad})
            if n_bad > 0 and severity == "error":
                error_frames.append(bad.withColumn("error_reason",
                                    F.lit(f"Regex mismatch in '{col}': expected {pattern}")))

    return results, error_frames


_dq_results, _error_frames = _run_all_dq(
    _DQ_RULES, silver_df, silver_count, bronze_count, source_count, identifier
)

# ── Print DQ report ───────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print(f"DQ REPORT  |  {identifier['datasetname']}  |  batch_id={batch_id}")
print(f"{'='*65}")
for r in _dq_results:
    icon = "✓" if r["status"] == "PASSED" else ("⚠" if r["status"] == "WARNING" else "✗")
    col_label = f"[{r['col']}]" if r["col"] else ""
    print(f"  {icon} {r['status']:<8}  {r['check_name']:<40} {col_label}")
    if r["status"] != "PASSED":
        print(f"             expected={r['expected']}  actual={r['actual']}")

# ── Write error rows to error table ──────────────────────────────────────────
_n_errors = 0
if _error_frames:
    from functools import reduce as _reduce
    _error_tbl  = identifier.get("badrecordtable", identifier["silver_table"] + "_error")
    _all_errors = _reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), _error_frames)
    _n_errors   = _all_errors.count()
    (_all_errors
        .withColumn("error_timestamp", F.current_timestamp())
        .write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(_error_tbl))
    print(f"\n  ✗  {_n_errors} error rows → {_error_tbl}")
else:
    print(f"\n  ✓  No error rows")

# ── Persist all DQ results to audit.dq_check_log ─────────────────────────────
for r in _dq_results:
    _dq_log(r["check_name"], r["target"], r["status"],
            expected=r["expected"], actual=r["actual"],
            column_name=r["col"], row_count=r["n"])

# COMMAND ----------

# Return JSON exit value (ADF reads this as notebook output)
_any_failed = any(r["status"] == "FAILED" for r in _dq_results)
result = {
    "dataset":       identifier["datasetname"],
    "batch_id":      batch_id,
    "source_rows":   source_count,
    "bronze_rows":   bronze_count,
    "silver_rows":   silver_count,
    "error_rows":    _n_errors,
    "bronze_table":  identifier["bronze_table"],
    "silver_table":  identifier["silver_table"],
    "dq_checks":     len(_dq_results),
    "status":        "DQ_FAILED" if _any_failed else "OK",
}
_audit_log(
    result["status"],
    rows_read=source_count, rows_written=silver_count, rows_rejected=_n_errors,
)
print(f"\nNotebook exit value: {json.dumps(result, indent=2)}")
dbutils.notebook.exit(json.dumps(result))
