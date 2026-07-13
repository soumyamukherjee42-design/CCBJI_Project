"""
Unit tests for scripts/validate_yamls.py.

All tests use in-memory dicts passed directly to the validator helpers,
so no real YAML files are needed and no Databricks/Spark context is required.
"""

import sys
import tempfile
import textwrap
from pathlib import Path

import pytest
import yaml

# Make scripts/ importable regardless of how pytest is invoked
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import validate_yamls as vy


@pytest.fixture(autouse=True)
def clear_errors():
    """Reset the module-level ERRORS list before every test."""
    vy.ERRORS.clear()
    yield
    vy.ERRORS.clear()


# ─── helpers ──────────────────────────────────────────────────────────────────

def _yaml_file(content: str) -> str:
    """Write YAML content to a temp file and return the path."""
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    )
    tmp.write(textwrap.dedent(content))
    tmp.flush()
    return tmp.name


# ─── _require ─────────────────────────────────────────────────────────────────

class TestRequire:
    def test_present_flat_key(self):
        assert vy._require({"a": 1}, ["a"], "f.yaml") is True
        assert vy.ERRORS == []

    def test_present_dotted_key(self):
        doc = {"meta": {"object_type": "fact"}}
        assert vy._require(doc, ["meta.object_type"], "f.yaml") is True

    def test_missing_top_level(self):
        assert vy._require({}, ["missing"], "f.yaml") is False
        assert any("missing" in e for e in vy.ERRORS)

    def test_missing_nested(self):
        doc = {"meta": {}}
        assert vy._require(doc, ["meta.object_type"], "f.yaml") is False
        assert any("meta.object_type" in e for e in vy.ERRORS)

    def test_partial_missing(self):
        doc = {"a": 1}
        result = vy._require(doc, ["a", "b"], "f.yaml")
        assert result is False
        assert len(vy.ERRORS) == 1   # only "b" missing


# ─── validate_sd ──────────────────────────────────────────────────────────────

VALID_SD = """\
dataset:
  identifier:
    datasetname:        sales_transactions
    ingestion_src_path: /mnt/landing/sales
    bronze_table:       bronze.sales_transactions
    silver_table:       silver.sales_transactions
    fileformat:         csv
  source:
    columns:
      - name: transaction_id
        dtype: StringType
      - name: amount
        dtype: DecimalType(18,2)
  target:
    columns:
      - name: transaction_id
        transforms: [trim]
      - name: amount
"""


class TestValidateSd:
    def test_valid_doc(self):
        path = _yaml_file(VALID_SD)
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_missing_required_key(self):
        doc = yaml.safe_load(VALID_SD)
        del doc["dataset"]["identifier"]["fileformat"]
        path = _yaml_file(yaml.dump(doc))
        vy.validate_sd(path)
        assert any("fileformat" in e for e in vy.ERRORS)

    def test_source_column_missing_dtype(self):
        doc = yaml.safe_load(VALID_SD)
        doc["dataset"]["source"]["columns"][0].pop("dtype")
        path = _yaml_file(yaml.dump(doc))
        vy.validate_sd(path)
        assert any("dtype" in e for e in vy.ERRORS)

    def test_target_column_missing_name(self):
        doc = yaml.safe_load(VALID_SD)
        doc["dataset"]["target"]["columns"][0].pop("name")
        path = _yaml_file(yaml.dump(doc))
        vy.validate_sd(path)
        assert any("missing 'name'" in e for e in vy.ERRORS)

    def test_unknown_transform(self):
        doc = yaml.safe_load(VALID_SD)
        doc["dataset"]["target"]["columns"][0]["transforms"] = ["upcase"]
        path = _yaml_file(yaml.dump(doc))
        vy.validate_sd(path)
        assert any("upcase" in e for e in vy.ERRORS)

    def test_all_allowed_transforms(self):
        doc = yaml.safe_load(VALID_SD)
        doc["dataset"]["target"]["columns"][0]["transforms"] = ["trim", "upper", "lower"]
        path = _yaml_file(yaml.dump(doc))
        vy.validate_sd(path)
        assert vy.ERRORS == []


# ─── validate_tf ──────────────────────────────────────────────────────────────

VALID_SCD2 = """\
version: 1.0.0
meta:
  object_type: dimension
  scd_type: 2
transformation:
  source_table:          silver.customer_master
  target_table:          gold.dim_customer
  natural_key:           customer_id
  surrogate_key:         customer_key
  tracked_columns:       [customer_name, region]
  effective_from_column: effective_from
"""

VALID_DATE = """\
version: 1.0.0
meta:
  object_type: dimension
transformation:
  target_table: gold.dim_date
  start_date:   2024-01-01
  end_date:     2030-12-31
"""

VALID_FACT = """\
version: 1.0.0
meta:
  object_type: fact
transformation:
  source_table:            silver.sales_transactions
  target_table:            gold.fact_sales
  late_bridge_table:       gold.late_arriving_dimension_bridge
  transaction_id_column:   transaction_id
  transaction_date_column: transaction_date
  transaction_hash_columns: [transaction_id, amount]
  dimensions:
    - name:                  customer
      table:                 gold.dim_customer
      natural_key_source:    customer_id
      natural_key_dimension: customer_id
      surrogate_key:         customer_key
      effective_from:        effective_from
      effective_to:          effective_to
      unknown_key:           -1
  measures:       [amount, quantity]
  degenerate_columns: [order_line]
"""

VALID_RESOLVE = """\
version: 1.0.0
meta:
  object_type: resolution
transformation:
  fact_table:   gold.fact_sales
  bridge_table: gold.late_arriving_dimension_bridge
  dimensions:
    - name:                  customer
      table:                 gold.dim_customer
      natural_key_dimension: customer_id
      surrogate_key:         customer_key
      effective_from:        effective_from
      effective_to:          effective_to
"""


class TestValidateTf:
    def test_valid_scd2(self):
        vy.validate_tf(_yaml_file(VALID_SCD2))
        assert vy.ERRORS == []

    def test_valid_date_dimension(self):
        vy.validate_tf(_yaml_file(VALID_DATE))
        assert vy.ERRORS == []

    def test_valid_fact(self):
        vy.validate_tf(_yaml_file(VALID_FACT))
        assert vy.ERRORS == []

    def test_valid_resolution(self):
        vy.validate_tf(_yaml_file(VALID_RESOLVE))
        assert vy.ERRORS == []

    def test_scd2_missing_natural_key(self):
        doc = yaml.safe_load(VALID_SCD2)
        del doc["transformation"]["natural_key"]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("natural_key" in e for e in vy.ERRORS)

    def test_fact_dimension_entry_missing_key(self):
        doc = yaml.safe_load(VALID_FACT)
        del doc["transformation"]["dimensions"][0]["unknown_key"]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("unknown_key" in e for e in vy.ERRORS)

    def test_resolution_dimension_missing_key(self):
        doc = yaml.safe_load(VALID_RESOLVE)
        del doc["transformation"]["dimensions"][0]["surrogate_key"]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("surrogate_key" in e for e in vy.ERRORS)

    def test_unknown_object_type(self):
        doc = yaml.safe_load(VALID_SCD2)
        doc["meta"]["object_type"] = "staging"
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("unknown object_type" in e for e in vy.ERRORS)

    def test_missing_meta_key(self):
        doc = yaml.safe_load(VALID_SCD2)
        del doc["meta"]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("meta.object_type" in e for e in vy.ERRORS)


# ─── validate_trigger ─────────────────────────────────────────────────────────

VALID_STORAGE_EVENT = """\
version: 1.0.0
meta:
  trigger_id: trg_event_sales
trigger:
  name:     TRG_Sales_Landing
  type:     storage_event
  enabled:  true
  event:    Microsoft.Storage.BlobCreated
  notebook: SD_Standardization_Engine
  parameters:
    sd_yml_path: Standardization/src/config/sd_sales_transactions.yaml
"""

VALID_SCHEDULE = """\
version: 1.0.0
meta:
  trigger_id: trg_schedule_gold
trigger:
  name:       TRG_Gold_Daily
  type:       schedule
  enabled:    true
  timezone:   Asia/Tokyo
  start_time: '2026-03-17T03:00:00+09:00'
  sequence:
    - step: 1
      name: Refresh_DimDate
"""


class TestValidateTrigger:
    def test_valid_storage_event(self):
        vy.validate_trigger(_yaml_file(VALID_STORAGE_EVENT))
        assert vy.ERRORS == []

    def test_valid_schedule(self):
        vy.validate_trigger(_yaml_file(VALID_SCHEDULE))
        assert vy.ERRORS == []

    def test_schedule_with_steps(self):
        doc = yaml.safe_load(VALID_SCHEDULE)
        del doc["trigger"]["sequence"]
        doc["trigger"]["steps"] = [{"step": 1, "name": "load"}]
        vy.validate_trigger(_yaml_file(yaml.dump(doc)))
        assert vy.ERRORS == []

    def test_storage_event_missing_notebook(self):
        doc = yaml.safe_load(VALID_STORAGE_EVENT)
        del doc["trigger"]["notebook"]
        vy.validate_trigger(_yaml_file(yaml.dump(doc)))
        assert any("notebook" in e for e in vy.ERRORS)

    def test_schedule_missing_steps_and_sequence(self):
        doc = yaml.safe_load(VALID_SCHEDULE)
        del doc["trigger"]["sequence"]
        vy.validate_trigger(_yaml_file(yaml.dump(doc)))
        assert any("steps" in e or "sequence" in e for e in vy.ERRORS)

    def test_schedule_missing_timezone(self):
        doc = yaml.safe_load(VALID_SCHEDULE)
        del doc["trigger"]["timezone"]
        vy.validate_trigger(_yaml_file(yaml.dump(doc)))
        assert any("timezone" in e for e in vy.ERRORS)

    def test_unknown_trigger_type(self):
        doc = yaml.safe_load(VALID_STORAGE_EVENT)
        doc["trigger"]["type"] = "webhook"
        vy.validate_trigger(_yaml_file(yaml.dump(doc)))
        assert any("unknown trigger.type" in e for e in vy.ERRORS)

    def test_missing_trigger_id(self):
        doc = yaml.safe_load(VALID_STORAGE_EVENT)
        del doc["meta"]["trigger_id"]
        vy.validate_trigger(_yaml_file(yaml.dump(doc)))
        assert any("trigger_id" in e for e in vy.ERRORS)


# ─── validate_env ─────────────────────────────────────────────────────────────

VALID_ENV = """\
environment:        dev
catalog_name:       ccbji_dev
bronze_schema:      bronze
silver_schema:      silver
gold_schema:        gold
datalake_name:      stccbjidev
adf_name:           adf-ccbji-dev
adf_resource_group: rg-ccbji-dev
"""


class TestValidateEnv:
    def test_valid_env(self):
        vy.validate_env(_yaml_file(VALID_ENV))
        assert vy.ERRORS == []

    @pytest.mark.parametrize("key", [
        "environment", "catalog_name", "bronze_schema", "silver_schema",
        "gold_schema", "datalake_name", "adf_name", "adf_resource_group",
    ])
    def test_missing_required_key(self, key):
        doc = yaml.safe_load(VALID_ENV)
        del doc[key]
        vy.validate_env(_yaml_file(yaml.dump(doc)))
        assert any(key in e for e in vy.ERRORS)


# ─── SD DQ block ──────────────────────────────────────────────────────────────

class TestValidateSdDq:
    def _sd_with_dq(self, rules: list) -> str:
        doc = yaml.safe_load(VALID_SD)
        doc["dataset"]["dq"] = {"rules": rules}
        return _yaml_file(yaml.dump(doc))

    def test_valid_row_count_and_not_null(self):
        path = self._sd_with_dq([
            {"check": "row_count",  "severity": "error"},
            {"check": "not_null",   "column": "transaction_id", "severity": "error"},
        ])
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_valid_allowed_values(self):
        path = self._sd_with_dq([
            {"check": "allowed_values", "column": "status",
             "values": ["ACTIVE", "INACTIVE"], "severity": "warning"},
        ])
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_valid_regex(self):
        path = self._sd_with_dq([
            {"check": "regex", "column": "email", "pattern": r"^.+@.+$", "severity": "warning"},
        ])
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_row_count_needs_no_column(self):
        # row_count has no column requirement — must not raise
        path = self._sd_with_dq([{"check": "row_count", "severity": "error"}])
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_not_null_missing_column(self):
        path = self._sd_with_dq([{"check": "not_null", "severity": "error"}])
        vy.validate_sd(path)
        assert any("not_null" in e and "column" in e for e in vy.ERRORS)

    def test_not_negative_missing_column(self):
        path = self._sd_with_dq([{"check": "not_negative", "severity": "warning"}])
        vy.validate_sd(path)
        assert any("not_negative" in e and "column" in e for e in vy.ERRORS)

    def test_allowed_values_missing_values_list(self):
        path = self._sd_with_dq([{"check": "allowed_values", "column": "status", "severity": "warning"}])
        vy.validate_sd(path)
        assert any("values" in e for e in vy.ERRORS)

    def test_regex_missing_pattern(self):
        path = self._sd_with_dq([{"check": "regex", "column": "email", "severity": "warning"}])
        vy.validate_sd(path)
        assert any("pattern" in e for e in vy.ERRORS)

    def test_unknown_check_rejected(self):
        path = self._sd_with_dq([{"check": "freshness", "severity": "warning"}])
        vy.validate_sd(path)
        assert any("freshness" in e for e in vy.ERRORS)

    def test_missing_check_key(self):
        path = self._sd_with_dq([{"column": "id", "severity": "error"}])
        vy.validate_sd(path)
        assert any("missing required key 'check'" in e for e in vy.ERRORS)

    def test_invalid_severity_rejected(self):
        path = self._sd_with_dq([{"check": "row_count", "severity": "critical"}])
        vy.validate_sd(path)
        assert any("severity" in e and "critical" in e for e in vy.ERRORS)

    def test_valid_severity_values(self):
        for sev in ("error", "warning"):
            vy.ERRORS.clear()
            path = self._sd_with_dq([{"check": "row_count", "severity": sev}])
            vy.validate_sd(path)
            assert vy.ERRORS == [], f"severity='{sev}' should be valid"


# ─── SD Security block ────────────────────────────────────────────────────────

class TestValidateSdSecurity:
    def _sd_with_security(self, security: dict) -> str:
        doc = yaml.safe_load(VALID_SD)
        doc["dataset"]["security"] = security
        return _yaml_file(yaml.dump(doc))

    def test_valid_column_mask(self):
        path = self._sd_with_security({"column_masks": [
            {"column": "customer_name", "mask_function": "cat.sec.fn", "exempt_roles": ["role1"]}
        ]})
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_valid_landing_encryption(self):
        path = self._sd_with_security({"landing_encryption": {
            "type": "cmk", "key_vault_name": "kv-name", "key_name": "key1"
        }})
        vy.validate_sd(path)
        assert vy.ERRORS == []

    def test_column_mask_missing_column(self):
        path = self._sd_with_security({"column_masks": [
            {"mask_function": "fn", "exempt_roles": ["r"]}
        ]})
        vy.validate_sd(path)
        assert any("column" in e for e in vy.ERRORS)

    def test_column_mask_missing_mask_function(self):
        path = self._sd_with_security({"column_masks": [
            {"column": "name", "exempt_roles": ["r"]}
        ]})
        vy.validate_sd(path)
        assert any("mask_function" in e for e in vy.ERRORS)

    def test_column_mask_missing_exempt_roles(self):
        path = self._sd_with_security({"column_masks": [
            {"column": "name", "mask_function": "fn"}
        ]})
        vy.validate_sd(path)
        assert any("exempt_roles" in e for e in vy.ERRORS)

    def test_landing_encryption_missing_key_vault(self):
        path = self._sd_with_security({"landing_encryption": {"type": "cmk", "key_name": "k"}})
        vy.validate_sd(path)
        assert any("key_vault_name" in e for e in vy.ERRORS)

    def test_landing_encryption_missing_type(self):
        path = self._sd_with_security({"landing_encryption": {"key_vault_name": "kv", "key_name": "k"}})
        vy.validate_sd(path)
        assert any("type" in e for e in vy.ERRORS)


# ─── validate_tf — additional structural coverage ─────────────────────────────

class TestValidateTfStructural:
    @pytest.mark.parametrize("field", [
        "surrogate_key", "natural_key", "tracked_columns",
        "effective_from_column", "source_table",
    ])
    def test_scd2_missing_required_field(self, field):
        doc = yaml.safe_load(VALID_SCD2)
        del doc["transformation"][field]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any(field in e for e in vy.ERRORS)

    @pytest.mark.parametrize("field", ["start_date", "end_date"])
    def test_date_dim_missing_required_field(self, field):
        doc = yaml.safe_load(VALID_DATE)
        del doc["transformation"][field]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any(field in e for e in vy.ERRORS)

    @pytest.mark.parametrize("field", [
        "transaction_hash_columns", "measures", "degenerate_columns", "late_bridge_table",
    ])
    def test_fact_missing_required_field(self, field):
        doc = yaml.safe_load(VALID_FACT)
        del doc["transformation"][field]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any(field in e for e in vy.ERRORS)

    def test_resolution_missing_fact_table(self):
        doc = yaml.safe_load(VALID_RESOLVE)
        del doc["transformation"]["fact_table"]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("fact_table" in e for e in vy.ERRORS)

    def test_resolution_missing_bridge_table(self):
        doc = yaml.safe_load(VALID_RESOLVE)
        del doc["transformation"]["bridge_table"]
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("bridge_table" in e for e in vy.ERRORS)

    def test_fact_multiple_dimension_entries_validated(self):
        doc = yaml.safe_load(VALID_FACT)
        # second dim entry with a missing key
        doc["transformation"]["dimensions"].append({
            "name": "region", "table": "gold.dim_region",
            "natural_key_source": "region_id", "natural_key_dimension": "region_id",
            "surrogate_key": "region_key", "effective_from": "effective_from",
            # missing effective_to and unknown_key
        })
        vy.validate_tf(_yaml_file(yaml.dump(doc)))
        assert any("effective_to" in e or "unknown_key" in e for e in vy.ERRORS)


# ─── TF DQ block ──────────────────────────────────────────────────────────────

class TestValidateTfDq:
    def _tf_with_dq(self, base_yaml: str, rules: list) -> str:
        doc = yaml.safe_load(base_yaml)
        doc["dq"] = {"rules": rules}
        return _yaml_file(yaml.dump(doc))

    def test_valid_not_null_and_duplicate_key(self):
        path = self._tf_with_dq(VALID_SCD2, [
            {"check": "not_null",      "column": "customer_key",  "severity": "error"},
            {"check": "duplicate_key", "columns": ["customer_id", "effective_from"], "severity": "error"},
        ])
        vy.validate_tf(path)
        assert vy.ERRORS == []

    def test_valid_referential_integrity(self):
        path = self._tf_with_dq(VALID_FACT, [
            {"check": "referential_integrity", "column": "customer_key",
             "ref_table": "gold.dim_customer", "ref_column": "customer_key",
             "severity": "warning"},
        ])
        vy.validate_tf(path)
        assert vy.ERRORS == []

    def test_valid_row_count_min(self):
        path = self._tf_with_dq(VALID_DATE, [
            {"check": "row_count_min", "min": 2557, "severity": "error"},
        ])
        vy.validate_tf(path)
        assert vy.ERRORS == []

    def test_valid_allowed_values(self):
        path = self._tf_with_dq(VALID_SCD2, [
            {"check": "allowed_values", "column": "status",
             "values": ["ACTIVE", "INACTIVE", "UNKNOWN"], "severity": "warning"},
        ])
        vy.validate_tf(path)
        assert vy.ERRORS == []

    def test_duplicate_key_with_single_column_passes(self):
        # 'column' is accepted as fallback when 'columns' (list) is absent
        path = self._tf_with_dq(VALID_SCD2, [
            {"check": "duplicate_key", "column": "customer_key", "severity": "error"},
        ])
        vy.validate_tf(path)
        assert vy.ERRORS == []

    def test_row_count_min_missing_min(self):
        path = self._tf_with_dq(VALID_DATE, [{"check": "row_count_min", "severity": "error"}])
        vy.validate_tf(path)
        assert any("row_count_min" in e and "min" in e for e in vy.ERRORS)

    def test_not_null_missing_column(self):
        path = self._tf_with_dq(VALID_SCD2, [{"check": "not_null", "severity": "error"}])
        vy.validate_tf(path)
        assert any("not_null" in e and "column" in e for e in vy.ERRORS)

    def test_zero_check_missing_column(self):
        path = self._tf_with_dq(VALID_FACT, [{"check": "zero_check", "severity": "warning"}])
        vy.validate_tf(path)
        assert any("zero_check" in e and "column" in e for e in vy.ERRORS)

    def test_duplicate_key_missing_both_columns_and_column(self):
        path = self._tf_with_dq(VALID_SCD2, [{"check": "duplicate_key", "severity": "error"}])
        vy.validate_tf(path)
        assert any("duplicate_key" in e and "columns" in e for e in vy.ERRORS)

    def test_referential_integrity_missing_ref_table(self):
        path = self._tf_with_dq(VALID_FACT, [
            {"check": "referential_integrity", "column": "customer_key",
             "ref_column": "customer_key", "severity": "warning"},
        ])
        vy.validate_tf(path)
        assert any("ref_table" in e for e in vy.ERRORS)

    def test_referential_integrity_missing_ref_column(self):
        path = self._tf_with_dq(VALID_FACT, [
            {"check": "referential_integrity", "column": "customer_key",
             "ref_table": "gold.dim_customer", "severity": "warning"},
        ])
        vy.validate_tf(path)
        assert any("ref_column" in e for e in vy.ERRORS)

    def test_unknown_check_rejected(self):
        path = self._tf_with_dq(VALID_SCD2, [{"check": "freshness_check", "severity": "warning"}])
        vy.validate_tf(path)
        assert any("freshness_check" in e for e in vy.ERRORS)

    def test_missing_check_key(self):
        path = self._tf_with_dq(VALID_SCD2, [{"column": "customer_key", "severity": "error"}])
        vy.validate_tf(path)
        assert any("missing required key 'check'" in e for e in vy.ERRORS)

    def test_invalid_severity_rejected(self):
        path = self._tf_with_dq(VALID_SCD2, [
            {"check": "not_null", "column": "customer_key", "severity": "info"},
        ])
        vy.validate_tf(path)
        assert any("severity" in e and "info" in e for e in vy.ERRORS)


# ─── TF Security block ────────────────────────────────────────────────────────

class TestValidateTfSecurity:
    def _tf_with_security(self, security: dict) -> str:
        doc = yaml.safe_load(VALID_SCD2)
        doc["security"] = security
        return _yaml_file(yaml.dump(doc))

    def test_valid_column_mask(self):
        path = self._tf_with_security({"column_masks": [
            {"column": "customer_name", "mask_function": "cat.sec.fn", "exempt_roles": ["role1"]}
        ]})
        vy.validate_tf(path)
        assert vy.ERRORS == []

    def test_column_mask_missing_mask_function(self):
        path = self._tf_with_security({"column_masks": [
            {"column": "customer_name", "exempt_roles": ["role1"]}
        ]})
        vy.validate_tf(path)
        assert any("mask_function" in e for e in vy.ERRORS)

    def test_column_mask_missing_exempt_roles(self):
        path = self._tf_with_security({"column_masks": [
            {"column": "customer_name", "mask_function": "fn"}
        ]})
        vy.validate_tf(path)
        assert any("exempt_roles" in e for e in vy.ERRORS)
