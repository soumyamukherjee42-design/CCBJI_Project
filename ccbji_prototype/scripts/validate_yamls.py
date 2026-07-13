"""
validate_yamls.py
─────────────────
Validates all SD, TF, and trigger YAML files in the repository.
Used by the CI pipeline (ci.yml) on every pull request.

Exit code 0 = all valid.
Exit code 1 = one or more validation failures (details printed to stdout).

Usage:
    python scripts/validate_yamls.py
"""

import sys
import glob
import yaml
from pathlib import Path

# Ensure UTF-8 output on Windows terminals that default to cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent
ERRORS: list[str] = []


# ─── helpers ─────────────────────────────────────────────────────────────────

def _load(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as fh:
            return yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        ERRORS.append(f"{path}: YAML parse error – {exc}")
        return None


def _require(doc: dict, keys: list[str], path: str) -> bool:
    """Check that each dot-path key exists in the document."""
    ok = True
    for dotkey in keys:
        parts = dotkey.split(".")
        node = doc
        for part in parts:
            if not isinstance(node, dict) or part not in node:
                ERRORS.append(f"{path}: missing required key '{dotkey}'")
                ok = False
                break
            node = node[part]
    return ok


# ─── Security block (shared by SD and TF) ────────────────────────────────────

_DQ_SUPPORTED = {"row_count", "not_null", "not_negative", "allowed_values", "regex"}
_DQ_NEEDS_COL = {"not_null", "not_negative", "allowed_values", "regex"}


def _validate_dq(doc: dict, path: str) -> None:
    """Validate the optional dq block in SD YAMLs."""
    dq = (doc.get("dataset") or {}).get("dq") or {}
    if not dq:
        return
    for i, rule in enumerate(dq.get("rules", [])):
        check = rule.get("check")
        if not check:
            ERRORS.append(f"{path}: dq.rules[{i}] missing required key 'check'")
            continue
        if check not in _DQ_SUPPORTED:
            ERRORS.append(
                f"{path}: dq.rules[{i}] unknown check '{check}'. "
                f"Supported: {sorted(_DQ_SUPPORTED)}"
            )
            continue
        if check in _DQ_NEEDS_COL and "column" not in rule:
            ERRORS.append(f"{path}: dq.rules[{i}] check '{check}' requires 'column'")
        if check == "allowed_values" and "values" not in rule:
            ERRORS.append(f"{path}: dq.rules[{i}] check 'allowed_values' requires 'values'")
        if check == "regex" and "pattern" not in rule:
            ERRORS.append(f"{path}: dq.rules[{i}] check 'regex' requires 'pattern'")
        severity = rule.get("severity", "warning")
        if severity not in ("error", "warning"):
            ERRORS.append(
                f"{path}: dq.rules[{i}] severity must be 'error' or 'warning', got '{severity}'"
            )


def _validate_security(sec: dict, path: str) -> None:
    """Validate the optional security block present in SD and TF YAMLs."""
    if not sec:
        return
    for i, mask in enumerate(sec.get("column_masks", [])):
        for req in ("column", "mask_function", "exempt_roles"):
            if req not in mask:
                ERRORS.append(
                    f"{path}: security.column_masks[{i}] missing required key '{req}'"
                )
    enc = sec.get("landing_encryption") or {}
    if enc:
        for req in ("type", "key_vault_name", "key_name"):
            if req not in enc:
                ERRORS.append(
                    f"{path}: security.landing_encryption missing required key '{req}'"
                )


# ─── TF DQ block ─────────────────────────────────────────────────────────────

_TF_DQ_SUPPORTED = {
    "row_count_min", "not_null", "not_negative", "zero_check",
    "allowed_values", "duplicate_key", "referential_integrity",
}
_TF_DQ_NEEDS_COL = {
    "not_null", "not_negative", "zero_check", "allowed_values", "referential_integrity",
}


def _validate_tf_dq(doc: dict, path: str) -> None:
    """Validate the optional top-level dq block in TF YAMLs."""
    dq = doc.get("dq") or {}
    if not dq:
        return
    for i, rule in enumerate(dq.get("rules", [])):
        check = rule.get("check")
        if not check:
            ERRORS.append(f"{path}: dq.rules[{i}] missing required key 'check'")
            continue
        if check not in _TF_DQ_SUPPORTED:
            ERRORS.append(
                f"{path}: dq.rules[{i}] unknown check '{check}'. "
                f"Supported: {sorted(_TF_DQ_SUPPORTED)}"
            )
            continue
        if check in _TF_DQ_NEEDS_COL and "column" not in rule:
            ERRORS.append(f"{path}: dq.rules[{i}] check '{check}' requires 'column'")
        if check == "allowed_values" and "values" not in rule:
            ERRORS.append(f"{path}: dq.rules[{i}] check 'allowed_values' requires 'values'")
        if check == "row_count_min" and "min" not in rule:
            ERRORS.append(f"{path}: dq.rules[{i}] check 'row_count_min' requires 'min'")
        if check == "duplicate_key" and "columns" not in rule and "column" not in rule:
            ERRORS.append(
                f"{path}: dq.rules[{i}] check 'duplicate_key' requires 'columns' (list) or 'column'"
            )
        if check == "referential_integrity":
            for req in ("ref_table", "ref_column"):
                if req not in rule:
                    ERRORS.append(
                        f"{path}: dq.rules[{i}] check 'referential_integrity' requires '{req}'"
                    )
        severity = rule.get("severity", "warning")
        if severity not in ("error", "warning"):
            ERRORS.append(
                f"{path}: dq.rules[{i}] severity must be 'error' or 'warning', got '{severity}'"
            )


# ─── SD YAML schema ──────────────────────────────────────────────────────────

SD_REQUIRED = [
    "dataset.identifier.datasetname",
    "dataset.identifier.ingestion_src_path",
    "dataset.identifier.bronze_table",
    "dataset.identifier.silver_table",
    "dataset.identifier.fileformat",
    "dataset.source.columns",
    "dataset.target.columns",
]


def validate_sd(path: str) -> None:
    doc = _load(path)
    if doc is None:
        return
    if not _require(doc, SD_REQUIRED, path):
        return

    # Each source column must have name + dtype
    for col in doc["dataset"]["source"]["columns"]:
        if "name" not in col or "dtype" not in col:
            ERRORS.append(f"{path}: source column missing 'name' or 'dtype' – {col}")

    # Each target column must have name
    for col in doc["dataset"]["target"]["columns"]:
        if "name" not in col:
            ERRORS.append(f"{path}: target column missing 'name' – {col}")

    # Allowed transforms
    allowed_transforms = {"trim", "upper", "lower"}
    for col in doc["dataset"]["target"]["columns"]:
        for t in col.get("transforms", []):
            if t not in allowed_transforms:
                ERRORS.append(
                    f"{path}: unknown transform '{t}' on column '{col['name']}'. "
                    f"Allowed: {allowed_transforms}"
                )

    _validate_dq(doc, path)
    _validate_security(doc["dataset"].get("security"), path)


# ─── TF YAML schema ──────────────────────────────────────────────────────────

TF_REQUIRED_BASE = ["meta.object_type", "transformation"]

TF_REQUIRED_SCD2 = [
    "transformation.source_table",
    "transformation.target_table",
    "transformation.natural_key",
    "transformation.surrogate_key",
    "transformation.tracked_columns",
    "transformation.effective_from_column",
]

TF_REQUIRED_DATE = [
    "transformation.target_table",
    "transformation.start_date",
    "transformation.end_date",
]

TF_REQUIRED_FACT = [
    "transformation.source_table",
    "transformation.target_table",
    "transformation.late_bridge_table",
    "transformation.transaction_id_column",
    "transformation.transaction_date_column",
    "transformation.transaction_hash_columns",
    "transformation.dimensions",
    "transformation.measures",
    "transformation.degenerate_columns",
]

TF_REQUIRED_RESOLVE = [
    "transformation.fact_table",
    "transformation.bridge_table",
    "transformation.dimensions",
]


def validate_tf(path: str) -> None:
    doc = _load(path)
    if doc is None:
        return
    if not _require(doc, TF_REQUIRED_BASE, path):
        return

    object_type = doc["meta"]["object_type"]
    scd_type    = doc["meta"].get("scd_type")

    if object_type == "dimension" and scd_type == 2:
        _require(doc, TF_REQUIRED_SCD2, path)

    elif object_type == "dimension" and not scd_type:
        _require(doc, TF_REQUIRED_DATE, path)

    elif object_type == "fact":
        if not _require(doc, TF_REQUIRED_FACT, path):
            return
        # Each dimension entry must declare these keys
        dim_required = {
            "name", "table", "natural_key_source", "natural_key_dimension",
            "surrogate_key", "effective_from", "effective_to", "unknown_key",
        }
        for dim in doc["transformation"]["dimensions"]:
            missing = dim_required - set(dim.keys())
            if missing:
                ERRORS.append(
                    f"{path}: dimension entry missing keys {sorted(missing)} – {dim.get('name','?')}"
                )

    elif object_type == "resolution":
        if not _require(doc, TF_REQUIRED_RESOLVE, path):
            return
        res_dim_required = {
            "name", "table", "natural_key_dimension",
            "surrogate_key", "effective_from", "effective_to",
        }
        for dim in doc["transformation"]["dimensions"]:
            missing = res_dim_required - set(dim.keys())
            if missing:
                ERRORS.append(
                    f"{path}: resolution dimension missing keys {sorted(missing)} – {dim.get('name','?')}"
                )

    else:
        ERRORS.append(
            f"{path}: unknown object_type='{object_type}' "
            "(expected: dimension, fact, resolution)"
        )

    _validate_tf_dq(doc, path)
    _validate_security(doc.get("security"), path)


# ─── Trigger YAML schema ─────────────────────────────────────────────────────

TRIGGER_REQUIRED = ["meta.trigger_id", "trigger.name", "trigger.type", "trigger.enabled"]


def validate_trigger(path: str) -> None:
    doc = _load(path)
    if doc is None:
        return
    _require(doc, TRIGGER_REQUIRED, path)

    ttype = doc.get("trigger", {}).get("type")

    if ttype == "storage_event":
        _require(doc, ["trigger.event", "trigger.notebook", "trigger.parameters"], path)

    elif ttype == "schedule":
        _require(doc, ["trigger.timezone", "trigger.start_time"], path)
        # Must have either steps (landing trigger) or sequence (gold trigger)
        trg = doc["trigger"]
        if "steps" not in trg and "sequence" not in trg:
            ERRORS.append(
                f"{path}: schedule trigger must define 'steps' or 'sequence'"
            )

    else:
        ERRORS.append(f"{path}: unknown trigger.type='{ttype}' (expected: storage_event, schedule)")


# ─── Environment config schema ────────────────────────────────────────────────

ENV_REQUIRED = [
    "environment", "catalog_name", "bronze_schema", "silver_schema",
    "gold_schema", "datalake_name", "adf_name", "adf_resource_group",
]


def validate_env(path: str) -> None:
    doc = _load(path)
    if doc is None:
        return
    _require(doc, ENV_REQUIRED, path)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    checks: list[tuple[str, callable]] = []

    for f in glob.glob(str(ROOT / "standardization/**/*.yaml"), recursive=True):
        checks.append((f, validate_sd))

    for f in glob.glob(str(ROOT / "transformation/**/*.yaml"), recursive=True):
        checks.append((f, validate_tf))

    for f in glob.glob(str(ROOT / "trigger/**/*.yaml"), recursive=True):
        checks.append((f, validate_trigger))

    for f in glob.glob(str(ROOT / "config/env/*.yaml")):
        checks.append((f, validate_env))

    passed = 0
    failed = 0
    for path, fn in checks:
        before = len(ERRORS)
        fn(path)
        if len(ERRORS) == before:
            print(f"  ✓  {Path(path).relative_to(ROOT)}")
            passed += 1
        else:
            failed += 1

    print(f"\n{'─'*60}")
    print(f"Validated {passed + failed} files  |  {passed} passed  |  {failed} failed")

    if ERRORS:
        print("\nErrors:")
        for err in ERRORS:
            print(f"  ✗  {err}")
        sys.exit(1)

    print("All configs valid.")


if __name__ == "__main__":
    main()
