-- ============================================================
-- security/mask_functions.sql
-- ============================================================
-- Unity Catalog column mask functions for PII protection.
-- Run once per catalog via DDL_Bootstrap.py or manually.
--
-- Access model:
--   Members of 'pii_reader' or 'data_engineer' → real value
--   All other roles                            → masked value
--
-- Applied to tables by SD_Standardization_Engine and
-- TF_Gold_Load_Engine when the YAML security.column_masks
-- block is present.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS {catalog_name}.security;

-- ── Full name mask ────────────────────────────────────────────────────────────
-- Returns: first char + asterisks + last char  (e.g. "Tanaka Kenji" → "T**********i")
-- Members of pii_reader or data_engineer see the unmasked value.

CREATE OR REPLACE FUNCTION {catalog_name}.security.mask_pii_name(val STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_member('pii_reader') OR is_member('data_engineer') THEN val
    ELSE CONCAT(
           LEFT(val, 1),
           REPEAT('*', GREATEST(LENGTH(val) - 2, 0)),
           RIGHT(val, 1)
         )
  END;

-- ── Email mask ────────────────────────────────────────────────────────────────
-- Returns: local part replaced with *** while domain is preserved
-- (e.g. "tanaka@ccbji.co.jp" → "***@ccbji.co.jp")

CREATE OR REPLACE FUNCTION {catalog_name}.security.mask_pii_email(val STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_member('pii_reader') OR is_member('data_engineer') THEN val
    ELSE CONCAT('***@', SPLIT_PART(val, '@', 2))
  END;

-- ── Generic partial mask ──────────────────────────────────────────────────────
-- Returns: first 2 chars + *** (for IDs, phone numbers, etc.)

CREATE OR REPLACE FUNCTION {catalog_name}.security.mask_pii_partial(val STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_member('pii_reader') OR is_member('data_engineer') THEN val
    ELSE CONCAT(LEFT(val, 2), '***')
  END;
