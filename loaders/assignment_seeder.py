"""
Assignment seeder — auto-generates DQ_RULE_ASSIGNMENT rows from DATA_ASSET metadata.
Supports intelligent rule assignment based on:
  1. Rule level (COLUMN, ROW, TABLE, DATASET, SCHEMA)
  2. Column names and patterns
  3. Data types and metadata flags
  4. Multiple rules per asset (one-to-many relationship)
Idempotent: checks assignment_exists() before each insert.
"""
import os
import sys
import logging
import re
from datetime import datetime
from decimal import Decimal
from numbers import Number
from typing import List, Dict, Any, Optional
# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from configs.logging_config import setup_logging
from configs.settings import DEFAULT_OWNER, DEFAULT_CREATED_BY
from db.asset_repo import (
    get_nullable_columns,
    get_pk_columns,
    get_table_assets,
    get_schema_assets,
    get_all_assets,
    get_columns_for_table,
    asset_exists,
)
from db.connection import get_connection
from db.rule_repo import get_all_active_rules, get_rule_by_code
from db.assignment_repo import insert_assignment, assignment_exists
from models.dq_rule_assignment import DQRuleAssignment
setup_logging()
logger = logging.getLogger(__name__)

def _safe_sql_name(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"

def _parse_qualified_name(qualified_name: str) -> Optional[tuple[str, str, str, str, str]]:
    if not qualified_name:
        return None
    parts = [part.strip() for part in qualified_name.split('.') if part.strip()]
    if len(parts) < 5:
        return None
    return tuple(parts[-5:])

def _get_column_sample_values(column: Dict[str, Any], limit: int = 50) -> list[Any]:
    parsed = _parse_qualified_name(column.get("qualified_name", ""))
    if not parsed:
        return []
    _, _, schema, table, column_name = parsed
    quoted_column = _safe_sql_name(column_name)
    quoted_table = f"{_safe_sql_name(schema)}.{_safe_sql_name(table)}"
    sql = f"SELECT TOP {limit} {quoted_column} FROM {quoted_table} WHERE {quoted_column} IS NOT NULL"
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    except Exception as exc:
        logger.debug(f"Sample query failed for {column.get('qualified_name')}: {exc}")
        return []
    finally:
        if conn is not None:
            conn.close()

def _is_date_value(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str):
        return False
    value = value.strip()
    if not value:
        return False
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        pass
    date_formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%d-%b-%Y",
        "%d-%b-%Y %H:%M:%S",
    ]
    for fmt in date_formats:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False

def _numeric_sample_values(column: Dict[str, Any], min_count: int = 3) -> list[Number]:
    samples = _get_column_sample_values(column, limit=50)
    numeric_values = []
    for value in samples:
        if isinstance(value, Number):
            numeric_values.append(value)
            continue
        if isinstance(value, str):
            cleaned = re.sub(r"[^0-9.\-]", "", value)
            if cleaned == "" or cleaned in {"-", ".", "-."}:
                continue
            try:
                numeric_values.append(Decimal(cleaned))
            except Exception:
                continue
    return numeric_values if len(numeric_values) >= min_count else []

def _column_name_matches(column: Dict[str, Any], terms: list[str]) -> bool:
    name = column.get("asset_name", "").lower()
    return any(term in name for term in terms)

def _has_email_like_values(column: Dict[str, Any]) -> bool:
    if _column_name_matches(column, ["email"]):
        return True
    return any(isinstance(value, str) and re.search(r"[^@\s]+@[^@\s]+\.[^@\s]+", value) for value in _get_column_sample_values(column, limit=50))

def _has_phone_like_values(column: Dict[str, Any]) -> bool:
    if _column_name_matches(column, ["phone", "mobile", "tel"]):
        return True
    for value in _get_column_sample_values(column, limit=50):
        if isinstance(value, str):
            digits = re.sub(r"\D", "", value)
            if len(digits) >= 7:
                return True
    return False

def _has_date_like_values(column: Dict[str, Any]) -> bool:
    if column.get("data_type", "").lower() in ["date", "datetime", "datetime2", "smalldatetime", "time", "timestamp"]:
        return True
    for value in _get_column_sample_values(column, limit=50):
        if _is_date_value(value):
            return True
    return False

def _has_percent_like_values(column: Dict[str, Any]) -> bool:
    if _column_name_matches(column, ["percent", "pct"]):
        return True
    values = _numeric_sample_values(column, min_count=3)
    return bool(values and all(0 <= float(value) <= 100 for value in values))

def _has_future_date_values(column: Dict[str, Any]) -> bool:
    if not _has_date_like_values(column):
        return False
    samples = _get_column_sample_values(column, limit=50)
    now = datetime.now()
    for value in samples:
        if isinstance(value, datetime) and value > now:
            return True
        if isinstance(value, str) and _is_date_value(value):
            try:
                candidate = datetime.fromisoformat(value.strip())
                if candidate > now:
                    return True
            except Exception:
                pass
    return False

def _has_high_uniqueness_ratio(column: Dict[str, Any]) -> bool:
    if _column_name_matches(column, ["id", "key", "code", "number", "num"]):
        values = _get_column_sample_values(column, limit=100)
        if not values:
            return False
        unique_ratio = len(set(values)) / len(values)
        return unique_ratio >= 0.9
    return False

def _has_cross_field_columns(table_asset: Dict[str, Any]) -> bool:
    columns = get_columns_for_table(table_asset.get("asset_id"))
    names = [col.get("asset_name", "").lower() for col in columns]
    return (
        ("start" in " ".join(names) and "end" in " ".join(names))
        or ("from" in " ".join(names) and "to" in " ".join(names))
        or ("min" in " ".join(names) and "max" in " ".join(names))
    )

_table_columns_cache: Dict[int, list[Dict[str, Any]]] = {}

def _get_table_columns(table_asset: Dict[str, Any]) -> list[Dict[str, Any]]:
    asset_id = table_asset.get("asset_id")
    if asset_id is None:
        return []
    if asset_id not in _table_columns_cache:
        _table_columns_cache[asset_id] = get_columns_for_table(asset_id)
    return _table_columns_cache[asset_id]

def _is_string_column(column: Dict[str, Any]) -> bool:
    data_type = column.get("data_type", "").upper()
    name = column.get("asset_name", "").lower()
    return data_type in ["VARCHAR", "NVARCHAR", "CHAR", "TEXT", "STRING"] or any(keyword in name for keyword in ["name", "code", "status", "type", "city", "country", "zip", "postal", "email", "phone"])

def _is_numeric_column(column: Dict[str, Any]) -> bool:
    return column.get("data_type", "").upper() in ["INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC", "SMALLINT", "TINYINT"]

def _is_datetime_column(column: Dict[str, Any]) -> bool:
    return column.get("data_type", "").upper() in ["DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIMESTAMP", "TIME"] or _column_name_matches(column, ["date", "time", "timestamp", "created_at", "updated_at", "event_time", "load_time"])

def _column_name_contains(column: Dict[str, Any], terms: list[str]) -> bool:
    name = column.get("asset_name", "").lower()
    return any(term in name for term in terms)

def _has_composite_primary_key(table_asset: Dict[str, Any]) -> bool:
    columns = _get_table_columns(table_asset)
    return sum(1 for col in columns if col.get("is_primary_key", False)) >= 2

def _has_foreign_key_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["_id", "id"]) and not col.get("is_primary_key", False) for col in _get_table_columns(table_asset))

def _has_lookup_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["lookup", "dim", "code", "type", "category"]) for col in _get_table_columns(table_asset))

def _has_parent_reference_column(column: Dict[str, Any]) -> bool:
    return _column_name_contains(column, ["parent_id", "manager_id", "supervisor_id", "ancestor_id", "reporting_id"])

def _table_has_string_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_is_string_column(col) for col in _get_table_columns(table_asset))

def _table_has_numeric_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_is_numeric_column(col) for col in _get_table_columns(table_asset))

def _table_has_datetime_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_is_datetime_column(col) for col in _get_table_columns(table_asset))

def _table_has_partition_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["partition", "part_"]) for col in _get_table_columns(table_asset))

def _table_has_flag_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["flag", "status", "indicator"]) for col in _get_table_columns(table_asset))

def _table_has_multiple_nullable_columns(table_asset: Dict[str, Any]) -> bool:
    return sum(1 for col in _get_table_columns(table_asset) if not col.get("is_nullable", True)) >= 3

def _table_has_date_order_columns(table_asset: Dict[str, Any]) -> bool:
    return _column_name_contains(table_asset, ["created_at", "updated_at"]) or _has_cross_field_columns(table_asset)

def _table_has_event_time_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["event_time", "load_time", "arrival_time", "ingestion_time"]) for col in _get_table_columns(table_asset))

def _table_has_derived_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["total", "subtotal", "amount", "tax", "price", "discount"]) for col in _get_table_columns(table_asset))

def _table_has_currency_columns(table_asset: Dict[str, Any]) -> bool:
    return any(_column_name_contains(col, ["currency", "amount", "price", "cost", "fee"])
               or _column_name_contains(col, ["usd", "eur", "gbp"]) for col in _get_table_columns(table_asset))

# Rule assignment patterns based on column names, types, and metadata
ASSIGNMENT_PATTERNS = {
    # Completeness rules
    "COMP_NULL_CHK": {
        "level": "COLUMN",
        "condition": lambda col: not col.get("is_nullable", True),
        "business_context": "Auto-generated: {asset_name} is non-nullable."
    },
    "COMP_EMPTY_STR": {
        "level": "COLUMN",
        "condition": lambda col: not col.get("is_nullable", True) and _is_string_column(col),
        "business_context": "Auto-generated: {asset_name} must not be empty string."
    },
    "COMP_COL_PRES": {
        "level": "DATASET",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} must contain expected columns."
    },
    "COMP_ROW_CNT": {
        "level": "DATASET",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} requires row count validation."
    },
    # Format rules
    "FMT_DTYPE": {
        "level": "COLUMN",
        "condition": lambda col: True,
        "business_context": "Auto-generated: {asset_name} must conform to declared data type."
    },
    "FMT_REGEX": {
        "level": "COLUMN",
        "condition": lambda col: _has_email_like_values(col) or _has_phone_like_values(col) or _column_name_contains(col, ["zip", "postal"]),
        "business_context": "Auto-generated: {asset_name} requires pattern validation."
    },
    "FMT_DATE_VAL": {
        "level": "COLUMN",
        "condition": _has_date_like_values,
        "business_context": "Auto-generated: {asset_name} requires date validation."
    },
    "FMT_STR_LEN": {
        "level": "COLUMN",
        "condition": lambda col: col.get("data_type", "").upper() in ["VARCHAR", "NVARCHAR", "CHAR"],
        "business_context": "Auto-generated: {asset_name} requires length validation."
    },
    "FMT_TRIM": {
        "level": "COLUMN",
        "condition": _is_string_column,
        "business_context": "Auto-generated: {asset_name} should not contain leading or trailing whitespace."
    },
    "FMT_CASE": {
        "level": "COLUMN",
        "condition": _is_string_column,
        "business_context": "Auto-generated: {asset_name} should follow expected case conventions."
    },
    "FMT_NO_SPCL": {
        "level": "COLUMN",
        "condition": _is_string_column,
        "business_context": "Auto-generated: {asset_name} should not contain special characters."
    },
    # Range rules
    "RNG_NUM_RANGE": {
        "level": "COLUMN",
        "condition": _is_numeric_column,
        "business_context": "Auto-generated: {asset_name} requires numeric range validation."
    },
    "RNG_NEG_VAL": {
        "level": "COLUMN",
        "condition": _is_numeric_column,
        "business_context": "Auto-generated: {asset_name} should not contain negative values."
    },
    "RNG_ENUM": {
        "level": "COLUMN",
        "condition": lambda col: _is_string_column(col) and _column_name_contains(col, ["status", "type", "category", "code", "flag"]),
        "business_context": "Auto-generated: {asset_name} should conform to an allowed value set."
    },
    "RNG_PCT_BOUNDS": {
        "level": "COLUMN",
        "condition": _has_percent_like_values,
        "business_context": "Auto-generated: {asset_name} must be between 0-100."
    },
    "RNG_FUTURE_DT": {
        "level": "COLUMN",
        "condition": _has_future_date_values,
        "business_context": "Auto-generated: {asset_name} should not be in the future."
    },
    "RNG_YEAR_SANITY": {
        "level": "COLUMN",
        # Only assign to DATE/DATETIME columns. Numeric columns look like "year"
        # values but TRY_CAST(float AS DATE) raises error 529 in SQL Server
        # even inside TRY_CAST — it is an explicit conversion that is never allowed.
        "condition": _has_date_like_values,
        "business_context": "Auto-generated: {asset_name} must fall within a sane year range."
    },
    # Uniqueness rules
    "UNIQ_PK": {
        "level": "COLUMN",
        "condition": lambda col: col.get("is_primary_key", False),
        "business_context": "Auto-generated: {asset_name} is a primary key."
    },
    "UNIQ_COMP_KEY": {
        "level": "ROW",
        "condition": lambda asset: _has_composite_primary_key(asset),
        "business_context": "Auto-generated: {asset_name} should enforce composite key uniqueness."
    },
    "UNIQ_DEDUP": {
        "level": "DATASET",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should not contain duplicate rows."
    },
    "UNIQ_BIZ_KEY": {
        "level": "COLUMN",
        "condition": lambda col: any(keyword in col.get("asset_name", "").lower() for keyword in ["id", "key", "code", "number", "num"]) or _has_high_uniqueness_ratio(col),
        "business_context": "Auto-generated: {asset_name} appears to be a business key."
    },
    # Referential integrity rules
    "REF_FK_INTG": {
        "level": "COLUMN",
        "condition": lambda col: "_id" in col.get("asset_name", "").lower() or col.get("asset_name", "").lower().endswith("id"),
        "business_context": "Auto-generated: {asset_name} appears to be a foreign key."
    },
    "REF_ORPHAN": {
        "level": "TABLE",
        "condition": _has_foreign_key_columns,
        "business_context": "Auto-generated: {asset_name} may contain orphaned child records."
    },
    "REF_LOOKUP": {
        "level": "TABLE",
        "condition": _has_lookup_columns,
        "business_context": "Auto-generated: {asset_name} may contain lookup/reference values."
    },
    "REF_SELF_REF": {
        "level": "COLUMN",
        "condition": _has_parent_reference_column,
        "business_context": "Auto-generated: {asset_name} appears to be a self-referencing foreign key."
    },
    # Consistency rules
    "CONS_XFLD": {
        "level": "ROW",
        "condition": _has_cross_field_columns,
        "business_context": "Auto-generated: {asset_name} requires cross-field validation."
    },
    "CONS_COND_NN": {
        "level": "ROW",
        "condition": lambda asset: _table_has_multiple_nullable_columns(asset),
        "business_context": "Auto-generated: {asset_name} likely contains conditional not-null constraints."
    },
    "CONS_DERIVED": {
        "level": "ROW",
        "condition": _table_has_derived_columns,
        "business_context": "Auto-generated: {asset_name} may contain derived/calculated fields."
    },
    "CONS_AGG": {
        "level": "DATASET",
        "condition": _table_has_numeric_columns,
        "business_context": "Auto-generated: {asset_name} may require aggregate consistency checks."
    },
    "CONS_MX_FLAGS": {
        "level": "ROW",
        "condition": _table_has_flag_columns,
        "business_context": "Auto-generated: {asset_name} likely contains mutually exclusive flag fields."
    },
    "CONS_ST_TRANS": {
        "level": "ROW",
        "condition": _table_has_flag_columns,
        "business_context": "Auto-generated: {asset_name} likely contains state transition fields."
    },
    "CONS_CURRENCY": {
        "level": "DATASET",
        "condition": _table_has_currency_columns,
        "business_context": "Auto-generated: {asset_name} likely contains currency/unit fields."
    },
    # Timeliness rules
    "TIME_FRESH": {
        "level": "DATASET",
        "condition": _table_has_datetime_columns,
        "business_context": "Auto-generated: {asset_name} should be checked for freshness."
    },
    "TIME_DATE_ORD": {
        "level": "ROW",
        "condition": _table_has_date_order_columns,
        "business_context": "Auto-generated: {asset_name} should preserve chronological order."
    },
    "TIME_LATE_ARR": {
        "level": "ROW",
        "condition": _table_has_event_time_columns,
        "business_context": "Auto-generated: {asset_name} may contain late-arriving records."
    },
    "TIME_STALE_REF": {
        "level": "DATASET",
        "condition": _table_has_datetime_columns,
        "business_context": "Auto-generated: {asset_name} may contain stale reference data."
    },
    "TIME_NO_FUTURE": {
        "level": "COLUMN",
        "condition": _has_date_like_values,
        "business_context": "Auto-generated: {asset_name} should not contain future timestamps."
    },
    # Statistical rules
    "STAT_ZSCORE": {
        "level": "COLUMN",
        "condition": _is_numeric_column,
        "business_context": "Auto-generated: {asset_name} may contain outliers based on statistical deviation."
    },
    "STAT_ZERO_RATIO": {
        "level": "COLUMN",
        "condition": _is_numeric_column,
        "business_context": "Auto-generated: {asset_name} may have unexpected zero-value ratios."
    },
    # Volume rules
    "VOLL_LOAD_COMP": {
        "level": "DATASET",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should validate load completeness."
    },
    "VOLL_INCR_CHK": {
        "level": "DATASET",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should be checked for incremental load spikes."
    },
    "VOLL_PART_CNT": {
        "level": "DATASET",
        "condition": _table_has_partition_columns,
        "business_context": "Auto-generated: {asset_name} may require partition row count validation."
    },
    "VOLL_EMPTY_GUARD": {
        "level": "TABLE",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} must not be empty."
    },
    # Schema rules
    "SCHM_COL_CNT": {
        "level": "TABLE",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} must have expected column count."
    },
    "SCHM_DRIFT": {
        "level": "SCHEMA",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "SCHEMA",
        "business_context": "Auto-generated: {asset_name} schema must not drift."
    },
    "SCHM_DTYPE_DRIFT": {
        "level": "COLUMN",
        "condition": lambda col: True,
        "business_context": "Auto-generated: {asset_name} should be validated for data type drift."
    },
    "SCHM_COL_ORDER": {
        "level": "TABLE",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should maintain expected column ordering."
    },
    "SCHM_HDR_VAL": {
        "level": "TABLE",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should maintain expected header values."
    },
    # Table rules
    "TBL_GROWTH_RATE": {
        "level": "TABLE",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should be monitored for growth rate changes."
    },
    "TBL_FRESHNESS": {
        "level": "TABLE",
        "condition": _table_has_datetime_columns,
        "business_context": "Auto-generated: {asset_name} freshness should be monitored."
    },
    "TBL_MULTICOL_NULL": {
        "level": "TABLE",
        "condition": _table_has_multiple_nullable_columns,
        "business_context": "Auto-generated: {asset_name} likely contains rows with multiple null columns."
    },
    "TBL_CHECKSUM": {
        "level": "TABLE",
        "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
        "business_context": "Auto-generated: {asset_name} should validate checksum consistency."
    }
}

def _matches_pattern(column: Dict[str, Any], rule_code: str) -> bool:
    """Check if a column matches the assignment pattern for a rule."""
    pattern = ASSIGNMENT_PATTERNS.get(rule_code)
    if not pattern:
        return False
    try:
        return pattern["condition"](column)
    except Exception as e:
        logger.warning(f"Error evaluating pattern for {rule_code}: {e}")
        return False

def _get_business_context(column: Dict[str, Any], rule_code: str) -> str:
    """Generate business context for the assignment."""
    pattern = ASSIGNMENT_PATTERNS.get(rule_code, {})
    template = pattern.get("business_context", "Auto-generated assignment for {asset_name}.")
    return template.format(asset_name=column.get("asset_name", "Unknown"))

def _get_assignment_scope(rule_level: str) -> str:
    """Map rule level to assignment scope."""
    level_to_scope = {
        "COLUMN": "COLUMN",
        "ROW": "TABLE",  # Row-level rules apply to tables
        "TABLE": "TABLE", 
        "DATASET": "TABLE",
        "SCHEMA": "SCHEMA"
    }
    return level_to_scope.get(rule_level, "COLUMN")

def seed_intelligent_assignments() -> tuple[int, int]:
    """
    Intelligently assign rules to assets based on patterns and metadata.
    Supports multiple rules per asset.
    Excludes rules that are already handled by mandatory seeders:
    - COMP_NULL_CHK (handled by seed_null_check_assignments)
    - UNIQ_PK (handled by seed_pk_uniqueness_assignments)  
    - VOLL_EMPTY_GUARD (handled by seed_empty_guard_assignments)
    - SCHM_DRIFT (handled by seed_schema_drift_assignments)
    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    # Rules handled by mandatory seeders — exclude from intelligent assignment
    mandatory_rules = {"COMP_NULL_CHK", "UNIQ_PK", "VOLL_EMPTY_GUARD", "SCHM_DRIFT"}
    rules = get_all_active_rules()
    assets = get_all_assets()
    inserted = 0
    skipped = 0
    # Group assets by type for efficient processing
    columns = [a for a in assets if a.get("asset_type", "").upper() == "COLUMN"]
    tables = [a for a in assets if a.get("asset_type", "").upper() == "TABLE"] 
    schemas = [a for a in assets if a.get("asset_type", "").upper() == "SCHEMA"]
    logger.info(f"Found {len(columns)} columns, {len(tables)} tables, {len(schemas)} schemas")
    # Process each rule
    for rule in rules:
        rule_code = rule.rule_code
        # Skip rules handled by mandatory seeders
        if rule_code in mandatory_rules:
            logger.debug(f"Skipping {rule_code} — handled by mandatory seeder")
            continue
        if rule_code not in ASSIGNMENT_PATTERNS:
            # Rule exists in rules_seed.json but has no assignment pattern yet.
            # Use a safe default: assign to all TABLE-level assets.
            # Add a dedicated entry to ASSIGNMENT_PATTERNS to get smarter targeting.
            logger.warning(
                f"No assignment pattern found for rule '{rule_code}'. "
                f"Using default TABLE-level fallback. "
                f"Add an entry to ASSIGNMENT_PATTERNS in assignment_seeder.py "
                f"to control which assets receive this rule."
            )
            _fallback_pattern = {
                "level": "TABLE",
                "condition": lambda asset: asset.get("asset_type", "").upper() == "TABLE",
                "business_context": f"Auto-generated (fallback): {{asset_name}} — rule {rule_code}.",
            }
            ASSIGNMENT_PATTERNS[rule_code] = _fallback_pattern
        pattern = ASSIGNMENT_PATTERNS[rule_code]
        expected_level = pattern["level"]
        # Get appropriate assets for this rule level
        if expected_level == "COLUMN":
            target_assets = columns
        elif expected_level in ["TABLE", "DATASET", "ROW"]:
            target_assets = tables
        elif expected_level == "SCHEMA":
            target_assets = schemas
        else:
            continue
        # Assign rule to matching assets
        for asset in target_assets:
            if _matches_pattern(asset, rule_code):
                platform_id = asset.get("platform_id")
                if expected_level == "COLUMN":
                    asset_id = asset.get("parent_asset_id")
                    column_asset_id = asset.get("asset_id")
                else:
                    asset_id = asset.get("asset_id")
                    column_asset_id = None
                if asset_id is None or not asset_exists(asset_id):
                    skipped += 1
                    logger.warning(
                        f"Skipping intelligent assignment for rule {rule_code} because asset_id {asset_id} does not exist or is invalid for asset {asset.get('asset_name')}"
                    )
                    continue
                # Check if assignment already exists
                if assignment_exists(rule.dq_rule_id, asset_id, column_asset_id):
                    skipped += 1
                    continue
                # Create assignment
                assignment = DQRuleAssignment(
                    dq_rule_id=rule.dq_rule_id,
                    asset_id=asset_id,
                    column_asset_id=column_asset_id,
                    platform_id=platform_id,
                    assignment_scope=_get_assignment_scope(rule.rule_level),
                    execution_mode="BLOCKING",
                    execution_frequency="EVERY_RUN",
                    business_context=_get_business_context(asset, rule_code),
                    owner_name=DEFAULT_OWNER,
                    # Critical and High severity rules are mandatory — failures
                    # must generate DQ_ISSUE records.  Medium rules are advisory.
                    is_mandatory=rule.severity in ("Critical", "High"),
                    is_active=True,
                    created_by=DEFAULT_CREATED_BY,
                )
                new_id = insert_assignment(assignment)
                if new_id is not None:
                    inserted += 1
                    logger.debug(f"Assigned {rule_code} to {asset.get('asset_name', 'Unknown')}")
                else:
                    logger.warning(f"Failed to assign {rule_code} to asset {asset_id}")
    logger.info(f"Intelligent assignments: {inserted} inserted, {skipped} skipped")
    return inserted, skipped

def seed_null_check_assignments() -> tuple[int, int]:
    rule = get_rule_by_code("COMP_NULL_CHK")
    if rule is None:
        logger.error("Rule COMP_NULL_CHK not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0
    columns = get_nullable_columns()
    inserted = 0
    skipped = 0
    for col in columns:
        asset_id = col.get("parent_asset_id")
        column_asset_id = col.get("asset_id")
        platform_id = col.get("platform_id")
        # pipeline_id = col.get("pipeline_id")
        # parse_run_id = col.get("parse_run_id")
        if assignment_exists(rule.dq_rule_id, asset_id, column_asset_id):
            skipped += 1
            continue
        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=column_asset_id,
            platform_id=platform_id,
            # pipeline_id=pipeline_id,
            # parse_run_id=parse_run_id,
            assignment_scope="COLUMN",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {col.get('asset_name', '')} is non-nullable.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )
        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert null-check assignment for column {column_asset_id}."
            )
    logger.info(f"Null Check assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped

def seed_pk_uniqueness_assignments() -> tuple[int, int]:
    """
    Auto-generate UNIQ_PK assignments for every primary key column.
    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("UNIQ_PK")
    if rule is None:
        logger.error("Rule UNIQ_PK not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0
    pk_columns = get_pk_columns()
    inserted = 0
    skipped = 0
    for col in pk_columns:
        asset_id = col.get("parent_asset_id")
        column_asset_id = col.get("asset_id")
        platform_id = col.get("platform_id")
        if assignment_exists(rule.dq_rule_id, asset_id, column_asset_id):
            skipped += 1
            continue
        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=column_asset_id,
            platform_id=platform_id,
            assignment_scope="COLUMN",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {col.get('asset_name', '')} is a primary key.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )
        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert PK-uniqueness assignment for column {column_asset_id}."
            )
    logger.info(f"PK Uniqueness assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped

def seed_empty_guard_assignments() -> tuple[int, int]:
    """
    Auto-generate VOLL_EMPTY_GUARD assignments for every TABLE asset.
    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("VOLL_EMPTY_GUARD")
    if rule is None:
        logger.error("Rule VOLL_EMPTY_GUARD not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0
    tables = get_table_assets()
    inserted = 0
    skipped = 0
    for table in tables:
        asset_id = table.get("asset_id")
        platform_id = table.get("platform_id")
        if assignment_exists(rule.dq_rule_id, asset_id, None):
            skipped += 1
            continue
        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=None,
            platform_id=platform_id,
            assignment_scope="TABLE",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {table.get('asset_name', '')} must not be empty.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )
        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert empty-guard assignment for table {asset_id}."
            )
    logger.info(f"Empty Guard assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped

def seed_schema_drift_assignments() -> tuple[int, int]:
    """
    Auto-generate SCHM_DRIFT assignments for every SCHEMA asset.
    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("SCHM_DRIFT")
    if rule is None:
        logger.error("Rule SCHM_DRIFT not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0
    schemas = get_schema_assets()
    inserted = 0
    skipped = 0
    for schema in schemas:
        asset_id = schema.get("asset_id")
        platform_id = schema.get("platform_id")
        if not asset_exists(asset_id):
            skipped += 1
            logger.warning(f"Skipping schema-drift assignment for schema {asset_id} — asset does not exist")
            continue
        if assignment_exists(rule.dq_rule_id, asset_id, None):
            skipped += 1
            continue
        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=None,
            platform_id=platform_id,
            assignment_scope="SCHEMA",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {schema.get('asset_name', '')} schema must not drift.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )
        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert schema-drift assignment for schema {asset_id}."
            )
    logger.info(f"Schema Drift assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped

def seed_all_assignments() -> None:
    """
    Run all assignment seeders and print a combined summary.
    Includes both mandatory and intelligent assignments.
    Returns:
        None
    """
    total_inserted = 0
    total_skipped = 0
    # Mandatory assignments (critical rules that must be applied)
    logger.info("Running mandatory assignment seeders...")
    ins, skip = seed_null_check_assignments()
    total_inserted += ins
    total_skipped += skip
    ins, skip = seed_pk_uniqueness_assignments()
    total_inserted += ins
    total_skipped += skip
    ins, skip = seed_empty_guard_assignments()
    total_inserted += ins
    total_skipped += skip
    ins, skip = seed_schema_drift_assignments()
    total_inserted += ins
    total_skipped += skip
    # Intelligent assignments (pattern-based suggestions)
    logger.info("Running intelligent assignment seeder...")
    ins, skip = seed_intelligent_assignments()
    total_inserted += ins
    total_skipped += skip
    # ── Summary ──
    summary = (
        f"\n{'='*60}\n"
        f"ASSIGNMENT SEEDER SUMMARY\n"
        f"{'='*60}\n"
        f"Total inserted: {total_inserted}\n"
        f"Total skipped : {total_skipped}\n"
        f"{'='*60}\n"
        f"Note: Multiple rules can be assigned to the same asset.\n"
        f"Mandatory rules ensure critical validations.\n"
        f"Intelligent rules provide pattern-based suggestions.\n"
        f"{'='*60}"
    )
    logger.info(summary)
    print(summary)

if __name__ == "__main__":
    seed_all_assignments()