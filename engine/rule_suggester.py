"""
rule_suggester.py — Suggests applicable DQ rules based on selected asset type and metadata.

Given a target asset (column, table, or schema), fetches all active rules from DQ_RULE
and filters them by rule_level compatibility and optional data-type matching for columns.

DESIGN: Data-type filtering is fully dynamic — it inspects each rule's rule_expression
and rule_dimension at runtime, so user-defined rules added via UI are automatically
handled without any code changes.
"""
import logging
import re
from typing import Optional
from db.rule_repo import get_all_active_rules
from models.dq_rule import DQRule

logger = logging.getLogger(__name__)

# ── Data-type category sets ──────────────────────────────────────────────────
_NUMERIC_TYPES = {
    "INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE", "NUMERIC",
    "SMALLINT", "TINYINT", "MONEY", "SMALLMONEY", "REAL",
}
_STRING_TYPES = {
    "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT", "STRING",
}
_DATE_TYPES = {
    "DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIMESTAMP",
    "TIME", "DATETIMEOFFSET",
}

# ── Keywords in rule_expression / rule_dimension that signal data-type affinity ──
# If a rule's expression contains any of these keywords, it's restricted to that type.
_NUMERIC_KEYWORDS = [
    r"\bNEGATIVE\b", r"\bZERO\b", r"\bRANGE\b", r"\bZSCORE\b", r"\bSTDDEV\b",
    r"\bAVG\b", r"\bSUM\b", r"\bMIN\b", r"\bMAX\b", r"\bPCT_BOUNDS\b",
    r"\bNUM_RANGE\b", r"\bNEG_VAL\b", r"\bZERO_RATIO\b",
    r"\b>\s*\d", r"\b<\s*\d", r"\bBETWEEN\b.*\bAND\b",
]
_STRING_KEYWORDS = [
    r"\bTRIM\b", r"\bLEN\b", r"\bREGEX\b", r"\bRLIKE\b", r"\bLIKE\b",
    r"\bUPPER\b", r"\bLOWER\b", r"\bEMPTY_STR\b", r"\bSTR_LEN\b",
    r"\bCASE\b.*\bWHEN\b", r"\bSPCL\b", r"\bPATTERN\b",
    r"''\s*(OR|AND)", r"\bCHARINDEX\b", r"\bPATINDEX\b",
]
_DATE_KEYWORDS = [
    r"\bGETDATE\b", r"\bCURRENT_DATE\b", r"\bDATEDIFF\b", r"\bDATE_VAL\b",
    r"\bFUTURE_DT\b", r"\bYEAR_SANITY\b", r"\bNO_FUTURE\b",
    r"\bISDATE\b", r"\bDATEADD\b", r"\bYEAR\b.*\bBETWEEN\b",
]

# Dimension names that strongly imply a data-type
_NUMERIC_DIMENSIONS = {"RANGE", "STATISTICAL"}
_STRING_DIMENSIONS = {"FORMAT"}
_DATE_DIMENSIONS = {"TIMELINESS"}

# Map asset_type selections to applicable rule levels
_ASSET_TYPE_TO_RULE_LEVELS = {
    "COLUMN":   ["COLUMN"],
    "TABLE":    ["TABLE", "DATASET", "ROW"],
    "SCHEMA":   ["SCHEMA"],
    "DATABASE": ["SCHEMA"],
    "SERVER":   [],
}


def _infer_data_type_affinity(rule: DQRule) -> Optional[str]:
    """
    Dynamically infer whether a rule is restricted to a specific data-type
    category by inspecting its rule_expression and rule_dimension.

    Returns:
        'NUMERIC', 'STRING', 'DATE', or None (meaning the rule applies to all types).
    """
    expr = (rule.rule_expression or "").upper()
    dim = (rule.rule_dimension or "").upper()

    # Check dimension first (fast path)
    if dim in _NUMERIC_DIMENSIONS:
        # FORMAT dimension has mixed rules — check expression too
        for pattern in _NUMERIC_KEYWORDS:
            if re.search(pattern, expr, re.IGNORECASE):
                return "NUMERIC"
        # Dimension alone is a strong signal for RANGE/STATISTICAL
        if dim in ("RANGE", "STATISTICAL"):
            return "NUMERIC"

    if dim in _DATE_DIMENSIONS:
        return "DATE"

    # Check expression keywords
    numeric_score = sum(1 for p in _NUMERIC_KEYWORDS if re.search(p, expr, re.IGNORECASE))
    string_score = sum(1 for p in _STRING_KEYWORDS if re.search(p, expr, re.IGNORECASE))
    date_score = sum(1 for p in _DATE_KEYWORDS if re.search(p, expr, re.IGNORECASE))

    # Only restrict if there's a clear signal (score >= 2) and no ambiguity
    max_score = max(numeric_score, string_score, date_score)
    if max_score >= 2:
        if numeric_score == max_score and string_score < max_score and date_score < max_score:
            return "NUMERIC"
        if string_score == max_score and numeric_score < max_score and date_score < max_score:
            return "STRING"
        if date_score == max_score and numeric_score < max_score and string_score < max_score:
            return "DATE"

    # Also check rule_code prefixes/suffixes as a hint (covers SYSTEM rules)
    code = (rule.rule_code or "").upper()
    if any(kw in code for kw in ("NEG_VAL", "PCT_BOUNDS", "NUM_RANGE", "ZSCORE", "ZERO_RATIO")):
        return "NUMERIC"
    if any(kw in code for kw in ("EMPTY_STR", "REGEX", "STR_LEN", "TRIM", "CASE", "NO_SPCL")):
        return "STRING"
    if any(kw in code for kw in ("DATE_VAL", "FUTURE_DT", "YEAR_SANITY", "NO_FUTURE")):
        return "DATE"

    # No clear affinity → rule applies to all data types
    return None


def _is_type_compatible(affinity: Optional[str], column_data_type: str) -> bool:
    """
    Check if a rule's data-type affinity is compatible with the column's type.

    Args:
        affinity: 'NUMERIC', 'STRING', 'DATE', or None (universal).
        column_data_type: The SQL data type (e.g. 'INT', 'VARCHAR(50)').

    Returns:
        True if compatible (or if the rule has no type restriction).
    """
    if affinity is None:
        return True  # Universal rule — always compatible

    # Strip precision info: VARCHAR(50) → VARCHAR, DECIMAL(10,2) → DECIMAL
    base_type = column_data_type.upper().split("(")[0].strip()

    if affinity == "NUMERIC":
        return base_type in _NUMERIC_TYPES
    elif affinity == "STRING":
        return base_type in _STRING_TYPES
    elif affinity == "DATE":
        return base_type in _DATE_TYPES

    return True  # Unknown affinity → don't restrict


def suggest_rules(
    asset_type: str,
    column_data_type: Optional[str] = None,
) -> list[DQRule]:
    """
    Suggest applicable DQ rules based on the selected asset type and optional
    column data type.

    All filtering is dynamic — new rules added to DQ_RULE (via UI or DB)
    are automatically picked up without code changes.

    Args:
        asset_type: The type of the selected asset (COLUMN, TABLE, SCHEMA, etc.)
        column_data_type: The SQL data type of the column (e.g., 'INT', 'VARCHAR').
                          Only relevant when asset_type == 'COLUMN'.

    Returns:
        list[DQRule]: Rules that are applicable to this asset, sorted by dimension.
    """
    all_rules = get_all_active_rules()
    if not all_rules:
        logger.warning("No active rules found in DQ_RULE.")
        return []

    applicable_levels = _ASSET_TYPE_TO_RULE_LEVELS.get(asset_type.upper(), [])
    if not applicable_levels:
        logger.info(f"No applicable rule levels for asset_type '{asset_type}'.")
        return []

    # Filter by rule_level
    candidates = [
        rule for rule in all_rules
        if rule.rule_level.upper() in applicable_levels
    ]

    # For column-level: dynamically filter by data-type compatibility
    if asset_type.upper() == "COLUMN" and column_data_type:
        filtered = []
        for rule in candidates:
            affinity = _infer_data_type_affinity(rule)
            if _is_type_compatible(affinity, column_data_type):
                filtered.append(rule)
            else:
                logger.debug(
                    f"Skipping {rule.rule_code} (affinity={affinity}) "
                    f"— incompatible with {column_data_type}"
                )
        candidates = filtered

    # Sort by dimension then by rule_code for consistent display
    candidates.sort(key=lambda r: (r.rule_dimension or "", r.rule_code or ""))

    logger.info(
        f"Suggested {len(candidates)} rules for asset_type={asset_type}"
        + (f", data_type={column_data_type}" if column_data_type else "")
    )
    return candidates


def format_rule_for_display(rule: DQRule, index: int) -> str:
    """
    Format a rule for interactive display in the CLI.

    Args:
        rule: The DQRule to format.
        index: The display index number.

    Returns:
        str: Formatted display string.
    """
    severity_icon = {
        "Critical": "🔴",
        "High":     "🟠",
        "Medium":   "🟡",
    }.get(rule.severity, "⚪")

    return (
        f"  [{index:>2}] {severity_icon} {rule.rule_code:<20} "
        f"| {rule.rule_name:<30} "
        f"| {rule.rule_dimension:<20} "
        f"| {rule.severity}"
    )
