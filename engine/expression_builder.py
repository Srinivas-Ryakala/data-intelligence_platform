"""
expression_builder.py — Translates DQ rule definitions into executable T-SQL.

Architecture:
  The rule_expression column in DQ_RULE now stores the FULL SQL query
  (e.g. "SELECT COUNT(*) AS observed_value FROM {table} WHERE {col} IS NULL").
  This module resolves placeholders ({col}, {table}, etc.) and converts
  pseudo-SQL functions to T-SQL equivalents.

  If the resolved expression still has unresolved placeholders, the module
  returns SELECT NULL AS observed_value so the run continues safely.

Placeholder resolution:
  {col}  / {pk_col} / {biz_key} / {event_ts} / {event_time} / {load_time}
  {dim_updated_at} / {created_at} / {updated_at}  →  column_name parameter
  {table} / {same_table}                           →  table_name parameter
  {dtype}                                          →  column data_type from DATA_ASSET
  {min_val} / {max_val} / {min} / {max}            →  from assignment overrides
  {pattern} / {allowed_values}                     →  from assignment business_context

Pseudo-SQL → T-SQL conversions:
  CURRENT_DATE() / CURRENT_TIMESTAMP  →  GETDATE()
  TRIM(x)                            →  LTRIM(RTRIM(x))
  RLIKE                              →  PATINDEX
  TRY_TO_DATE(x, fmt)               →  TRY_CAST(x AS DATE)
  STDDEV(x)                         →  STDEV(x)
  {col1}||'|'||{col2}               →  CONCAT({col1},'|',{col2})
"""
import logging
import re
from typing import Optional
from models.dq_rule import DQRule
from models.dq_rule_assignment import DQRuleAssignment
logger = logging.getLogger(__name__)

_NULL_SQL = "SELECT NULL AS observed_value"

# ─── Placeholder mapping ─────────────────────────────────────────────────────
# All of these placeholders map to the column_name parameter.
_COLUMN_PLACEHOLDERS = {
    "{col}", "{pk_col}", "{biz_key}", "{event_ts}", "{event_time}",
    "{load_time}", "{dim_updated_at}", "{created_at}", "{updated_at}",
    "{fk_col}", "{parent_id}", "{id}", "{condition_col}", "{required_col}",
    "{computed_col}", "{flag1}", "{flag2}", "{flag3}",
    "{prev_status}", "{new_status}", "{currency_col}", "{group_col}",
    "{line_item_col}", "{header_total_col}", "{dim_id}",
}
# These map to the table_name parameter
_TABLE_PLACEHOLDERS = {
    "{table}", "{same_table}", "{parent_table}", "{dim_table}",
}


def _has_unresolved_placeholders(expression: str) -> bool:
    """Return True if expression still contains {placeholder} tokens."""
    return bool(re.search(r"\{[^}]+\}", expression))


def _resolve_placeholders(
    expression: str,
    table_name: str,
    column_name: Optional[str],
    assignment: Optional[DQRuleAssignment] = None,
    column_data_type: Optional[str] = None,
) -> str:
    """
    Replace known placeholders in a rule_expression template with actual values.
    Returns the expression with as many placeholders resolved as possible.
    """
    if not expression:
        return expression

    resolved = expression

    # ── Column placeholders → column_name ──
    if column_name:
        for ph in _COLUMN_PLACEHOLDERS:
            resolved = resolved.replace(ph, column_name)

    # ── Table placeholders → table_name ──
    if table_name:
        for ph in _TABLE_PLACEHOLDERS:
            resolved = resolved.replace(ph, table_name)

    # ── Data type placeholder ──
    if column_data_type:
        resolved = resolved.replace("{dtype}", column_data_type)

    # ── Numeric range placeholders from assignment overrides ──
    if assignment:
        if assignment.threshold_value_override is not None:
            tv = str(assignment.threshold_value_override)
            resolved = resolved.replace("{min_val}", tv)
            resolved = resolved.replace("{max_val}", tv)
            resolved = resolved.replace("{min}", tv)
            resolved = resolved.replace("{max}", tv)

        # Try to extract parameters from business_context JSON-like patterns
        bc = assignment.business_context or ""

        # Pattern: allowed_values=val1,val2,val3
        av_match = re.search(r"allowed_values=([^\s;]+)", bc)
        if av_match:
            resolved = resolved.replace("{allowed_values}", av_match.group(1))

        # Pattern: pattern=<regex>
        p_match = re.search(r"pattern=([^\s;]+)", bc)
        if p_match:
            resolved = resolved.replace("{pattern}", p_match.group(1))

        # Pattern: expected_case=UPPER|LOWER|TITLE
        ec_match = re.search(r"expected_case=([^\s;]+)", bc)
        if ec_match:
            resolved = resolved.replace("{expected_case}", ec_match.group(1))

        # Pattern: source_expr=<expression>
        se_match = re.search(r"source_expr=([^\s;]+)", bc)
        if se_match:
            resolved = resolved.replace("{source_expr}", se_match.group(1))

        # Pattern: allowed_transitions=<list>
        at_match = re.search(r"allowed_transitions=([^\s;]+)", bc)
        if at_match:
            resolved = resolved.replace("{allowed_transitions}", at_match.group(1))

        # Pattern: value=<value> (for conditional not-null)
        v_match = re.search(r"(?:^|;)value=([^\s;]+)", bc)
        if v_match:
            resolved = resolved.replace("{value}", v_match.group(1))

    return resolved


def _pseudo_sql_to_tsql(expression: str) -> str:
    """
    Convert pseudo-SQL functions to T-SQL equivalents.
    """
    result = expression

    # CURRENT_DATE() / CURRENT_TIMESTAMP → GETDATE()
    result = re.sub(r"CURRENT_DATE\(\)", "GETDATE()", result, flags=re.IGNORECASE)
    result = re.sub(r"CURRENT_TIMESTAMP", "GETDATE()", result, flags=re.IGNORECASE)

    # TRIM(x) → LTRIM(RTRIM(x))  — careful not to match LTRIM/RTRIM themselves
    result = re.sub(
        r"(?<!L)(?<!R)TRIM\(([^)]+)\)",
        r"LTRIM(RTRIM(\1))",
        result,
        flags=re.IGNORECASE,
    )

    # TRY_TO_DATE(x, 'fmt') → TRY_CAST(x AS DATE)
    result = re.sub(
        r"TRY_TO_DATE\(([^,]+),\s*'[^']*'\)",
        r"TRY_CAST(\1 AS DATE)",
        result,
        flags=re.IGNORECASE,
    )

    # STDDEV(x) → STDEV(x)
    result = re.sub(r"STDDEV\(", "STDEV(", result, flags=re.IGNORECASE)

    # col1||'|'||col2 → CONCAT(col1,'|',col2) for concatenation
    result = re.sub(
        r"(\w+)\|\|'([^']*)'\|\|(\w+)",
        r"CONCAT(\1,'\2',\3)",
        result,
    )

    # RLIKE → PATINDEX workaround (basic conversion)
    # "col NOT RLIKE 'pattern'" → "PATINDEX('%pattern%', col) = 0"
    result = re.sub(
        r"(\w+)\s+NOT\s+RLIKE\s+'([^']*)'",
        r"PATINDEX('%\2%', CAST(\1 AS NVARCHAR(MAX))) = 0",
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(
        r"(\w+)\s+RLIKE\s+'([^']*)'",
        r"PATINDEX('%\2%', CAST(\1 AS NVARCHAR(MAX))) > 0",
        result,
        flags=re.IGNORECASE,
    )

    return result


def build_sql(
    assignment: DQRuleAssignment,
    rule: DQRule,
    table_name: str,
    column_name: Optional[str] = None,
) -> str:
    """
    Build a T-SQL SELECT from the full query stored in rule.rule_expression.

    The rule_expression now contains the complete SQL query with placeholders.
    This function resolves placeholders, converts pseudo-SQL to T-SQL,
    applies optional filter conditions, and returns the final executable SQL.

    Returns:
        Executable T-SQL string.  Never raises — returns _NULL_SQL on error.
    """
    rule_code = rule.rule_code
    try:
        expression = rule.rule_expression
        if not expression:
            logger.warning(
                f"Rule {rule_code} has no rule_expression — returning NULL."
            )
            return _NULL_SQL

        # Fetch column data type for {dtype} placeholder if column_asset_id is set
        column_data_type = None
        if assignment.column_asset_id is not None:
            try:
                from db.asset_repo import get_column_data_type
                column_data_type = get_column_data_type(assignment.column_asset_id)
            except (ImportError, Exception):
                pass  # Non-critical

        # Step 1: Resolve placeholders
        resolved = _resolve_placeholders(
            expression, table_name, column_name, assignment, column_data_type
        )

        # Step 2: Convert pseudo-SQL functions to T-SQL
        resolved = _pseudo_sql_to_tsql(resolved)

        # Step 3: Reject if unresolved placeholders remain
        if _has_unresolved_placeholders(resolved):
            logger.warning(
                f"{rule_code} expression still has unresolved placeholders after "
                f"resolution: {resolved[:120]!r} — returning NULL."
            )
            return _NULL_SQL

        # Step 4: Apply optional filter condition from the assignment
        sql = _apply_filter(resolved.strip(), assignment)

        logger.info(f"Built SQL for {rule_code}: {sql.strip()[:200]}")
        return sql.strip()

    except Exception as e:
        logger.error(f"SQL build failed for {rule_code}: {e}")
        return _NULL_SQL


def _apply_filter(sql: str, assignment: DQRuleAssignment) -> str:
    """Apply optional filter condition from the assignment."""
    if assignment.filter_condition:
        cond = assignment.filter_condition.strip()
        if cond.upper().startswith("WHERE"):
            cond = cond[5:].strip()
        if "WHERE" in sql.upper():
            sql += f" AND ({cond})"
        else:
            sql += f" WHERE {cond}"
    return sql


def build_sample_sql(
    rule_code: str,
    table_name: str,
    column_name: Optional[str],
    limit: int = 5,
    rule: Optional[DQRule] = None,
) -> Optional[str]:
    """
    Build a T-SQL query that returns sample failing values for a rule.
    Used to populate sample_failed_value in DQ_RESULT.

    Derives the WHERE clause from the rule's full query expression
    and uses it to fetch sample failing rows.

    Returns:
        SQL string or None if a sample query is not applicable for this rule.
    """
    if not column_name:
        return None

    col = column_name

    # Try to extract WHERE clause from the rule's full query
    if rule and rule.rule_expression:
        expr = rule.rule_expression
        # Resolve column placeholders
        for ph in _COLUMN_PLACEHOLDERS:
            expr = expr.replace(ph, col)
        # Resolve table placeholders
        for ph in _TABLE_PLACEHOLDERS:
            expr = expr.replace(ph, table_name)
        # Convert pseudo-SQL
        expr = _pseudo_sql_to_tsql(expr)
        # Extract WHERE clause
        where_match = re.search(r"\bWHERE\b\s+(.*)", expr, re.IGNORECASE)
        if where_match and not _has_unresolved_placeholders(where_match.group(1)):
            where_clause = where_match.group(1).strip()
            return (
                f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) AS sample_val "
                f"FROM {table_name} WHERE {where_clause}"
            )

    return None


def build_row_count_sql(table_name: str, filter_condition: Optional[str] = None) -> str:
    """Build the row-count SQL used for context in rule execution."""
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if filter_condition:
        cond = filter_condition.strip()
        if cond.upper().startswith("WHERE"):
            cond = cond[5:].strip()
        sql += f" WHERE {cond}"
    return sql