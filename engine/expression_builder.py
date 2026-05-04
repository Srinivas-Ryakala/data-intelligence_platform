"""
expression_builder.py — Translates DQ rule definitions into executable T-SQL.

Architecture:
  1. FAST-PATH  — explicit SQL branch per rule_code (most reliable, battle-tested)
  2. DYNAMIC    — auto-generates SQL from DQ_RULE.rule_expression by resolving
                  placeholders ({col}, {table}, etc.) and converting pseudo-SQL
                  to T-SQL.  This path enables user-added rules to execute
                  without any code changes.
  3. FALLBACK   — if neither fast-path nor dynamic can produce valid SQL,
                  returns SELECT NULL AS observed_value so the run continues.

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

    # SET(expected_cols) ⊆ SET(actual_cols) → not dynamically resolvable, skip
    return result


def _expression_to_select(expression: str, table_name: str) -> str:
    """
    Convert a resolved rule_expression into a full SELECT ... FROM ... statement.

    Handles patterns like:
      - "COUNT(*) WHERE col IS NULL"        → SELECT COUNT(*) AS observed_value FROM table WHERE col IS NULL
      - "COUNT(*) - COUNT(DISTINCT col)"    → SELECT COUNT(*) - COUNT(DISTINCT col) AS observed_value FROM table
      - "CAST(COUNT(col) AS FLOAT) / ..."   → SELECT CAST(...) AS observed_value FROM table
      - "ABS(SUM(x) - MAX(y))"             → SELECT ABS(SUM(x) - MAX(y)) AS observed_value FROM table
      - "DATEDIFF(...)"                     → SELECT DATEDIFF(...) AS observed_value FROM table
    """
    expr = expression.strip()

    # If expression already has SELECT, return as-is
    if expr.upper().startswith("SELECT"):
        return expr

    # Split on WHERE if present in the expression template
    where_clause = ""
    # Look for WHERE that's not inside parentheses
    # Simple heuristic: find top-level WHERE
    where_match = re.search(r"\bWHERE\b", expr, re.IGNORECASE)
    if where_match:
        select_part = expr[:where_match.start()].strip()
        where_clause = expr[where_match.end():].strip()
    else:
        select_part = expr

    # Build the full SQL
    sql = f"SELECT {select_part} AS observed_value FROM {table_name}"
    if where_clause:
        sql += f" WHERE {where_clause}"

    return sql


def build_sql(
    assignment: DQRuleAssignment,
    rule: DQRule,
    table_name: str,
    column_name: Optional[str] = None,
) -> str:
    """
    Build a T-SQL SELECT that yields a single numeric AS observed_value.

    Strategy:
      1. Try the fast-path (hardcoded branches for battle-tested rules)
      2. If no fast-path match, use dynamic builder from rule_expression
      3. If dynamic builder fails (unresolved placeholders), return NULL

    Returns:
        Executable T-SQL string.  Never raises — returns _NULL_SQL on error.
    """
    rule_code = rule.rule_code
    try:
        # ── Fast-path: Explicit SQL branches (battle-tested T-SQL) ────────
        sql = _try_fast_path(assignment, rule, table_name, column_name)
        if sql is not None:
            # Apply optional filter condition and return
            sql = _apply_filter(sql, assignment)
            logger.info(f"Built SQL for {rule_code} (fast-path): {sql.strip()[:200]}")
            return sql.strip()

        # ── Dynamic path: Build from rule_expression ──────────────────────
        sql = _try_dynamic_build(assignment, rule, table_name, column_name)
        if sql is not None:
            sql = _apply_filter(sql, assignment)
            logger.info(f"Built SQL for {rule_code} (dynamic): {sql.strip()[:200]}")
            return sql.strip()

        # ── Fallback: return NULL ─────────────────────────────────────────
        logger.warning(
            f"{rule_code} could not be built (fast-path miss + dynamic build failed). "
            f"Returning NULL."
        )
        return _NULL_SQL

    except Exception as e:
        logger.error(f"SQL build failed for {rule_code}: {e}")
        return _NULL_SQL


def _try_fast_path(
    assignment: DQRuleAssignment,
    rule: DQRule,
    table_name: str,
    column_name: Optional[str],
) -> Optional[str]:
    """
    Return hardcoded SQL for known rule_codes, or None if no branch matches.
    These are the battle-tested T-SQL queries that are guaranteed correct.
    """
    rule_code = rule.rule_code

    if rule_code == "COMP_NULL_CHK":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NULL
            """
    elif rule_code == "COMP_EMPTY_STR":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE LTRIM(RTRIM({column_name})) = '' OR {column_name} IS NULL
            """
    elif rule_code == "COMP_RATIO":
        return f"""
            SELECT
                CAST(COUNT({column_name}) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS observed_value
            FROM {table_name}
            """
    elif rule_code == "COMP_ROW_CNT":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            """
    elif rule_code == "UNIQ_PK":
        return f"""
            SELECT COUNT(*) - COUNT(DISTINCT {column_name}) AS observed_value
            FROM {table_name}
            """
    elif rule_code == "UNIQ_DEDUP":
        return f"""
            SELECT COUNT(*) - COUNT(DISTINCT CAST(CHECKSUM(*) AS VARCHAR(20))) AS observed_value
            FROM {table_name}
            """
    elif rule_code == "VOLL_EMPTY_GUARD":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            """
    elif rule_code == "RNG_NEG_VAL":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} < 0
            """
    elif rule_code == "RNG_PCT_BOUNDS":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} NOT BETWEEN 0 AND 100
            """
    elif rule_code == "RNG_FUTURE_DT":
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} > GETDATE()
            """
    elif rule_code == "RNG_YEAR_SANITY":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND YEAR({column_name}) NOT BETWEEN 1900 AND 2100
            """
    elif rule_code == "FMT_DATE_VAL":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND TRY_CAST({column_name} AS DATE) IS NULL
            """
    elif rule_code == "FMT_TRIM":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND {column_name} != LTRIM(RTRIM({column_name}))
            """
    elif rule_code == "FMT_NO_SPCL":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE PATINDEX('%[^A-Za-z0-9 _-]%',
                           CAST({column_name} AS NVARCHAR(MAX))) > 0
            """
    elif rule_code == "TIME_NO_FUTURE":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} > GETDATE()
            """
    elif rule_code == "STAT_ZERO_RATIO":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT
                CAST(SUM(CASE WHEN {column_name} = 0 THEN 1.0 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0) AS observed_value
            FROM {table_name}
            """
    elif rule_code == "STAT_ZSCORE":
        if not column_name:
            logger.warning(f"{rule_code} requires a column — returning NULL.")
            return _NULL_SQL
        return f"""
            SELECT COUNT(*) AS observed_value
            FROM (
                SELECT
                    CAST({column_name} AS FLOAT)                        AS val,
                    AVG(CAST({column_name} AS FLOAT)) OVER ()           AS avg_val,
                    STDEV(CAST({column_name} AS FLOAT)) OVER ()         AS std_val
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            ) z
            WHERE z.std_val > 0
              AND ABS(z.val - z.avg_val) / z.std_val > 3
            """

    # No fast-path match
    return None


def _try_dynamic_build(
    assignment: DQRuleAssignment,
    rule: DQRule,
    table_name: str,
    column_name: Optional[str],
) -> Optional[str]:
    """
    Dynamically build SQL from rule.rule_expression by resolving placeholders.
    Returns executable SQL string or None if the expression can't be resolved.
    """
    expression = rule.rule_expression
    if not expression:
        logger.error(
            f"Rule {rule.rule_code} has an empty expression and no fast-path "
            f"branch — returning NULL."
        )
        return None

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
            f"{rule.rule_code} expression still has unresolved placeholders after "
            f"dynamic resolution: {resolved[:120]!r} — cannot execute."
        )
        return None

    # Step 4: Reject dangerous internal columns
    if "today_rows" in resolved or "yesterday_rows" in resolved:
        logger.error(
            f"{rule.rule_code} expression references invalid internal columns — "
            f"cannot execute."
        )
        return None

    # Step 5: Convert the resolved expression template into a SELECT statement
    sql = _expression_to_select(resolved, table_name)

    return sql


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

    Strategy:
      1. Try hardcoded sample map (reliable)
      2. If no match + rule provided, try dynamic from rule_expression WHERE clause

    Returns:
        SQL string or None if a sample query is not applicable for this rule.
    """
    if not column_name:
        return None

    col = column_name

    # ── Fast-path: hardcoded sample queries ───────────────────────────────
    sample_map: dict[str, str] = {
        "COMP_NULL_CHK": (
            f"SELECT TOP {limit} '(null)' AS sample_val "
            f"FROM {table_name} WHERE {col} IS NULL"
        ),
        "COMP_EMPTY_STR": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} "
            f"WHERE LTRIM(RTRIM({col})) = '' OR {col} IS NULL"
        ),
        "RNG_NEG_VAL": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} WHERE {col} < 0"
        ),
        "RNG_PCT_BOUNDS": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} WHERE {col} NOT BETWEEN 0 AND 100"
        ),
        "RNG_FUTURE_DT": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} WHERE {col} > GETDATE()"
        ),
        "TIME_NO_FUTURE": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} WHERE {col} > GETDATE()"
        ),
        "UNIQ_PK": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} GROUP BY {col} HAVING COUNT(*) > 1"
        ),
        "FMT_DATE_VAL": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} "
            f"WHERE TRY_CAST({col} AS DATE) IS NULL AND {col} IS NOT NULL"
        ),
        "FMT_TRIM": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} "
            f"WHERE {col} IS NOT NULL AND {col} != LTRIM(RTRIM({col}))"
        ),
        "FMT_NO_SPCL": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} "
            f"WHERE PATINDEX('%[^A-Za-z0-9 _-]%', CAST({col} AS NVARCHAR(MAX))) > 0"
        ),
        "RNG_YEAR_SANITY": (
            f"SELECT TOP {limit} CAST({col} AS NVARCHAR(500)) "
            f"FROM {table_name} "
            f"WHERE {col} IS NOT NULL "
            f"AND TRY_CAST(YEAR(TRY_CAST({col} AS DATE)) AS INT) NOT BETWEEN 1900 AND 2100"
        ),
        "STAT_ZSCORE": (
            f"SELECT TOP {limit} CAST(val AS NVARCHAR(500)) FROM ("
            f"SELECT CAST({col} AS FLOAT) AS val, "
            f"AVG(CAST({col} AS FLOAT)) OVER() AS avg_val, "
            f"STDEV(CAST({col} AS FLOAT)) OVER() AS std_val "
            f"FROM {table_name} WHERE {col} IS NOT NULL) z "
            f"WHERE z.std_val > 0 AND ABS(z.val - z.avg_val)/z.std_val > 3"
        ),
    }

    fast_result = sample_map.get(rule_code)
    if fast_result:
        return fast_result

    # ── Dynamic path: derive WHERE clause from rule_expression ────────────
    if rule and rule.rule_expression:
        expr = rule.rule_expression
        # Resolve column placeholders
        for ph in _COLUMN_PLACEHOLDERS:
            expr = expr.replace(ph, col)
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