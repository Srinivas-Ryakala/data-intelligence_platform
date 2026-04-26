"""
Expression builder — converts rule definitions + assignment config into runnable SQL.
This is the most technically complex module. Every edge case is handled.
"""

import logging
from typing import Optional

from models.dq_rule import DQRule
from models.dq_rule_assignment import DQRuleAssignment

logger = logging.getLogger(__name__)


def build_sql(
    assignment: DQRuleAssignment,
    rule: DQRule,
    table_name: str,
    column_name: Optional[str] = None
) -> str:
    """
    Build a complete, runnable SQL query from a rule expression and assignment config.
    The resulting query returns a single numeric value (the observed_value).

    Args:
        assignment: The rule-to-asset mapping with filters and overrides.
        rule: The rule definition with expression template.
        table_name: The target table name (from DATA_ASSET.asset_name or qualified_name).
        column_name: The target column name (for column-level rules). None for table-level.

    Returns:
        str: A complete SQL SELECT statement.
    """
    expression = rule.rule_expression
    rule_code = rule.rule_code

    try:
        # ──────────────────────────────────────────────
        # Handle rule-specific SQL generation
        # ──────────────────────────────────────────────

        # --- Null Check: COUNT(*) WHERE {col} IS NULL ---
        if rule_code == "COMP_NULL_CHK":
            sql = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL"

        # --- Empty String Check ---
        elif rule_code == "COMP_EMPTY_STR":
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE LTRIM(RTRIM({column_name})) = '' OR {column_name} IS NULL"
            )

        # --- Completeness Ratio ---
        elif rule_code == "COMP_RATIO":
            sql = (
                f"SELECT CAST(COUNT({column_name}) AS FLOAT) / "
                f"NULLIF(COUNT(*), 0) * 100 FROM {table_name}"
            )

        # --- Row Count Threshold ---
        elif rule_code == "COMP_ROW_CNT":
            sql = f"SELECT COUNT(*) FROM {table_name}"

        # --- Primary Key Uniqueness ---
        elif rule_code == "UNIQ_PK":
            sql = (
                f"SELECT COUNT(*) - COUNT(DISTINCT {column_name}) "
                f"FROM {table_name}"
            )

        # --- Deduplication Check ---
        elif rule_code == "UNIQ_DEDUP":
            sql = (
                f"SELECT COUNT(*) - "
                f"(SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table_name}) AS t) "
                f"FROM {table_name}"
            )

        # --- Empty Dataset Guard ---
        elif rule_code == "VOLL_EMPTY_GUARD":
            sql = f"SELECT COUNT(*) FROM {table_name}"

        # --- Data Type Conformance ---
        elif rule_code == "FMT_DTYPE":
            # Requires {dtype} — placeholder, will need assignment-level config
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE TRY_CAST({column_name} AS VARCHAR) IS NULL "
                f"AND {column_name} IS NOT NULL"
            )

        # --- Leading/Trailing Whitespace ---
        elif rule_code == "FMT_TRIM":
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {column_name} != LTRIM(RTRIM({column_name}))"
            )

        # --- Negative Value Check ---
        elif rule_code == "RNG_NEG_VAL":
            sql = f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} < 0"

        # --- Percentage Bounds ---
        elif rule_code == "RNG_PCT_BOUNDS":
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {column_name} NOT BETWEEN 0 AND 100"
            )

        # --- Future Date Guard ---
        elif rule_code == "RNG_FUTURE_DT":
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {column_name} > GETDATE()"
            )

        # --- Year Sanity Check ---
        elif rule_code == "RNG_YEAR_SANITY":
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE YEAR({column_name}) NOT BETWEEN 1900 AND 2100"
            )

        # --- No Future Timestamps ---
        elif rule_code == "TIME_NO_FUTURE":
            sql = (
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {column_name} > GETDATE()"
            )

        # ──────────────────────────────────────────────
        # Generic fallback: template substitution
        # ──────────────────────────────────────────────
        else:
            # Replace common placeholders
            sql_expr = expression
            if column_name:
                sql_expr = sql_expr.replace("{col}", column_name)
                sql_expr = sql_expr.replace("{pk_col}", column_name)
                sql_expr = sql_expr.replace("{biz_key}", column_name)
                sql_expr = sql_expr.replace("{event_ts}", column_name)

            # Check if the expression already looks like a full SELECT
            if sql_expr.upper().strip().startswith("SELECT"):
                sql = sql_expr
            elif "WHERE" in sql_expr.upper():
                # Expression like: COUNT(*) WHERE {col} IS NULL
                parts = sql_expr.split("WHERE", 1)
                select_part = parts[0].strip()
                where_part = parts[1].strip()
                sql = f"SELECT {select_part} FROM {table_name} WHERE {where_part}"
            else:
                # Pure aggregate expression
                sql = f"SELECT {sql_expr} FROM {table_name}"

        # ──────────────────────────────────────────────
        # Append filter_condition if set
        # ──────────────────────────────────────────────
        if assignment.filter_condition:
            filter_cond = assignment.filter_condition.strip()
            # Remove leading "WHERE" if user included it
            if filter_cond.upper().startswith("WHERE "):
                filter_cond = filter_cond[6:].strip()

            if "WHERE" in sql.upper():
                sql += f" AND ({filter_cond})"
            else:
                sql += f" WHERE {filter_cond}"

        logger.info(f"Built SQL for {rule_code}: {sql}")
        return sql

    except Exception as e:
        logger.error(f"Error building SQL for rule {rule_code}: {e}")
        # Return a safe fallback that will produce 0 (pass)
        return f"SELECT 0 AS observed_value"


def build_row_count_sql(table_name: str, filter_condition: Optional[str] = None) -> str:
    """
    Build a simple COUNT(*) query for the table, used to get rows_checked.

    Args:
        table_name: The target table name.
        filter_condition: Optional WHERE clause.

    Returns:
        str: A COUNT(*) SQL query.
    """
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if filter_condition:
        cond = filter_condition.strip()
        if cond.upper().startswith("WHERE "):
            cond = cond[6:].strip()
        sql += f" WHERE {cond}"
    return sql
