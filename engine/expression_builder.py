"""
expression_builder.py — Translates DQ rule codes into executable T-SQL.
Rules fall into three tiers:
  1. HARDCODED     — explicit SQL branch per rule_code (most reliable)
  2. IMPLEMENTABLE — generated from column_name alone, added as branches
  3. UNSUPPORTED   — needs external context (schema registry, parent tables,
                     min/max thresholds, etc.).
                     Returns SELECT NULL AS observed_value so the run
                     continues without crashing.
Any rule whose seed expression still contains {placeholders} would produce
a SQL syntax error.  The generic fallback now detects this and returns NULL
instead of forwarding broken SQL to the server.
"""
import logging
import re
from typing import Optional
from models.dq_rule import DQRule
from models.dq_rule_assignment import DQRuleAssignment
logger = logging.getLogger(__name__)
# Rules that cannot be executed without external context unavailable at run-time.
_UNSUPPORTED_RULES = {
    "VOLL_INCR_CHK",    # requires today_rows vs yesterday_rows snapshot
    "SCHM_COL_CNT",     # requires expected column count
    "COMP_COL_PRES",    # requires expected column list
    "FMT_DTYPE",        # requires declared dtype per column
    "FMT_REGEX",        # requires pattern string
    "FMT_STR_LEN",      # requires min/max length values
    "FMT_CASE",         # requires expected case convention
    "RNG_NUM_RANGE",    # requires domain min/max values
    "RNG_ENUM",         # requires allowed-values list
    "UNIQ_COMP_KEY",    # requires composite key column names
    "UNIQ_BIZ_KEY",     # requires business key column name(s)
    "REF_FK_INTG",      # requires parent table + pk column
    "REF_ORPHAN",       # requires parent table join info
    "REF_LOOKUP",       # requires dimension table info
    "REF_SELF_REF",     # requires self-referencing column names
    "CONS_XFLD",        # requires field_a, field_b, operator
    "CONS_COND_NN",     # requires condition_col + required_col
    "CONS_DERIVED",     # requires computed_col + source expression
    "CONS_AGG",         # requires line-item and header-total col names
    "CONS_MX_FLAGS",    # requires flag column names
    "CONS_ST_TRANS",    # requires status col + allowed transition list
    "CONS_CURRENCY",    # requires currency_col + group_col
    "TIME_DATE_ORD",    # requires created_at + updated_at col names
    "TIME_LATE_ARR",    # requires event_time + load_time col names
    "TIME_STALE_REF",   # requires dim_updated_at col name
    "TIME_FRESH",       # requires updated_at col name
    "VOLL_LOAD_COMP",   # requires external source row count
    "VOLL_PART_CNT",    # requires partition column + partition value
    "SCHM_DRIFT",       # requires schema snapshot comparison
    "SCHM_DTYPE_DRIFT", # requires schema registry
    "SCHM_COL_ORDER",   # requires expected column order list
    "SCHM_HDR_VAL",     # requires expected header values
    "TBL_GROWTH_RATE",  # requires previous run row count (lag value)
    "TBL_FRESHNESS",    # requires datetime column name
    "TBL_MULTICOL_NULL",# requires specific column names
    "TBL_CHECKSUM",     # requires expected checksum value
}
_NULL_SQL = "SELECT NULL AS observed_value"

def _has_unresolved_placeholders(expression: str) -> bool:
    """Return True if expression still contains {placeholder} tokens."""
    return bool(re.search(r"\{[^}]+\}", expression))

def build_sql(
    assignment: DQRuleAssignment,
    rule: DQRule,
    table_name: str,
    column_name: Optional[str] = None,
) -> str:
    """
    Build a T-SQL SELECT that yields a single numeric AS observed_value.
    Returns:
        Executable T-SQL string.  Never raises — returns _NULL_SQL on error.
    """
    rule_code = rule.rule_code
    try:
        # ── Tier 0: Unsupported — return NULL immediately ─────────────────
        if rule_code in _UNSUPPORTED_RULES:
            logger.warning(
                f"{rule_code} is not supported in the SQL engine "
                f"(requires external context not available at run-time). "
                f"Returning NULL. Add an explicit branch or provide context."
            )
            return _NULL_SQL
        # ── Tier 1 & 2: Explicit SQL branches ────────────────────────────
        if rule_code == "COMP_NULL_CHK":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NULL
            """
        elif rule_code == "COMP_EMPTY_STR":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE LTRIM(RTRIM({column_name})) = '' OR {column_name} IS NULL
            """
        elif rule_code == "COMP_RATIO":
            sql = f"""
            SELECT
                CAST(COUNT({column_name}) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS observed_value
            FROM {table_name}
            """
        elif rule_code == "COMP_ROW_CNT":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            """
        elif rule_code == "UNIQ_PK":
            sql = f"""
            SELECT COUNT(*) - COUNT(DISTINCT {column_name}) AS observed_value
            FROM {table_name}
            """
        elif rule_code == "UNIQ_DEDUP":
            # Approximate duplicate count via CHECKSUM(*).
            # SQL Server does not support COUNT(DISTINCT *) directly.
            sql = f"""
            SELECT COUNT(*) - COUNT(DISTINCT CAST(CHECKSUM(*) AS VARCHAR(20))) AS observed_value
            FROM {table_name}
            """
        elif rule_code == "VOLL_EMPTY_GUARD":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            """
        elif rule_code == "RNG_NEG_VAL":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} < 0
            """
        elif rule_code == "RNG_PCT_BOUNDS":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} NOT BETWEEN 0 AND 100
            """
        elif rule_code == "RNG_FUTURE_DT":
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} > GETDATE()
            """
        elif rule_code == "RNG_YEAR_SANITY":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            # YEAR() works directly on DATE/DATETIME columns.
            # Do NOT use TRY_CAST(col AS DATE) here — float→DATE is an explicit
            # conversion not allowed by SQL Server (error 529, not suppressible).
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND YEAR({column_name}) NOT BETWEEN 1900 AND 2100
            """
        elif rule_code == "FMT_DATE_VAL":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND TRY_CAST({column_name} AS DATE) IS NULL
            """
        elif rule_code == "FMT_TRIM":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND {column_name} != LTRIM(RTRIM({column_name}))
            """
        elif rule_code == "FMT_NO_SPCL":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE PATINDEX('%[^A-Za-z0-9 _-]%',
                           CAST({column_name} AS NVARCHAR(MAX))) > 0
            """
        elif rule_code == "TIME_NO_FUTURE":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            sql = f"""
            SELECT COUNT(*) AS observed_value
            FROM {table_name}
            WHERE {column_name} > GETDATE()
            """
        elif rule_code == "STAT_ZERO_RATIO":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            sql = f"""
            SELECT
                CAST(SUM(CASE WHEN {column_name} = 0 THEN 1.0 ELSE 0 END) AS FLOAT)
                / NULLIF(COUNT(*), 0) AS observed_value
            FROM {table_name}
            """
        elif rule_code == "STAT_ZSCORE":
            if not column_name:
                logger.warning(f"{rule_code} requires a column — returning NULL.")
                return _NULL_SQL
            sql = f"""
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
        # ── Tier 3: Generic fallback from rule_expression ─────────────────
        else:
            expression = rule.rule_expression
            if not expression:
                logger.error(
                    f"Rule {rule_code} has an empty expression and no explicit "
                    f"branch — returning NULL."
                )
                return _NULL_SQL
            # Reject expressions with unresolved {placeholders} — they would
            # produce invalid SQL and a confusing database error.
            if _has_unresolved_placeholders(expression):
                logger.warning(
                    f"{rule_code} expression contains unresolved placeholders "
                    f"({expression[:80]!r}) — returning NULL. "
                    f"Add an explicit branch in expression_builder.py to support this rule."
                )
                return _NULL_SQL
            # Reject dangerous legacy internal column names
            if "today_rows" in expression or "yesterday_rows" in expression:
                logger.error(
                    f"{rule_code} expression references invalid internal columns "
                    f"— returning NULL."
                )
                return _NULL_SQL
            sql = f"""
            SELECT {expression} AS observed_value
            FROM {table_name}
            """
        # ── Apply optional filter condition ───────────────────────────────
        if assignment.filter_condition:
            cond = assignment.filter_condition.strip()
            if cond.upper().startswith("WHERE"):
                cond = cond[5:].strip()
            if "WHERE" in sql.upper():
                sql += f" AND ({cond})"
            else:
                sql += f" WHERE {cond}"
        logger.info(f"Built SQL for {rule_code}: {sql.strip()[:200]}")
        return sql.strip()
    except Exception as e:
        logger.error(f"SQL build failed for {rule_code}: {e}")
        return _NULL_SQL

def build_sample_sql(
    rule_code: str,
    table_name: str,
    column_name: Optional[str],
    limit: int = 5,
) -> Optional[str]:
    """
    Build a T-SQL query that returns sample failing values for a rule.
    Used to populate sample_failed_value in DQ_RESULT.
    Returns:
        SQL string or None if a sample query is not applicable for this rule.
    """
    if not column_name:
        return None
    col = column_name
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
    return sample_map.get(rule_code)

def build_row_count_sql(table_name: str, filter_condition: Optional[str] = None) -> str:
    """Build the row-count SQL used for context in rule execution."""
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if filter_condition:
        cond = filter_condition.strip()
        if cond.upper().startswith("WHERE"):
            cond = cond[5:].strip()
        sql += f" WHERE {cond}"
    return sql