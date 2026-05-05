"""
expression_builder.py — Translates DQ rule definitions into executable T-SQL.

Architecture:
  The rule_expression column in DQ_RULE stores the FULL SQL query
  (e.g. "SELECT COUNT(*) AS observed_value FROM {table} WHERE {col} IS NULL").
  This module resolves placeholders ({col}, {table}, etc.) and converts
  pseudo-SQL functions to T-SQL equivalents.

  If the resolved expression still has unresolved placeholders, the module
  returns SELECT NULL AS observed_value so the run continues safely.

Placeholder resolution (fully dynamic — NO hardcoded placeholder lists):
  1. {table} is ALWAYS resolved from the table_name parameter.
  2. {col}   is ALWAYS resolved from the column_name parameter (if available).
  3. {dtype} is resolved from the column's data type in DATA_ASSET.
  4. ALL OTHER {placeholder} tokens are resolved from the assignment's
     business_context field, which stores key=value pairs separated by ';'.
     Example business_context:
       "field_a=start_date;operator=<=;field_b=end_date"
       "flag1=is_active;flag2=is_deleted;flag3=is_archived"
       "col1=name;col2=email;col3=phone"

  This means:
    - No placeholder names are hardcoded in this module.
    - If someone adds a new rule with new placeholders (e.g. {my_custom_col}),
      they just need to supply my_custom_col=some_value in business_context
      when creating the assignment. No code changes required.

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


# ─── Placeholder extraction ──────────────────────────────────────────────────

def _extract_placeholders(expression: str) -> set[str]:
    """
    Dynamically extract all {placeholder} tokens from an expression.
    Returns a set of placeholder names (without the braces).

    Example:
        "SELECT ... FROM {table} WHERE {col} IS NULL"
        → {'table', 'col'}
    """
    return set(re.findall(r"\{([^}]+)\}", expression))


def _has_unresolved_placeholders(expression: str) -> bool:
    """Return True if expression still contains {placeholder} tokens."""
    return bool(re.search(r"\{[^}]+\}", expression))


def _parse_business_context(business_context: Optional[str]) -> dict[str, str]:
    """
    Parse key=value pairs from the assignment's business_context field.
    Supports ';' and newline as delimiters.

    Example input:
        "field_a=start_date;operator=<=;field_b=end_date"
    Returns:
        {'field_a': 'start_date', 'operator': '<=', 'field_b': 'end_date'}

    This is the primary mechanism for resolving rule-specific placeholders.
    Any new placeholder in any new rule is automatically resolved if the
    assignment's business_context contains the matching key=value pair.
    """
    result = {}
    if not business_context:
        return result

    # Split by ';' or newlines
    pairs = re.split(r"[;\n]+", business_context.strip())
    for pair in pairs:
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        # Split on first '=' only (value may contain '=')
        key, _, value = pair.partition("=")
        key = key.strip()
        value = value.strip()
        if key and value:
            result[key] = value

    return result


def _resolve_placeholders(
    expression: str,
    table_name: str,
    column_name: Optional[str],
    assignment: Optional[DQRuleAssignment] = None,
    column_data_type: Optional[str] = None,
) -> str:
    """
    Replace placeholders in a rule_expression template with actual values.

    Resolution order:
      1. {table} → table_name parameter (always available)
      2. {col}   → column_name parameter (if column-level rule)
      3. {dtype} → column data type from DATA_ASSET
      4. All other placeholders → parsed from assignment.business_context

    This is fully dynamic: no placeholder names are hardcoded.
    New rules with new placeholders work automatically as long as the
    assignment provides the values in business_context.
    """
    if not expression:
        return expression

    resolved = expression

    # Step 1: Resolve {table} — the target table is always known
    if table_name:
        resolved = resolved.replace("{table}", table_name)

    # Step 2: Resolve {col} — the target column (column-level rules)
    if column_name:
        resolved = resolved.replace("{col}", column_name)

    # Step 3: Resolve {dtype} — column data type from DATA_ASSET
    if column_data_type:
        resolved = resolved.replace("{dtype}", column_data_type)

    # Step 4: Resolve ALL remaining placeholders from business_context
    #         This is the dynamic part — handles ANY placeholder name.
    if assignment:
        bc_params = _parse_business_context(assignment.business_context)

        # Find what placeholders are still unresolved
        remaining = _extract_placeholders(resolved)
        for placeholder_name in remaining:
            if placeholder_name in bc_params:
                resolved = resolved.replace(
                    "{" + placeholder_name + "}", bc_params[placeholder_name]
                )

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

    Placeholder resolution is fully dynamic:
      - {table} and {col} are resolved from function parameters.
      - {dtype} is resolved from DATA_ASSET metadata.
      - ALL other placeholders are resolved from assignment.business_context.
      - No placeholder names are hardcoded — new rules with new placeholders
        work automatically.

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

        # Step 1: Resolve placeholders (fully dynamic)
        resolved = _resolve_placeholders(
            expression, table_name, column_name, assignment, column_data_type
        )

        # Step 2: Convert pseudo-SQL functions to T-SQL
        resolved = _pseudo_sql_to_tsql(resolved)

        # Step 3: Check for unresolved placeholders
        if _has_unresolved_placeholders(resolved):
            unresolved = _extract_placeholders(resolved)
            logger.warning(
                f"{rule_code} expression has unresolved placeholders: "
                f"{unresolved}. Provide these as key=value pairs in "
                f"business_context when assigning this rule. "
                f"Expression: {resolved[:200]!r} — returning NULL."
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
    assignment: Optional[DQRuleAssignment] = None,
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

        # Resolve {table} and {col} first
        expr = expr.replace("{table}", table_name)
        expr = expr.replace("{col}", col)

        # Resolve remaining placeholders from business_context (dynamic)
        if assignment:
            bc_params = _parse_business_context(assignment.business_context)
            remaining = _extract_placeholders(expr)
            for ph in remaining:
                if ph in bc_params:
                    expr = expr.replace("{" + ph + "}", bc_params[ph])

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