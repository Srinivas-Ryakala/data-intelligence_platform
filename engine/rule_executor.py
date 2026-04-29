"""
Rule executor — the core DQ engine.
Reads active assignments, executes rule expressions against the DB, and returns results.
A single rule erroring must NOT stop the rest of the run.
"""
import os
import logging
from datetime import datetime
from typing import Optional

from db.connection import get_connection
from db.assignment_repo import get_active_assignments
from db.rule_repo import get_rule_by_id
from db.asset_repo import get_table_name, get_qualified_name, get_parent_table_for_column
from engine.expression_builder import build_sql, build_row_count_sql
from models.dq_result import DQResult
from models.dq_rule import DQRule
from models.dq_rule_assignment import DQRuleAssignment

logger = logging.getLogger(__name__)




def _normalize_table_name(raw_name: str, asset_type: str) -> Optional[str]:
    """
    Builds a correctly quoted SQL table reference from qualified_name.

    Compares the server in qualified_name against DB_SERVER in .env:
        - Same server  → 3-part name: [database].[schema].[table]
        - Diff server  → 4-part name: [server].[database].[schema].[table]
                         (requires linked server configured in SSMS)
    """
    if not raw_name:
        return None

    parts = [p.strip() for p in raw_name.split(".") if p.strip()]

    # Local server from .env — what we are currently connected to
    local_server = os.getenv("DB_SERVER", "").strip()

    if asset_type in ("PLATFORM", "DATABASE"):
        return None  # not directly queryable

    elif asset_type == "SCHEMA":
        # parts: server(s).database.schema
        asset_server = ".".join(parts[:-2])
        database     = parts[-2]
        schema       = parts[-1]

        if asset_server == local_server:
            return f"[{database}].[{schema}]"
        else:
            return f"[{asset_server}].[{database}].[{schema}]"

    elif asset_type == "TABLE":
        # parts: server(s).database.schema.table
        asset_server = ".".join(parts[:-3])
        database     = parts[-3]
        schema       = parts[-2]
        table        = parts[-1]

        if asset_server == local_server:
            # Same server — 3-part name, no server prefix needed
            return f"[{database}].[{schema}].[{table}]"
        else:
            # Different server — 4-part name, needs linked server in SSMS
            logger.info(
                f"Cross-server reference detected: "
                f"asset server={asset_server}, local server={local_server}. "
                f"Using 4-part name — ensure linked server is configured in SSMS."
            )
            return f"[{asset_server}].[{database}].[{schema}].[{table}]"

    elif asset_type == "COLUMN":
        # parts: server(s).database.schema.table.column
        # drop column — used separately as column_name
        asset_server = ".".join(parts[:-4])
        database     = parts[-4]
        schema       = parts[-3]
        table        = parts[-2]

        if asset_server == local_server:
            return f"[{database}].[{schema}].[{table}]"
        else:
            logger.info(
                f"Cross-server reference detected: "
                f"asset server={asset_server}, local server={local_server}. "
                f"Using 4-part name — ensure linked server is configured in SSMS."
            )
            return f"[{asset_server}].[{database}].[{schema}].[{table}]"

    else:
        # Unknown type — best effort treat as TABLE
        asset_server = ".".join(parts[:-3])
        database     = parts[-3]
        schema       = parts[-2]
        table        = parts[-1]

        if asset_server == local_server:
            return f"[{database}].[{schema}].[{table}]"
        else:
            return f"[{asset_server}].[{database}].[{schema}].[{table}]"

def _resolve_threshold(assignment: DQRuleAssignment, rule: DQRule) -> tuple[Optional[float], Optional[str]]:
    """
    Determine the effective threshold value and operator for an assignment.
    Assignment overrides take priority over rule defaults.

    Args:
        assignment: The assignment with possible overrides.
        rule: The base rule with defaults.

    Returns:
        tuple: (threshold_value, threshold_operator)
    """
    threshold_value = (
        assignment.threshold_value_override
        if assignment.threshold_value_override is not None
        else rule.default_threshold_value
    )
    threshold_operator = (
        assignment.threshold_operator_override
        if assignment.threshold_operator_override is not None
        else rule.threshold_operator
    )
    return threshold_value, threshold_operator


def _evaluate_threshold(
    observed_value: float,
    threshold_value: Optional[float],
    threshold_operator: Optional[str],
) -> str:
    """
    Compare observed_value against threshold to determine PASSED or FAILED.

    Args:
        observed_value: The numeric result from the rule execution.
        threshold_value: The benchmark to compare against.
        threshold_operator: The comparison operator (>=, <=, =, !=, >, <).

    Returns:
        str: 'PASSED' or 'FAILED'.
    """
    if threshold_value is None or threshold_operator is None:
        # No threshold defined — treat as PASSED if we got a valid value
        return "PASSED"

    try:
        op = threshold_operator.strip()
        if op == ">=":
            return "PASSED" if observed_value >= threshold_value else "FAILED"
        elif op == "<=":
            return "PASSED" if observed_value <= threshold_value else "FAILED"
        elif op == "=":
            return "PASSED" if observed_value == threshold_value else "FAILED"
        elif op == "!=":
            return "PASSED" if observed_value != threshold_value else "FAILED"
        elif op == ">":
            return "PASSED" if observed_value > threshold_value else "FAILED"
        elif op == "<":
            return "PASSED" if observed_value < threshold_value else "FAILED"
        else:
            logger.warning(f"Unknown threshold operator '{op}' — defaulting to PASSED.")
            return "PASSED"
    except Exception as e:
        logger.error(f"Error evaluating threshold: {e}")
        return "FAILED"


def _build_result_message(
    rule: DQRule,
    result_status: str,
    observed_value: float,
    threshold_value: Optional[float],
    threshold_operator: Optional[str],
    table_name: str,
    column_name: Optional[str],
    rows_checked: Optional[int],
    failed_row_count: Optional[int],
) -> str:
    """
    Build a human-readable result message for notifications and dashboards.

    Args:
        rule: The rule definition.
        result_status: PASSED or FAILED.
        observed_value: The computed value.
        threshold_value: The benchmark.
        threshold_operator: The operator.
        table_name: The table name.
        column_name: The column name (or None).
        rows_checked: Total rows checked.
        failed_row_count: Number of failed rows.

    Returns:
        str: A descriptive result message.
    """
    target = f"{table_name}.{column_name}" if column_name else table_name
    msg = f"{result_status}: {rule.rule_name} on {target}. "
    msg += f"Observed: {observed_value}"
    if threshold_value is not None:
        msg += f", Threshold: {threshold_operator} {threshold_value}"
    if rows_checked is not None and failed_row_count is not None:
        pass_pct = ((rows_checked - failed_row_count) / rows_checked * 100) if rows_checked > 0 else 0
        msg += f". {rows_checked} rows checked, {failed_row_count} failed ({pass_pct:.2f}% pass rate)"
    return msg


def execute_all(run_id: int) -> list[DQResult]:
    """
    Execute all active DQ rule assignments and return results.
    Each rule is wrapped in try/except — a single failure does not stop the run.

    Args:
        run_id: The dq_run_id for this execution.

    Returns:
        list[DQResult]: One result per assignment executed.
    """
    assignments = get_active_assignments()
    logger.info(f"Fetched {len(assignments)} active assignments for run {run_id}.")

    results: list[DQResult] = []

    for assignment in assignments:
        result = _execute_single_assignment(run_id, assignment)
        results.append(result)

    passed = sum(1 for r in results if r.result_status == "PASSED")
    failed = sum(1 for r in results if r.result_status == "FAILED")
    errors = sum(1 for r in results if r.result_status == "ERROR")
    logger.info(
        f"Execution complete: {len(results)} rules — "
        f"{passed} passed, {failed} failed, {errors} errors."
    )

    return results


def _execute_single_assignment(run_id: int, assignment: DQRuleAssignment) -> DQResult:
    """
    Execute a single rule assignment and return the result.
    Wrapped in try/except — returns ERROR status on exception.

    Args:
        run_id: The current run ID.
        assignment: The rule-to-asset mapping to execute.

    Returns:
        DQResult: The outcome of this execution.
    """
    now = datetime.now()

    try:
        # ── Fetch rule definition ──
        rule = get_rule_by_id(assignment.dq_rule_id)
        if rule is None:
            logger.error(f"Rule ID {assignment.dq_rule_id} not found.")
            return DQResult(
                dq_run_id=run_id,
                dq_rule_assignment_id=assignment.dq_rule_assignment_id,
                dq_rule_id=assignment.dq_rule_id,
                asset_id=assignment.asset_id,
                column_asset_id=assignment.column_asset_id,
                result_status="ERROR",
                result_message=f"Rule ID {assignment.dq_rule_id} not found in DQ_RULE.",
                executed_at=now,
            )

                # ── Resolve table and column names ──
        # get_qualified_name now returns (qualified_name, asset_type) tuple
        qualified_name, asset_type = get_qualified_name(assignment.asset_id)

        if qualified_name:
            table_name = _normalize_table_name(qualified_name, asset_type)
        else:
            # fallback to plain asset_name if qualified_name is missing
            table_name = get_table_name(assignment.asset_id)

        if table_name is None:
            table_name = f"ASSET_{assignment.asset_id}"

        column_name = None
        if assignment.column_asset_id is not None:
            # column_name is just the plain name — not the full qualified path
            column_name = get_table_name(assignment.column_asset_id)

            # For column-level rules, get the parent TABLE's qualified name
            # because the SQL runs against the TABLE not the column
            parent = get_parent_table_for_column(assignment.column_asset_id)
            if parent:
                parent_qualified = parent.get("qualified_name")
                parent_type      = parent.get("asset_type", "TABLE")
                if parent_qualified:
                    table_name = _normalize_table_name(parent_qualified, parent_type) or table_name

        # ── Build and execute the SQL query ──
        sql = build_sql(assignment, rule, table_name, column_name)
        logger.info(f"Executing: {sql}")

        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            observed_value = float(row[0]) if row and row[0] is not None else 0.0

            # ── Get rows_checked for context ──
            rows_checked = None
            try:
                count_sql = build_row_count_sql(table_name, assignment.filter_condition)
                cursor.execute(count_sql)
                count_row = cursor.fetchone()
                rows_checked = int(count_row[0]) if count_row and count_row[0] is not None else 0
            except Exception:
                pass  # Non-critical — rows_checked is informational
        finally:
            conn.close()

        # ── Evaluate threshold ──
        threshold_value, threshold_operator = _resolve_threshold(assignment, rule)
        result_status = _evaluate_threshold(observed_value, threshold_value, threshold_operator)

        # ── Compute row counts ──
        # For "count of bad rows = 0" style rules, observed_value IS the failed count.
        # For "pass rate >= threshold" style rules, derive failed count from rows_checked.
        if rows_checked and rows_checked > 0:
            if threshold_operator in ("=", "<=", "<") and rule.default_threshold_value == 0:
                # observed_value is the count of failing rows (e.g. null count)
                failed_row_count = int(observed_value)
            elif threshold_operator in (">=", ">") and observed_value <= 1.0:
                # observed_value is a pass rate (0-1 or 0-100); derive failed count
                rate = observed_value if observed_value <= 1.0 else observed_value / 100.0
                failed_row_count = int(rows_checked * (1.0 - rate))
            else:
                failed_row_count = None
        else:
            failed_row_count = None

        passed_row_count = (rows_checked - failed_row_count) if rows_checked and failed_row_count is not None else None
        pass_percentage = (
            (passed_row_count / rows_checked * 100)
            if rows_checked and passed_row_count is not None and rows_checked > 0
            else None
        )

        # ── Build result message ──
        result_message = _build_result_message(
            rule, result_status, observed_value,
            threshold_value, threshold_operator,
            table_name, column_name,
            rows_checked, failed_row_count,
        )

        # ── Compute confidence score ──
        confidence_score = min(1.0, (rows_checked / 1000.0)) if rows_checked and rows_checked > 0 else 0.5

        return DQResult(
            dq_run_id=run_id,
            dq_rule_assignment_id=assignment.dq_rule_assignment_id,
            dq_rule_id=assignment.dq_rule_id,
            asset_id=assignment.asset_id,
            column_asset_id=assignment.column_asset_id,
            result_status=result_status,
            threshold_value_applied=threshold_value,
            threshold_operator_applied=threshold_operator,
            observed_value=observed_value,
            pass_percentage=pass_percentage,
            rows_checked=rows_checked,
            passed_row_count=passed_row_count,
            failed_row_count=failed_row_count,
            result_message=result_message,
            confidence_score=confidence_score,
            executed_at=now,
        )

    except Exception as e:
        logger.error(
            f"ERROR executing assignment {assignment.dq_rule_assignment_id} "
            f"(rule {assignment.dq_rule_id}): {e}"
        )
        return DQResult(
            dq_run_id=run_id,
            dq_rule_assignment_id=assignment.dq_rule_assignment_id,
            dq_rule_id=assignment.dq_rule_id,
            asset_id=assignment.asset_id,
            column_asset_id=assignment.column_asset_id,
            result_status="ERROR",
            result_message=f"Execution error: {str(e)[:500]}",
            executed_at=now,
        )
