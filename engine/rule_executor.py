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
from engine.expression_builder import build_sql, build_row_count_sql, build_sample_sql
from models.dq_result import DQResult
from models.dq_rule import DQRule
from models.dq_rule_assignment import DQRuleAssignment
logger = logging.getLogger(__name__)

def _normalize_table_name(raw_name: str, asset_type: str) -> Optional[str]:
    """
    Builds a correctly quoted SQL table reference from qualified_name.
    Same server  → 3-part name: [database].[schema].[table]
    Diff server  → 4-part name: [server].[database].[schema].[table]
                   (requires linked server configured in SSMS)
    """
    if not raw_name:
        return None
    parts = [p.strip() for p in raw_name.split(".") if p.strip()]
    local_server = os.getenv("DB_SERVER", "").strip()
    if asset_type in ("PLATFORM", "DATABASE"):
        return None
    elif asset_type == "SCHEMA":
        asset_server = ".".join(parts[:-2])
        database     = parts[-2]
        schema       = parts[-1]
        if asset_server == local_server:
            return f"[{database}].[{schema}]"
        else:
            return f"[{asset_server}].[{database}].[{schema}]"
    elif asset_type == "TABLE":
        asset_server = ".".join(parts[:-3])
        database     = parts[-3]
        schema       = parts[-2]
        table        = parts[-1]
        if asset_server == local_server:
            return f"[{database}].[{schema}].[{table}]"
        else:
            logger.info(
                f"Cross-server reference detected: "
                f"asset server={asset_server}, local server={local_server}. "
                f"Using 4-part name — ensure linked server is configured."
            )
            return f"[{asset_server}].[{database}].[{schema}].[{table}]"
    elif asset_type == "COLUMN":
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
                f"Using 4-part name — ensure linked server is configured."
            )
            return f"[{asset_server}].[{database}].[{schema}].[{table}]"
    else:
        # Unknown type — best-effort treat as TABLE
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
    Determine the effective threshold value and operator.
    Assignment overrides take priority over rule defaults.
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
    Compare observed_value against threshold → PASSED or FAILED.
    """
    if threshold_value is None or threshold_operator is None:
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

def _compute_row_counts(
    rule: DQRule,
    assignment: DQRuleAssignment,
    observed_value: float,
    rows_checked: Optional[int],
    threshold_value: Optional[float],
    threshold_operator: Optional[str],
) -> tuple[Optional[int], Optional[int]]:
    """
    Derive failed_row_count and passed_row_count from the observed value.
    There are two patterns:
      A) observed_value IS the count of bad rows (rules where threshold=0, op in <=/<)
         e.g. COMP_NULL_CHK → observed_value = null_count → failed = null_count
      B) observed_value IS a pass-rate (rules where op in >=/>)
         e.g. COMP_RATIO → observed_value = 85.0 (percent) or 0.85 (ratio)
         Normalise to 0-1 fraction, then failed = rows_checked * (1 - rate)
    Returns:
        (failed_row_count, passed_row_count)
    """
    if not rows_checked or rows_checked <= 0:
        return None, None
    # Pattern A: observed_value is directly the failed-row count
    if threshold_operator in ("=", "<=", "<") and (
        threshold_value is None or threshold_value == 0
    ):
        failed = int(observed_value)
        passed = rows_checked - failed
        return max(failed, 0), max(passed, 0)
    # Pattern B: observed_value is a pass rate
    if threshold_operator in (">=", ">"):
        if 0.0 <= observed_value <= 1.0:
            # Already a 0-1 fraction
            rate = observed_value
        elif 1.0 < observed_value <= 100.0:
            # Percentage 0-100 → convert to fraction
            rate = observed_value / 100.0
        else:
            # Can't reliably interpret this as a rate
            return None, None
        failed = int(rows_checked * (1.0 - rate))
        passed = rows_checked - failed
        return max(failed, 0), max(passed, 0)
    return None, None

def _fetch_sample_failed_values(
    rule_code: str,
    table_name: str,
    column_name: Optional[str],
) -> Optional[str]:
    """
    Run a TOP-5 query to get sample failing values for display in DQ_RESULT.
    Non-critical — failure returns None.
    """
    sample_sql = build_sample_sql(rule_code, table_name, column_name)
    if not sample_sql:
        return None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sample_sql)
        rows = cursor.fetchall()
        conn.close()
        values = [str(r[0]) for r in rows if r and r[0] is not None]
        if values:
            return ", ".join(values[:5])
        return None
    except Exception as e:
        logger.debug(f"Sample failed-value query failed for {rule_code}: {e}")
        return None

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
    target = f"{table_name}.{column_name}" if column_name else table_name
    msg = f"{result_status}: {rule.rule_name} on {target}. "
    msg += f"Observed: {observed_value}"
    if threshold_value is not None:
        msg += f", Threshold: {threshold_operator} {threshold_value}"
    if rows_checked is not None and failed_row_count is not None:
        pass_pct = (
            (rows_checked - failed_row_count) / rows_checked * 100
            if rows_checked > 0 else 0
        )
        msg += (
            f". {rows_checked} rows checked, "
            f"{failed_row_count} failed ({pass_pct:.2f}% pass rate)"
        )
    return msg

def execute_all(run_id: int) -> list[DQResult]:
    """
    Execute all active DQ rule assignments and return results.
    Each rule is wrapped in try/except — a single failure does not stop the run.
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
    """
    now = datetime.now()
    # Initialise sql here so the except block can always log it safely.
    sql = "N/A"
    try:
        # ── Fetch rule definition ──────────────────────────────────────────
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
        # ── Resolve table and column names ─────────────────────────────────
        qualified_name, asset_type = get_qualified_name(assignment.asset_id)
        if qualified_name:
            table_name = _normalize_table_name(qualified_name, asset_type)
        else:
            table_name = get_table_name(assignment.asset_id)
        if table_name is None:
            table_name = f"ASSET_{assignment.asset_id}"
        column_name = None
        if assignment.column_asset_id is not None:
            column_name = get_table_name(assignment.column_asset_id)
            parent = get_parent_table_for_column(assignment.column_asset_id)
            if parent:
                parent_qualified = parent.get("qualified_name")
                parent_type      = parent.get("asset_type", "TABLE")
                if parent_qualified:
                    table_name = _normalize_table_name(parent_qualified, parent_type) or table_name
        # ── Build and execute the SQL query ───────────────────────────────
        sql = build_sql(assignment, rule, table_name, column_name)
        logger.info(f"Executing: {sql[:300]}")
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            # NULL observed_value (e.g. unsupported rules) → treat as 0
            observed_value = float(row[0]) if row and row[0] is not None else 0.0
            # ── Row count for context ──────────────────────────────────────
            rows_checked = None
            try:
                count_sql = build_row_count_sql(table_name, assignment.filter_condition)
                cursor.execute(count_sql)
                count_row = cursor.fetchone()
                rows_checked = int(count_row[0]) if count_row and count_row[0] is not None else 0
            except Exception:
                pass  # Non-critical
        finally:
            conn.close()
        # ── Evaluate threshold ─────────────────────────────────────────────
        threshold_value, threshold_operator = _resolve_threshold(assignment, rule)
        result_status = _evaluate_threshold(observed_value, threshold_value, threshold_operator)
        # ── Compute row counts ─────────────────────────────────────────────
        failed_row_count, passed_row_count = _compute_row_counts(
            rule, assignment, observed_value, rows_checked,
            threshold_value, threshold_operator,
        )
        pass_percentage = (
            (passed_row_count / rows_checked * 100)
            if rows_checked and passed_row_count is not None and rows_checked > 0
            else None
        )
        # ── Fetch sample failing values ────────────────────────────────────
        sample_failed_value = None
        if result_status == "FAILED" and failed_row_count and failed_row_count > 0:
            sample_failed_value = _fetch_sample_failed_values(
                rule.rule_code, table_name, column_name
            )
        # ── Build result message ───────────────────────────────────────────
        result_message = _build_result_message(
            rule, result_status, observed_value,
            threshold_value, threshold_operator,
            table_name, column_name,
            rows_checked, failed_row_count,
        )
        # ── Confidence score ───────────────────────────────────────────────
        confidence_score = (
            min(1.0, rows_checked / 1000.0)
            if rows_checked and rows_checked > 0 else 0.5
        )
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
            sample_failed_value=sample_failed_value,
            result_message=result_message,
            confidence_score=confidence_score,
            executed_at=now,
        )
    except Exception as e:
        logger.error(
            f"ERROR executing assignment {assignment.dq_rule_assignment_id} "
            f"(rule {assignment.dq_rule_id}): {e}"
        )
        logger.error(f"FAILED SQL: {sql}")
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