"""
Issue generator — creates DQ_ISSUE records for failed mandatory rule assignments.
Detects recurring issues by checking for prior failures of the same rule+asset combo.
"""
import logging
from datetime import datetime
from typing import Optional
from db.assignment_repo import get_active_assignments
from db.rule_repo import get_rule_by_id
from db.asset_repo import get_table_name
from db.issue_repo import find_prior_issues
from models.dq_result import DQResult
from models.dq_issue import DQIssue
from models.dq_rule_assignment import DQRuleAssignment
logger = logging.getLogger(__name__)
# Statuses that should trigger issue creation.
# Include ERROR here because main.py normalises ERROR→FAILED before calling us,
# but being explicit makes this module safe to call independently too.
_FAILURE_STATUSES = {"FAILED", "ERROR"}

def generate_issues(results: list[DQResult]) -> list[DQIssue]:
    """
    Generate DQ_ISSUE objects for failed/errored mandatory rule results.
    Args:
        results: List of DQResult objects from the current run.
                 dq_result_id MUST be populated before calling this
                 (call result_repo.populate_result_ids first).
    Returns:
        list[DQIssue]: Issue objects ready to insert into DQ_ISSUE.
    """
    if not results:
        logger.info("No results to generate issues from.")
        return []
    # Build assignment lookup
    assignments = get_active_assignments()
    assignment_map: dict[int, DQRuleAssignment] = {
        a.dq_rule_assignment_id: a for a in assignments
    }
    issues: list[DQIssue] = []
    now = datetime.now()
    for result in results:
        # Only process failures (FAILED or ERROR)
        if result.result_status not in _FAILURE_STATUSES:
            continue
        # Guard: dq_result_id must exist so the FK in DQ_ISSUE is valid
        if result.dq_result_id is None:
            logger.warning(
                f"result for assignment {result.dq_rule_assignment_id} has no "
                f"dq_result_id — skipping issue creation. "
                f"Ensure populate_result_ids() was called before generate_issues()."
            )
            continue
        assignment = assignment_map.get(result.dq_rule_assignment_id)
        if assignment is None:
            logger.warning(
                f"Assignment {result.dq_rule_assignment_id} not found — skipping."
            )
            continue
        rule = get_rule_by_id(result.dq_rule_id)
        if rule is None:
            logger.warning(f"Rule {result.dq_rule_id} not found — skipping.")
            continue
        if not assignment.is_mandatory:
            # Non-mandatory rules still generate advisory issues for Critical/High
            # severity so failures are visible without blocking the pipeline.
            if rule.severity not in ("Critical", "High"):
                continue
            issue_status = "ADVISORY"
        else:
            issue_status = "OPEN"
        # Recurrence check
        is_recurring = _check_recurrence(result.dq_rule_id, result.asset_id)
        # Issue code uses the real dq_result_id now (not 'NEW')
        dimension_short = (
            rule.rule_dimension[:4].upper()
            if rule.rule_dimension else "UNKN"
        )
        issue_code = f"ISS-{dimension_short}-{result.dq_result_id}"
        # Target resolution
        table_name = get_table_name(result.asset_id) or f"Asset_{result.asset_id}"
        column_name = (
            get_table_name(result.column_asset_id)
            if result.column_asset_id
            else None
        )
        target = f"{table_name}.{column_name}" if column_name else table_name
        # Title & description
        issue_title = f"{rule.rule_name} failed on {target}"
        issue_description = (
            f"{result.result_message or ''}\n\n"
            f"Rule: {rule.rule_code} ({rule.rule_name})\n"
            f"Dimension: {rule.rule_dimension}\n"
            f"Severity: {rule.severity}\n"
            f"Target: {target}\n"
        )
        if result.observed_value is not None:
            issue_description += f"Observed value: {result.observed_value}\n"
        if result.failed_row_count is not None:
            issue_description += f"Failed rows: {result.failed_row_count}\n"
        if result.sample_failed_value:
            issue_description += f"Sample failing values: {result.sample_failed_value}\n"
        issue = DQIssue(
            dq_result_id=result.dq_result_id,
            asset_id=result.asset_id,
            column_asset_id=result.column_asset_id,
            issue_code=issue_code,
            issue_title=issue_title,
            issue_description=issue_description,
            severity=rule.severity,
            issue_status=issue_status,
            root_cause_category="UNKNOWN",
            assigned_to=assignment.owner_name,
            reported_by="DQ_ENGINE",
            opened_at=now,
            is_recurring=is_recurring,
            created_at=now,
            updated_at=now,
        )
        issues.append(issue)
    logger.info(f"Generated {len(issues)} issues from {len(results)} results.")
    return issues

def _check_recurrence(dq_rule_id: int, asset_id: Optional[int]) -> bool:
    """Check if the same rule+asset combination has failed before."""
    if asset_id is None:
        return False
    try:
        prior_issues = find_prior_issues(dq_rule_id, asset_id)
        return len(prior_issues) > 0
    except Exception as e:
        logger.error(
            f"Error checking recurrence for rule {dq_rule_id}, asset {asset_id}: {e}"
        )
        return False