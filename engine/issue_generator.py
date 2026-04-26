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


def generate_issues(results: list[DQResult]) -> list[DQIssue]:
    """
    Generate DQ_ISSUE objects for failed mandatory rule results.
    Checks for recurring issues by querying prior failures.

    Args:
        results: List of DQResult objects from the current run.

    Returns:
        list[DQIssue]: Issue objects to be inserted into DQ_ISSUE.
    """
    if not results:
        logger.info("No results to generate issues from.")
        return []

    # Build a lookup of assignments by ID for quick access
    assignments = get_active_assignments()
    assignment_map: dict[int, DQRuleAssignment] = {
        a.dq_rule_assignment_id: a for a in assignments
    }

    issues: list[DQIssue] = []
    now = datetime.now()

    for result in results:
        # Only process FAILED results
        if result.result_status != "FAILED":
            continue

        # Look up the assignment to check is_mandatory
        assignment = assignment_map.get(result.dq_rule_assignment_id)
        if assignment is None:
            logger.warning(
                f"Assignment {result.dq_rule_assignment_id} not found — skipping issue."
            )
            continue

        if not assignment.is_mandatory:
            logger.info(
                f"Result {result.dq_result_id} failed but assignment is not mandatory — no issue."
            )
            continue

        # Look up the rule for severity and metadata
        rule = get_rule_by_id(result.dq_rule_id)
        if rule is None:
            logger.warning(f"Rule {result.dq_rule_id} not found — skipping issue.")
            continue

        # Check for recurrence
        is_recurring = _check_recurrence(result.dq_rule_id, result.asset_id)

        # Build issue code: ISS-{dimension_short}-{result_id}
        dimension_short = rule.rule_dimension[:4].upper() if rule.rule_dimension else "UNKN"
        issue_code = f"ISS-{dimension_short}-{result.dq_result_id or 'NEW'}"

        # Build issue title
        table_name = get_table_name(result.asset_id) or f"Asset_{result.asset_id}"
        column_name = get_table_name(result.column_asset_id) if result.column_asset_id else None
        target = f"{table_name}.{column_name}" if column_name else table_name
        issue_title = f"{rule.rule_name} failed on {target}"

        # Build issue description
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

        issue = DQIssue(
            dq_result_id=result.dq_result_id,
            asset_id=result.asset_id,
            column_asset_id=result.column_asset_id,
            issue_code=issue_code,
            issue_title=issue_title,
            issue_description=issue_description,
            severity=rule.severity,
            issue_status="OPEN",
            root_cause_category="UNKNOWN",
            assigned_to=assignment.owner_name,
            reported_by="DQ_ENGINE",
            opened_at=now,
            is_recurring=is_recurring,
            created_at=now,
            updated_at=now,
        )

        issues.append(issue)
        logger.info(
            f"Generated issue: {issue_title} (recurring={is_recurring})"
        )

    logger.info(f"Generated {len(issues)} issues from {len(results)} results.")
    return issues


def _check_recurrence(dq_rule_id: int, asset_id: Optional[int]) -> bool:
    """
    Check if the same rule+asset combination has failed before.

    Args:
        dq_rule_id: The rule that failed.
        asset_id: The asset where it failed.

    Returns:
        bool: True if prior issues exist for this combo.
    """
    if asset_id is None:
        return False

    try:
        prior_issues = find_prior_issues(dq_rule_id, asset_id)
        return len(prior_issues) > 0
    except Exception as e:
        logger.error(f"Error checking recurrence for rule {dq_rule_id}, asset {asset_id}: {e}")
        return False
