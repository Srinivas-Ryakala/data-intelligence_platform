"""
Repository for DQ_ISSUE table.
Manages issue lifecycle — creation, querying, and recurring detection.
"""

import logging
from datetime import datetime
from typing import Optional

from db.connection import get_connection
from models.dq_issue import DQIssue

logger = logging.getLogger(__name__)


def insert_issue(issue: DQIssue) -> Optional[int]:
    """
    Insert a new issue into DQ_ISSUE.

    Args:
        issue: The DQIssue object to insert.

    Returns:
        int or None: The new dq_issue_id, or None on error.
    """
    try:
        now = datetime.now()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO DQ_ISSUE (
                dq_result_id, asset_id, column_asset_id,
                issue_code, issue_title, issue_description,
                severity, issue_status, root_cause_category,
                assigned_to, reported_by,
                opened_at, acknowledged_at, resolved_at,
                resolution_notes, is_recurring,
                created_at, updated_at
            )
            OUTPUT INSERTED.dq_issue_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            issue.dq_result_id,
            issue.asset_id,
            issue.column_asset_id,
            issue.issue_code,
            issue.issue_title,
            issue.issue_description,
            issue.severity,
            issue.issue_status,
            issue.root_cause_category,
            issue.assigned_to,
            issue.reported_by,
            issue.opened_at or now,
            issue.acknowledged_at,
            issue.resolved_at,
            issue.resolution_notes,
            issue.is_recurring,
            issue.created_at or now,
            issue.updated_at or now,
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        logger.info(f"Inserted DQ_ISSUE ID {new_id}: {issue.issue_title}")
        return new_id
    except Exception as e:
        logger.error(f"Failed to insert DQ_ISSUE: {e}")
        return None


def get_open_issues() -> list[DQIssue]:
    """
    Fetch all issues with status OPEN or ACKNOWLEDGED.

    Returns:
        list[DQIssue]: Open or acknowledged issues.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM DQ_ISSUE
            WHERE issue_status IN ('OPEN', 'ACKNOWLEDGED', 'IN_PROGRESS')
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [DQIssue(**dict(zip(columns, row))) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch open issues: {e}")
        return []


def get_recurring_issues() -> list[DQIssue]:
    """
    Fetch all issues where is_recurring = True.

    Returns:
        list[DQIssue]: Issues that have recurred across runs.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM DQ_ISSUE WHERE is_recurring = 1"
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [DQIssue(**dict(zip(columns, row))) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch recurring issues: {e}")
        return []


def find_prior_issues(dq_rule_id: int, asset_id: int) -> list[DQIssue]:
    """
    Find prior issues with the same rule + asset combination, used to detect recurrence.

    Args:
        dq_rule_id: The rule that failed.
        asset_id: The asset where it failed.

    Returns:
        list[DQIssue]: Any prior issues for this rule+asset combo.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT i.*
            FROM DQ_ISSUE i
            JOIN DQ_RESULT r ON i.dq_result_id = r.dq_result_id
            WHERE r.dq_rule_id = ?
              AND i.asset_id = ?
            """,
            dq_rule_id, asset_id
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [DQIssue(**dict(zip(columns, row))) for row in rows]
    except Exception as e:
        logger.error(f"Failed to find prior issues for rule {dq_rule_id}, asset {asset_id}: {e}")
        return []
