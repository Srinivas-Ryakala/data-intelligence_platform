"""
Repository for DQ_RULE_ASSIGNMENT table.
Manages rule-to-asset mappings and their execution configuration.
"""

import logging
from datetime import datetime
from typing import Optional

from db.connection import get_connection
from models.dq_rule_assignment import DQRuleAssignment

logger = logging.getLogger(__name__)


def get_active_assignments() -> list[DQRuleAssignment]:
    """
    Fetch all active assignments with joined context from DQ_RULE and DATA_ASSET.

    Returns:
        list[DQRuleAssignment]: All active rule-to-asset mappings.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.*
            FROM DQ_RULE_ASSIGNMENT a
            WHERE a.is_active = 1
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [DQRuleAssignment(**dict(zip(columns, row))) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch active assignments: {e}")
        return []


def assignment_exists(
    dq_rule_id: int,
    asset_id: int,
    column_asset_id: Optional[int] = None
) -> bool:
    """
    Check if an ACTIVE assignment already exists for the given rule + asset + column.

    Args:
        dq_rule_id: The rule ID.
        asset_id: The table-level asset ID.
        column_asset_id: The column-level asset ID (None for table-level rules).

    Returns:
        bool: True if an active assignment already exists.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if column_asset_id is None:
            cursor.execute(
                """
                SELECT COUNT(*) FROM DQ_RULE_ASSIGNMENT
                WHERE dq_rule_id = ?
                  AND asset_id = ?
                  AND column_asset_id IS NULL
                  AND is_active = 1
                """,
                dq_rule_id, asset_id
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM DQ_RULE_ASSIGNMENT
                WHERE dq_rule_id = ?
                  AND asset_id = ?
                  AND column_asset_id = ?
                  AND is_active = 1
                """,
                dq_rule_id, asset_id, column_asset_id
            )

        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        logger.error(f"Failed to check assignment existence: {e}")
        return False


def insert_assignment(assignment: DQRuleAssignment) -> Optional[int]:
    """
    Insert a new assignment into DQ_RULE_ASSIGNMENT.

    Args:
        assignment: The DQRuleAssignment object to insert.

    Returns:
        int or None: The new dq_rule_assignment_id, or None on error.
    """
    try:
        now = datetime.now()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO DQ_RULE_ASSIGNMENT (
                dq_rule_id, asset_id, column_asset_id, platform_id,
                assignment_scope, execution_mode, execution_frequency,
                threshold_value_override, threshold_operator_override,
                filter_condition, business_context, owner_name,
                is_mandatory, is_active, created_by,
                created_at, updated_at
            )
            OUTPUT INSERTED.dq_rule_assignment_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            assignment.dq_rule_id,
            assignment.asset_id,
            assignment.column_asset_id,
            assignment.platform_id,
            assignment.assignment_scope,
            assignment.execution_mode,
            assignment.execution_frequency,
            assignment.threshold_value_override,
            assignment.threshold_operator_override,
            assignment.filter_condition,
            assignment.business_context,
            assignment.owner_name,
            assignment.is_mandatory,
            assignment.is_active,
            assignment.created_by,
            assignment.created_at or now,
            assignment.updated_at or now,
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        logger.info(f"Inserted assignment ID {new_id} (rule={assignment.dq_rule_id}, asset={assignment.asset_id}).")
        return new_id
    except Exception as e:
        logger.error(f"Failed to insert assignment: {e}")
        return None


def deactivate_assignment(dq_rule_assignment_id: int) -> bool:
    """
    Soft-delete an assignment by setting is_active = False.

    Args:
        dq_rule_assignment_id: The assignment to deactivate.

    Returns:
        bool: True if successfully deactivated.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE DQ_RULE_ASSIGNMENT
            SET is_active = 0, updated_at = ?
            WHERE dq_rule_assignment_id = ?
            """,
            datetime.now(), dq_rule_assignment_id
        )
        conn.commit()
        conn.close()
        logger.info(f"Deactivated assignment ID {dq_rule_assignment_id}.")
        return True
    except Exception as e:
        logger.error(f"Failed to deactivate assignment {dq_rule_assignment_id}: {e}")
        return False


def get_assignment_by_id(dq_rule_assignment_id: int) -> Optional[DQRuleAssignment]:
    """
    Fetch a single DQ_RULE_ASSIGNMENT by its primary key.

    Args:
        dq_rule_assignment_id: The assignment to look up.

    Returns:
        DQRuleAssignment or None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM DQ_RULE_ASSIGNMENT WHERE dq_rule_assignment_id = ?",
            dq_rule_assignment_id
        )
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        data = dict(zip(columns, row))
        return DQRuleAssignment(**{k: v for k, v in data.items() if k in DQRuleAssignment.__dataclass_fields__})
    except Exception as e:
        logger.error(f"Failed to fetch assignment {dq_rule_assignment_id}: {e}")
        return None


def list_active_assignments_summary() -> list[dict]:
    """
    Fetch a summary of all active assignments with rule and asset names.
    Used by the CLI for display.

    Returns:
        list[dict]: Active assignments with rule_code, asset_name, etc.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                a.dq_rule_assignment_id,
                r.rule_code,
                r.rule_name,
                da.asset_name AS table_name,
                ca.asset_name AS column_name,
                a.created_at
            FROM DQ_RULE_ASSIGNMENT a
            JOIN DQ_RULE r ON a.dq_rule_id = r.dq_rule_id
            LEFT JOIN DATA_ASSET da ON a.asset_id = da.asset_id
            LEFT JOIN DATA_ASSET ca ON a.column_asset_id = ca.asset_id
            WHERE a.is_active = 1
            ORDER BY a.dq_rule_assignment_id
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to list active assignments: {e}")
        return []


def deactivate_all_assignments() -> int:
    """
    Deactivate ALL active assignments. Used for a clean reset before re-assigning.

    Returns:
        int: Number of assignments deactivated.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE DQ_RULE_ASSIGNMENT
            SET is_active = 0, updated_at = GETDATE()
            WHERE is_active = 1
            """
        )
        count = cursor.rowcount
        conn.commit()
        conn.close()
        logger.info(f"Deactivated {count} assignments.")
        return count
    except Exception as e:
        logger.error(f"Failed to deactivate all assignments: {e}")
        return 0
