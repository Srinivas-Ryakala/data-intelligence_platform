"""
Repository for DQ_RUN table.
Manages execution lifecycle — create with RUNNING, update on completion.
"""

import logging
from datetime import datetime
from typing import Optional

from db.connection import get_connection
from models.dq_run import DQRun

logger = logging.getLogger(__name__)


def create_run(run: DQRun) -> Optional[int]:
    """
    Insert a new DQ_RUN row with status=RUNNING. Returns the new dq_run_id.

    Args:
        run: The DQRun object to insert.

    Returns:
        int or None: The new dq_run_id, or None on error.
    """
    print("platform id:", run.platform_id)
    try:
        now = datetime.now()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO DQ_RUN (
                platform_id, pipeline_id, parse_run_id,
                run_name, run_type, run_status,
                triggered_by, trigger_source, environment_name,
                started_at, ended_at,
                total_rules_executed, total_passed, total_failed, total_warned,
                run_summary, error_message,
                created_at, updated_at
            )
            OUTPUT INSERTED.dq_run_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            run.platform_id,
            run.pipeline_id,
            run.parse_run_id,
            run.run_name,
            run.run_type,
            "RUNNING",
            run.triggered_by,
            run.trigger_source,
            run.environment_name,
            run.started_at or now,
            None,  # ended_at is NULL while RUNNING
            0, 0, 0, 0,  # counts start at zero
            None,  # run_summary
            None,  # error_message
            now,
            now,
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        logger.info(f"Created DQ_RUN with ID {new_id} (status=RUNNING).")
        return new_id
    except Exception as e:
        logger.error(f"Failed to create DQ_RUN: {e}")
        return None


def update_run_status(
    dq_run_id: int,
    run_status: str,
    total_rules_executed: int,
    total_passed: int,
    total_failed: int,
    total_warned: int,
    run_summary: Optional[str] = None,
    error_message: Optional[str] = None,
    ended_at: Optional[datetime] = None
) -> bool:
    """
    Update a DQ_RUN row with final status, counts, and end time.

    Args:
        dq_run_id: The run to update.
        run_status: Final status (SUCCESS, FAILED, PARTIAL).
        total_rules_executed: Total rules that ran.
        total_passed: Count of PASSED results.
        total_failed: Count of FAILED results.
        total_warned: Count of WARNED results.
        run_summary: Human-readable summary.
        error_message: Exception message if the job crashed.
        ended_at: Completion timestamp (defaults to now).

    Returns:
        bool: True if successfully updated.
    """
    try:
        now = ended_at or datetime.now()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE DQ_RUN
            SET run_status = ?,
                total_rules_executed = ?,
                total_passed = ?,
                total_failed = ?,
                total_warned = ?,
                run_summary = ?,
                error_message = ?,
                ended_at = ?,
                updated_at = ?
            WHERE dq_run_id = ?
            """,
            run_status,
            total_rules_executed,
            total_passed,
            total_failed,
            total_warned,
            run_summary,
            error_message,
            now,
            now,
            dq_run_id,
        )
        conn.commit()
        conn.close()
        logger.info(f"Updated DQ_RUN {dq_run_id} to status={run_status}.")
        return True
    except Exception as e:
        logger.error(f"Failed to update DQ_RUN {dq_run_id}: {e}")
        return False
