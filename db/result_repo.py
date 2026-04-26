"""
Repository for DQ_RESULT table.
Bulk inserts validation results — performance critical.
"""

import logging
from datetime import datetime

from db.connection import get_connection
from models.dq_result import DQResult

logger = logging.getLogger(__name__)


def bulk_insert_results(results: list[DQResult]) -> int:
    """
    Bulk insert DQ_RESULT rows using executemany() for performance.
    Never loop individual inserts.

    Args:
        results: List of DQResult objects to insert.

    Returns:
        int: Number of rows inserted, or 0 on error.
    """
    if not results:
        logger.info("No results to insert.")
        return 0

    try:
        now = datetime.now()
        conn = get_connection()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO DQ_RESULT (
                dq_run_id, dq_rule_assignment_id, dq_rule_id,
                asset_id, column_asset_id,
                result_status, threshold_value_applied, threshold_operator_applied,
                observed_value, pass_percentage,
                rows_checked, passed_row_count, failed_row_count,
                sample_failed_value, result_message,
                execution_output_location, confidence_score,
                executed_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                r.dq_run_id,
                r.dq_rule_assignment_id,
                r.dq_rule_id,
                r.asset_id,
                r.column_asset_id,
                r.result_status,
                r.threshold_value_applied,
                r.threshold_operator_applied,
                r.observed_value,
                r.pass_percentage,
                r.rows_checked,
                r.passed_row_count,
                r.failed_row_count,
                r.sample_failed_value,
                r.result_message,
                r.execution_output_location,
                r.confidence_score,
                r.executed_at or now,
                r.created_at or now,
                r.updated_at or now,
            )
            for r in results
        ]

        cursor.executemany(insert_sql, params)
        conn.commit()
        row_count = len(results)
        conn.close()
        logger.info(f"Bulk inserted {row_count} DQ_RESULT rows.")
        return row_count
    except Exception as e:
        logger.error(f"Failed to bulk insert DQ_RESULT: {e}")
        return 0
