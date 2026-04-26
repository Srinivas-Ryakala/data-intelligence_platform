"""
Repository for DQ_SCORE_SUMMARY table.
Bulk inserts pre-aggregated quality scores.
"""

import logging
from datetime import datetime

from db.connection import get_connection
from models.dq_score_summary import DQScoreSummary

logger = logging.getLogger(__name__)


def bulk_insert_scores(scores: list[DQScoreSummary]) -> int:
    """
    Bulk insert DQ_SCORE_SUMMARY rows using executemany().

    Args:
        scores: List of DQScoreSummary objects to insert.

    Returns:
        int: Number of rows inserted, or 0 on error.
    """
    if not scores:
        logger.info("No score summaries to insert.")
        return 0

    try:
        now = datetime.now()
        conn = get_connection()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO DQ_SCORE_SUMMARY (
                dq_run_id, asset_id, score_level, rule_dimension,
                score_value, total_rules, passed_rules, failed_rules, warned_rules,
                summary_status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                s.dq_run_id,
                s.asset_id,
                s.score_level,
                s.rule_dimension,
                s.score_value,
                s.total_rules,
                s.passed_rules,
                s.failed_rules,
                s.warned_rules,
                s.summary_status,
                s.created_at or now,
                s.updated_at or now,
            )
            for s in scores
        ]

        cursor.executemany(insert_sql, params)
        conn.commit()
        row_count = len(scores)
        conn.close()
        logger.info(f"Bulk inserted {row_count} DQ_SCORE_SUMMARY rows.")
        return row_count
    except Exception as e:
        logger.error(f"Failed to bulk insert DQ_SCORE_SUMMARY: {e}")
        return 0
