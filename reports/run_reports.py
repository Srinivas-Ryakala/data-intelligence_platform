"""
Run report — generates a formatted summary of a DQ run.
Shows pass/fail counts per dimension, top failing rules, and owner-wise breakdown.
"""

import sys
import os
import logging

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from db.connection import get_connection
from db.rule_repo import get_rule_by_id
from db.asset_repo import get_table_name

logger = logging.getLogger(__name__)


def generate_run_report(dq_run_id: int) -> str:
    """
    Generate a formatted summary report for a specific DQ run.

    Args:
        dq_run_id: The run to report on.

    Returns:
        str: Formatted report string.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ── Run summary ──
        cursor.execute(
            "SELECT * FROM DQ_RUN WHERE dq_run_id = ?", dq_run_id
        )
        run_cols = [desc[0] for desc in cursor.description]
        run_row = cursor.fetchone()
        if run_row is None:
            return f"Run {dq_run_id} not found."
        run = dict(zip(run_cols, run_row))

        # ── Results ──
        cursor.execute(
            "SELECT * FROM DQ_RESULT WHERE dq_run_id = ?", dq_run_id
        )
        result_cols = [desc[0] for desc in cursor.description]
        result_rows = cursor.fetchall()
        results = [dict(zip(result_cols, r)) for r in result_rows]

        conn.close()

        # ── Build report ──
        report_lines = []
        report_lines.append("=" * 70)
        report_lines.append(f"DQ RUN REPORT — Run ID: {dq_run_id}")
        report_lines.append("=" * 70)
        report_lines.append(f"Run Name    : {run.get('run_name', 'N/A')}")
        report_lines.append(f"Status      : {run.get('run_status', 'N/A')}")
        report_lines.append(f"Started     : {run.get('started_at', 'N/A')}")
        report_lines.append(f"Ended       : {run.get('ended_at', 'N/A')}")
        report_lines.append(f"Total Rules : {run.get('total_rules_executed', 0)}")
        report_lines.append(f"Passed      : {run.get('total_passed', 0)}")
        report_lines.append(f"Failed      : {run.get('total_failed', 0)}")
        report_lines.append(f"Warned      : {run.get('total_warned', 0)}")
        report_lines.append(f"Summary     : {run.get('run_summary', 'N/A')}")

        if run.get('error_message'):
            report_lines.append(f"Error       : {run['error_message']}")

        # ── Pass/fail per dimension ──
        report_lines.append("")
        report_lines.append("-" * 70)
        report_lines.append("RESULTS BY DIMENSION")
        report_lines.append("-" * 70)

        dimension_stats: dict[str, dict] = defaultdict(
            lambda: {"passed": 0, "failed": 0, "warned": 0, "error": 0, "total": 0}
        )

        for result in results:
            rule = get_rule_by_id(result.get("dq_rule_id"))
            dimension = rule.rule_dimension if rule else "UNKNOWN"
            status = result.get("result_status", "UNKNOWN")

            dimension_stats[dimension]["total"] += 1
            if status == "PASSED":
                dimension_stats[dimension]["passed"] += 1
            elif status == "FAILED":
                dimension_stats[dimension]["failed"] += 1
            elif status == "WARNED":
                dimension_stats[dimension]["warned"] += 1
            elif status == "ERROR":
                dimension_stats[dimension]["error"] += 1

        report_lines.append(
            f"{'Dimension':<25} {'Total':>6} {'Pass':>6} {'Fail':>6} {'Warn':>6} {'Err':>6}"
        )
        report_lines.append("-" * 70)

        for dim in sorted(dimension_stats.keys()):
            stats = dimension_stats[dim]
            report_lines.append(
                f"{dim:<25} {stats['total']:>6} {stats['passed']:>6} "
                f"{stats['failed']:>6} {stats['warned']:>6} {stats['error']:>6}"
            )

        # ── Top 5 failing rules ──
        report_lines.append("")
        report_lines.append("-" * 70)
        report_lines.append("TOP FAILING RULES")
        report_lines.append("-" * 70)

        failed_results = [r for r in results if r.get("result_status") == "FAILED"]
        # Count failures per rule
        rule_fail_counts: dict[int, int] = defaultdict(int)
        for r in failed_results:
            rule_fail_counts[r.get("dq_rule_id", 0)] += 1

        top_failures = sorted(rule_fail_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        for rule_id, count in top_failures:
            rule = get_rule_by_id(rule_id)
            rule_name = rule.rule_code if rule else f"Rule_{rule_id}"
            report_lines.append(f"  {rule_name:<30} — {count} failure(s)")

        if not top_failures:
            report_lines.append("  No failures — all rules passed!")

        report_lines.append("")
        report_lines.append("=" * 70)

        report = "\n".join(report_lines)
        return report

    except Exception as e:
        logger.error(f"Failed to generate run report for run {dq_run_id}: {e}")
        return f"Error generating report: {e}"


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python reports/run_reports.py <dq_run_id>")
        sys.exit(1)

    from configs.logging_config import setup_logging
    setup_logging()

    run_id = int(sys.argv[1])
    report = generate_run_report(run_id)
    print(report)
