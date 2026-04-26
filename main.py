"""
main.py — The single entry point that runs the full DQ pipeline end to end.
This file imports from engine/ and db/ only. No business logic here — only orchestration.

Usage: python main.py
"""

import sys
import os
import logging
from datetime import datetime

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from configs.logging_config import setup_logging
from db.run_repo import create_run, update_run_status
from db.result_repo import bulk_insert_results
from db.score_repo import bulk_insert_scores
from db.issue_repo import insert_issue
from engine.rule_executor import execute_all
from engine.score_calculator import calculate_scores
from engine.issue_generator import generate_issues
from models.dq_run import DQRun


def main() -> None:
    """
    Orchestrate a full DQ run from start to finish.

    Steps:
        1. Create a DQ_RUN row with status=RUNNING
        2. Execute all active rule assignments
        3. Bulk insert results to DQ_RESULT
        4. Calculate and insert score summaries
        5. Generate and insert issues for failed mandatory rules
        6. Update DQ_RUN with final status, counts, and summary

    The run is always closed — never left with status=RUNNING.

    Returns:
        None
    """
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("DQ FRAMEWORK — Starting pipeline run")
    logger.info("=" * 60)

    run_id = None

    try:
        # ──────────────────────────────────────────────
        # Step 1: Create DQ_RUN row
        # ──────────────────────────────────────────────
        run = DQRun(
            run_name=f"DQ Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            run_type="MANUAL",
            run_status="RUNNING",
            triggered_by=os.getenv("USERNAME", "system"),
            trigger_source="Manual",
            environment_name=os.getenv("DQ_ENVIRONMENT", "DEV"),
            started_at=datetime.now(),
        )
        run_id = create_run(run)

        if run_id is None:
            logger.error("Failed to create DQ_RUN — aborting.")
            return

        logger.info(f"Step 1: Created DQ_RUN with ID {run_id} (status=RUNNING)")

        # ──────────────────────────────────────────────
        # Step 2: Execute all rule assignments
        # ──────────────────────────────────────────────
        logger.info("Step 2: Executing all active rule assignments...")
        results = execute_all(run_id)
        logger.info(f"Step 2: Executed {len(results)} rules.")

        # ──────────────────────────────────────────────
        # Step 3: Bulk insert results to DQ_RESULT
        # ──────────────────────────────────────────────
        logger.info("Step 3: Inserting results into DQ_RESULT...")
        rows_inserted = bulk_insert_results(results)
        logger.info(f"Step 3: Inserted {rows_inserted} result rows.")

        # ──────────────────────────────────────────────
        # Step 4: Calculate and insert score summaries
        # ──────────────────────────────────────────────
        logger.info("Step 4: Calculating score summaries...")
        scores = calculate_scores(results, run_id)
        scores_inserted = bulk_insert_scores(scores)
        logger.info(f"Step 4: Inserted {scores_inserted} score summary rows.")

        # ──────────────────────────────────────────────
        # Step 5: Generate and insert issues
        # ──────────────────────────────────────────────
        logger.info("Step 5: Generating issues for failed mandatory rules...")
        issues = generate_issues(results)
        issues_inserted = 0
        for issue in issues:
            issue_id = insert_issue(issue)
            if issue_id is not None:
                issues_inserted += 1
        logger.info(f"Step 5: Inserted {issues_inserted} issues.")

        # ──────────────────────────────────────────────
        # Step 6: Compute final counts and update DQ_RUN
        # ──────────────────────────────────────────────
        total_passed = sum(1 for r in results if r.result_status == "PASSED")
        total_failed = sum(1 for r in results if r.result_status == "FAILED")
        total_warned = sum(1 for r in results if r.result_status == "WARNED")
        total_rules_executed = len(results)

        # Determine final run_status
        # FAILED if any Critical-severity rule failed
        has_critical_failure = False
        for r in results:
            if r.result_status == "FAILED":
                from db.rule_repo import get_rule_by_id
                rule = get_rule_by_id(r.dq_rule_id)
                if rule and rule.severity == "Critical":
                    has_critical_failure = True
                    break

        run_status = "FAILED" if has_critical_failure else "SUCCESS"

        run_summary = (
            f"{total_passed} passed, {total_failed} failed, {total_warned} warned "
            f"out of {total_rules_executed} rules executed. "
            f"{issues_inserted} issues created."
        )

        logger.info("Step 6: Updating DQ_RUN with final status...")
        update_run_status(
            dq_run_id=run_id,
            run_status=run_status,
            total_rules_executed=total_rules_executed,
            total_passed=total_passed,
            total_failed=total_failed,
            total_warned=total_warned,
            run_summary=run_summary,
            ended_at=datetime.now(),
        )

        logger.info("=" * 60)
        logger.info(f"DQ RUN COMPLETE — Status: {run_status}")
        logger.info(f"Summary: {run_summary}")
        logger.info("=" * 60)

        print(f"\nDQ Run {run_id} complete — {run_status}")
        print(f"Summary: {run_summary}")

    except Exception as e:
        logger.error(f"UNHANDLED EXCEPTION in DQ pipeline: {e}", exc_info=True)

        # Always close the run — never leave it as RUNNING
        if run_id is not None:
            try:
                update_run_status(
                    dq_run_id=run_id,
                    run_status="PARTIAL",
                    total_rules_executed=0,
                    total_passed=0,
                    total_failed=0,
                    total_warned=0,
                    run_summary="Run aborted due to unhandled exception.",
                    error_message=str(e)[:2000],
                    ended_at=datetime.now(),
                )
                logger.info(f"DQ_RUN {run_id} closed with status=PARTIAL.")
            except Exception as close_err:
                logger.error(f"Failed to close DQ_RUN {run_id}: {close_err}")

        print(f"\nDQ Run FAILED with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
