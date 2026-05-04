"""
main.py — Orchestrates the full DQ pipeline end to end.
"""
import sys
import os
import logging
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from configs.logging_config import setup_logging
from db.run_repo import create_run, update_run_status
from db.result_repo import bulk_insert_results, populate_result_ids
from db.score_repo import bulk_insert_scores
from db.issue_repo import insert_issue
from engine.rule_executor import execute_all
from engine.score_calculator import calculate_scores
from engine.issue_generator import generate_issues
from models.dq_run import DQRun
from db.assignment_repo import get_active_assignments

def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("DQ FRAMEWORK — Starting pipeline run")
    logger.info("=" * 60)
    run_id = None
    # ── Pre-flight: fetch assignments ──────────────────────────────────────
    assignments = get_active_assignments()
    if not assignments:
        logger.error("No active DQ_RULE_ASSIGNMENT rows found. Aborting DQ run.")
        return
    platform_id = next(
        (a.platform_id for a in assignments if getattr(a, "platform_id", None) is not None),
        None,
    )
    if platform_id is None:
        env_platform_id = os.getenv("DQ_PLATFORM_ID")
        if env_platform_id:
            try:
                platform_id = int(env_platform_id)
                logger.info(f"Using DQ_PLATFORM_ID from environment: {platform_id}")
            except ValueError:
                logger.warning("DQ_PLATFORM_ID env var is not a valid integer.")
    if platform_id is None:
        logger.error("No platform_id found. Aborting run.")
        return
    try:
        # ── Step 1: Create DQ_RUN ──────────────────────────────────────────
        run = DQRun(
            platform_id=platform_id,
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
        logger.info(f"Step 1: Created DQ_RUN with ID {run_id}")
        # ── Step 2: Execute rules ──────────────────────────────────────────
        logger.info("Step 2: Executing rule assignments...")
        results = execute_all(run_id)
        logger.info(f"Executed {len(results)} rules.")
        # Normalise ERROR → FAILED so they count in scores and trigger issues
        for r in results:
            if r.result_status == "ERROR":
                r.result_status = "FAILED"
        # ── Step 3: Insert results ─────────────────────────────────────────
        logger.info("Step 3: Inserting results into DQ_RESULT...")
        rows_inserted = bulk_insert_results(results)
        logger.info(f"Inserted {rows_inserted} rows into DQ_RESULT.")
        # ── Step 3b: Populate dq_result_id on in-memory objects ───────────
        # This is required so that issue generation can link DQ_ISSUE.dq_result_id.
        logger.info("Step 3b: Fetching generated dq_result_ids...")
        populated = populate_result_ids(results, run_id)
        logger.info(f"Populated dq_result_id for {populated} results.")
        # ── Step 4: Score calculation ──────────────────────────────────────
        logger.info("Step 4: Calculating scores...")
        scores = calculate_scores(results, run_id)
        logger.info(f"Scores generated: {len(scores)}")
        if scores:
            scores_inserted = bulk_insert_scores(scores)
            logger.info(f"Inserted {scores_inserted} score rows.")
        else:
            logger.warning("No scores generated — skipping insert.")
        # ── Step 5: Issue generation ───────────────────────────────────────
        logger.info("Step 5: Generating issues...")
        issues = generate_issues(results)
        issues_inserted = 0
        for issue in issues:
            issue_id = insert_issue(issue)
            if issue_id:
                issues_inserted += 1
        logger.info(f"Issues inserted: {issues_inserted}")
        # ── Step 6: Update DQ_RUN status ──────────────────────────────────
        total_passed  = sum(1 for r in results if r.result_status == "PASSED")
        total_failed  = sum(1 for r in results if r.result_status == "FAILED")
        total_warned  = sum(1 for r in results if r.result_status == "WARNED")
        total_rules   = len(results)
        # ── Determine run status: SUCCESS / FAILED / PARTIAL ──────────
        if total_rules == 0:
            run_status = "SUCCESS"
        elif total_failed == total_rules:
            run_status = "FAILED"
        elif total_failed == 0:
            run_status = "SUCCESS"
        else:
            run_status = "PARTIAL"
        run_summary = (
            f"{total_passed} passed, {total_failed} failed, {total_warned} warned "
            f"out of {total_rules} rules. {issues_inserted} issues created."
        )
        logger.info("Updating run status...")
        update_run_status(
            dq_run_id=run_id,
            run_status=run_status,
            total_rules_executed=total_rules,
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
        logger.error(f"UNHANDLED ERROR: {e}", exc_info=True)
        if run_id:
            update_run_status(
                dq_run_id=run_id,
                run_status="PARTIAL",
                total_rules_executed=0,
                total_passed=0,
                total_failed=0,
                total_warned=0,
                run_summary="Run failed due to exception",
                error_message=str(e)[:2000],
                ended_at=datetime.now(),
            )
        sys.exit(1)

if __name__ == "__main__":
    main()