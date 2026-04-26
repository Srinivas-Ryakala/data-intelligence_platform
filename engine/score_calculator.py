"""
Score calculator — aggregates DQ_RESULT into DQ_SCORE_SUMMARY per asset per dimension.
Produces one summary row per unique (run_id, asset_id, rule_dimension) combination,
plus a PIPELINE-level row aggregating across all assets.
"""

import logging
from collections import defaultdict
from datetime import datetime

from db.rule_repo import get_rule_by_id
from models.dq_result import DQResult
from models.dq_score_summary import DQScoreSummary

logger = logging.getLogger(__name__)


def calculate_scores(results: list[DQResult], run_id: int) -> list[DQScoreSummary]:
    """
    Aggregate DQ_RESULT into DQ_SCORE_SUMMARY rows.
    Groups by asset_id + rule_dimension, computes score_value = passed / total.
    Also generates a PIPELINE-level summary across all assets.

    Args:
        results: List of DQResult objects from the current run.
        run_id: The dq_run_id for this execution.

    Returns:
        list[DQScoreSummary]: One row per asset+dimension, plus pipeline-level rows.
    """
    if not results:
        logger.info("No results to score.")
        return []

    # ──────────────────────────────────────────────
    # Step 1: Group results by (asset_id, rule_dimension)
    # ──────────────────────────────────────────────
    groups: dict[tuple[int, str], list[DQResult]] = defaultdict(list)

    for result in results:
        if result.result_status == "ERROR" or result.result_status == "SKIPPED":
            continue  # Skip errored/skipped results from scoring

        # Look up rule dimension from the rule
        rule = get_rule_by_id(result.dq_rule_id)
        if rule is None:
            logger.warning(f"Rule {result.dq_rule_id} not found — skipping from scoring.")
            continue

        dimension = rule.rule_dimension
        asset_id = result.asset_id

        if asset_id is not None:
            groups[(asset_id, dimension)].append(result)

    # ──────────────────────────────────────────────
    # Step 2: Build TABLE-level summaries
    # ──────────────────────────────────────────────
    summaries: list[DQScoreSummary] = []
    now = datetime.now()

    # Track pipeline-level statistics
    pipeline_totals: dict[str, dict] = defaultdict(
        lambda: {"total": 0, "passed": 0, "failed": 0, "warned": 0, "has_critical": False}
    )

    for (asset_id, dimension), group_results in groups.items():
        total = len(group_results)
        passed = sum(1 for r in group_results if r.result_status == "PASSED")
        failed = sum(1 for r in group_results if r.result_status == "FAILED")
        warned = sum(1 for r in group_results if r.result_status == "WARNED")

        score_value = passed / total if total > 0 else 0.0

        # Determine summary_status
        summary_status = _determine_status(group_results)

        summary = DQScoreSummary(
            dq_run_id=run_id,
            asset_id=asset_id,
            score_level="TABLE",
            rule_dimension=dimension,
            score_value=round(score_value, 4),
            total_rules=total,
            passed_rules=passed,
            failed_rules=failed,
            warned_rules=warned,
            summary_status=summary_status,
            created_at=now,
            updated_at=now,
        )
        summaries.append(summary)

        # Accumulate for pipeline-level
        pipeline_totals[dimension]["total"] += total
        pipeline_totals[dimension]["passed"] += passed
        pipeline_totals[dimension]["failed"] += failed
        pipeline_totals[dimension]["warned"] += warned
        if summary_status == "CRITICAL":
            pipeline_totals[dimension]["has_critical"] = True

    # ──────────────────────────────────────────────
    # Step 3: Build PIPELINE-level summaries
    # ──────────────────────────────────────────────
    for dimension, totals in pipeline_totals.items():
        total = totals["total"]
        passed = totals["passed"]
        failed = totals["failed"]
        warned = totals["warned"]
        score_value = passed / total if total > 0 else 0.0

        if totals["has_critical"]:
            pipeline_status = "CRITICAL"
        elif failed > 0 or warned > 0:
            pipeline_status = "DEGRADED"
        else:
            pipeline_status = "HEALTHY"

        pipeline_summary = DQScoreSummary(
            dq_run_id=run_id,
            asset_id=None,
            score_level="PIPELINE",
            rule_dimension=dimension,
            score_value=round(score_value, 4),
            total_rules=total,
            passed_rules=passed,
            failed_rules=failed,
            warned_rules=warned,
            summary_status=pipeline_status,
            created_at=now,
            updated_at=now,
        )
        summaries.append(pipeline_summary)

    logger.info(f"Calculated {len(summaries)} score summaries for run {run_id}.")
    return summaries


def _determine_status(results: list[DQResult]) -> str:
    """
    Determine summary_status from a group of results.
    HEALTHY = all passed.
    CRITICAL = any Critical-severity rule failed.
    DEGRADED = warnings or non-critical failures.

    Args:
        results: Group of DQResult objects for one asset+dimension combo.

    Returns:
        str: HEALTHY, DEGRADED, or CRITICAL.
    """
    has_failed = False
    has_warned = False
    has_critical_failure = False

    for result in results:
        if result.result_status == "FAILED":
            has_failed = True
            # Check if this was a Critical severity rule
            rule = get_rule_by_id(result.dq_rule_id)
            if rule and rule.severity == "Critical":
                has_critical_failure = True
        elif result.result_status == "WARNED":
            has_warned = True

    if has_critical_failure:
        return "CRITICAL"
    elif has_failed or has_warned:
        return "DEGRADED"
    else:
        return "HEALTHY"
