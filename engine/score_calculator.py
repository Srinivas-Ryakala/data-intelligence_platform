"""
Score calculator — aggregates DQ_RESULT into DQ_SCORE_SUMMARY per asset per dimension.
"""

import logging
from collections import defaultdict
from datetime import datetime

from db.rule_repo import get_rule_by_id
from models.dq_result import DQResult
from models.dq_score_summary import DQScoreSummary

logger = logging.getLogger(__name__)


def calculate_scores(results: list[DQResult], run_id: int) -> list[DQScoreSummary]:
    if not results:
        logger.info("No results to score.")
        return []

    # ──────────────────────────────────────────────
    # STEP 0: Cache all rules to avoid repeated DB calls
    # ──────────────────────────────────────────────
    rule_cache = {}
    skipped_results = 0

    for r in results:
        if r.dq_rule_id not in rule_cache:
            rule_cache[r.dq_rule_id] = get_rule_by_id(r.dq_rule_id)

    # ──────────────────────────────────────────────
    # STEP 1: Group results
    # ──────────────────────────────────────────────
    groups = defaultdict(list)

    for result in results:
        if result.result_status in ("ERROR", "SKIPPED"):
            skipped_results += 1
            continue

        rule = rule_cache.get(result.dq_rule_id)
        if rule is None:
            logger.warning(f"Rule {result.dq_rule_id} not found — skipping.")
            continue

        dimension = rule.rule_dimension or "UNKNOWN"
        asset_id = result.asset_id

        if asset_id is not None:
            groups[(asset_id, dimension)].append(result)

    logger.info(f"Skipped {skipped_results} results due to ERROR/SKIPPED.")

    # ──────────────────────────────────────────────
    # STEP 2: Asset-level summaries
    # ──────────────────────────────────────────────
    summaries = []
    now = datetime.now()

    pipeline_totals = defaultdict(
        lambda: {"total": 0, "passed": 0, "failed": 0, "warned": 0, "critical": False}
    )

    for (asset_id, dimension), group_results in groups.items():
        total = len(group_results)
        passed = sum(r.result_status == "PASSED" for r in group_results)
        failed = sum(r.result_status == "FAILED" for r in group_results)
        warned = sum(r.result_status == "WARNED" for r in group_results)

        score_value = passed / total if total else 0.0

        summary_status = _determine_status(group_results, rule_cache)

        summaries.append(
            DQScoreSummary(
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
        )

        # Aggregate for pipeline
        pt = pipeline_totals[dimension]
        pt["total"] += total
        pt["passed"] += passed
        pt["failed"] += failed
        pt["warned"] += warned
        if summary_status == "CRITICAL":
            pt["critical"] = True

    # ──────────────────────────────────────────────
    # STEP 3: Pipeline-level summaries
    # ──────────────────────────────────────────────
    for dimension, totals in pipeline_totals.items():
        total = totals["total"]
        passed = totals["passed"]
        failed = totals["failed"]
        warned = totals["warned"]

        score_value = passed / total if total else 0.0

        if totals["critical"]:
            status = "CRITICAL"
        elif failed > 0 or warned > 0:
            status = "DEGRADED"
        else:
            status = "HEALTHY"

        summaries.append(
            DQScoreSummary(
                dq_run_id=run_id,
                asset_id=None,
                score_level="PIPELINE",
                rule_dimension=dimension,
                score_value=round(score_value, 4),
                total_rules=total,
                passed_rules=passed,
                failed_rules=failed,
                warned_rules=warned,
                summary_status=status,
                created_at=now,
                updated_at=now,
            )
        )

    logger.info(f"Calculated {len(summaries)} score summaries for run {run_id}.")
    return summaries


def _determine_status(results: list[DQResult], rule_cache: dict) -> str:
    has_failed = False
    has_warned = False
    has_critical_failure = False

    for result in results:
        if result.result_status == "FAILED":
            has_failed = True

            rule = rule_cache.get(result.dq_rule_id)
            if rule and getattr(rule, "severity", None) == "Critical":
                has_critical_failure = True

        elif result.result_status == "WARNED":
            has_warned = True

    if has_critical_failure:
        return "CRITICAL"
    elif has_failed or has_warned:
        return "DEGRADED"
    else:
        return "HEALTHY"