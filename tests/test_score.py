"""
Unit tests for engine/score_calculator.py.
Tests HEALTHY, DEGRADED, and CRITICAL scoring scenarios.
No live DB required — uses mock DQResult objects.
"""

import sys
import os
import pytest
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.dq_result import DQResult
from models.dq_rule import DQRule
from engine.score_calculator import calculate_scores


def _make_rule(rule_id: int, dimension: str, severity: str = "Medium") -> DQRule:
    """Helper to create a DQRule for testing."""
    return DQRule(
        dq_rule_id=rule_id,
        rule_code=f"TEST_{rule_id}",
        rule_dimension=dimension,
        severity=severity,
    )


def _make_result(
    rule_id: int, asset_id: int, status: str = "PASSED"
) -> DQResult:
    """Helper to create a DQResult for testing."""
    return DQResult(
        dq_run_id=1,
        dq_rule_id=rule_id,
        asset_id=asset_id,
        result_status=status,
    )


class TestScoreCalculator:
    """Tests for score_calculator.calculate_scores()."""

    @patch("engine.score_calculator.get_rule_by_id")
    def test_all_passed_is_healthy(self, mock_get_rule):
        """When all rules pass, summary_status should be HEALTHY with score 1.0."""
        rule = _make_rule(1, "COMPLETENESS")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "PASSED"),
        ]

        scores = calculate_scores(results, run_id=1)

        # Should have TABLE-level + PIPELINE-level
        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 1
        assert table_scores[0].summary_status == "HEALTHY"
        assert table_scores[0].score_value == 1.0
        assert table_scores[0].passed_rules == 3
        assert table_scores[0].failed_rules == 0

    @patch("engine.score_calculator.get_rule_by_id")
    def test_critical_failure_is_critical(self, mock_get_rule):
        """When a Critical-severity rule fails, summary_status should be CRITICAL."""
        rule = _make_rule(1, "COMPLETENESS", severity="Critical")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "FAILED"),  # Critical failure
            _make_result(1, 100, "PASSED"),
        ]

        scores = calculate_scores(results, run_id=1)

        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 1
        assert table_scores[0].summary_status == "CRITICAL"
        assert table_scores[0].failed_rules == 1

    @patch("engine.score_calculator.get_rule_by_id")
    def test_medium_failure_is_degraded(self, mock_get_rule):
        """When a Medium-severity rule fails, summary_status should be DEGRADED."""
        rule = _make_rule(1, "FORMAT", severity="Medium")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "FAILED"),  # Medium failure
            _make_result(1, 100, "PASSED"),
        ]

        scores = calculate_scores(results, run_id=1)

        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 1
        assert table_scores[0].summary_status == "DEGRADED"

    @patch("engine.score_calculator.get_rule_by_id")
    def test_warned_is_degraded(self, mock_get_rule):
        """Warnings should produce DEGRADED status."""
        rule = _make_rule(1, "RANGE", severity="Medium")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "WARNED"),
        ]

        scores = calculate_scores(results, run_id=1)

        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 1
        assert table_scores[0].summary_status == "DEGRADED"
        assert table_scores[0].warned_rules == 1

    @patch("engine.score_calculator.get_rule_by_id")
    def test_score_value_calculation(self, mock_get_rule):
        """score_value should be passed / total."""
        rule = _make_rule(1, "COMPLETENESS")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "FAILED"),
            _make_result(1, 100, "PASSED"),
        ]

        scores = calculate_scores(results, run_id=1)

        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 1
        assert table_scores[0].score_value == 0.75  # 3/4

    @patch("engine.score_calculator.get_rule_by_id")
    def test_multiple_assets_get_separate_scores(self, mock_get_rule):
        """Different assets should get separate score summary rows."""
        rule = _make_rule(1, "COMPLETENESS")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 200, "FAILED"),
        ]

        scores = calculate_scores(results, run_id=1)

        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 2  # One per asset

    @patch("engine.score_calculator.get_rule_by_id")
    def test_pipeline_level_summary_generated(self, mock_get_rule):
        """A PIPELINE-level summary should be generated alongside TABLE-level."""
        rule = _make_rule(1, "COMPLETENESS")
        mock_get_rule.return_value = rule

        results = [_make_result(1, 100, "PASSED")]

        scores = calculate_scores(results, run_id=1)

        pipeline_scores = [s for s in scores if s.score_level == "PIPELINE"]
        assert len(pipeline_scores) >= 1

    def test_empty_results_returns_empty(self):
        """Empty results should return empty scores."""
        scores = calculate_scores([], run_id=1)
        assert scores == []

    @patch("engine.score_calculator.get_rule_by_id")
    def test_error_results_excluded_from_scoring(self, mock_get_rule):
        """ERROR status results should not be included in scoring."""
        rule = _make_rule(1, "COMPLETENESS")
        mock_get_rule.return_value = rule

        results = [
            _make_result(1, 100, "PASSED"),
            _make_result(1, 100, "ERROR"),  # Should be excluded
        ]

        scores = calculate_scores(results, run_id=1)

        table_scores = [s for s in scores if s.score_level == "TABLE"]
        assert len(table_scores) == 1
        assert table_scores[0].total_rules == 1  # Only the PASSED one counted


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
