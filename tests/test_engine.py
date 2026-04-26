"""
Unit tests for engine/expression_builder.py and engine/rule_executor.py.
Uses mock DB connections — no live SSMS required.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.dq_rule import DQRule
from models.dq_rule_assignment import DQRuleAssignment
from models.dq_result import DQResult
from engine.expression_builder import build_sql, build_row_count_sql


# ──────────────────────────────────────────────
# Expression Builder Tests
# ──────────────────────────────────────────────

class TestBuildSql:
    """Tests for expression_builder.build_sql()."""

    def _make_rule(self, rule_code: str, expression: str, **kwargs) -> DQRule:
        """Helper to create a DQRule for testing."""
        return DQRule(
            dq_rule_id=1,
            rule_name="Test Rule",
            rule_code=rule_code,
            rule_expression=expression,
            **kwargs
        )

    def _make_assignment(self, **kwargs) -> DQRuleAssignment:
        """Helper to create a DQRuleAssignment for testing."""
        defaults = {
            "dq_rule_assignment_id": 1,
            "dq_rule_id": 1,
            "asset_id": 100,
        }
        defaults.update(kwargs)
        return DQRuleAssignment(**defaults)

    def test_null_check_basic(self):
        """COMP_NULL_CHK should produce a COUNT WHERE col IS NULL query."""
        rule = self._make_rule("COMP_NULL_CHK", "COUNT(*) WHERE {col} IS NULL")
        assignment = self._make_assignment()
        sql = build_sql(assignment, rule, "customers", "email")
        assert "COUNT(*)" in sql
        assert "email IS NULL" in sql
        assert "customers" in sql

    def test_null_check_with_filter(self):
        """With filter_condition, AND clause should be appended."""
        rule = self._make_rule("COMP_NULL_CHK", "COUNT(*) WHERE {col} IS NULL")
        assignment = self._make_assignment(filter_condition="region = 'SOUTH'")
        sql = build_sql(assignment, rule, "customers", "email")
        assert "email IS NULL" in sql
        assert "region = 'SOUTH'" in sql
        assert "AND" in sql

    def test_pk_uniqueness(self):
        """UNIQ_PK should produce COUNT(*) - COUNT(DISTINCT col)."""
        rule = self._make_rule("UNIQ_PK", "COUNT(*) - COUNT(DISTINCT {pk_col})")
        assignment = self._make_assignment()
        sql = build_sql(assignment, rule, "orders", "order_id")
        assert "COUNT(DISTINCT order_id)" in sql
        assert "orders" in sql

    def test_empty_guard(self):
        """VOLL_EMPTY_GUARD should produce a simple COUNT(*)."""
        rule = self._make_rule("VOLL_EMPTY_GUARD", "COUNT(*)")
        assignment = self._make_assignment()
        sql = build_sql(assignment, rule, "fact_sales", None)
        assert "COUNT(*)" in sql
        assert "fact_sales" in sql

    def test_completeness_ratio(self):
        """COMP_RATIO should produce a ratio query."""
        rule = self._make_rule("COMP_RATIO", "(COUNT({col}) / COUNT(*)) * 100")
        assignment = self._make_assignment()
        sql = build_sql(assignment, rule, "customers", "name")
        assert "COUNT(name)" in sql
        assert "COUNT(*)" in sql

    def test_no_filter_condition(self):
        """When filter_condition is None, no AND clause should appear."""
        rule = self._make_rule("COMP_NULL_CHK", "COUNT(*) WHERE {col} IS NULL")
        assignment = self._make_assignment(filter_condition=None)
        sql = build_sql(assignment, rule, "customers", "email")
        # Should NOT have a dangling AND None
        assert "AND None" not in sql
        assert "AND null" not in sql.lower()

    def test_filter_with_where_prefix(self):
        """Filter condition starting with 'WHERE' should not produce double WHERE."""
        rule = self._make_rule("COMP_NULL_CHK", "COUNT(*) WHERE {col} IS NULL")
        assignment = self._make_assignment(filter_condition="WHERE status = 'ACTIVE'")
        sql = build_sql(assignment, rule, "customers", "email")
        # Should not have "WHERE ... WHERE"
        assert sql.upper().count("WHERE") == 1 or "AND" in sql.upper()

    def test_generic_expression_with_where(self):
        """Generic rule with WHERE in expression should split correctly."""
        rule = self._make_rule(
            "RNG_NEG_VAL", "COUNT(*) WHERE {col} < 0",
            rule_code="RNG_NEG_VAL"
        )
        assignment = self._make_assignment()
        sql = build_sql(assignment, rule, "transactions", "amount")
        assert "amount < 0" in sql
        assert "transactions" in sql


class TestBuildRowCountSql:
    """Tests for expression_builder.build_row_count_sql()."""

    def test_basic_count(self):
        """Simple COUNT(*) without filter."""
        sql = build_row_count_sql("orders")
        assert sql == "SELECT COUNT(*) FROM orders"

    def test_count_with_filter(self):
        """COUNT(*) with a WHERE clause."""
        sql = build_row_count_sql("orders", "status = 'ACTIVE'")
        assert "WHERE status = 'ACTIVE'" in sql

    def test_count_with_where_prefix_filter(self):
        """Filter starting with WHERE should not double."""
        sql = build_row_count_sql("orders", "WHERE status = 'ACTIVE'")
        assert sql.upper().count("WHERE") == 1


# ──────────────────────────────────────────────
# Rule Executor Tests (mocked DB)
# ──────────────────────────────────────────────

class TestRuleExecutor:
    """Tests for rule_executor module with mocked database."""

    @patch("engine.rule_executor.get_active_assignments")
    @patch("engine.rule_executor.get_rule_by_id")
    @patch("engine.rule_executor.get_table_name")
    @patch("engine.rule_executor.get_parent_table_for_column")
    @patch("engine.rule_executor.get_connection")
    def test_passed_result(
        self, mock_conn, mock_parent, mock_table_name, mock_rule, mock_assignments
    ):
        """A null check returning 0 should produce PASSED."""
        from engine.rule_executor import _execute_single_assignment

        # Setup mocks
        assignment = DQRuleAssignment(
            dq_rule_assignment_id=1,
            dq_rule_id=1,
            asset_id=100,
            column_asset_id=200,
        )

        rule = DQRule(
            dq_rule_id=1,
            rule_code="COMP_NULL_CHK",
            rule_name="Null Check",
            rule_expression="COUNT(*) WHERE {col} IS NULL",
            default_threshold_value=0,
            threshold_operator="=",
            severity="Critical",
            rule_dimension="COMPLETENESS",
        )

        mock_rule.return_value = rule
        mock_table_name.return_value = "customers"
        mock_parent.return_value = {"asset_name": "customers"}

        # Mock DB: rule query returns 0 (no nulls), count returns 1000
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(0,), (1000,)]
        mock_conn_instance = MagicMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_conn_instance

        result = _execute_single_assignment(1, assignment)

        assert result.result_status == "PASSED"
        assert result.observed_value == 0.0

    @patch("engine.rule_executor.get_active_assignments")
    @patch("engine.rule_executor.get_rule_by_id")
    @patch("engine.rule_executor.get_table_name")
    @patch("engine.rule_executor.get_parent_table_for_column")
    @patch("engine.rule_executor.get_connection")
    def test_failed_result(
        self, mock_conn, mock_parent, mock_table_name, mock_rule, mock_assignments
    ):
        """A null check returning 50 should produce FAILED (threshold=0)."""
        from engine.rule_executor import _execute_single_assignment

        assignment = DQRuleAssignment(
            dq_rule_assignment_id=2,
            dq_rule_id=1,
            asset_id=100,
            column_asset_id=200,
        )

        rule = DQRule(
            dq_rule_id=1,
            rule_code="COMP_NULL_CHK",
            rule_name="Null Check",
            rule_expression="COUNT(*) WHERE {col} IS NULL",
            default_threshold_value=0,
            threshold_operator="=",
            severity="Critical",
            rule_dimension="COMPLETENESS",
        )

        mock_rule.return_value = rule
        mock_table_name.return_value = "customers"
        mock_parent.return_value = {"asset_name": "customers"}

        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(50,), (1000,)]
        mock_conn_instance = MagicMock()
        mock_conn_instance.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_conn_instance

        result = _execute_single_assignment(1, assignment)

        assert result.result_status == "FAILED"
        assert result.observed_value == 50.0

    @patch("engine.rule_executor.get_rule_by_id")
    @patch("engine.rule_executor.get_table_name")
    @patch("engine.rule_executor.get_parent_table_for_column")
    @patch("engine.rule_executor.get_connection")
    def test_error_result(
        self, mock_conn, mock_parent, mock_table_name, mock_rule
    ):
        """If DB execution throws an exception, result should be ERROR."""
        from engine.rule_executor import _execute_single_assignment

        assignment = DQRuleAssignment(
            dq_rule_assignment_id=3,
            dq_rule_id=1,
            asset_id=100,
            column_asset_id=200,
        )

        rule = DQRule(
            dq_rule_id=1,
            rule_code="COMP_NULL_CHK",
            rule_name="Null Check",
            rule_expression="COUNT(*) WHERE {col} IS NULL",
            default_threshold_value=0,
            threshold_operator="=",
            severity="Critical",
        )

        mock_rule.return_value = rule
        mock_table_name.return_value = "customers"
        mock_parent.return_value = {"asset_name": "customers"}
        mock_conn.side_effect = Exception("DB connection failed")

        result = _execute_single_assignment(1, assignment)

        assert result.result_status == "ERROR"
        assert "error" in result.result_message.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
