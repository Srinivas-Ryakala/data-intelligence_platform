"""
Integration tests for the db/ repository layer.
REQUIRES a live SSMS connection — these tests read/write actual tables.
Run only when DB is available and test data can be safely inserted.

Usage: pytest tests/test_db.py -v
"""

import sys
import os
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ──────────────────────────────────────────────
# Skip all tests if no DB connection available
# ──────────────────────────────────────────────
def _can_connect() -> bool:
    """Check if we can establish a DB connection."""
    try:
        from db.connection import get_connection
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _can_connect(),
    reason="SSMS connection not available — skipping integration tests"
)


class TestConnection:
    """Tests for db/connection.py."""

    def test_connection_returns_valid_object(self):
        """get_connection() should return a usable connection."""
        from db.connection import get_connection
        conn = get_connection()
        assert conn is not None
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        conn.close()


class TestAssetRepo:
    """Tests for db/asset_repo.py — read-only queries on DATA_ASSET."""

    def test_get_all_assets_returns_list(self):
        """get_all_assets() should return a list (may be empty)."""
        from db.asset_repo import get_all_assets
        assets = get_all_assets()
        assert isinstance(assets, list)

    def test_get_table_assets_returns_tables(self):
        """get_table_assets() should return only TABLE-type assets."""
        from db.asset_repo import get_table_assets
        tables = get_table_assets()
        assert isinstance(tables, list)
        for t in tables:
            assert t.get("asset_type") == "TABLE"


class TestRuleRepo:
    """Tests for db/rule_repo.py."""

    def test_rule_exists_returns_bool(self):
        """rule_exists() should return False for a non-existent rule code."""
        from db.rule_repo import rule_exists
        result = rule_exists("NONEXISTENT_RULE_CODE_XYZ")
        assert result is False

    def test_get_all_active_rules_returns_list(self):
        """get_all_active_rules() should return a list."""
        from db.rule_repo import get_all_active_rules
        rules = get_all_active_rules()
        assert isinstance(rules, list)


class TestRunRepo:
    """Tests for db/run_repo.py."""

    def test_create_and_update_run(self):
        """Create a run, then update it. Verify status changes."""
        from datetime import datetime
        from db.run_repo import create_run, update_run_status
        from db.connection import get_connection
        from models.dq_run import DQRun

        run = DQRun(
            run_name="Integration Test Run",
            run_type="MANUAL",
            triggered_by="pytest",
            trigger_source="Integration Test",
            environment_name="DEV",
        )

        run_id = create_run(run)
        assert run_id is not None
        assert isinstance(run_id, int)

        # Verify it was created with RUNNING status
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT run_status FROM DQ_RUN WHERE dq_run_id = ?", run_id
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "RUNNING"

        # Update to SUCCESS
        update_run_status(
            dq_run_id=run_id,
            run_status="SUCCESS",
            total_rules_executed=10,
            total_passed=8,
            total_failed=2,
            total_warned=0,
            run_summary="Test run completed.",
            ended_at=datetime.now(),
        )

        cursor.execute(
            "SELECT run_status FROM DQ_RUN WHERE dq_run_id = ?", run_id
        )
        row = cursor.fetchone()
        assert row[0] == "SUCCESS"

        conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
