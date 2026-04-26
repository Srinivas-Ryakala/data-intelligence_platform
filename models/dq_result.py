"""
Dataclass model for the DQ_RESULT table.
Represents one rule execution outcome — the most granular table in the framework.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class DQResult:
    """
    Maps to every column in the DQ_RESULT table.

    Attributes:
        dq_result_id: Auto-incremented PK. None when creating new.
        dq_run_id: FK to DQ_RUN — groups results by execution.
        dq_rule_assignment_id: FK to DQ_RULE_ASSIGNMENT — which mapping produced this.
        dq_rule_id: FK to DQ_RULE — denormalized for direct querying.
        asset_id: FK to DATA_ASSET (table level) — denormalized.
        column_asset_id: FK to DATA_ASSET (column level). None for table-level rules.
        result_status: PASSED, FAILED, WARNED, SKIPPED, or ERROR.
        threshold_value_applied: The actual threshold in effect during execution.
        threshold_operator_applied: The actual comparison operator used.
        observed_value: The number computed by the rule expression.
        pass_percentage: Proportion of rows passing (0-100).
        rows_checked: Total rows the rule was evaluated against.
        passed_row_count: Rows that satisfied the rule condition.
        failed_row_count: Rows that violated the rule condition.
        sample_failed_value: Sample of bad values for debugging.
        result_message: Human-readable summary of this result.
        execution_output_location: File path for full failed row export.
        confidence_score: 0-1 reliability score based on sample size.
        executed_at: When this rule finished executing.
        created_at: When this row was written.
        updated_at: Timestamp of most recent update.
    """

    dq_result_id: Optional[int] = None
    dq_run_id: Optional[int] = None
    dq_rule_assignment_id: Optional[int] = None
    dq_rule_id: Optional[int] = None
    asset_id: Optional[int] = None
    column_asset_id: Optional[int] = None
    result_status: str = "PASSED"
    threshold_value_applied: Optional[float] = None
    threshold_operator_applied: Optional[str] = None
    observed_value: Optional[float] = None
    pass_percentage: Optional[float] = None
    rows_checked: Optional[int] = None
    passed_row_count: Optional[int] = None
    failed_row_count: Optional[int] = None
    sample_failed_value: Optional[str] = None
    result_message: Optional[str] = None
    execution_output_location: Optional[str] = None
    confidence_score: Optional[float] = None
    executed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """
        Convert this dataclass to a plain dictionary for DB inserts.

        Returns:
            dict: All fields as key-value pairs.
        """
        return asdict(self)
