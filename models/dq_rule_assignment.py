"""
Dataclass model for the DQ_RULE_ASSIGNMENT table.
Represents a mapping of a generic rule to a specific asset with configuration.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class DQRuleAssignment:
    """
    Maps to every column in the DQ_RULE_ASSIGNMENT table.

    Attributes:
        dq_rule_assignment_id: Auto-incremented PK. None when creating new.
        dq_rule_id: FK to DQ_RULE.
        asset_id: FK to DATA_ASSET (table level).
        column_asset_id: FK to DATA_ASSET (column level). None for table-level rules.
        platform_id: FK to DATA_ASSET (platform).
        assignment_scope: COLUMN, TABLE, SCHEMA, or PIPELINE.
        execution_mode: BLOCKING, NON_BLOCKING, or ADVISORY.
        execution_frequency: EVERY_RUN, DAILY, WEEKLY, or ON_DEMAND.
        threshold_value_override: Overrides default from DQ_RULE. None = use default.
        threshold_operator_override: Overrides operator from DQ_RULE. None = use default.
        filter_condition: SQL WHERE clause to scope data subset. None = full table.
        business_context: Why this rule is assigned to this asset.
        owner_name: Person or team responsible for this assignment.
        is_mandatory: Whether failure blocks downstream processing.
        is_active: Soft-delete flag.
        created_by: Username that created this assignment.
        created_at: Timestamp when created.
        updated_at: Timestamp of most recent change.
    """

    dq_rule_assignment_id: Optional[int] = None
    dq_rule_id: Optional[int] = None
    asset_id: Optional[int] = None
    column_asset_id: Optional[int] = None
    platform_id: Optional[int] = None
    assignment_scope: str = "COLUMN"
    execution_mode: str = "NON_BLOCKING"
    execution_frequency: str = "EVERY_RUN"
    threshold_value_override: Optional[float] = None
    threshold_operator_override: Optional[str] = None
    filter_condition: Optional[str] = None
    business_context: Optional[str] = None
    owner_name: str = "dq-automation"
    is_mandatory: bool = False
    is_active: bool = True
    created_by: str = "system"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """
        Convert this dataclass to a plain dictionary for DB inserts.

        Returns:
            dict: All fields as key-value pairs.
        """
        return asdict(self)
