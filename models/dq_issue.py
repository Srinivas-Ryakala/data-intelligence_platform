"""
Dataclass model for the DQ_ISSUE table.
Business-facing issue ticket tracking problems from detection to resolution.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class DQIssue:
    """
    Maps to every column in the DQ_ISSUE table.

    Attributes:
        dq_issue_id: Auto-incremented PK. None when creating new.
        dq_result_id: FK to DQ_RESULT — the specific failed result.
        asset_id: FK to DATA_ASSET (table level) — denormalized.
        column_asset_id: FK to DATA_ASSET (column level). None for table-level issues.
        issue_code: Machine-readable identifier (e.g. ISS-COMP-001).
        issue_title: Short human-readable summary of the problem.
        issue_description: Full description with business impact.
        severity: Inherited from the DQ_RULE that failed (Critical, High, Medium).
        issue_status: Lifecycle state (OPEN, ACKNOWLEDGED, IN_PROGRESS, RESOLVED, etc.).
        root_cause_category: Why this occurred (SOURCE_SYSTEM, PIPELINE_BUG, etc.).
        assigned_to: Person or team responsible for resolution.
        reported_by: Who or what raised this issue (DQ_ENGINE or person).
        opened_at: When the issue was first created.
        acknowledged_at: When someone took ownership. None until acknowledged.
        resolved_at: When confirmed fixed. None until resolved.
        resolution_notes: What was done to fix this issue.
        is_recurring: True if same rule+asset combo has failed before.
        created_at: When this issue record was created.
        updated_at: Timestamp of most recent update.
    """

    dq_issue_id: Optional[int] = None
    dq_result_id: Optional[int] = None
    asset_id: Optional[int] = None
    column_asset_id: Optional[int] = None
    issue_code: str = ""
    issue_title: str = ""
    issue_description: Optional[str] = None
    severity: str = "Medium"
    issue_status: str = "OPEN"
    root_cause_category: str = "UNKNOWN"
    assigned_to: Optional[str] = None
    reported_by: str = "DQ_ENGINE"
    opened_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
    is_recurring: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """
        Convert this dataclass to a plain dictionary for DB inserts.

        Returns:
            dict: All fields as key-value pairs.
        """
        return asdict(self)
