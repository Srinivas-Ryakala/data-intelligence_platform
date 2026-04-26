"""
Dataclass model for the DQ_SCORE_SUMMARY table.
Pre-aggregated quality scorecard computed after every run.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class DQScoreSummary:
    """
    Maps to every column in the DQ_SCORE_SUMMARY table.

    Attributes:
        dq_score_summary_id: Auto-incremented PK. None when creating new.
        dq_run_id: FK to DQ_RUN — which run produced this score.
        asset_id: FK to DATA_ASSET — which asset this score applies to.
        score_level: Granularity (COLUMN, TABLE, SCHEMA, PIPELINE).
        rule_dimension: DQ category (COMPLETENESS, FORMAT, etc.).
        score_value: Computed quality score as a decimal 0-1.
        total_rules: Total rules evaluated for this asset+dimension combo.
        passed_rules: Count of rules that passed.
        failed_rules: Count of rules that failed.
        warned_rules: Count of rules with warnings.
        summary_status: HEALTHY, DEGRADED, CRITICAL, or UNKNOWN.
        created_at: When this summary row was computed.
        updated_at: Timestamp of most recent update.
    """

    dq_score_summary_id: Optional[int] = None
    dq_run_id: Optional[int] = None
    asset_id: Optional[int] = None
    score_level: str = "TABLE"
    rule_dimension: str = ""
    score_value: float = 0.0
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    warned_rules: int = 0
    summary_status: str = "UNKNOWN"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """
        Convert this dataclass to a plain dictionary for DB inserts.

        Returns:
            dict: All fields as key-value pairs.
        """
        return asdict(self)
