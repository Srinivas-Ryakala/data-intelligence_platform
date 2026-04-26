"""
Dataclass model for the DQ_RUN table.
Represents a single execution of the DQ validation pipeline.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class DQRun:
    """
    Maps to every column in the DQ_RUN table.

    Attributes:
        dq_run_id: Auto-incremented PK. None when creating new.
        platform_id: FK to DATA_ASSET (which platform executed this run).
        pipeline_id: FK to DATA_ASSET (which pipeline triggered this run).
        parse_run_id: FK linking to the data ingestion run that preceded this DQ run.
        run_name: Human-readable label (e.g. 'Silver Layer DQ - Daily Run').
        run_type: SCHEDULED, MANUAL, INCREMENTAL, or FULL.
        run_status: RUNNING, SUCCESS, FAILED, or PARTIAL.
        triggered_by: Who or what initiated this run.
        trigger_source: The system that triggered (ADF, Databricks, Manual, API).
        environment_name: DEV, UAT, or PROD.
        started_at: When the run began.
        ended_at: When the run completed. None while RUNNING.
        total_rules_executed: Count of rules evaluated.
        total_passed: Count of PASSED results.
        total_failed: Count of FAILED results.
        total_warned: Count of WARNED results.
        run_summary: Human-readable summary of the outcome.
        error_message: Exception message if the DQ job itself crashed. None for normal runs.
        created_at: Timestamp when run record was created.
        updated_at: Timestamp of most recent update.
    """

    dq_run_id: Optional[int] = None
    platform_id: Optional[int] = None
    pipeline_id: Optional[int] = None
    parse_run_id: Optional[int] = None
    run_name: str = ""
    run_type: str = "MANUAL"
    run_status: str = "RUNNING"
    triggered_by: str = "system"
    trigger_source: str = "Manual"
    environment_name: str = "DEV"
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    total_rules_executed: int = 0
    total_passed: int = 0
    total_failed: int = 0
    total_warned: int = 0
    run_summary: Optional[str] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """
        Convert this dataclass to a plain dictionary for DB inserts.

        Returns:
            dict: All fields as key-value pairs.
        """
        return asdict(self)
