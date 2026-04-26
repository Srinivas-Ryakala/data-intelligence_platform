"""
Global constants for the DQ Framework.
All configurable values are defined here — never hardcode values inside functions.
"""

# ──────────────────────────────────────────────
# Severity levels — matches DQ_RULE.severity
# ──────────────────────────────────────────────
SEVERITY_LEVELS: list[str] = ["Critical", "High", "Medium"]

# ──────────────────────────────────────────────
# Execution modes — matches DQ_RULE_ASSIGNMENT.execution_mode
# ──────────────────────────────────────────────
EXECUTION_MODES: list[str] = ["BLOCKING", "NON_BLOCKING", "ADVISORY"]

# ──────────────────────────────────────────────
# Execution frequencies — matches DQ_RULE_ASSIGNMENT.execution_frequency
# ──────────────────────────────────────────────
EXECUTION_FREQUENCIES: list[str] = ["EVERY_RUN", "DAILY", "WEEKLY", "ON_DEMAND"]

# ──────────────────────────────────────────────
# Default thresholds
# ──────────────────────────────────────────────
DEFAULT_COMPLETENESS_THRESHOLD: float = 0.95
DEFAULT_VOLUME_DRIFT_THRESHOLD: float = 0.30
DEFAULT_ZSCORE_THRESHOLD: float = 3.0

# ──────────────────────────────────────────────
# Result statuses — matches DQ_RESULT.result_status
# ──────────────────────────────────────────────
RESULT_STATUSES: list[str] = ["PASSED", "FAILED", "WARNED", "SKIPPED", "ERROR"]

# ──────────────────────────────────────────────
# Run statuses — matches DQ_RUN.run_status
# ──────────────────────────────────────────────
RUN_STATUSES: list[str] = ["RUNNING", "SUCCESS", "FAILED", "PARTIAL"]

# ──────────────────────────────────────────────
# Run types — matches DQ_RUN.run_type
# ──────────────────────────────────────────────
RUN_TYPES: list[str] = ["SCHEDULED", "MANUAL", "INCREMENTAL", "FULL"]

# ──────────────────────────────────────────────
# Issue statuses — matches DQ_ISSUE.issue_status
# ──────────────────────────────────────────────
ISSUE_STATUSES: list[str] = [
    "OPEN", "ACKNOWLEDGED", "IN_PROGRESS",
    "RESOLVED", "WONT_FIX", "RECURRING"
]

# ──────────────────────────────────────────────
# Root cause categories — matches DQ_ISSUE.root_cause_category
# ──────────────────────────────────────────────
ROOT_CAUSE_CATEGORIES: list[str] = [
    "SOURCE_SYSTEM", "PIPELINE_BUG", "SCHEMA_CHANGE",
    "LATE_ARRIVAL", "CONFIGURATION", "UNKNOWN"
]

# ──────────────────────────────────────────────
# Summary statuses — matches DQ_SCORE_SUMMARY.summary_status
# ──────────────────────────────────────────────
SUMMARY_STATUSES: list[str] = ["HEALTHY", "DEGRADED", "CRITICAL", "UNKNOWN"]

# ──────────────────────────────────────────────
# Score levels — matches DQ_SCORE_SUMMARY.score_level
# ──────────────────────────────────────────────
SCORE_LEVELS: list[str] = ["COLUMN", "TABLE", "SCHEMA", "PIPELINE"]

# ──────────────────────────────────────────────
# Rule dimensions — the 10 DQ categories
# ──────────────────────────────────────────────
RULE_DIMENSIONS: list[str] = [
    "COMPLETENESS", "FORMAT", "RANGE", "UNIQUENESS",
    "REFERENTIAL_INTEGRITY", "CONSISTENCY", "TIMELINESS",
    "STATISTICAL", "VOLUME", "SCHEMA"
]

# ──────────────────────────────────────────────
# Assignment scopes
# ──────────────────────────────────────────────
ASSIGNMENT_SCOPES: list[str] = ["COLUMN", "TABLE", "SCHEMA", "PIPELINE"]

# ──────────────────────────────────────────────
# Asset types — matches DATA_ASSET.asset_type
# ──────────────────────────────────────────────
ASSET_TYPES: list[str] = ["PLATFORM", "SCHEMA", "TABLE", "VIEW", "FILE", "COLUMN"]

# ──────────────────────────────────────────────
# Threshold operators
# ──────────────────────────────────────────────
THRESHOLD_OPERATORS: list[str] = [">=", "<=", "=", "!=", ">", "<", "BETWEEN"]

# ──────────────────────────────────────────────
# Environment names
# ──────────────────────────────────────────────
ENVIRONMENT_NAMES: list[str] = ["DEV", "UAT", "PROD"]

# ──────────────────────────────────────────────
# Default owner for auto-generated assignments
# ──────────────────────────────────────────────
DEFAULT_OWNER: str = "dq-automation"
DEFAULT_CREATED_BY: str = "system"
