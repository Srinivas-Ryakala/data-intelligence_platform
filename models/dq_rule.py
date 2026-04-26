"""
Dataclass model for the DQ_RULE table.
Represents a single validation rule definition in the master catalogue.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class DQRule:
    """
    Maps to every column in the DQ_RULE table.

    Attributes:
        dq_rule_id: Auto-incremented PK. None when creating a new rule.
        rule_name: Human-readable short name (e.g. 'Null Check').
        rule_code: Machine-readable unique code (e.g. 'COMP_NULL_CHK').
        rule_type: How the rule is evaluated (THRESHOLD, EXPRESSION, AGGREGATE, etc.).
        rule_dimension: DQ category (COMPLETENESS, FORMAT, RANGE, etc.).
        rule_level: Scope (COLUMN, ROW, DATASET, CROSS_TABLE).
        expression_type: Engine used to execute (SQL, PYSPARK, REGEX, etc.).
        rule_expression: The actual logic string the DQ engine executes.
        expected_condition: The pass condition as a string.
        default_threshold_value: Numeric benchmark for comparison. None for pure expression rules.
        threshold_operator: Comparison operator (>=, <=, =, !=, >, <).
        severity: Business impact (Critical, High, Medium).
        description: Plain English explanation of the rule.
        rule_source: Who defined it (SYSTEM, BUSINESS, STATISTICAL).
        created_by: Username that created this rule.
        is_active: Soft-delete flag.
        created_at: Timestamp when rule was created.
        updated_at: Timestamp of most recent modification.
    """

    dq_rule_id: Optional[int] = None
    rule_name: str = ""
    rule_code: str = ""
    rule_type: str = ""
    rule_dimension: str = ""
    rule_level: str = ""
    expression_type: str = ""
    rule_expression: str = ""
    expected_condition: Optional[str] = None
    default_threshold_value: Optional[float] = None
    threshold_operator: Optional[str] = None
    severity: str = "Medium"
    description: Optional[str] = None
    rule_source: str = "SYSTEM"
    created_by: str = "admin"
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """
        Convert this dataclass to a plain dictionary for DB inserts.

        Returns:
            dict: All fields as key-value pairs.
        """
        return asdict(self)
