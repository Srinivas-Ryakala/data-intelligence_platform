"""
Repository for DQ_RULE table.
CRUD operations for validation rule definitions.
"""

import logging
from datetime import datetime
from typing import Optional

from db.connection import get_connection
from models.dq_rule import DQRule

logger = logging.getLogger(__name__)


def get_all_active_rules() -> list[DQRule]:
    """
    Fetch all active rules from DQ_RULE.

    Returns:
        list[DQRule]: All rules where is_active = True.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM DQ_RULE WHERE is_active = 1")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [DQRule(**dict(zip(columns, row))) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch active rules: {e}")
        return []


def get_rule_by_code(rule_code: str) -> Optional[DQRule]:
    """
    Fetch a single rule by its unique rule_code.

    Args:
        rule_code: The machine-readable rule code (e.g. 'COMP_NULL_CHK').

    Returns:
        DQRule or None: The matching rule, or None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM DQ_RULE WHERE rule_code = ?",
            rule_code
        )
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        conn.close()
        if row is None:
            return None
        return DQRule(**dict(zip(columns, row)))
    except Exception as e:
        logger.error(f"Failed to fetch rule by code '{rule_code}': {e}")
        return None


def get_rule_by_id(dq_rule_id: int) -> Optional[DQRule]:
    """
    Fetch a single rule by its primary key.

    Args:
        dq_rule_id: The dq_rule_id to look up.

    Returns:
        DQRule or None: The matching rule, or None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM DQ_RULE WHERE dq_rule_id = ?",
            dq_rule_id
        )
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        conn.close()
        if row is None:
            return None
        return DQRule(**dict(zip(columns, row)))
    except Exception as e:
        logger.error(f"Failed to fetch rule by ID {dq_rule_id}: {e}")
        return None


def rule_exists(rule_code: str) -> bool:
    """
    Check if a rule with the given rule_code already exists.

    Args:
        rule_code: The machine-readable rule code.

    Returns:
        bool: True if the rule exists, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM DQ_RULE WHERE rule_code = ?",
            rule_code
        )
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        logger.error(f"Failed to check if rule '{rule_code}' exists: {e}")
        return False


def insert_rule(rule: DQRule) -> Optional[int]:
    """
    Insert a new rule into DQ_RULE. Checks for duplicate rule_code before inserting.

    Args:
        rule: The DQRule object to insert.

    Returns:
        int or None: The new dq_rule_id if inserted, None if it already exists or on error.
    """
    if rule_exists(rule.rule_code):
        logger.info(f"Rule '{rule.rule_code}' already exists — skipping insert.")
        return None

    try:
        now = datetime.now()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO DQ_RULE (
                rule_name, rule_code, rule_type, rule_dimension, rule_level,
                expression_type, rule_expression, expected_condition,
                default_threshold_value, threshold_operator, severity,
                description, rule_source, created_by, is_active,
                created_at, updated_at
            )
            OUTPUT INSERTED.dq_rule_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rule.rule_name,
            rule.rule_code,
            rule.rule_type,
            rule.rule_dimension,
            rule.rule_level,
            rule.expression_type,
            rule.rule_expression,
            rule.expected_condition,
            rule.default_threshold_value,
            rule.threshold_operator,
            rule.severity,
            rule.description,
            rule.rule_source,
            rule.created_by,
            rule.is_active,
            rule.created_at or now,
            rule.updated_at or now,
        )
        new_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        logger.info(f"Inserted rule '{rule.rule_code}' with ID {new_id}.")
        return new_id
    except Exception as e:
        logger.error(f"Failed to insert rule '{rule.rule_code}': {e}")
        return None
