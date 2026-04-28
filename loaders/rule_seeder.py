"""
Rule seeder — loads all DQ rules from data/rules_seed.json into the DQ_RULE table.
Idempotent: checks rule_exists() before each insert. Safe to run multiple times.
"""

import json
import os
import sys
import logging
from datetime import datetime
from decimal import Decimal

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from configs.logging_config import setup_logging
from db.rule_repo import (
    get_all_rule_codes,
    get_rule_by_code,
    insert_rule,
    update_rule,
    deactivate_rule,
)
from models.dq_rule import DQRule

setup_logging()
logger = logging.getLogger(__name__)

# Path to the seed file
SEED_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "rules_seed.json"
)


def load_seed_data() -> list[dict]:
    """
    Read rules_seed.json and return the list of rule dictionaries.

    Returns:
        list[dict]: Parsed JSON rule objects.

    Raises:
        FileNotFoundError: If the seed file does not exist.
        json.JSONDecodeError: If the JSON is malformed.
    """
    if not os.path.exists(SEED_FILE):
        raise FileNotFoundError(f"Seed file not found: {SEED_FILE}")

    with open(SEED_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info(f"Loaded {len(data)} rules from {SEED_FILE}.")
    return data


def _values_equal(existing_value, candidate_value) -> bool:
    if existing_value == candidate_value:
        return True

    if existing_value is None or candidate_value is None:
        return False

    numeric_types = (int, float, Decimal)
    if isinstance(existing_value, numeric_types) and isinstance(candidate_value, numeric_types):
        try:
            return Decimal(str(existing_value)) == Decimal(str(candidate_value))
        except Exception:
            return False

    return False


def _rule_changed(existing: DQRule, candidate: DQRule) -> bool:
    compare_fields = [
        "rule_name",
        "rule_type",
        "rule_dimension",
        "rule_level",
        "expression_type",
        "rule_expression",
        "expected_condition",
        "default_threshold_value",
        "threshold_operator",
        "severity",
        "description",
        "rule_source",
        "created_by",
        "is_active",
    ]
    for field in compare_fields:
        if not _values_equal(getattr(existing, field), getattr(candidate, field)):
            return True
    return False


def seed_rules() -> None:
    """
    Main seeder function. Reads seed data and inserts or updates rules in DQ_RULE.
    Prints a summary at the end.

    Returns:
        None
    """
    rules_data = load_seed_data()

    inserted_count = 0
    updated_count = 0
    deactivated_count = 0
    skipped_count = 0
    error_count = 0

    seed_codes = set()
    for idx, rule_dict in enumerate(rules_data, start=1):
        rule_code = rule_dict.get("rule_code", "")

        if not rule_code:
            logger.warning(f"Rule at index {idx} has no rule_code — skipping.")
            skipped_count += 1
            continue

        seed_codes.add(rule_code)

        # Build DQRule object from seed data
        rule = DQRule(
            rule_name=rule_dict.get("rule_name", ""),
            rule_code=rule_code,
            rule_type=rule_dict.get("rule_type", ""),
            rule_dimension=rule_dict.get("rule_dimension", ""),
            rule_level=rule_dict.get("rule_level", ""),
            expression_type=rule_dict.get("expression_type"),
            rule_expression=rule_dict.get("rule_expression", ""),
            expected_condition=rule_dict.get("expected_condition"),
            default_threshold_value=rule_dict.get("default_threshold_value"),
            threshold_operator=rule_dict.get("threshold_operator"),
            severity=rule_dict.get("severity", "Medium"),
            description=rule_dict.get("description"),
            rule_source=rule_dict.get("rule_source", "SYSTEM"),
            created_by=rule_dict.get("created_by", "admin"),
            is_active=rule_dict.get("is_active", True),
            created_at=rule_dict.get("created_at"),
            updated_at=rule_dict.get("updated_at"),
        )

        try:
            existing_rule = get_rule_by_code(rule_code)

            if existing_rule is None:
                new_id = insert_rule(rule)
                if new_id is not None:
                    logger.info(f"[{idx}] Inserted '{rule_code}' — ID: {new_id}")
                    inserted_count += 1
                else:
                    logger.warning(f"[{idx}] Could not insert '{rule_code}'.")
                    error_count += 1
                continue

            if _rule_changed(existing_rule, rule):
                rule.created_at = existing_rule.created_at or rule.created_at
                rule.updated_at = datetime.now()
                if update_rule(rule):
                    logger.info(f"[{idx}] Updated '{rule_code}' — ID: {existing_rule.dq_rule_id}")
                    updated_count += 1
                else:
                    logger.warning(f"[{idx}] Could not update '{rule_code}'.")
                    error_count += 1
            else:
                logger.info(f"[{idx}] Rule '{rule_code}' is unchanged — skipped.")
                skipped_count += 1

        except Exception as e:
            logger.error(f"[{idx}] Error processing rule '{rule_code}': {e}")
            error_count += 1

    try:
        existing_codes = get_all_rule_codes()
        removed_codes = existing_codes - seed_codes
        deactivated_count = 0
        for removed_code in removed_codes:
            if deactivate_rule(removed_code):
                logger.info(f"Deactivated removed rule '{removed_code}'.")
                deactivated_count += 1
    except Exception as e:
        logger.error(f"Failed to deactivate removed rules: {e}")
        error_count += 1

    # ── Summary ──
    summary = (
        f"\n{'='*50}\n"
        f"RULE SEEDER SUMMARY\n"
        f"{'='*50}\n"
        f"Total rules in seed file : {len(rules_data)}\n"
        f"Inserted                 : {inserted_count}\n"
        f"Updated                  : {updated_count}\n"
        f"Deactivated              : {deactivated_count}\n"
        f"Already existed (skipped): {skipped_count}\n"
        f"Errors                   : {error_count}\n"
        f"{'='*50}"
    )
    logger.info(summary)
    print(summary)


if __name__ == "__main__":
    seed_rules()
