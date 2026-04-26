"""
Rule seeder — loads all DQ rules from data/rules_seed.json into the DQ_RULE table.
Idempotent: checks rule_exists() before each insert. Safe to run multiple times.
"""

import json
import os
import sys
import logging

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from configs.logging_config import setup_logging
from db.rule_repo import insert_rule, rule_exists
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


def seed_rules() -> None:
    """
    Main seeder function. Reads seed data and inserts each rule into DQ_RULE.
    Skips rules that already exist (idempotent).
    Prints a summary at the end.

    Returns:
        None
    """
    rules_data = load_seed_data()

    inserted_count = 0
    skipped_count = 0
    error_count = 0

    for idx, rule_dict in enumerate(rules_data, start=1):
        rule_code = rule_dict.get("rule_code", "")

        if not rule_code:
            logger.warning(f"Rule at index {idx} has no rule_code — skipping.")
            skipped_count += 1
            continue

        # Check idempotency
        if rule_exists(rule_code):
            logger.info(f"[{idx}] Rule '{rule_code}' already exists — skipped.")
            skipped_count += 1
            continue

        # Build DQRule object
        try:
            rule = DQRule(
                rule_name=rule_dict.get("rule_name", ""),
                rule_code=rule_code,
                rule_type=rule_dict.get("rule_type", ""),
                rule_dimension=rule_dict.get("rule_dimension", ""),
                rule_level=rule_dict.get("rule_level", ""),
                expression_type=rule_dict.get("expression_type", ""),
                rule_expression=rule_dict.get("rule_expression", ""),
                expected_condition=rule_dict.get("expected_condition"),
                default_threshold_value=rule_dict.get("default_threshold_value"),
                threshold_operator=rule_dict.get("threshold_operator"),
                severity=rule_dict.get("severity", "Medium"),
                description=rule_dict.get("description"),
                rule_source=rule_dict.get("rule_source", "SYSTEM"),
                created_by=rule_dict.get("created_by", "admin"),
                is_active=rule_dict.get("is_active", True),
            )

            new_id = insert_rule(rule)
            if new_id is not None:
                logger.info(f"[{idx}] Inserted '{rule_code}' — ID: {new_id}")
                inserted_count += 1
            else:
                logger.warning(f"[{idx}] Could not insert '{rule_code}'.")
                error_count += 1

        except Exception as e:
            logger.error(f"[{idx}] Error processing rule '{rule_code}': {e}")
            error_count += 1

    # ── Summary ──
    summary = (
        f"\n{'='*50}\n"
        f"RULE SEEDER SUMMARY\n"
        f"{'='*50}\n"
        f"Total rules in seed file : {len(rules_data)}\n"
        f"Inserted                 : {inserted_count}\n"
        f"Already existed (skipped): {skipped_count}\n"
        f"Errors                   : {error_count}\n"
        f"{'='*50}"
    )
    logger.info(summary)
    print(summary)


if __name__ == "__main__":
    seed_rules()
