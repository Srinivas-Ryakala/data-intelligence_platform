"""
Assignment seeder — auto-generates DQ_RULE_ASSIGNMENT rows from DATA_ASSET metadata.
Reads is_nullable and is_primary_key from DATA_ASSET to create assignments for:
  1. Null check (COMP_NULL_CHK) on every non-nullable column
  2. PK uniqueness (UNIQ_PK) on every primary key column
  3. Empty dataset guard (VOLL_EMPTY_GUARD) on every TABLE asset
  4. Schema drift (SCHM_DRIFT) on every SCHEMA asset
Idempotent: checks assignment_exists() before each insert.
"""

import os
import sys
import logging

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from configs.logging_config import setup_logging
from configs.settings import DEFAULT_OWNER, DEFAULT_CREATED_BY
from db.asset_repo import (
    get_nullable_columns,
    get_pk_columns,
    get_table_assets,
    get_schema_assets,
)
from db.rule_repo import get_rule_by_code
from db.assignment_repo import insert_assignment, assignment_exists
from models.dq_rule_assignment import DQRuleAssignment

setup_logging()
logger = logging.getLogger(__name__)


def seed_null_check_assignments() -> tuple[int, int]:
    """
    Auto-generate COMP_NULL_CHK assignments for every non-nullable column.

    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("COMP_NULL_CHK")
    if rule is None:
        logger.error("Rule COMP_NULL_CHK not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0

    columns = get_nullable_columns()
    inserted = 0
    skipped = 0

    for col in columns:
        asset_id = col.get("parent_asset_id")
        column_asset_id = col.get("asset_id")
        platform_id = col.get("platform_id")

        if assignment_exists(rule.dq_rule_id, asset_id, column_asset_id):
            skipped += 1
            continue

        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=column_asset_id,
            platform_id=platform_id,
            assignment_scope="COLUMN",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {col.get('asset_name', '')} is non-nullable.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )

        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert null-check assignment for column {column_asset_id}."
            )

    logger.info(f"Null Check assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped


def seed_pk_uniqueness_assignments() -> tuple[int, int]:
    """
    Auto-generate UNIQ_PK assignments for every primary key column.

    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("UNIQ_PK")
    if rule is None:
        logger.error("Rule UNIQ_PK not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0

    pk_columns = get_pk_columns()
    inserted = 0
    skipped = 0

    for col in pk_columns:
        asset_id = col.get("parent_asset_id")
        column_asset_id = col.get("asset_id")
        platform_id = col.get("platform_id")

        if assignment_exists(rule.dq_rule_id, asset_id, column_asset_id):
            skipped += 1
            continue

        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=column_asset_id,
            platform_id=platform_id,
            assignment_scope="COLUMN",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {col.get('asset_name', '')} is a primary key.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )

        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert PK-uniqueness assignment for column {column_asset_id}."
            )

    logger.info(f"PK Uniqueness assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped


def seed_empty_guard_assignments() -> tuple[int, int]:
    """
    Auto-generate VOLL_EMPTY_GUARD assignments for every TABLE asset.

    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("VOLL_EMPTY_GUARD")
    if rule is None:
        logger.error("Rule VOLL_EMPTY_GUARD not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0

    tables = get_table_assets()
    inserted = 0
    skipped = 0

    for table in tables:
        asset_id = table.get("asset_id")
        platform_id = table.get("platform_id")

        if assignment_exists(rule.dq_rule_id, asset_id, None):
            skipped += 1
            continue

        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=None,
            platform_id=platform_id,
            assignment_scope="TABLE",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {table.get('asset_name', '')} must not be empty.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )

        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert empty-guard assignment for table {asset_id}."
            )

    logger.info(f"Empty Guard assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped


def seed_schema_drift_assignments() -> tuple[int, int]:
    """
    Auto-generate SCHM_DRIFT assignments for every SCHEMA asset.

    Returns:
        tuple[int, int]: (inserted_count, skipped_count)
    """
    rule = get_rule_by_code("SCHM_DRIFT")
    if rule is None:
        logger.error("Rule SCHM_DRIFT not found in DQ_RULE. Run rule_seeder first.")
        return 0, 0

    schemas = get_schema_assets()
    inserted = 0
    skipped = 0

    for schema in schemas:
        asset_id = schema.get("asset_id")
        platform_id = schema.get("platform_id")

        if assignment_exists(rule.dq_rule_id, asset_id, None):
            skipped += 1
            continue

        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=asset_id,
            column_asset_id=None,
            platform_id=platform_id,
            assignment_scope="SCHEMA",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Auto-generated: {schema.get('asset_name', '')} schema must not drift.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=True,
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )

        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
        else:
            logger.warning(
                f"Failed to insert schema-drift assignment for schema {asset_id}."
            )

    logger.info(f"Schema Drift assignments: {inserted} inserted, {skipped} skipped.")
    return inserted, skipped


def seed_all_assignments() -> None:
    """
    Run all four assignment seeders and print a combined summary.

    Returns:
        None
    """
    total_inserted = 0
    total_skipped = 0

    ins, skip = seed_null_check_assignments()
    total_inserted += ins
    total_skipped += skip

    ins, skip = seed_pk_uniqueness_assignments()
    total_inserted += ins
    total_skipped += skip

    ins, skip = seed_empty_guard_assignments()
    total_inserted += ins
    total_skipped += skip

    ins, skip = seed_schema_drift_assignments()
    total_inserted += ins
    total_skipped += skip

    # ── Summary ──
    summary = (
        f"\n{'='*50}\n"
        f"ASSIGNMENT SEEDER SUMMARY\n"
        f"{'='*50}\n"
        f"Total inserted: {total_inserted}\n"
        f"Total skipped : {total_skipped}\n"
        f"{'='*50}"
    )
    logger.info(summary)
    print(summary)


if __name__ == "__main__":
    seed_all_assignments()
