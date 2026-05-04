"""
manual_assigner.py — Interactive CLI for manually assigning DQ rules to data assets.

Workflow:
  1. Browse the DATA_ASSET hierarchy: Server → Database → Schema → Table → Column
     (user can stop at any level — schema, table, or column)
  2. Suggest applicable DQ rules based on the selected asset level and metadata
  3. User selects which rules to assign
  4. Create DQ_RULE_ASSIGNMENT entries for each selection

Usage:
    python loaders/manual_assigner.py
"""
import os
import sys
import logging
from datetime import datetime
from typing import Optional

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from configs.logging_config import setup_logging
from configs.settings import DEFAULT_OWNER, DEFAULT_CREATED_BY
from db.asset_repo import (
    get_platforms,
    get_children_by_type,
    get_columns_for_table,
    get_asset_by_id,
    asset_exists,
)
from db.assignment_repo import (
    insert_assignment,
    assignment_exists,
    deactivate_assignment,
    deactivate_all_assignments,
    list_active_assignments_summary,
)
from engine.rule_suggester import suggest_rules, format_rule_for_display
from models.dq_rule_assignment import DQRuleAssignment

setup_logging()
logger = logging.getLogger(__name__)

# ─── Display helpers ──────────────────────────────────────────────────────────

def _print_header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def _deduplicate_assets(assets: list[dict]) -> list[dict]:
    """
    Remove duplicate assets by (asset_name, qualified_name), keeping one entry.
    Stores all duplicate asset_ids in '_all_ids' so child queries can search
    across all duplicates (e.g., tables parented under different schema IDs
    that share the same name).
    """
    groups: dict[tuple, list[dict]] = {}
    for asset in assets:
        key = (asset.get("asset_name", ""), asset.get("qualified_name", ""))
        groups.setdefault(key, []).append(asset)

    unique = []
    for key, dupes in groups.items():
        # Keep the first entry but tag it with all IDs
        entry = dupes[0].copy()
        entry["_all_ids"] = [d.get("asset_id") for d in dupes]
        unique.append(entry)
    return unique


def _print_assets(assets: list[dict], label: str) -> None:
    """Print a numbered list of assets."""
    if not assets:
        print(f"\n  No {label} found.")
        return
    print(f"\n  Available {label}:")
    print(f"  {'-'*60}")
    for i, asset in enumerate(assets, 1):
        name = asset.get("asset_name", "Unknown")
        qualified = asset.get("qualified_name", "")
        extra = f"  ({qualified})" if qualified else ""
        print(f"  [{i:>3}] {name}{extra}")
    print()



def _get_user_choice(prompt: str, max_val: int, allow_skip: bool = False) -> Optional[int]:
    """
    Get a numbered choice from the user.

    Args:
        prompt: The input prompt to show.
        max_val: Maximum valid selection number.
        allow_skip: If True, allow 's' to skip/proceed.

    Returns:
        int: 1-based selection, or None if skipped.
    """
    while True:
        raw = input(prompt).strip()
        if allow_skip and raw.lower() in ("s", "skip", ""):
            return None
        try:
            choice = int(raw)
            if 1 <= choice <= max_val:
                return choice
            print(f"  ⚠ Please enter a number between 1 and {max_val}.")
        except ValueError:
            if allow_skip:
                print(f"  ⚠ Enter a number (1-{max_val}) or 's' to skip.")
            else:
                print(f"  ⚠ Enter a number between 1 and {max_val}.")


def _get_multi_choice(prompt: str, max_val: int) -> list[int]:
    """
    Get multiple numbered choices (comma-separated) or 'all'.

    Returns:
        list[int]: List of 1-based selections.
    """
    while True:
        raw = input(prompt).strip()
        if raw.lower() == "all":
            return list(range(1, max_val + 1))
        if raw.lower() in ("q", "quit", "none", ""):
            return []
        try:
            choices = []
            for part in raw.split(","):
                part = part.strip()
                if "-" in part:
                    # Range: e.g., "1-5"
                    start, end = part.split("-", 1)
                    start, end = int(start.strip()), int(end.strip())
                    choices.extend(range(start, end + 1))
                else:
                    choices.append(int(part))
            # Validate
            valid = [c for c in choices if 1 <= c <= max_val]
            if valid:
                return sorted(set(valid))
            print(f"  ⚠ No valid selections. Enter numbers 1-{max_val}, ranges (1-5), 'all', or 'q' to quit.")
        except ValueError:
            print(f"  ⚠ Invalid input. Enter numbers separated by commas (e.g., 1,3,5), ranges (1-5), 'all', or 'q'.")


# ─── Core workflow ────────────────────────────────────────────────────────────

def browse_and_select_asset() -> Optional[dict]:
    """
    Guide the user through browsing the asset hierarchy and selecting a target.

    Returns:
        dict: Selected asset info with keys:
            - asset_id: int
            - asset_type: str (SCHEMA, TABLE, COLUMN)
            - asset_name: str
            - platform_id: int
            - parent_asset_id: int or None (table ID for column selections)
            - column_data_type: str or None
        or None if cancelled.
    """
    _print_header("MANUAL RULE ASSIGNMENT — Asset Browser")

    # ── Step 1: Select Server ──
    platforms = get_platforms()
    if not platforms:
        print("\n  ❌ No servers found in DATA_ASSET. Ensure data is loaded.")
        return None

    _print_assets(platforms, "Servers")
    choice = _get_user_choice("  Select server [number]: ", len(platforms))
    if choice is None:
        return None
    selected_platform = platforms[choice - 1]
    platform_id = selected_platform.get("asset_id")
    print(f"  ✓ Selected: {selected_platform.get('asset_name')}")

    # ── Step 2: Select Database ──
    databases = _deduplicate_assets(get_children_by_type(platform_id, "DATABASE"))
    if not databases:
        # Some hierarchies skip DATABASE level — try SCHEMA directly
        schemas = _deduplicate_assets(get_children_by_type(platform_id, "SCHEMA"))
        if schemas:
            databases = None  # skip to schema
        else:
            print("\n  ❌ No databases or schemas found under this server.")
            return None

    if databases:
        _print_assets(databases, "Databases")
        choice = _get_user_choice("  Select database [number]: ", len(databases))
        if choice is None:
            return None
        selected_db = databases[choice - 1]
        db_id = selected_db.get("asset_id")
        print(f"  ✓ Selected: {selected_db.get('asset_name')}")

        # ── Step 3: Select Schema ──
        schemas = _deduplicate_assets(get_children_by_type(db_id, "SCHEMA"))
    else:
        schemas = _deduplicate_assets(get_children_by_type(platform_id, "SCHEMA"))

    if not schemas:
        print("\n  ❌ No schemas found.")
        return None

    _print_assets(schemas, "Schemas")
    print("  💡 Enter 's' to assign rules at schema level, or select a schema to drill down.")
    choice = _get_user_choice("  Select schema [number or 's']: ", len(schemas), allow_skip=False)
    if choice is None:
        return None
    selected_schema = schemas[choice - 1]
    schema_id = selected_schema.get("asset_id")
    print(f"  ✓ Selected: {selected_schema.get('asset_name')}")

    # Ask: assign at schema level or drill down?
    drill = input("\n  Assign rules at SCHEMA level? (y/n, default=n): ").strip().lower()
    if drill == "y":
        return {
            "asset_id": schema_id,
            "asset_type": "SCHEMA",
            "asset_name": selected_schema.get("asset_name"),
            "platform_id": platform_id,
            "parent_asset_id": None,
            "column_data_type": None,
        }

    # ── Step 4: Select Table ──
    # Query tables across ALL duplicate schema IDs (data may have duplicates)
    all_schema_ids = selected_schema.get("_all_ids", [schema_id])
    tables = []
    for sid in all_schema_ids:
        tables.extend(get_children_by_type(sid, "TABLE"))
    tables = _deduplicate_assets(tables)
    if not tables:
        print("\n  ❌ No tables found under this schema.")
        return None

    _print_assets(tables, "Tables")
    choice = _get_user_choice("  Select table [number]: ", len(tables))
    if choice is None:
        return None
    selected_table = tables[choice - 1]
    table_id = selected_table.get("asset_id")
    print(f"  ✓ Selected: {selected_table.get('asset_name')}")

    # Ask: assign at table level or drill to columns?
    drill = input("\n  Assign rules at TABLE level? (y/n, default=n): ").strip().lower()
    if drill == "y":
        return {
            "asset_id": table_id,
            "asset_type": "TABLE",
            "asset_name": selected_table.get("asset_name"),
            "platform_id": platform_id,
            "parent_asset_id": None,
            "column_data_type": None,
        }

    # ── Step 5: Select Column ──
    columns = get_columns_for_table(table_id)
    if not columns:
        print("\n  ❌ No columns found under this table. Assigning at TABLE level instead.")
        return {
            "asset_id": table_id,
            "asset_type": "TABLE",
            "asset_name": selected_table.get("asset_name"),
            "platform_id": platform_id,
            "parent_asset_id": None,
            "column_data_type": None,
        }

    # Show columns with data type
    print(f"\n  Available Columns:")
    print(f"  {'-'*60}")
    for i, col in enumerate(columns, 1):
        name = col.get("asset_name", "Unknown")
        dtype = col.get("data_type", "?")
        nullable = "NULL" if col.get("is_nullable") else "NOT NULL"
        pk = " [PK]" if col.get("is_primary_key") else ""
        print(f"  [{i:>3}] {name:<30} {dtype:<15} {nullable}{pk}")
    print()

    choice = _get_user_choice("  Select column [number]: ", len(columns))
    if choice is None:
        return None
    selected_column = columns[choice - 1]
    print(f"  ✓ Selected: {selected_column.get('asset_name')} ({selected_column.get('data_type', '?')})")

    return {
        "asset_id": selected_column.get("asset_id"),
        "asset_type": "COLUMN",
        "asset_name": selected_column.get("asset_name"),
        "platform_id": platform_id,
        "parent_asset_id": table_id,  # The table this column belongs to
        "column_data_type": selected_column.get("data_type"),
    }


def select_and_assign_rules(asset_info: dict) -> int:
    """
    Suggest applicable rules and let the user select which ones to assign.

    Args:
        asset_info: Dict from browse_and_select_asset().

    Returns:
        int: Number of rules successfully assigned.
    """
    asset_type = asset_info["asset_type"]
    asset_id = asset_info["asset_id"]
    platform_id = asset_info["platform_id"]
    column_data_type = asset_info.get("column_data_type")

    _print_header(f"APPLICABLE RULES for {asset_info['asset_name']} ({asset_type})")

    # Get suggested rules
    rules = suggest_rules(asset_type, column_data_type)
    if not rules:
        print("\n  ❌ No applicable rules found for this asset type.")
        return 0

    # Display rules
    print(f"\n  {'#':>4}  {'':3} {'Rule Code':<20} | {'Rule Name':<30} | {'Dimension':<20} | Severity")
    print(f"  {'-'*95}")
    for i, rule in enumerate(rules, 1):
        print(format_rule_for_display(rule, i))
    print(f"\n  Total: {len(rules)} applicable rules")

    # Get user selections
    print("\n  💡 Enter rule numbers separated by commas (e.g., 1,3,5)")
    print("     Use ranges (e.g., 1-10), 'all' for all rules, or 'q' to cancel.")
    selections = _get_multi_choice("  Select rules to assign: ", len(rules))

    if not selections:
        print("\n  ⚠ No rules selected.")
        return 0

    selected_rules = [rules[i - 1] for i in selections]
    print(f"\n  Selected {len(selected_rules)} rules for assignment.")

    # Determine asset_id and column_asset_id based on asset type
    if asset_type == "COLUMN":
        assign_asset_id = asset_info.get("parent_asset_id")  # Table ID
        column_asset_id = asset_id                            # Column ID
    else:
        assign_asset_id = asset_id
        column_asset_id = None

    # Validate asset exists
    if assign_asset_id and not asset_exists(assign_asset_id):
        print(f"\n  ❌ Asset ID {assign_asset_id} does not exist. Aborting.")
        return 0

    # Create assignments
    inserted = 0
    skipped = 0
    for rule in selected_rules:
        # Check for duplicates
        if assignment_exists(rule.dq_rule_id, assign_asset_id, column_asset_id):
            print(f"  ⚠ {rule.rule_code} already assigned — skipping.")
            skipped += 1
            continue

        assignment = DQRuleAssignment(
            dq_rule_id=rule.dq_rule_id,
            asset_id=assign_asset_id,
            column_asset_id=column_asset_id,
            platform_id=platform_id,
            assignment_scope=asset_type if asset_type != "COLUMN" else "COLUMN",
            execution_mode="BLOCKING",
            execution_frequency="EVERY_RUN",
            business_context=f"Manual assignment: {rule.rule_name} on {asset_info['asset_name']}.",
            owner_name=DEFAULT_OWNER,
            is_mandatory=rule.severity in ("Critical", "High"),
            is_active=True,
            created_by=DEFAULT_CREATED_BY,
        )

        new_id = insert_assignment(assignment)
        if new_id is not None:
            inserted += 1
            print(f"  ✓ Assigned {rule.rule_code} → ID {new_id}")
        else:
            print(f"  ❌ Failed to assign {rule.rule_code}")

    # Summary
    print(f"\n{'='*70}")
    print(f"  ASSIGNMENT SUMMARY")
    print(f"{'='*70}")
    print(f"  Target  : {asset_info['asset_name']} ({asset_type})")
    print(f"  Assigned: {inserted}")
    print(f"  Skipped : {skipped} (already existed)")
    print(f"  Failed  : {len(selected_rules) - inserted - skipped}")
    print(f"{'='*70}")

    return inserted


def _show_active_assignments() -> None:
    """Display all currently active assignments."""
    _print_header("ACTIVE RULE ASSIGNMENTS")
    assignments = list_active_assignments_summary()
    if not assignments:
        print("\n  No active assignments found.")
        return

    print(f"\n  {'ID':>4}  {'Rule Code':<20} {'Rule Name':<30} {'Table':<20} {'Column':<15} {'Created'}")
    print(f"  {'-'*110}")
    for a in assignments:
        col = a.get('column_name') or '-'
        tbl = a.get('table_name') or '-'
        dt = str(a.get('created_at', ''))[:19]
        print(
            f"  {a['dq_rule_assignment_id']:>4}  {a['rule_code']:<20} "
            f"{a['rule_name']:<30} {tbl:<20} {col:<15} {dt}"
        )
    print(f"\n  Total: {len(assignments)} active assignments")


def _reset_all_assignments() -> None:
    """Deactivate all active assignments after confirmation."""
    assignments = list_active_assignments_summary()
    if not assignments:
        print("\n  No active assignments to reset.")
        return

    print(f"\n  ⚠ This will deactivate {len(assignments)} active assignments.")
    confirm = input("  Are you sure? (yes/no): ").strip().lower()
    if confirm in ("yes", "y"):
        count = deactivate_all_assignments()
        print(f"  ✓ Deactivated {count} assignments. You can now assign fresh rules.")
    else:
        print("  Cancelled.")


def _deactivate_specific() -> None:
    """Deactivate a specific assignment by ID."""
    _show_active_assignments()
    raw = input("\n  Enter assignment ID to deactivate (or 'q' to cancel): ").strip()
    if raw.lower() in ('q', 'quit', ''):
        return
    try:
        aid = int(raw)
        if deactivate_assignment(aid):
            print(f"  ✓ Deactivated assignment ID {aid}.")
        else:
            print(f"  ❌ Failed to deactivate ID {aid}.")
    except ValueError:
        print("  ⚠ Invalid ID.")


def run_manual_assignment() -> None:
    """
    Main entry point for the manual assignment CLI.
    Provides a menu to view, reset, assign, or quit.
    """
    _print_header("DQ FRAMEWORK — Manual Rule Assignment Tool")
    print("  This tool lets you manually assign DQ rules to data assets.")
    print("  Browse: Server → Database → Schema → Table → Column")
    print("  You can assign rules at any level (schema, table, or column).")

    # ── Startup: check for leftover active assignments ──
    existing = list_active_assignments_summary()
    if existing:
        print(f"\n  ⚠ Found {len(existing)} active assignments from previous sessions:")
        for a in existing:
            col = a.get('column_name') or '-'
            tbl = a.get('table_name') or '-'
            print(f"    • {a['rule_code']} on {tbl}.{col}")
        print()
        print("  [1] Keep them (new assignments will be ADDED to these)")
        print("  [2] Reset all (deactivate old, start fresh)")
        choice = input("  Select (1 or 2, default=2): ").strip()
        if choice != "1":
            count = deactivate_all_assignments()
            print(f"  ✓ Deactivated {count} old assignments. Starting fresh.")

    while True:
        print(f"\n  {'─'*50}")
        print("  MAIN MENU:")
        print("  [1] Assign rules to an asset")
        print("  [2] View active assignments")
        print("  [3] Deactivate a specific assignment")
        print("  [4] Reset ALL assignments (deactivate all)")
        print("  [q] Quit")
        choice = input("  Select option: ").strip().lower()

        if choice == "1":
            asset_info = browse_and_select_asset()
            if asset_info is None:
                print("\n  ⚠ No asset selected.")
                continue
            select_and_assign_rules(asset_info)

        elif choice == "2":
            _show_active_assignments()

        elif choice == "3":
            _deactivate_specific()

        elif choice == "4":
            _reset_all_assignments()

        elif choice in ("q", "quit"):
            break

        else:
            print("  ⚠ Invalid choice.")

    print("\n  Done. Use 'python main.py' to execute the DQ pipeline.\n")


if __name__ == "__main__":
    run_manual_assignment()
