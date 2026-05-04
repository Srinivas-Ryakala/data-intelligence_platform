"""
Repository for DATA_ASSET table.
Read-only — DATA_ASSET is provided by another team. Never INSERT or UPDATE.
"""

import logging
from typing import Optional

from db.connection import get_connection

logger = logging.getLogger(__name__)


def get_all_assets() -> list[dict]:
    """
    Fetch all active assets from DATA_ASSET.

    Returns:
        list[dict]: Each dict contains all columns for an active asset row.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM DATA_ASSET WHERE is_active = 1"
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch all assets: {e}")
        return []


def get_columns_for_table(table_asset_id: int) -> list[dict]:
    """
    Fetch all COLUMN-type assets whose parent is the given table asset.

    Args:
        table_asset_id: The asset_id of the parent TABLE.

    Returns:
        list[dict]: Column assets belonging to this table.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM DATA_ASSET
            WHERE parent_asset_id = ?
              AND asset_type = 'COLUMN'
              AND is_active = 1
            """,
            table_asset_id
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch columns for table {table_asset_id}: {e}")
        return []


def get_nullable_columns() -> list[dict]:
    """
    Fetch all COLUMN assets where is_nullable = False (non-nullable columns).
    These are candidates for automatic null-check rule assignment.

    Returns:
        list[dict]: Non-nullable column assets.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT c.*, t.asset_name AS table_name, t.qualified_name AS table_qualified_name
            FROM DATA_ASSET c
            JOIN DATA_ASSET t ON c.parent_asset_id = t.asset_id
            WHERE c.asset_type = 'COLUMN'
              AND c.is_nullable = 0
              AND c.is_active = 1
              AND t.is_active = 1
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch non-nullable columns: {e}")
        return []


def get_pk_columns() -> list[dict]:
    """
    Fetch all COLUMN assets where is_primary_key = True.
    These are candidates for automatic uniqueness rule assignment.

    Returns:
        list[dict]: Primary key column assets.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT c.*, t.asset_name AS table_name, t.qualified_name AS table_qualified_name
            FROM DATA_ASSET c
            JOIN DATA_ASSET t ON c.parent_asset_id = t.asset_id
            WHERE c.asset_type = 'COLUMN'
              AND c.is_primary_key = 1
              AND c.is_active = 1
              AND t.is_active = 1
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch PK columns: {e}")
        return []


def get_table_assets() -> list[dict]:
    """
    Fetch all TABLE-type assets.

    Returns:
        list[dict]: Table-level assets.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM DATA_ASSET
            WHERE asset_type = 'TABLE'
              AND is_active = 1
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch table assets: {e}")
        return []


def get_schema_assets() -> list[dict]:
    """
    Fetch all SCHEMA-type assets.

    Returns:
        list[dict]: Schema-level assets.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM DATA_ASSET
            WHERE asset_type = 'SCHEMA'
              AND is_active = 1
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch schema assets: {e}")
        return []


def get_table_name(asset_id: int) -> Optional[str]:
    """
    Get the asset_name for a given asset_id.

    Args:
        asset_id: The asset_id to look up.

    Returns:
        str or None: The asset_name, or None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT asset_name FROM DATA_ASSET WHERE asset_id = ?",
            asset_id
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.error(f"Failed to get table name for asset {asset_id}: {e}")
        return None


def get_qualified_name(asset_id: int) -> tuple[Optional[str], Optional[str]]:
    """
    Get the qualified_name and asset_type for a given asset_id.

    Returns:
        tuple: (qualified_name, asset_type) — both None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT qualified_name, asset_type FROM DATA_ASSET WHERE asset_id = ?",
            asset_id
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None, None
        return row[0], row[1]
    except Exception as e:
        logger.error(f"Failed to get qualified name for asset {asset_id}: {e}")
        return None, None


def get_parent_table_for_column(column_asset_id: int) -> Optional[dict]:
    """
    Get the parent TABLE asset for a given COLUMN asset.

    Args:
        column_asset_id: The asset_id of the column.

    Returns:
        dict or None: The parent table asset, or None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT t.*
            FROM DATA_ASSET t
            JOIN DATA_ASSET c ON c.parent_asset_id = t.asset_id
            WHERE c.asset_id = ?
              AND t.asset_type = 'TABLE'
            """,
            column_asset_id
        )
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        conn.close()
        return dict(zip(columns, row)) if row else None
    except Exception as e:
        logger.error(f"Failed to get parent table for column {column_asset_id}: {e}")
        return None


def asset_exists(asset_id: int) -> bool:
    """
    Check if an asset with the given asset_id exists in DATA_ASSET.

    Args:
        asset_id: The asset_id to check.

    Returns:
        bool: True if exists, False otherwise.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DATA_ASSET WHERE asset_id = ?", asset_id)
        row = cursor.fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        logger.error(f"Failed to check if asset {asset_id} exists: {e}")
        return False


def get_column_data_type(column_asset_id: int) -> Optional[str]:
    """
    Get the data_type for a COLUMN asset.

    Args:
        column_asset_id: The asset_id of the column.

    Returns:
        str or None: The data type string (e.g. 'INT', 'VARCHAR'), or None.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT data_type FROM DATA_ASSET WHERE asset_id = ?",
            column_asset_id
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"Failed to get data type for column asset {column_asset_id}: {e}")
        return None


def get_platforms() -> list[dict]:
    """
    Fetch all top-level SERVER assets (top of the hierarchy).

    Returns:
        list[dict]: Server assets.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM DATA_ASSET
            WHERE asset_type = 'SERVER'
              AND is_active = 1
            """
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch servers: {e}")
        return []


def get_children_by_type(parent_asset_id: int, asset_type: str) -> list[dict]:
    """
    Fetch all children of a parent asset filtered by asset_type.

    Args:
        parent_asset_id: The parent asset_id.
        asset_type: The child asset type (DATABASE, SCHEMA, TABLE, COLUMN).

    Returns:
        list[dict]: Child assets of the specified type.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM DATA_ASSET
            WHERE parent_asset_id = ?
              AND asset_type = ?
              AND is_active = 1
            ORDER BY asset_name
            """,
            parent_asset_id, asset_type
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch {asset_type} children of asset {parent_asset_id}: {e}")
        return []


def get_asset_by_id(asset_id: int) -> Optional[dict]:
    """
    Fetch a single asset by its ID.

    Args:
        asset_id: The asset_id to look up.

    Returns:
        dict or None: The asset data, or None if not found.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM DATA_ASSET WHERE asset_id = ?",
            asset_id
        )
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        conn.close()
        return dict(zip(columns, row)) if row else None
    except Exception as e:
        logger.error(f"Failed to fetch asset {asset_id}: {e}")
        return None

