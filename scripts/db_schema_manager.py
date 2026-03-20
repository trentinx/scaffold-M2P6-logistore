"""
Helpers to keep the PostgreSQL `products` table aligned with catalogue contracts.

The ingestion pipeline should call `ensure_products_columns` before inserting
catalogue data so new contract versions automatically add the required columns.
"""
from __future__ import annotations

from typing import Dict

try:
    import psycopg2
    from psycopg2 import sql
except ModuleNotFoundError:  # pragma: no cover - optional in tests
    psycopg2 = None
    sql = None

from contracts.catalogue_schema import (
    get_catalogue_column_types,
    get_catalogue_storage_columns,
)

PRODUCTS_TABLE = "products"
DEFAULT_SCHEMA = "public"


def _fetch_existing_columns(conn) -> Dict[str, str]:
    query = """
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (DEFAULT_SCHEMA, PRODUCTS_TABLE))
        rows = cur.fetchall()
    existing = {}
    for name, data_type, udt_name in rows:
        existing[name] = data_type.upper() or udt_name.upper()
    return existing


def ensure_products_columns(conn) -> None:
    """Create any missing columns required by the catalogue contracts."""
    if psycopg2 is None or sql is None:  # pragma: no cover
        raise ImportError("psycopg2 is required to manage PostgreSQL schema.")
    required_columns = get_catalogue_storage_columns()
    column_types = get_catalogue_column_types()
    existing = _fetch_existing_columns(conn)

    if not required_columns:
        return

    statements = []
    for column in required_columns:
        if column in existing:
            continue
        sql_type = column_types.get(column, "TEXT")
        statements.append(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(PRODUCTS_TABLE),
                sql.Identifier(column),
                sql.SQL(sql_type),
            )
        )

    if not statements:
        return

    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
