"""
duckdb_store.py — Level 4 ECS: DuckDB Columnar Store

Provides a managed DuckDB connection that:
  1. Creates the `data/components/` directory for Parquet files.
  2. Registers component Parquet files as DuckDB views for fast SQL queries.
  3. Exposes `get_connection()` context manager.

DuckDB queries Parquet files directly with zero-copy reads (Apache Arrow).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import duckdb

from src.config import PROJECT_ROOT

# ── Paths ────────────────────────────────────────────────────
DUCKDB_PATH = os.path.join(PROJECT_ROOT, "data", "level4.duckdb")
PARQUET_DIR = os.path.join(PROJECT_ROOT, "data", "components")

# ── Component → Parquet file mapping ─────────────────────────
COMPONENT_FILES = {
    "market_data":           "market_data.parquet",
    "fundamental":           "fundamental.parquet",
    "macro":                 "macro.parquet",
    "feature":               "feature.parquet",
    "action_intent":         "action_intent.parquet",
    "target_portfolio":      "target_portfolio.parquet",
    "risk_audit":            "risk_audit.parquet",
    "xgboost_audit":         "xgboost_audit.parquet",
    "traders":               "traders.parquet",
    "portfolios":            "portfolios.parquet",
    "trader_constraints":    "trader_constraints.parquet",
}


def _ensure_dirs() -> None:
    """Ensure data directories exist."""
    os.makedirs(PARQUET_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)


def _register_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Register existing Parquet files as DuckDB views.

    Only registers views for files that already exist on disk.
    This allows SQL queries like: SELECT * FROM market_data WHERE ...
    """
    for view_name, filename in COMPONENT_FILES.items():
        parquet_path = os.path.join(PARQUET_DIR, filename)
        if os.path.exists(parquet_path):
            conn.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{parquet_path}')
            """)


def init_store() -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB store and return a persistent connection.

    Creates directories, opens the database, and registers all
    existing Parquet component files as views.
    """
    _ensure_dirs()
    conn = duckdb.connect(DUCKDB_PATH)

    # Enable Parquet and Arrow extensions (built-in in modern duckdb)
    conn.execute("SET enable_progress_bar = false")

    _register_views(conn)
    return conn


@contextmanager
def get_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager for a DuckDB connection with auto-close.

    Usage:
        with get_connection() as conn:
            df = conn.execute("SELECT * FROM market_data LIMIT 10").pl()
    """
    _ensure_dirs()
    conn = duckdb.connect(DUCKDB_PATH)
    _register_views(conn)
    try:
        yield conn
    finally:
        conn.close()


def get_parquet_path(component_name: str) -> str:
    """Get the absolute path for a component's Parquet file.

    Args:
        component_name: Key from COMPONENT_FILES (e.g. "market_data")

    Returns:
        Absolute path to the Parquet file.

    Raises:
        KeyError: If component_name is not in the registry.
    """
    if component_name not in COMPONENT_FILES:
        raise KeyError(
            f"Unknown component '{component_name}'. "
            f"Valid: {list(COMPONENT_FILES.keys())}"
        )
    _ensure_dirs()
    return os.path.join(PARQUET_DIR, COMPONENT_FILES[component_name])
