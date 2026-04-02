"""
migrate_sqlite_to_parquet.py — Level 4 Phase 2: Data Migration

Reads the existing Level 3 SQLite database and converts all relevant tables
into Polars DataFrames written as Parquet component files.

Key transformations:
  1. Assigns integer entity_id to each ticker (via EntityMap).
  2. Pre-computes daily_return per entity (fixes pct_change teleportation bug).
  3. Casts all columns to ECS schema types (Int32, Float32, Date).
  4. Migrates computed features (factor_betas, cross_sectional_scores,
     ml_features, ml_predictions, target_portfolio) as Feature/Intent/Portfolio
     components.
  5. Persists the entity_map as a small Parquet file for later lookup.

Run with: python3 -m src.core.migrate_sqlite_to_parquet
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import polars as pl

from src.config import DB_PATH, PROJECT_ROOT
from src.core.entity_map import EntityMap
from src.core.duckdb_store import PARQUET_DIR

# ── Ensure output dir ────────────────────────────────────────
os.makedirs(PARQUET_DIR, exist_ok=True)


def _read_sqlite_table(conn: sqlite3.Connection, query: str) -> pl.DataFrame:
    """Read a SQLite query result into a Polars DataFrame via row fetching."""
    cursor = conn.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        return pl.DataFrame(schema={c: pl.Utf8 for c in columns})
    return pl.DataFrame(rows, schema=columns, orient="row")


def migrate_entity_map(conn: sqlite3.Connection) -> EntityMap:
    """Build and persist the EntityMap from all tickers in daily_bars."""
    print("  Building EntityMap...", end=" ")
    df = _read_sqlite_table(conn, "SELECT DISTINCT ticker FROM daily_bars ORDER BY ticker")
    tickers = df["ticker"].to_list()
    em = EntityMap()
    em.register(tickers)

    # Persist as Parquet for later use
    entity_df = pl.DataFrame({
        "entity_id": pl.Series(em.all_ids(), dtype=pl.Int32),
        "ticker": pl.Series(em.all_tickers(), dtype=pl.Utf8),
    })
    path = os.path.join(PARQUET_DIR, "entity_map.parquet")
    entity_df.write_parquet(path)
    print(f"✓ {len(em)} tickers → {path}")
    return em


def migrate_market_data(conn: sqlite3.Connection, em: EntityMap) -> None:
    """Migrate daily_bars → market_data.parquet with pre-computed daily_return."""
    print("  Migrating daily_bars → market_data.parquet...", end=" ")

    df = _read_sqlite_table(
        conn,
        "SELECT ticker, date, adj_close, volume FROM daily_bars ORDER BY ticker, date"
    )

    if df.is_empty():
        print("⚠ No data")
        return

    # Add entity_id
    ticker_map = {t: i for t, i in zip(em.all_tickers(), em.all_ids())}
    df = df.with_columns(
        pl.col("ticker").replace_strict(ticker_map).cast(pl.Int32).alias("entity_id"),
        pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
    )

    # Pre-compute daily_return per entity (avoids the pct_change teleportation bug)
    df = df.sort(["entity_id", "date"])
    df = df.with_columns(
        (pl.col("adj_close") / pl.col("adj_close").shift(1).over("entity_id") - 1)
        .alias("daily_return")
    )

    # Cast and select final schema
    df = df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("date"),
        pl.col("adj_close").cast(pl.Float32),
        pl.col("volume").cast(pl.Int64),
        pl.col("daily_return").cast(pl.Float32),
    ])

    path = os.path.join(PARQUET_DIR, "market_data.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_fundamentals(conn: sqlite3.Connection, em: EntityMap) -> None:
    """Migrate quarterly_fundamentals → fundamental.parquet."""
    print("  Migrating quarterly_fundamentals → fundamental.parquet...", end=" ")

    df = _read_sqlite_table(
        conn,
        """SELECT ticker, filing_date, revenue, total_debt,
                  cash_and_equivalents AS cash, shares_outstanding AS shares_out
           FROM quarterly_fundamentals ORDER BY ticker, filing_date"""
    )

    if df.is_empty():
        print("⚠ No data")
        return

    ticker_map = {t: i for t, i in zip(em.all_tickers(), em.all_ids())}
    df = df.with_columns(
        pl.col("ticker").replace_strict(ticker_map).cast(pl.Int32).alias("entity_id"),
        pl.col("filing_date").str.to_date("%Y-%m-%d").alias("filing_date"),
    )

    df = df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("filing_date"),
        pl.col("revenue").cast(pl.Float32),
        pl.col("total_debt").cast(pl.Float32),
        pl.col("cash").cast(pl.Float32),
        pl.col("shares_out").cast(pl.Float32),
    ])

    path = os.path.join(PARQUET_DIR, "fundamental.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_macro(conn: sqlite3.Connection) -> None:
    """Migrate macro_factors → macro.parquet."""
    print("  Migrating macro_factors → macro.parquet...", end=" ")

    df = _read_sqlite_table(
        conn,
        """SELECT date, vix_close AS vix, vix3m_close AS vix3m,
                  tnx_close AS tnx, irx_close AS irx, spy_close AS spy
           FROM macro_factors ORDER BY date"""
    )

    if df.is_empty():
        print("⚠ No data")
        return

    df = df.with_columns(
        pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
    )

    df = df.select([
        pl.col("date"),
        pl.col("vix").cast(pl.Float32),
        pl.col("vix3m").cast(pl.Float32),
        pl.col("tnx").cast(pl.Float32),
        pl.col("irx").cast(pl.Float32),
        pl.col("spy").cast(pl.Float32),
    ])

    path = os.path.join(PARQUET_DIR, "macro.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_features(conn: sqlite3.Connection, em: EntityMap) -> None:
    """Migrate ml_features + factor_betas + _dcf_staging → feature.parquet."""
    print("  Migrating ml_features → feature.parquet...", end=" ")

    df = _read_sqlite_table(
        conn,
        """SELECT ticker, date, ev_sales_zscore, dynamic_discount_rate,
                  dcf_npv_gap, beta_spy, beta_10y AS beta_tnx, beta_vix
           FROM ml_features ORDER BY ticker, date"""
    )

    if df.is_empty():
        print("⚠ No data")
        return

    ticker_map = {t: i for t, i in zip(em.all_tickers(), em.all_ids())}
    df = df.with_columns(
        pl.col("ticker").replace_strict(ticker_map).cast(pl.Int32).alias("entity_id"),
        pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
    )

    df = df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("date"),
        pl.col("ev_sales_zscore").cast(pl.Float32),
        pl.col("dynamic_discount_rate").cast(pl.Float32),
        pl.col("dcf_npv_gap").cast(pl.Float32),
        pl.col("beta_spy").cast(pl.Float32),
        pl.col("beta_tnx").cast(pl.Float32),
        pl.col("beta_vix").cast(pl.Float32),
    ])

    path = os.path.join(PARQUET_DIR, "feature.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_action_intent(conn: sqlite3.Connection, em: EntityMap) -> None:
    """Migrate ml_predictions → action_intent.parquet."""
    print("  Migrating ml_predictions → action_intent.parquet...", end=" ")

    df = _read_sqlite_table(
        conn,
        """SELECT ticker, date, raw_weight FROM ml_predictions
           WHERE raw_weight > 0 ORDER BY ticker, date"""
    )

    if df.is_empty():
        print("⚠ No data")
        return

    ticker_map = {t: i for t, i in zip(em.all_tickers(), em.all_ids())}
    df = df.with_columns(
        pl.col("ticker").replace_strict(ticker_map).cast(pl.Int32).alias("entity_id"),
        pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
        pl.lit("xgboost").alias("strategy_id"),
    )

    df = df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("date"),
        pl.col("strategy_id"),
        pl.col("raw_weight").cast(pl.Float32),
    ])

    path = os.path.join(PARQUET_DIR, "action_intent.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_target_portfolio(conn: sqlite3.Connection, em: EntityMap) -> None:
    """Migrate target_portfolio → target_portfolio.parquet."""
    print("  Migrating target_portfolio → target_portfolio.parquet...", end=" ")

    df = _read_sqlite_table(
        conn,
        """SELECT ticker, date, target_weight, mcr
           FROM target_portfolio ORDER BY ticker, date"""
    )

    if df.is_empty():
        print("⚠ No data")
        return

    ticker_map = {t: i for t, i in zip(em.all_tickers(), em.all_ids())}
    df = df.with_columns(
        pl.col("ticker").replace_strict(ticker_map).cast(pl.Int32).alias("entity_id"),
        pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
        pl.lit("xgboost").alias("strategy_id"),
    )

    df = df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("date"),
        pl.col("strategy_id"),
        pl.col("target_weight").cast(pl.Float32),
        pl.col("mcr").cast(pl.Float32),
    ])

    path = os.path.join(PARQUET_DIR, "target_portfolio.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_traders(conn: sqlite3.Connection) -> None:
    """Migrate traders → traders.parquet."""
    print("  Migrating traders → traders.parquet...", end=" ")
    df = _read_sqlite_table(conn, "SELECT * FROM traders")
    if df.is_empty():
        print("⚠ No data (table empty or not yet populated)")
        return
    path = os.path.join(PARQUET_DIR, "traders.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_portfolios(conn: sqlite3.Connection) -> None:
    """Migrate portfolios → portfolios.parquet."""
    print("  Migrating portfolios → portfolios.parquet...", end=" ")
    df = _read_sqlite_table(conn, "SELECT * FROM portfolios")
    if df.is_empty():
        print("⚠ No data (table empty or not yet populated)")
        return
    path = os.path.join(PARQUET_DIR, "portfolios.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def migrate_trader_constraints(conn: sqlite3.Connection) -> None:
    """Migrate trader_constraints → trader_constraints.parquet."""
    print("  Migrating trader_constraints → trader_constraints.parquet...", end=" ")
    df = _read_sqlite_table(conn, "SELECT * FROM trader_constraints")
    if df.is_empty():
        print("⚠ No data (table empty or not yet populated)")
        return
    path = os.path.join(PARQUET_DIR, "trader_constraints.parquet")
    df.write_parquet(path)
    print(f"✓ {len(df):,} rows")


def run_migration() -> None:
    """Execute the full SQLite → Parquet migration."""
    start = datetime.now()

    print("=" * 60)
    print("PHASE 2: Data Migration (SQLite → Parquet Components)")
    print("=" * 60)
    print(f"  Source: {DB_PATH}")
    print(f"  Target: {PARQUET_DIR}/")
    print()

    if not os.path.exists(DB_PATH):
        print(f"  ⚠ SQLite database not found at {DB_PATH}")
        print("  Nothing to migrate.")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        em = migrate_entity_map(conn)
        migrate_market_data(conn, em)
        migrate_fundamentals(conn, em)
        migrate_macro(conn)
        migrate_features(conn, em)
        migrate_action_intent(conn, em)
        migrate_target_portfolio(conn, em)
        migrate_traders(conn)
        migrate_portfolios(conn)
        migrate_trader_constraints(conn)
    finally:
        conn.close()

    elapsed = (datetime.now() - start).total_seconds()

    # Summary
    print()
    print(f"  ✓ Migration complete in {elapsed:.1f}s")
    print(f"  Parquet files:")
    for f in sorted(os.listdir(PARQUET_DIR)):
        if f.endswith(".parquet"):
            size = os.path.getsize(os.path.join(PARQUET_DIR, f))
            print(f"    • {f}: {size / 1024:.1f} KB")
    print()


if __name__ == "__main__":
    run_migration()
