"""
migrate_sqlite_to_pg.py — One-time data migration: SQLite → PostgreSQL

Reads all transactional records from the existing SQLite database
(data/level3_trading.db) and inserts them into the new Postgres tables
using SQLAlchemy ORM bulk operations.

Usage:
    python -m src.scripts.migrate_sqlite_to_pg

IMPORTANT: Run this only AFTER Alembic has created the Postgres tables.
"""

import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config import DB_PATH
from src.core.database import SessionLocal
from src.core.models import (
    Trader, TraderConstraint, Portfolio, PaperExecution,
)


SQLITE_PATH = DB_PATH


def migrate():
    if not os.path.exists(SQLITE_PATH):
        print(f"❌ SQLite database not found at {SQLITE_PATH}")
        print("   Nothing to migrate.")
        return

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    pg_session = SessionLocal()

    try:
        # ── Traders ──────────────────────────────────────────
        cursor = sqlite_conn.execute("SELECT * FROM traders")
        rows = [dict(row) for row in cursor.fetchall()]
        for row in rows:
            pg_session.add(Trader(
                id=row["id"],
                name=row["name"],
                total_capital=row["total_capital"],
                unallocated_capital=row["unallocated_capital"],
            ))
        print(f"  ✅ traders: {len(rows)} rows migrated")

        # ── Trader Constraints ───────────────────────────────
        cursor = sqlite_conn.execute("SELECT * FROM trader_constraints")
        rows = [dict(row) for row in cursor.fetchall()]
        for row in rows:
            pg_session.add(TraderConstraint(
                trader_id=row["trader_id"],
                max_drawdown_pct=row["max_drawdown_pct"],
                max_open_positions=row["max_open_positions"],
                max_capital_per_trade=row["max_capital_per_trade"],
                halt_trading_flag=bool(row.get("halt_trading_flag", 0)),
            ))
        print(f"  ✅ trader_constraints: {len(rows)} rows migrated")

        # ── Portfolios ───────────────────────────────────────
        cursor = sqlite_conn.execute("SELECT * FROM portfolios")
        rows = [dict(row) for row in cursor.fetchall()]
        for row in rows:
            pg_session.add(Portfolio(
                id=row["id"],
                trader_id=row["trader_id"],
                name=row["name"],
                allocated_capital=row["allocated_capital"],
                strategy_id=row.get("strategy_id"),
                rebalance_freq=row.get("rebalance_freq", "Daily"),
                next_rebalance_date=row.get("next_rebalance_date"),
            ))
        print(f"  ✅ portfolios: {len(rows)} rows migrated")

        # ── Paper Executions ─────────────────────────────────
        cursor = sqlite_conn.execute("SELECT * FROM paper_executions")
        rows = [dict(row) for row in cursor.fetchall()]
        for row in rows:
            pg_session.add(PaperExecution(
                id=row["id"],
                ticker=row["ticker"],
                action=row["action"],
                quantity=row["quantity"],
                simulated_price=row["simulated_price"],
                strategy_id=row.get("strategy_id", "sma_crossover"),
                trader_id=row.get("trader_id"),
                portfolio_id=row.get("portfolio_id"),
            ))
        print(f"  ✅ paper_executions: {len(rows)} rows migrated")

        pg_session.commit()
        print("\n🎉 Migration complete.")

    except Exception as e:
        pg_session.rollback()
        print(f"\n❌ Migration failed: {e}")
        raise
    finally:
        pg_session.close()
        sqlite_conn.close()


if __name__ == "__main__":
    print("=" * 50)
    print("SQLite → PostgreSQL Data Migration")
    print("=" * 50)
    migrate()
