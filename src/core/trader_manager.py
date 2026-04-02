"""
trader_manager.py — Trader instantiation, validation, and capital allocation.

Core business logic for the Trader → Portfolio hierarchy:
  - create_trader(): Creates a trader + 10 sub-portfolios with equal capital
  - get_trader(): Fetches trader details + constraints
  - assign_strategy(): Maps exactly one strategy to a portfolio
  - update_constraints(): Updates risk limits for a trader
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import List, Optional

from src.config import DB_PATH
from src.ecs.strategy_registry import get_all_strategy_ids


# ── Trader Lifecycle ────────────────────────────────────────────

def create_trader(
    name: str,
    capital: float = 10000.0,
    num_portfolios: int = 10,
    capital_per_portfolio: Optional[float] = None,
) -> int:
    """Create a new trader and auto-generate sub-portfolios.

    Args:
        name: Unique trader name.
        capital: Total trader capital.
        num_portfolios: Number of portfolios to create (1-12).
        capital_per_portfolio: Per-portfolio allocation. If None,
            defaults to capital / num_portfolios.

    Returns the new trader_id.
    """
    num_portfolios = max(1, min(12, num_portfolios))
    per_portfolio = capital_per_portfolio if capital_per_portfolio else capital / num_portfolios

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Insert trader
        cursor.execute(
            "INSERT INTO traders (name, total_capital, unallocated_capital) VALUES (?, ?, ?)",
            (name, capital, 0.0),
        )
        trader_id = cursor.lastrowid

        # Insert default constraints
        cursor.execute(
            """INSERT INTO trader_constraints
               (trader_id, max_drawdown_pct, max_open_positions, max_capital_per_trade)
               VALUES (?, 0.20, 50, ?)""",
            (trader_id, per_portfolio),
        )

        # Auto-create portfolios
        today = datetime.now().strftime("%Y-%m-%d")
        for i in range(1, num_portfolios + 1):
            cursor.execute(
                """INSERT INTO portfolios
                   (trader_id, name, allocated_capital, strategy_id,
                    rebalance_freq, next_rebalance_date)
                   VALUES (?, ?, ?, NULL, 'Daily', ?)""",
                (trader_id, f"Portfolio {i}", per_portfolio, today),
            )

        conn.commit()
        return trader_id

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_trader(trader_id: int) -> Optional[dict]:
    """Fetch trader details including constraints."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM traders WHERE id = ?", (trader_id,)
        ).fetchone()
        if not row:
            return None

        trader = dict(row)

        # Attach constraints
        constraint_row = conn.execute(
            "SELECT * FROM trader_constraints WHERE trader_id = ?",
            (trader_id,),
        ).fetchone()
        if constraint_row:
            trader["constraints"] = dict(constraint_row)

        return trader
    finally:
        conn.close()


def list_traders() -> List[dict]:
    """List all traders with basic info."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM traders ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Portfolio Management ────────────────────────────────────────

def get_portfolios(trader_id: int) -> List[dict]:
    """Fetch all portfolios for a trader."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM portfolios WHERE trader_id = ? ORDER BY id",
            (trader_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def assign_strategy(portfolio_id: int, strategy_id: str) -> None:
    """Assign a single strategy to a portfolio.

    Validates:
      1. strategy_id exists in STRATEGY_REGISTRY
      2. No other portfolio of the same trader already uses this strategy
    """
    valid_ids = get_all_strategy_ids()
    if strategy_id not in valid_ids:
        raise ValueError(
            f"Unknown strategy '{strategy_id}'. Valid: {valid_ids}"
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        # Get trader_id for this portfolio
        row = conn.execute(
            "SELECT trader_id FROM portfolios WHERE id = ?",
            (portfolio_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Portfolio {portfolio_id} not found")
        trader_id = row[0]

        # Check uniqueness: no other portfolio for this trader uses this strategy
        conflict = conn.execute(
            """SELECT id, name FROM portfolios
               WHERE trader_id = ? AND strategy_id = ? AND id != ?""",
            (trader_id, strategy_id, portfolio_id),
        ).fetchone()
        if conflict:
            raise ValueError(
                f"Strategy '{strategy_id}' is already assigned to "
                f"'{conflict[1]}' (id={conflict[0]}). "
                f"Each strategy can only be used once per trader."
            )

        conn.execute(
            "UPDATE portfolios SET strategy_id = ? WHERE id = ?",
            (strategy_id, portfolio_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_schedule(portfolio_id: int, rebalance_freq: str) -> None:
    """Update the rebalance frequency for a portfolio."""
    valid_freqs = {"Daily", "Weekly", "Monthly"}
    if rebalance_freq not in valid_freqs:
        raise ValueError(
            f"Invalid frequency '{rebalance_freq}'. Valid: {valid_freqs}"
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        # Reset next_rebalance_date to today when frequency changes
        today = datetime.now().strftime("%Y-%m-%d")
        cursor = conn.execute(
            """UPDATE portfolios
               SET rebalance_freq = ?, next_rebalance_date = ?
               WHERE id = ?""",
            (rebalance_freq, today, portfolio_id),
        )
        if cursor.rowcount == 0:
            raise ValueError(f"Portfolio {portfolio_id} not found")
        conn.commit()
    finally:
        conn.close()


# ── Constraint Management ───────────────────────────────────────

def update_constraints(
    trader_id: int,
    max_drawdown_pct: Optional[float] = None,
    max_open_positions: Optional[int] = None,
    max_capital_per_trade: Optional[float] = None,
    halt_trading_flag: Optional[bool] = None,
) -> None:
    """Update risk constraints for a trader."""
    conn = sqlite3.connect(DB_PATH)
    try:
        updates = []
        values = []
        if max_drawdown_pct is not None:
            updates.append("max_drawdown_pct = ?")
            values.append(max_drawdown_pct)
        if max_open_positions is not None:
            updates.append("max_open_positions = ?")
            values.append(max_open_positions)
        if max_capital_per_trade is not None:
            updates.append("max_capital_per_trade = ?")
            values.append(max_capital_per_trade)
        if halt_trading_flag is not None:
            updates.append("halt_trading_flag = ?")
            values.append(1 if halt_trading_flag else 0)

        if not updates:
            return

        values.append(trader_id)
        sql = f"UPDATE trader_constraints SET {', '.join(updates)} WHERE trader_id = ?"
        cursor = conn.execute(sql, values)
        if cursor.rowcount == 0:
            raise ValueError(f"No constraints found for trader {trader_id}")
        conn.commit()
    finally:
        conn.close()


def load_trader_constraints(trader_id: int) -> Optional[dict]:
    """Load constraints for a given trader."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM trader_constraints WHERE trader_id = ?",
            (trader_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
