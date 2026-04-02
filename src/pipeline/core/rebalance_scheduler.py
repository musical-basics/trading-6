"""
rebalance_scheduler.py — Portfolio rebalance scheduling.

Evaluates each portfolio's rebalance_freq against next_rebalance_date
to determine which portfolios are due for rebalancing today.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import List

from src.config import DB_PATH


FREQ_DAYS = {
    "Daily": 1,
    "Weekly": 7,
    "Monthly": 30,
}


def get_due_portfolios() -> List[dict]:
    """Return portfolios whose next_rebalance_date <= today and
    have a strategy assigned.

    Returns:
        List of portfolio dicts with all columns + trader_name.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        rows = conn.execute(
            """SELECT p.*, t.name as trader_name
               FROM portfolios p
               JOIN traders t ON p.trader_id = t.id
               WHERE p.strategy_id IS NOT NULL
                 AND p.next_rebalance_date <= ?
               ORDER BY p.trader_id, p.id""",
            (today,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def advance_rebalance_date(portfolio_id: int) -> None:
    """After a successful rebalance, advance next_rebalance_date
    based on the portfolio's rebalance_freq.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            "SELECT rebalance_freq FROM portfolios WHERE id = ?",
            (portfolio_id,),
        ).fetchone()
        if row:
            days = FREQ_DAYS.get(row[0], 1)
            new_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
            conn.execute(
                "UPDATE portfolios SET next_rebalance_date = ? WHERE id = ?",
                (new_date, portfolio_id),
            )
            conn.commit()
    finally:
        conn.close()
