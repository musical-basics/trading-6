"""
models.py — Pydantic models for Traders & Portfolios API.

Uses Optional[] syntax for Python 3.9 compatibility.
"""

from __future__ import annotations

from typing import Optional, List
from pydantic import BaseModel


# ── Request Models ──────────────────────────────────────────────

class TraderCreate(BaseModel):
    name: str
    total_capital: float = 10000.0
    num_portfolios: int = 10
    capital_per_portfolio: Optional[float] = None


class TraderConstraintUpdate(BaseModel):
    max_drawdown_pct: Optional[float] = None
    max_open_positions: Optional[int] = None
    max_capital_per_trade: Optional[float] = None
    halt_trading_flag: Optional[bool] = None


class StrategyAssignment(BaseModel):
    strategy_id: str


class ScheduleUpdate(BaseModel):
    rebalance_freq: str  # "Daily", "Weekly", "Monthly"


# ── Response Models ─────────────────────────────────────────────

class TraderConstraintResponse(BaseModel):
    trader_id: int
    max_drawdown_pct: float
    max_open_positions: int
    max_capital_per_trade: float
    halt_trading_flag: bool


class TraderResponse(BaseModel):
    id: int
    name: str
    total_capital: float
    unallocated_capital: float
    created_at: Optional[str] = None
    constraints: Optional[TraderConstraintResponse] = None
    portfolios_count: int = 10


class PortfolioResponse(BaseModel):
    id: int
    trader_id: int
    name: str
    allocated_capital: float
    strategy_id: Optional[str] = None
    rebalance_freq: str = "Daily"
    next_rebalance_date: Optional[str] = None
