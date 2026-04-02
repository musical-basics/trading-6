"""
portfolios.py — FastAPI Router for Portfolio management.

Endpoints:
  GET  /api/traders/{id}/portfolios       — List portfolios for a trader
  PUT  /api/portfolios/{id}/strategy      — Assign a strategy to a portfolio
  PUT  /api/portfolios/{id}/schedule      — Update rebalance frequency
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from src.core.trader_manager import (
    get_portfolios,
    assign_strategy,
    update_schedule,
)
from src.ecs.strategy_registry import STRATEGY_NAMES
from src.api.routers.models import StrategyAssignment, ScheduleUpdate

router = APIRouter(prefix="/api", tags=["portfolios"])


@router.get("/traders/{trader_id}/portfolios")
async def api_get_portfolios(trader_id: int):
    """Get all portfolios for a trader."""
    portfolios = get_portfolios(trader_id)
    if not portfolios:
        raise HTTPException(
            status_code=404,
            detail=f"No portfolios found for trader {trader_id}",
        )

    # Enrich with strategy display names
    for p in portfolios:
        sid = p.get("strategy_id")
        p["strategy_name"] = STRATEGY_NAMES.get(sid, sid) if sid else None

    return portfolios


@router.put("/portfolios/{portfolio_id}/strategy")
async def api_update_strategy(portfolio_id: int, req: StrategyAssignment):
    """Assign or swap the strategy for a portfolio."""
    try:
        assign_strategy(portfolio_id, req.strategy_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "status": "updated",
        "portfolio_id": portfolio_id,
        "strategy_id": req.strategy_id,
        "strategy_name": STRATEGY_NAMES.get(req.strategy_id, req.strategy_id),
    }


@router.put("/portfolios/{portfolio_id}/schedule")
async def api_update_schedule(portfolio_id: int, req: ScheduleUpdate):
    """Update the rebalance frequency for a portfolio."""
    try:
        update_schedule(portfolio_id, req.rebalance_freq)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "status": "updated",
        "portfolio_id": portfolio_id,
        "rebalance_freq": req.rebalance_freq,
    }
