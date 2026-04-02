"""
tournament.py — FastAPI router for strategy tournament endpoints.
"""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from src.ecs.strategy_registry import get_all_strategy_ids, STRATEGY_NAMES
from src.ecs.tournament_system import run_tournament

router = APIRouter(prefix="/api/strategies", tags=["strategies"])


class TournamentRequest(BaseModel):
    strategies: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    starting_capital: float = 10000.0


@router.get("/list")
async def list_strategies():
    """Return all available strategies with display names."""
    return {
        "strategies": [
            {"id": sid, "name": STRATEGY_NAMES.get(sid, sid)}
            for sid in get_all_strategy_ids()
        ]
    }


@router.post("/tournament")
async def run_strategy_tournament(req: TournamentRequest):
    """Run a tournament of selected strategies and return results."""
    results = run_tournament(
        strategy_ids=req.strategies,
        start_date=req.start_date,
        end_date=req.end_date,
        starting_capital=req.starting_capital,
    )
    return results
