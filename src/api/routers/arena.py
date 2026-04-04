"""
src/api/routers/arena.py — Hedge Fund Swarm REST + WebSocket API (Plan 5)

Exposes the Arena orchestration layer via REST endpoints and real-time
WebSocket events. Allows the Next.js frontend to:
  - Trigger game ticks (POST /api/arena/tick/{trader_id})
  - Poll tick status (GET /api/arena/tick/{tick_id}/status)
  - View live logs (GET /api/arena/logs)
  - Inspect cost analytics (GET /api/arena/costs/{trader_id})
  - Get arena state (GET /api/arena/state/{trader_id})
  - Discover available models (GET /api/arena/models)
  - Run health check (GET /api/arena/health)

Background task pattern mirrors pipeline.py for consistency.
Complies with User Rule #13: live model discovery, not hardcoded names.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from datetime import date
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel

from src.arena.schemas import DailyTickResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/arena", tags=["Arena"])


# ═══════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════

class TickRequest(BaseModel):
    """POST /api/arena/tick/{trader_id} request body."""
    model_overrides: dict[str, str] | None = None
    dry_run: bool = False


class TickStatusResponse(BaseModel):
    """GET /api/arena/tick/{tick_id}/status response."""
    status: Literal["running", "complete", "error", "cancelled"]

    phase: str | None = None
    current_agent: str | None = None
    elapsed_seconds: float = 0.0
    completed_agents: list[str] = []
    pending_agents: list[str] = []
    result: dict | None = None
    error: str | None = None


# ═══════════════════════════════════════════════════════════════
# IN-MEMORY TICK STATE (per-process, suitable for single-worker)
# ═══════════════════════════════════════════════════════════════

# tick_id → {status, started_at, result, error, progress}
_active_ticks: dict[str, dict] = {}

# Arena logs ring buffer (shared across all ticks)
_arena_logs: deque = deque(maxlen=500)


def _log(level: str, agent: str, msg: str):
    """Append a structured log entry to the arena log buffer."""
    _arena_logs.append({
        "ts": time.strftime("%H:%M:%S"),
        "level": level,
        "agent": agent,
        "msg": msg,
    })


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@router.post("/tick/{trader_id}")
async def trigger_tick(trader_id: int, req: TickRequest | None = None):
    """Trigger one daily game loop for a specific trader/fund.
    
    Runs in a background asyncio Task (non-blocking).
    Returns immediately with tick_id for status polling.
    """
    req = req or TickRequest()
    tick_id = str(uuid.uuid4())

    _active_ticks[tick_id] = {
        "status": "running",
        "phase": "init",
        "started_at": time.time(),
        "trader_id": trader_id,
        "result": None,
        "error": None,
        "progress": None,
        "task": None,  # will be set after create_task
    }

    # Get trader name for logging
    fund_name = f"Fund-{trader_id}"
    try:
        from src.core.duckdb_store import get_store
        row = get_store().execute("SELECT name FROM traders WHERE id = ?", [trader_id]).fetchone()
        if row:
            fund_name = row[0]
    except Exception:
        pass

    _log("INFO", "orchestrator", f"🚀 Tick started for {fund_name} (trader_id={trader_id}, dry_run={req.dry_run})")

    task = asyncio.create_task(_execute_tick(tick_id, trader_id, req))
    _active_ticks[tick_id]["task"] = task

    return {
        "status": "started",
        "tick_id": tick_id,
        "trader_id": trader_id,
        "message": f"Daily tick started for fund '{fund_name}'",
        "dry_run": req.dry_run,
    }


@router.delete("/tick/{tick_id}")
async def cancel_tick(tick_id: str):
    """Cancel a running tick immediately.
    
    Cancels the asyncio Task and marks it as cancelled in the state dict.
    Token costs already incurred are still deducted from P/L.
    """
    tick = _active_ticks.get(tick_id)
    if tick is None:
        return {"ok": False, "error": "Tick ID not found"}
    if tick["status"] != "running":
        return {"ok": False, "error": f"Tick is not running (status: {tick['status']})"}

    task: asyncio.Task | None = tick.get("task")
    if task and not task.done():
        task.cancel()

    _active_ticks[tick_id]["status"] = "cancelled"
    _log("WARNING", "orchestrator", f"⏹️ Tick {tick_id[:8]} cancelled by user")
    return {"ok": True, "tick_id": tick_id, "status": "cancelled"}


async def _execute_tick(tick_id: str, trader_id: int, req: TickRequest):
    """Background task: execute the full daily game loop."""
    from src.arena.orchestrator import run_daily_tick, TickProgress

    progress = TickProgress()
    _active_ticks[tick_id]["progress"] = progress

    async def ws_broadcast(event_type: str, data: dict):
        """Broadcast WebSocket events and log them locally."""
        try:
            from src.api.ws_manager import manager
            await manager.broadcast(event_type, data)
        except Exception:
            pass
        agent = data.get("agent_name", "orchestrator")
        _log("INFO", agent, f"{event_type}: {data}")

    try:
        result = await run_daily_tick(
            trader_id=trader_id,
            model_overrides=req.model_overrides,
            dry_run=req.dry_run,
            ws_broadcast=ws_broadcast,
            progress=progress,
        )
        _active_ticks[tick_id].update({
            "status": "complete",
            "result": result.model_dump(mode="json"),
        })
        _log("INFO", "orchestrator", f"✅ Tick complete: ${result.api_cost_deducted_usd:.6f} cost, {result.elapsed_seconds:.1f}s")

    except Exception as e:
        logger.exception(f"[ArenaRouter] Tick {tick_id} failed: {e}")
        _active_ticks[tick_id].update({
            "status": "error",
            "error": str(e),
        })
        _log("ERROR", "orchestrator", f"❌ Tick failed: {e}")


@router.get("/tick/{tick_id}/status", response_model=TickStatusResponse)
async def get_tick_status(tick_id: str):
    """Get status and results of a running or completed tick."""
    tick = _active_ticks.get(tick_id)
    if tick is None:
        return TickStatusResponse(status="error", error="Tick ID not found")

    progress = tick.get("progress")
    elapsed = time.time() - tick.get("started_at", time.time())

    return TickStatusResponse(
        status=tick["status"],
        phase=progress.phase if progress else tick.get("phase"),
        current_agent=progress.current_agent if progress else None,
        elapsed_seconds=elapsed,
        completed_agents=progress.completed_agents if progress else [],
        pending_agents=progress.pending_agents if progress else [],
        result=tick.get("result"),
        error=tick.get("error"),
    )


@router.get("/logs")
async def get_arena_logs(since: int = 0):
    """Get arena logs in real-time.
    
    Pass 'since' as the last log index you received for polling.
    Pattern mirrors /api/pipeline/logs for consistency.
    """
    logs = list(_arena_logs)
    running = any(t["status"] == "running" for t in _active_ticks.values())
    return {
        "logs": logs[since:],
        "total": len(logs),
        "running": running,
    }


@router.get("/costs/{trader_id}")
async def get_cost_history(trader_id: int):
    """Return API cost history for a trader fund.
    
    Reads from api_cost_ledger.parquet.
    Returns: daily costs, cumulative costs, breakdown by agent/model.
    """
    from src.arena.accountant import get_cost_history
    return get_cost_history(trader_id)


@router.get("/state/{trader_id}")
async def get_arena_state(trader_id: int):
    """Return current arena state and dashboard summary for a fund."""
    try:
        from src.core.duckdb_store import get_store, get_parquet_path
        import os
        import polars as pl

        store = get_store()
        trader = store.execute(
            "SELECT id, name, total_capital FROM traders WHERE id = ?",
            [trader_id],
        ).fetchone()

        if not trader:
            return {"error": f"Trader {trader_id} not found"}

        # Find last tick result from active_ticks or cost ledger
        last_tick_date = None
        last_result_summary = None

        # Get last tick from completed ticks
        completed = [(k, v) for k, v in _active_ticks.items()
                     if v.get("trader_id") == trader_id and v["status"] == "complete"]
        if completed:
            latest = max(completed, key=lambda x: x[1].get("started_at", 0))
            last_result = latest[1].get("result", {})
            last_tick_date = last_result.get("tick_date")
            last_result_summary = last_result

        # Get active strategies from last PM decisions
        active_strategies = []
        if last_result_summary:
            desk_results = last_result_summary.get("desk_results", [])
            active_strategies = [d.get("strategy_id") for d in desk_results if d.get("strategy_id")]

        # Get cost totals
        from src.arena.accountant import get_cost_history
        cost_data = get_cost_history(trader_id)

        return {
            "trader_id": trader[0],
            "fund_name": trader[1],
            "current_equity": float(trader[2]),
            "last_tick_date": last_tick_date,
            "active_strategies": active_strategies,
            "total_api_cost_usd": cost_data.get("total_cost_usd", 0.0),
            "ticks_run": cost_data.get("ticks_run", 0),
            "last_tick_result": last_result_summary,
        }

    except Exception as e:
        logger.error(f"[ArenaRouter] Failed to get state for trader {trader_id}: {e}")
        return {"error": str(e)}


@router.get("/models")
async def list_available_models():
    """Return available Anthropic models for frontend dropdown.
    
    Implements User Rule #13: live discovery, not hardcoded names.
    Returns live model list with tier metadata for A/B testing dropdowns.
    """
    from src.arena.llm_client import list_available_models
    return {"models": await list_available_models()}


@router.get("/health")
async def arena_health():
    """Check LLM connectivity and arena system health."""
    from src.arena.llm_client import check_llm_health
    llm_status = await check_llm_health()
    return {
        "arena_status": "healthy",
        "llm": llm_status,
        "active_ticks": len(_active_ticks),
        "running_ticks": sum(1 for t in _active_ticks.values() if t["status"] == "running"),
        "log_entries": len(_arena_logs),
    }


@router.post("/arena/tick")
async def trigger_arena_tick(req: TickRequest | None = None):
    """Trigger a daily tick for ALL active funds concurrently.
    
    Multi-fund arena mode (Plan 8 Phase 2 scaling).
    """
    req = req or TickRequest()

    async def ws_broadcast(event_type: str, data: dict):
        try:
            from src.api.ws_manager import manager
            await manager.broadcast(event_type, data)
        except Exception:
            pass

    from src.arena.orchestrator import run_arena_tick
    result = await run_arena_tick(
        tick_date=date.today(),
        model_overrides=req.model_overrides,
        ws_broadcast=ws_broadcast,
    )
    return result
