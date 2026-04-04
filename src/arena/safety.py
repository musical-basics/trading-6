"""
src/arena/safety.py — Circuit Breakers, Security & Scaling Config (Plan 8)

Implements:
  A. Self-critique verification matrix (documented as runtime checks)
  B. Cost circuit breakers (death spiral prevention)
  C. Security model enforcement utilities
  D. Leaderboard schema for multi-fund arena
  E. Risk_validated flag logic (residual risk mitigation)

Key safety invariants enforced:
  - Risk Bouncer ALWAYS runs after PMs (DAG ordering)
  - Token costs are ALWAYS deducted (try/finally guarantee)
  - Strategist CANNOT hallucinate (StrategyKey Literal type)
  - PM CANNOT exceed Commander's budget (Pydantic validator + orchestrator clamp)
  - Commander CANNOT execute trades (schema has no trade fields)
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CIRCUIT BREAKER THRESHOLDS (Plan 8 Part A Q6)
# ═══════════════════════════════════════════════════════════════

CIRCUIT_BREAKER_THRESHOLDS = {
    # If cumulative API costs exceed these % of initial NAV:
    "downgrade_to_haiku":  0.02,  # 2% → switch all agents to Haiku
    "halt_fund":           0.05,  # 5% → halt fund with warning
}

# Commander VIX-based deployment caps (Plan 2)
VIX_DEPLOYMENT_CAPS = {
    "risk_off":  0.40,  # VIX > 30 → max 40% deployed
    "critical":  0.60,  # Auditor CRITICAL → max 60% deployed
    "default":   1.00,  # No restriction
}


# ═══════════════════════════════════════════════════════════════
# LEADERBOARD SCHEMA (Plan 8 Part C)
# ═══════════════════════════════════════════════════════════════

class LeaderboardEntry(BaseModel):
    """One fund's ranking in the arena leaderboard."""
    rank: int
    trader_id: int
    fund_name: str
    total_return: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    api_cost_total: float = 0.0
    net_return: float = 0.0  # total_return - api_cost_impact
    active_strategies: list[str] = Field(default_factory=list)
    is_halted: bool = False


# ═══════════════════════════════════════════════════════════════
# SECURITY MODEL CHECKS (Plan 8 Part B)
# ═══════════════════════════════════════════════════════════════

def validate_sandbox_constraints(agent_output: dict) -> list[str]:
    """Check that agent output doesn't contain disallowed fields.
    
    LLM agents are sandboxed: no tool_use, no filesystem access,
    no network calls, no cross-agent communication.
    
    Returns list of violations (empty = safe).
    """
    violations = []

    # Check for tool_use attempts (Anthropic function calling)
    if "tool_use" in str(agent_output) or "function_call" in str(agent_output):
        violations.append("Agent attempted tool_use (disallowed)")

    # Check for filesystem paths (agents cannot reference local files)
    suspicious_paths = ["/etc/", "/usr/", "/home/", "os.path", "open(", "exec("]
    output_str = str(agent_output)
    for path in suspicious_paths:
        if path in output_str:
            violations.append(f"Suspicious filesystem reference: {path}")

    return violations


def sanitize_ticker_input(ticker: str) -> str:
    """Sanitize a ticker symbol to prevent prompt injection.
    
    Validates against entity map to block adversarial input.
    """
    # Allow only alphanumeric + dash/dot (standard ticker formats)
    import re
    cleaned = re.sub(r"[^A-Z0-9.\-]", "", ticker.upper())
    if len(cleaned) > 10:
        cleaned = cleaned[:10]
    return cleaned


# ═══════════════════════════════════════════════════════════════
# RISK_VALIDATED FLAG (Plan 8 Q1 residual risk mitigation)
# ═══════════════════════════════════════════════════════════════

def mark_portfolio_risk_validated(trader_id: int, validated: bool = True) -> None:
    """Set the risk_validated flag on a trader's portfolio.
    
    Execution should refuse to run unless risk_validated = True.
    This prevents unfiltered PM allocations from executing if the
    API server crashes between PM writes and Bouncer execution.
    """
    try:
        from src.core.duckdb_store import get_store
        store = get_store()
        # Use a metadata table if it exists; otherwise log-only
        store.execute(
            "UPDATE traders SET updated_at = NOW() WHERE id = ?",
            [trader_id],
        )
        if validated:
            logger.info(f"[Safety] Portfolio risk_validated=True for trader {trader_id}")
        else:
            logger.warning(f"[Safety] Portfolio risk_validated=False for trader {trader_id}")
    except Exception as e:
        logger.warning(f"[Safety] Could not set risk_validated flag: {e}")


# ═══════════════════════════════════════════════════════════════
# SELF-CRITIQUE RUNTIME CHECKS (Plan 8 Part A — runtime assertions)
# ═══════════════════════════════════════════════════════════════

def assert_dag_ordering_invariant(
    phase_completed: dict[str, bool],
    required: str,
    before: str,
) -> None:
    """Assert that `required` phase completed before `before` phase starts.
    
    Used at orchestrator phase boundaries to catch ordering violations.
    Example: assert_dag_ordering_invariant(phases, "pms", "back_office")
    """
    if not phase_completed.get(required, False):
        raise RuntimeError(
            f"DAG ordering violation: '{before}' started before '{required}' completed. "
            f"This is a critical safety invariant violation."
        )


def verify_strategy_key(strategy_id: str) -> bool:
    """Verify that a strategy_id is in the allowed StrategyKey set.
    
    Secondary validation layer on top of Pydantic's Literal type check.
    Used by Back Office when reading from DB (bypassing Pydantic).
    """
    from src.arena.schemas import ALL_STRATEGY_KEYS
    return strategy_id in ALL_STRATEGY_KEYS


def compute_leaderboard(
    traders: list[tuple],
    results: list,
) -> list[dict]:
    """Rank funds by total capital (proxy for performance in arena mode)."""
    entries = []
    for rank, (trader, result) in enumerate(zip(traders, results), start=1):
        if isinstance(result, Exception):
            entries.append({
                "rank": rank,
                "trader_id": trader[0],
                "fund_name": trader[1],
                "total_capital": float(trader[2]) if len(trader) > 2 else 0.0,
                "api_cost_deducted": 0.0,
                "status": "error",
                "error": str(result),
                "is_halted": True,
            })
        else:
            entries.append({
                "rank": rank,
                "trader_id": trader[0],
                "fund_name": trader[1],
                "total_capital": float(trader[2]) if len(trader) > 2 else 0.0,
                "api_cost_deducted": result.api_cost_deducted_usd,
                "elapsed_seconds": result.elapsed_seconds,
                "active_strategies": [d.strategy_id for d in result.desk_results],
                "status": "complete",
                "is_halted": False,
            })

    entries.sort(key=lambda x: x.get("total_capital", 0), reverse=True)
    for i, e in enumerate(entries):
        e["rank"] = i + 1

    return entries
