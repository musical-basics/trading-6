"""
src/arena/accountant.py — Token Cost Engine (Plan 4)

The Accountant tracks every LLM API call across all agents in the swarm,
calculates the real USD cost, and deducts it directly from the fund's P/L.

This is the core game mechanic: agents that are too verbose literally lose
money for their fund. Natural selection pressure for efficiency.

Pricing (prefix-matched to handle version suffixes):
  - claude-opus-4:    $15.00/MTok in,  $75.00/MTok out
  - claude-sonnet-4:  $3.00/MTok in,   $15.00/MTok out
  - claude-haiku-4:   $0.25/MTok in,   $1.25/MTok out
"""

from __future__ import annotations

import os
import logging
from datetime import date, datetime
from typing import Optional

import polars as pl
from pydantic import BaseModel

from src.arena.schemas import TokenUsage

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# PRICING TABLE — prefix-matched to handle model version suffixes
# ═══════════════════════════════════════════════════════════════

# (input_cost_per_mtok, output_cost_per_mtok)
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-4":    (15.00, 75.00),
    "claude-sonnet-4":  (3.00,  15.00),
    "claude-haiku-4":   (0.25,  1.25),
    # Fallback for unknown models
    "default":          (3.00,  15.00),
}

# Anti-gaming: minimum 100-token charge per agent call
MIN_TOKENS_PER_CALL = 100


# ═══════════════════════════════════════════════════════════════
# OUTPUT SCHEMA
# ═══════════════════════════════════════════════════════════════

class TickCostSummary(BaseModel):
    """Aggregated token cost summary for one daily tick."""
    total_input_tokens: int
    total_output_tokens: int
    total_cost_usd: float
    cost_by_agent: dict[str, float]
    cost_by_model: dict[str, float]
    cost_as_pct_of_nav: float
    agent_count: int


class AccountingReport(BaseModel):
    """Full accounting report returned by run_accountant."""
    trader_id: int
    tick_date: date
    total_cost_usd: float
    cost_by_agent: dict[str, float]
    cost_as_pct_of_nav: float
    nav_before: float
    nav_after: float
    circuit_breaker_triggered: bool = False
    downgrade_to_haiku: bool = False


# ═══════════════════════════════════════════════════════════════
# CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def _match_model_prefix(model_id: str) -> str:
    """Match a model ID to its pricing tier by prefix."""
    model_lower = model_id.lower()
    for prefix in MODEL_PRICING:
        if prefix != "default" and model_lower.startswith(prefix):
            return prefix
    return "default"


def calculate_cost(usage: TokenUsage) -> float:
    """Compute the USD cost for a single agent interaction.
    
    Applies minimum token floor (100 tokens) to prevent gaming via empty responses.
    """
    # Apply minimum token floor (anti-gaming)
    effective_input = max(usage.input_tokens, MIN_TOKENS_PER_CALL)
    effective_output = max(usage.output_tokens, 0)

    prefix = _match_model_prefix(usage.model_id)
    in_rate, out_rate = MODEL_PRICING.get(prefix, MODEL_PRICING["default"])

    cost = (effective_input / 1_000_000 * in_rate) + (effective_output / 1_000_000 * out_rate)
    return round(cost, 8)


def aggregate_tick_cost(
    token_ledger: list[TokenUsage],
    current_nav: float = 10_000.0,
) -> TickCostSummary:
    """Calculate total API cost across all agents in one tick.
    
    Args:
        token_ledger: List of TokenUsage from every agent call
        current_nav: Current fund NAV for cost-as-pct calculation
    """
    total_input = 0
    total_output = 0
    total_cost = 0.0
    cost_by_agent: dict[str, float] = {}
    cost_by_model: dict[str, float] = {}

    for usage in token_ledger:
        cost = calculate_cost(usage)
        total_input += usage.input_tokens
        total_output += usage.output_tokens
        total_cost += cost

        agent = usage.agent_name or "unknown"
        cost_by_agent[agent] = cost_by_agent.get(agent, 0.0) + cost

        model = usage.model_id or "unknown"
        cost_by_model[model] = cost_by_model.get(model, 0.0) + cost

    nav_pct = (total_cost / current_nav) if current_nav > 0 else 0.0

    return TickCostSummary(
        total_input_tokens=total_input,
        total_output_tokens=total_output,
        total_cost_usd=round(total_cost, 6),
        cost_by_agent=cost_by_agent,
        cost_by_model=cost_by_model,
        cost_as_pct_of_nav=round(nav_pct, 6),
        agent_count=len(token_ledger),
    )


def deduct_from_pnl(trader_id: int, cost_usd: float) -> float:
    """Deduct API costs from the trader's total equity.
    
    Implementation:
    1. Read current total_capital from traders table via Supabase
    2. Subtract cost_usd
    3. Write updated capital back
    4. Return the new NAV
    
    Returns the new total_capital after deduction.
    """
    try:
        from src.core.duckdb_store import get_store
        store = get_store()

        # Read current capital
        result = store.execute(
            "SELECT total_capital FROM traders WHERE id = ?", [trader_id]
        ).fetchone()

        if result is None:
            logger.warning(f"[Accountant] Trader {trader_id} not found — skipping deduction")
            return 0.0

        current_capital = float(result[0])
        new_capital = max(current_capital - cost_usd, 0.0)

        # Write back
        store.execute(
            "UPDATE traders SET total_capital = ? WHERE id = ?",
            [new_capital, trader_id],
        )

        logger.info(
            f"[Accountant] Trader {trader_id}: ${current_capital:,.4f} → "
            f"${new_capital:,.4f} (deducted ${cost_usd:.6f})"
        )
        return new_capital

    except Exception as e:
        logger.error(f"[Accountant] Failed to deduct from P/L for trader {trader_id}: {e}")
        return 0.0


def persist_cost_ledger(
    trader_id: int,
    tick_date: date,
    entries: list[TokenUsage],
) -> None:
    """Write individual agent costs to a Parquet file for historical analysis.
    
    Output: data/components/api_cost_ledger.parquet
    Schema: [trader_id, tick_date, agent_name, model_id, input_tokens, output_tokens, cost_usd]
    """
    try:
        from src.core.duckdb_store import get_parquet_path

        records = []
        for usage in entries:
            cost = calculate_cost(usage)
            records.append({
                "trader_id": trader_id,
                "tick_date": tick_date,
                "agent_name": usage.agent_name or "unknown",
                "model_id": usage.model_id or "unknown",
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cost_usd": cost,
            })

        if not records:
            return

        new_df = pl.DataFrame(records)
        ledger_path = get_parquet_path("api_cost_ledger")

        if os.path.exists(ledger_path):
            existing = pl.read_parquet(ledger_path)
            combined = pl.concat([existing, new_df])
        else:
            combined = new_df

        combined.write_parquet(ledger_path)
        logger.info(f"[Accountant] Persisted {len(records)} cost entries to ledger")

    except Exception as e:
        logger.error(f"[Accountant] Failed to persist cost ledger: {e}")


def run_accountant(
    trader_id: int,
    tick_date: date,
    token_ledger: list[TokenUsage],
) -> AccountingReport:
    """Calculate and deduct API costs from fund P/L.
    
    Process:
    1. Sum all token costs across all agents
    2. Apply model-specific pricing
    3. Deduct total from trader's total_capital
    4. Persist per-agent costs to api_cost_ledger.parquet
    5. Check circuit breakers
    6. Return detailed accounting report
    """
    # Get current NAV before deduction
    nav_before = _get_current_nav(trader_id)

    # Aggregate costs
    summary = aggregate_tick_cost(token_ledger, current_nav=nav_before)

    # Check circuit breakers (Plan 8)
    cumulative_cost_pct = _get_cumulative_cost_pct(trader_id, nav_before)
    circuit_triggered = cumulative_cost_pct > 0.05  # >5% of initial NAV
    downgrade_haiku = 0.02 < cumulative_cost_pct <= 0.05  # 2-5%: switch to Haiku

    if circuit_triggered:
        logger.warning(
            f"[Accountant] CIRCUIT BREAKER: Trader {trader_id} cumulative API costs "
            f"({cumulative_cost_pct:.1%}) exceed 5% of NAV — fund halted!"
        )
    elif downgrade_haiku:
        logger.warning(
            f"[Accountant] DOWNGRADE WARNING: Trader {trader_id} cumulative API costs "
            f"({cumulative_cost_pct:.1%}) exceed 2% of NAV — switching to Haiku"
        )

    # Deduct from P/L
    nav_after = deduct_from_pnl(trader_id, summary.total_cost_usd)

    # Persist ledger
    persist_cost_ledger(trader_id, tick_date, token_ledger)

    logger.info(
        f"[Accountant] Tick summary: {summary.agent_count} agents, "
        f"{summary.total_input_tokens}in+{summary.total_output_tokens}out tokens, "
        f"${summary.total_cost_usd:.6f}"
    )

    return AccountingReport(
        trader_id=trader_id,
        tick_date=tick_date,
        total_cost_usd=summary.total_cost_usd,
        cost_by_agent=summary.cost_by_agent,
        cost_as_pct_of_nav=summary.cost_as_pct_of_nav,
        nav_before=nav_before,
        nav_after=nav_after,
        circuit_breaker_triggered=circuit_triggered,
        downgrade_to_haiku=downgrade_haiku,
    )


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _get_current_nav(trader_id: int) -> float:
    """Read current total_capital for a trader."""
    try:
        from src.core.duckdb_store import get_store
        store = get_store()
        result = store.execute(
            "SELECT total_capital FROM traders WHERE id = ?", [trader_id]
        ).fetchone()
        return float(result[0]) if result else 10_000.0
    except Exception:
        return 10_000.0


def _get_cumulative_cost_pct(trader_id: int, current_nav: float) -> float:
    """Estimate cumulative API cost as % of NAV from ledger."""
    try:
        from src.core.duckdb_store import get_parquet_path
        ledger_path = get_parquet_path("api_cost_ledger")
        if not os.path.exists(ledger_path):
            return 0.0

        df = pl.read_parquet(ledger_path).filter(pl.col("trader_id") == trader_id)
        if df.is_empty():
            return 0.0

        total_spent = df["cost_usd"].sum()
        return total_spent / max(current_nav, 1.0)
    except Exception:
        return 0.0


def get_cost_history(trader_id: int) -> dict:
    """Return cost history for a trader (for /api/arena/costs endpoint)."""
    try:
        from src.core.duckdb_store import get_parquet_path
        ledger_path = get_parquet_path("api_cost_ledger")
        if not os.path.exists(ledger_path):
            return {"trader_id": trader_id, "total_cost_usd": 0.0, "ticks_run": 0, "daily_breakdown": []}

        df = pl.read_parquet(ledger_path).filter(pl.col("trader_id") == trader_id)
        if df.is_empty():
            return {"trader_id": trader_id, "total_cost_usd": 0.0, "ticks_run": 0, "daily_breakdown": []}

        total_cost = df["cost_usd"].sum()
        ticks_run = df["tick_date"].n_unique()
        avg_cost = total_cost / max(ticks_run, 1)

        daily = (
            df.group_by("tick_date")
            .agg([
                pl.col("cost_usd").sum().alias("cost_usd"),
                pl.col("agent_name").count().alias("agents"),
                pl.col("input_tokens").sum().alias("total_input_tokens"),
                pl.col("output_tokens").sum().alias("total_output_tokens"),
            ])
            .sort("tick_date", descending=True)
            .to_dicts()
        )

        cost_by_role = (
            df.group_by("agent_name")
            .agg(pl.col("cost_usd").sum())
            .to_dicts()
        )

        return {
            "trader_id": trader_id,
            "total_cost_usd": round(total_cost, 6),
            "ticks_run": ticks_run,
            "avg_cost_per_tick": round(avg_cost, 6),
            "daily_breakdown": [
                {
                    "date": str(r["tick_date"]),
                    "cost_usd": round(r["cost_usd"], 6),
                    "agents": r["agents"],
                    "total_tokens": r["total_input_tokens"] + r["total_output_tokens"],
                }
                for r in daily
            ],
            "cost_by_agent_role": {r["agent_name"]: round(r["cost_usd"], 6) for r in cost_by_role},
        }
    except Exception as e:
        logger.error(f"[Accountant] Failed to get cost history: {e}")
        return {"trader_id": trader_id, "total_cost_usd": 0.0, "ticks_run": 0, "daily_breakdown": []}
