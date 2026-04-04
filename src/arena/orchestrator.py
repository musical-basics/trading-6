"""
src/arena/orchestrator.py — Daily Game Loop (Plan 3)

The central nervous system of the Hedge Fund Swarm. Coordinates all agent calls
in correct topological order, enforces hierarchy, tracks token costs, and triggers
the deterministic Back Office.

Execution DAG:
  Phase 0: Morning Data Fetch        (concurrent)
  Phase 1: C-Suite Consultants       (concurrent)
  Phase 2: Commander                 (sequential, depends on Phase 1)
  Phase 3: Trading Desks             (concurrent desks, sequential within desk)
  Phase 4: Back Office               (deterministic Python, sequential)
  Phase 5: Publish Results           (WebSocket broadcast)

Token costs are ALWAYS deducted from P/L, even if the tick fails mid-way.
This is enforced via try/finally in run_daily_tick().
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any

import httpx

from src.arena.schemas import (
    MacroBrief, AuditBrief, ScoutBrief,
    CommanderDirective, AnalystInsight, StrategistRecommendation, PMDecision,
    DailyTickResult, DeskAllocation, TokenUsage,
)
from src.arena.prompts import (
    market_consultant_prompt, market_consultant_user_message,
    auditor_prompt, auditor_user_message,
    scout_prompt, scout_user_message,
    commander_prompt, commander_user_message,
    analyst_prompt, analyst_user_message,
    strategist_prompt, strategist_user_message,
    pm_prompt, pm_user_message,
)
from src.arena.llm_client import call_llm, DEFAULT_MODEL_CONFIG, HAIKU_MODEL_CONFIG, LLMError
from src.arena.back_office import run_back_office, BackOfficeHaltError

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

ARENA_API_BASE_URL = os.getenv("ARENA_API_BASE_URL", "http://localhost:8000")
ARENA_LLM_MODEL = os.getenv("ARENA_LLM_MODEL", "claude-sonnet-4-6-20251001")
ARENA_MAX_RETRIES = int(os.getenv("ARENA_MAX_RETRIES", "2"))
ARENA_TIMEOUT_SECONDS = int(os.getenv("ARENA_TIMEOUT_SECONDS", "30"))


# ═══════════════════════════════════════════════════════════════
# PROGRESS TRACKING (for tick status endpoint)
# ═══════════════════════════════════════════════════════════════

class TickProgress:
    """Thread-safe tick progress tracker."""
    def __init__(self):
        self.phase: str = "init"
        self.current_agent: str = ""
        self.completed_agents: list[str] = []
        self.pending_agents: list[str] = []
        self.started_at: float = time.time()

    def start_agent(self, agent_name: str):
        self.current_agent = agent_name
        logger.info(f"[Orchestrator] ▶ {agent_name}")

    def complete_agent(self, agent_name: str):
        if agent_name in self.pending_agents:
            self.pending_agents.remove(agent_name)
        self.completed_agents.append(agent_name)
        self.current_agent = ""
        logger.info(f"[Orchestrator] ✓ {agent_name}")

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self.started_at


# ═══════════════════════════════════════════════════════════════
# PHASE 0: DATA FETCH LAYER
# ═══════════════════════════════════════════════════════════════

async def _fetch_risk_summary(client: httpx.AsyncClient) -> dict:
    """GET /api/risk/summary — VIX, yields, macro regime."""
    try:
        resp = await client.get(f"{ARENA_API_BASE_URL}/api/risk/summary", timeout=ARENA_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"[Orchestrator] Failed to fetch risk summary: {e}")
        return {
            "macro_regime": "Caution",
            "vix": 20.0,
            "ten_year_yield": 4.5,
            "error": str(e),
        }


async def _fetch_pipeline_coverage(client: httpx.AsyncClient) -> dict:
    """GET /api/diagnostics/pipeline-coverage — data staleness."""
    try:
        resp = await client.get(f"{ARENA_API_BASE_URL}/api/diagnostics/pipeline-coverage", timeout=ARENA_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"[Orchestrator] Failed to fetch pipeline coverage: {e}")
        return {"status": "unknown", "error": str(e)}


async def _fetch_competitors(client: httpx.AsyncClient, exclude_trader_id: int) -> list[dict]:
    """GET /api/traders — all traders except self."""
    try:
        resp = await client.get(f"{ARENA_API_BASE_URL}/api/traders", timeout=ARENA_TIMEOUT_SECONDS)
        resp.raise_for_status()
        traders = resp.json()
        if isinstance(traders, list):
            return [t for t in traders if t.get("id") != exclude_trader_id]
        return []
    except Exception as e:
        logger.error(f"[Orchestrator] Failed to fetch competitors: {e}")
        return []


async def _fetch_positions(client: httpx.AsyncClient, trader_id: int) -> dict:
    """GET /api/traders/{id}/positions — current holdings."""
    try:
        resp = await client.get(
            f"{ARENA_API_BASE_URL}/api/traders/{trader_id}/positions",
            timeout=ARENA_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"[Orchestrator] Failed to fetch positions: {e}")
        return {}


async def _fetch_trader_info(client: httpx.AsyncClient, trader_id: int) -> dict:
    """GET /api/traders/{id} — fund name, capital."""
    try:
        resp = await client.get(
            f"{ARENA_API_BASE_URL}/api/traders/{trader_id}",
            timeout=ARENA_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"[Orchestrator] Failed to fetch trader info: {e}")
        return {"id": trader_id, "name": f"Fund-{trader_id}", "total_capital": 10000.0}


async def _fetch_ticker_indicators(client: httpx.AsyncClient, tickers: list[str]) -> dict:
    """GET /api/indicators/{ticker} for each ticker and aggregate."""
    results = {}
    for ticker in tickers[:10]:  # limit to 10 to control token load
        try:
            resp = await client.get(
                f"{ARENA_API_BASE_URL}/api/indicators/{ticker}",
                timeout=ARENA_TIMEOUT_SECONDS,
            )
            if resp.status_code == 200:
                data = resp.json()
                # Take only the latest row to keep token usage low
                if isinstance(data, list) and data:
                    results[ticker] = data[-1]
                elif isinstance(data, dict):
                    results[ticker] = data
        except Exception:
            pass
    return results


async def _apply_pm_decision(client: httpx.AsyncClient, pm: PMDecision) -> bool:
    """PUT /api/portfolios/{id}/strategy — write PM decision to DB."""
    try:
        payload = {
            "strategy_id": pm.strategy_id,
            "allocated_capital": pm.allocated_capital,
        }
        resp = await client.put(
            f"{ARENA_API_BASE_URL}/api/portfolios/{pm.portfolio_id}/strategy",
            json=payload,
            timeout=ARENA_TIMEOUT_SECONDS,
        )
        if resp.status_code in (200, 201, 204):
            logger.info(f"[Orchestrator] PM Desk {pm.desk_id}: assigned {pm.strategy_id} to portfolio {pm.portfolio_id}")
            return True
        else:
            logger.warning(f"[Orchestrator] PM Desk {pm.desk_id}: PUT returned {resp.status_code}")
            return False
    except Exception as e:
        logger.error(f"[Orchestrator] PM Desk {pm.desk_id}: Failed to apply decision: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# PHASE 1: C-SUITE CONSULTANTS
# ═══════════════════════════════════════════════════════════════

async def _run_market_consultant(
    risk_data: dict,
    fund_name: str,
    model_config: dict[str, str],
    token_ledger: list[TokenUsage],
    progress: TickProgress,
    ws_broadcast,
) -> MacroBrief:
    """Run Market Consultant agent."""
    progress.start_agent("consultant")
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": "consultant", "model_id": model_config["consultant"], "phase": "consultants"})

    try:
        system = market_consultant_prompt(fund_name)
        user = market_consultant_user_message(risk_data)
        parsed, usage = await call_llm(
            system_prompt=system,
            user_message=user,
            response_model=MacroBrief,
            agent_name="consultant",
            model_id=model_config["consultant"],
            max_retries=ARENA_MAX_RETRIES,
        )
        usage.agent_name = "consultant"
        token_ledger.append(usage)
        parsed.token_cost = usage

        progress.complete_agent("consultant")
        if ws_broadcast:
            await ws_broadcast("arena.agent_completed", {
                "agent_name": "consultant",
                "tokens": {"input": usage.input_tokens, "output": usage.output_tokens},
                "cost_usd": usage.estimated_cost_usd,
            })
        return parsed

    except LLMError as e:
        logger.error(f"[Orchestrator] Market Consultant failed: {e}")
        # Fallback: use raw data to construct a basic brief
        return MacroBrief(
            macro_regime="Caution",
            vix_level=float(risk_data.get("vix", 20.0)),
            ten_year_yield=float(risk_data.get("ten_year_yield", 4.5)),
            risk_assessment="Fallback: LLM unavailable. Caution regime applied.",
            token_cost=TokenUsage(agent_name="consultant"),
        )


async def _run_auditor(
    coverage_data: dict,
    fund_name: str,
    model_config: dict[str, str],
    token_ledger: list[TokenUsage],
    progress: TickProgress,
    ws_broadcast,
) -> AuditBrief:
    """Run Data Integrity Auditor agent."""
    progress.start_agent("auditor")
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": "auditor", "model_id": model_config["consultant"], "phase": "consultants"})

    try:
        system = auditor_prompt(fund_name)
        user = auditor_user_message(coverage_data)
        parsed, usage = await call_llm(
            system_prompt=system,
            user_message=user,
            response_model=AuditBrief,
            agent_name="auditor",
            model_id=model_config["consultant"],
            max_retries=ARENA_MAX_RETRIES,
        )
        usage.agent_name = "auditor"
        token_ledger.append(usage)
        parsed.token_cost = usage

        progress.complete_agent("auditor")
        if ws_broadcast:
            await ws_broadcast("arena.agent_completed", {
                "agent_name": "auditor",
                "tokens": {"input": usage.input_tokens, "output": usage.output_tokens},
                "cost_usd": usage.estimated_cost_usd,
            })
        return parsed

    except LLMError as e:
        logger.error(f"[Orchestrator] Auditor failed: {e}")
        return AuditBrief(
            data_freshness_ok=True,
            stale_tickers=[],
            risk_breaches=[],
            overall_health="WARNING",
            token_cost=TokenUsage(agent_name="auditor"),
        )


async def _run_scout(
    competitor_data: list[dict],
    trader_id: int,
    fund_name: str,
    model_config: dict[str, str],
    token_ledger: list[TokenUsage],
    progress: TickProgress,
    ws_broadcast,
) -> ScoutBrief:
    """Run Competitive Intelligence Scout agent."""
    progress.start_agent("scout")
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": "scout", "model_id": model_config["consultant"], "phase": "consultants"})

    try:
        system = scout_prompt(fund_name)
        user = scout_user_message(competitor_data, trader_id)
        parsed, usage = await call_llm(
            system_prompt=system,
            user_message=user,
            response_model=ScoutBrief,
            agent_name="scout",
            model_id=model_config["consultant"],
            max_retries=ARENA_MAX_RETRIES,
        )
        usage.agent_name = "scout"
        token_ledger.append(usage)
        parsed.token_cost = usage

        progress.complete_agent("scout")
        if ws_broadcast:
            await ws_broadcast("arena.agent_completed", {
                "agent_name": "scout",
                "tokens": {"input": usage.input_tokens, "output": usage.output_tokens},
                "cost_usd": usage.estimated_cost_usd,
            })
        return parsed

    except LLMError as e:
        logger.error(f"[Orchestrator] Scout failed: {e}")
        return ScoutBrief(
            competitor_count=len(competitor_data),
            top_threat={},
            avg_sharpe=0.0,
            strategic_summary="Fallback: Scout LLM unavailable.",
            token_cost=TokenUsage(agent_name="scout"),
        )


# ═══════════════════════════════════════════════════════════════
# PHASE 2: COMMANDER
# ═══════════════════════════════════════════════════════════════

async def _run_commander(
    macro: MacroBrief,
    audit: AuditBrief,
    scout: ScoutBrief,
    positions: dict,
    total_capital: float,
    fund_name: str,
    model_config: dict[str, str],
    token_ledger: list[TokenUsage],
    progress: TickProgress,
    ws_broadcast,
) -> CommanderDirective:
    """Run Commander (CEO) agent."""
    progress.phase = "commander"
    progress.start_agent("commander")
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": "commander", "model_id": model_config["commander"], "phase": "commander"})

    system = commander_prompt(fund_name, total_capital)
    user = commander_user_message(
        macro.model_dump(mode="json"),
        audit.model_dump(mode="json"),
        scout.model_dump(mode="json"),
        positions,
    )

    parsed, usage = await call_llm(
        system_prompt=system,
        user_message=user,
        response_model=CommanderDirective,
        agent_name="commander",
        model_id=model_config["commander"],
        max_retries=ARENA_MAX_RETRIES,
        max_tokens=1024,
    )
    usage.agent_name = "commander"
    token_ledger.append(usage)
    parsed.token_cost = usage

    progress.complete_agent("commander")
    if ws_broadcast:
        await ws_broadcast("arena.agent_completed", {
            "agent_name": "commander",
            "tokens": {"input": usage.input_tokens, "output": usage.output_tokens},
            "cost_usd": usage.estimated_cost_usd,
        })

    logger.info(
        f"[Orchestrator] Commander: deploying {parsed.total_deployed_pct:.0%} "
        f"across {len(parsed.desk_allocations)} desks, "
        f"cash reserve {parsed.cash_reserve_pct:.0%}"
    )
    return parsed


# ═══════════════════════════════════════════════════════════════
# PHASE 3: TRADING DESKS (concurrent)
# ═══════════════════════════════════════════════════════════════

async def _run_desk(
    alloc: DeskAllocation,
    fund_name: str,
    model_config: dict[str, str],
    token_ledger: list[TokenUsage],
    progress: TickProgress,
    ws_broadcast,
    http_client: httpx.AsyncClient,
    portfolio_id: int,
    dry_run: bool = False,
) -> PMDecision:
    """Run Analyst → Strategist → PM pipeline for one desk."""

    # Fetch indicator data for this desk's tickers
    indicators = await _fetch_ticker_indicators(http_client, alloc.tickers)

    # ── Analyst ──────────────────────────────────────────────
    analyst_agent = f"analyst_d{alloc.desk_id}"
    progress.start_agent(analyst_agent)
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": analyst_agent, "model_id": model_config["analyst"], "phase": "desks"})

    analyst_insight, a_usage = await call_llm(
        system_prompt=analyst_prompt(fund_name, alloc.desk_id, alloc.strategic_directive, alloc.tickers),
        user_message=analyst_user_message(indicators),
        response_model=AnalystInsight,
        agent_name=analyst_agent,
        model_id=model_config["analyst"],
        max_retries=ARENA_MAX_RETRIES,
    )
    a_usage.agent_name = analyst_agent
    token_ledger.append(a_usage)
    analyst_insight.token_cost = a_usage
    progress.complete_agent(analyst_agent)

    if ws_broadcast:
        await ws_broadcast("arena.agent_completed", {
            "agent_name": analyst_agent,
            "tokens": {"input": a_usage.input_tokens, "output": a_usage.output_tokens},
            "cost_usd": a_usage.estimated_cost_usd,
        })

    # ── Strategist ───────────────────────────────────────────
    strat_agent = f"strategist_d{alloc.desk_id}"
    progress.start_agent(strat_agent)
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": strat_agent, "model_id": model_config["strategist"], "phase": "desks"})

    strategy_rec, s_usage = await call_llm(
        system_prompt=strategist_prompt(fund_name, alloc.desk_id),
        user_message=strategist_user_message(
            analyst_insight.model_dump(mode="json"),
            alloc.strategic_directive,
            alloc.risk_tolerance,
        ),
        response_model=StrategistRecommendation,
        agent_name=strat_agent,
        model_id=model_config["strategist"],
        max_retries=ARENA_MAX_RETRIES,
    )
    s_usage.agent_name = strat_agent
    token_ledger.append(s_usage)
    strategy_rec.token_cost = s_usage
    progress.complete_agent(strat_agent)

    logger.info(
        f"[Orchestrator] Desk {alloc.desk_id} Strategist: "
        f"{strategy_rec.recommended_strategy} "
        f"(confidence: {strategy_rec.confidence:.2f})"
    )

    if ws_broadcast:
        await ws_broadcast("arena.agent_completed", {
            "agent_name": strat_agent,
            "tokens": {"input": s_usage.input_tokens, "output": s_usage.output_tokens},
            "cost_usd": s_usage.estimated_cost_usd,
            "strategy": strategy_rec.recommended_strategy,
        })

    # ── Portfolio Manager ────────────────────────────────────
    pm_agent = f"pm_d{alloc.desk_id}"
    progress.start_agent(pm_agent)
    if ws_broadcast:
        await ws_broadcast("arena.agent_started", {"agent_name": pm_agent, "model_id": model_config["pm"], "phase": "desks"})

    pm_decision, p_usage = await call_llm(
        system_prompt=pm_prompt(
            fund_name, alloc.desk_id, alloc.capital_budget_usd,
            strategy_rec.recommended_strategy, portfolio_id,
        ),
        user_message=pm_user_message(
            alloc.desk_id, alloc.capital_budget_usd,
            strategy_rec.recommended_strategy, portfolio_id,
        ),
        response_model=PMDecision,
        agent_name=pm_agent,
        model_id=model_config["pm"],
        max_retries=ARENA_MAX_RETRIES,
    )
    p_usage.agent_name = pm_agent
    token_ledger.append(p_usage)
    pm_decision.token_cost = p_usage

    # Orchestrator-level budget clamp (safety net per Plan 8)
    if pm_decision.allocated_capital > alloc.capital_budget_usd:
        logger.warning(
            f"[Orchestrator] PM Desk {alloc.desk_id} attempted to exceed budget "
            f"(${pm_decision.allocated_capital:.2f} > ${alloc.capital_budget_usd:.2f}). Clamped."
        )
        pm_decision = pm_decision.model_copy(update={"allocated_capital": alloc.capital_budget_usd})

    progress.complete_agent(pm_agent)

    if ws_broadcast:
        await ws_broadcast("arena.agent_completed", {
            "agent_name": pm_agent,
            "tokens": {"input": p_usage.input_tokens, "output": p_usage.output_tokens},
            "cost_usd": p_usage.estimated_cost_usd,
        })

    # Apply PM decision to DB (unless dry_run)
    if not dry_run:
        await _apply_pm_decision(http_client, pm_decision)

    return pm_decision


# ═══════════════════════════════════════════════════════════════
# MASTER ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

async def run_daily_tick(
    trader_id: int,
    model_overrides: dict[str, str] | None = None,
    dry_run: bool = False,
    ws_broadcast=None,
    progress: TickProgress | None = None,
) -> DailyTickResult:
    """Run one complete daily game loop for a hedge fund.
    
    Args:
        trader_id: The fund's trader ID
        model_overrides: Override per-role model IDs (for A/B testing per Rule #16)
        dry_run: If True, runs agents but does NOT commit PM decisions to DB
        ws_broadcast: Async callable for WebSocket event broadcasting
        progress: Progress tracker for status endpoint
    
    Returns:
        DailyTickResult with all agent outputs and cost breakdown
    
    Notes:
        Token costs are ALWAYS deducted via try/finally, even on failure.
    """
    start_time = time.time()
    tick_date = date.today()
    token_ledger: list[TokenUsage] = []

    if progress is None:
        progress = TickProgress()

    # Build model config (support A/B test per Rule #16)
    model_config = {**DEFAULT_MODEL_CONFIG, **(model_overrides or {})}

    # Check if circuit breaker demands Haiku downgrade
    from src.arena.accountant import _get_cumulative_cost_pct, _get_current_nav
    current_nav = _get_current_nav(trader_id)
    cumulative_cost_pct = _get_cumulative_cost_pct(trader_id, current_nav)
    if cumulative_cost_pct > 0.02:  # >2% → downgrade to Haiku
        logger.warning(f"[Orchestrator] Cost circuit breaker: switching to Haiku config")
        model_config = {**HAIKU_MODEL_CONFIG, **(model_overrides or {})}

    logger.info(f"[Orchestrator] ═══ Daily Tick: trader_id={trader_id}, date={tick_date} ═══")
    if ws_broadcast:
        await ws_broadcast("arena.tick_started", {"trader_id": trader_id, "tick_date": str(tick_date)})

    try:
        async with httpx.AsyncClient() as http_client:
            # ── Fetch trader info ──────────────────────────────────
            trader_info = await _fetch_trader_info(http_client, trader_id)
            fund_name = trader_info.get("name", f"Fund-{trader_id}")
            total_capital = float(trader_info.get("total_capital", 10_000.0))

            # Get portfolio ID (use trader_id as default, fetch if exists)
            portfolio_id = trader_id  # Simplified: use trader_id as portfolio_id

            # ── Phase 0: Morning Data Fetch (concurrent) ──────────
            progress.phase = "data_fetch"
            logger.info("[Orchestrator] Phase 0: Morning data fetch...")
            risk_data, coverage_data, competitor_data = await asyncio.gather(
                _fetch_risk_summary(http_client),
                _fetch_pipeline_coverage(http_client),
                _fetch_competitors(http_client, trader_id),
            )

            # ── Phase 1: C-Suite Consultants (concurrent) ─────────
            progress.phase = "consultants"
            progress.pending_agents = ["consultant", "auditor", "scout"]
            logger.info("[Orchestrator] Phase 1: Running C-Suite consultants...")

            macro_brief, audit_brief, scout_brief = await asyncio.gather(
                _run_market_consultant(risk_data, fund_name, model_config, token_ledger, progress, ws_broadcast),
                _run_auditor(coverage_data, fund_name, model_config, token_ledger, progress, ws_broadcast),
                _run_scout(competitor_data, trader_id, fund_name, model_config, token_ledger, progress, ws_broadcast),
            )

            if ws_broadcast:
                await ws_broadcast("arena.phase_completed", {"phase_name": "consultants", "agents_completed": 3})

            # ── Phase 2: Commander (sequential) ──────────────────
            positions = await _fetch_positions(http_client, trader_id)
            logger.info("[Orchestrator] Phase 2: Running Commander...")

            commander_directive = await _run_commander(
                macro_brief, audit_brief, scout_brief,
                positions, total_capital, fund_name,
                model_config, token_ledger, progress, ws_broadcast,
            )

            if ws_broadcast:
                await ws_broadcast("arena.phase_completed", {"phase_name": "commander", "agents_completed": 1})

            # ── Phase 3: Trading Desks (concurrent) ──────────────
            progress.phase = "desks"
            desk_agent_names = []
            for alloc in commander_directive.desk_allocations:
                desk_agent_names.extend([
                    f"analyst_d{alloc.desk_id}",
                    f"strategist_d{alloc.desk_id}",
                    f"pm_d{alloc.desk_id}",
                ])
            progress.pending_agents = desk_agent_names
            logger.info(f"[Orchestrator] Phase 3: Running {len(commander_directive.desk_allocations)} desks concurrently...")

            desk_results = await asyncio.gather(
                *[
                    _run_desk(
                        alloc=alloc,
                        fund_name=fund_name,
                        model_config=model_config,
                        token_ledger=token_ledger,
                        progress=progress,
                        ws_broadcast=ws_broadcast,
                        http_client=http_client,
                        portfolio_id=portfolio_id,
                        dry_run=dry_run,
                    )
                    for alloc in commander_directive.desk_allocations
                ]
            )
            desk_results_list = list(desk_results)

            if ws_broadcast:
                await ws_broadcast("arena.phase_completed", {"phase_name": "desks", "agents_completed": len(desk_agent_names)})

            # ── Phase 4: Back Office (deterministic Python) ───────
            progress.phase = "back_office"
            logger.info("[Orchestrator] Phase 4: Running Back Office...")

            try:
                integrity_report, risk_report, accounting_report = await run_back_office(
                    trader_id=trader_id,
                    pm_decisions=desk_results_list,
                    token_ledger=token_ledger,
                    tick_date=tick_date,
                )
                api_cost_deducted = accounting_report.total_cost_usd if not dry_run else 0.0
            except BackOfficeHaltError as e:
                logger.error(f"[Orchestrator] Back Office HALT: {e}")
                raise
            except Exception as e:
                logger.error(f"[Orchestrator] Back Office error (non-fatal): {e}")
                api_cost_deducted = 0.0

    except Exception:
        # ALWAYS deduct token costs on failure (try/finally guarantee from Plan 8)
        raise

    finally:
        # GUARANTEE: deduct costs even if tick fails mid-way
        if token_ledger and not dry_run:
            try:
                from src.arena.accountant import aggregate_tick_cost, deduct_from_pnl
                summary = aggregate_tick_cost(token_ledger, current_nav=current_nav)
                if not hasattr(locals(), 'api_cost_deducted'):
                    deduct_from_pnl(trader_id, summary.total_cost_usd)
            except Exception as e:
                logger.error(f"[Orchestrator] Failed to deduct costs in finally block: {e}")

    # ── Phase 5: Build result & publish ──────────────────────
    from src.arena.accountant import aggregate_tick_cost
    total_usage_summary = aggregate_tick_cost(token_ledger, current_nav=current_nav)

    elapsed = time.time() - start_time
    result = DailyTickResult(
        tick_date=tick_date,
        trader_id=trader_id,
        commander_directive=commander_directive,
        desk_results=desk_results_list,
        total_token_cost=TokenUsage(
            input_tokens=total_usage_summary.total_input_tokens,
            output_tokens=total_usage_summary.total_output_tokens,
            estimated_cost_usd=total_usage_summary.total_cost_usd,
        ),
        api_cost_deducted_usd=api_cost_deducted if not dry_run else 0.0,
        elapsed_seconds=elapsed,
        macro_brief=macro_brief,
        audit_brief=audit_brief,
        scout_brief=scout_brief,
    )

    logger.info(
        f"[Orchestrator] ✅ Tick complete: {len(token_ledger)} agents, "
        f"{total_usage_summary.total_input_tokens}in+{total_usage_summary.total_output_tokens}out tokens, "
        f"${total_usage_summary.total_cost_usd:.6f}, {elapsed:.1f}s"
    )

    if ws_broadcast:
        await ws_broadcast("arena.tick_completed", {
            "trader_id": trader_id,
            "total_cost_usd": total_usage_summary.total_cost_usd,
            "elapsed_seconds": elapsed,
            "agents_run": len(token_ledger),
        })

    return result


# ═══════════════════════════════════════════════════════════════
# MULTI-FUND ARENA TICK (Plan 8: Phase 2 scaling)
# ═══════════════════════════════════════════════════════════════

async def run_arena_tick(
    tick_date: date | None = None,
    model_overrides: dict[str, str] | None = None,
    ws_broadcast=None,
) -> dict:
    """Run a daily tick for ALL active funds concurrently.
    
    Each fund runs its own run_daily_tick() — isolated portfolios,
    shared market data. Results are ranked by cumulative return.
    """
    from src.core.duckdb_store import get_store

    store = get_store()
    traders = store.execute("SELECT id, name, total_capital FROM traders").fetchall()

    if not traders:
        return {"error": "No traders found"}

    logger.info(f"[ArenaOrchestrator] Running arena tick for {len(traders)} funds...")

    results = await asyncio.gather(
        *[run_daily_tick(t[0], model_overrides=model_overrides, ws_broadcast=ws_broadcast)
          for t in traders],
        return_exceptions=True,
    )

    # Build leaderboard
    leaderboard = []
    for i, (trader, result) in enumerate(zip(traders, results)):
        if isinstance(result, Exception):
            logger.error(f"[ArenaOrchestrator] Fund {trader[0]} failed: {result}")
        else:
            leaderboard.append({
                "trader_id": trader[0],
                "fund_name": trader[1],
                "total_capital": float(trader[2]),
                "api_cost_deducted": result.api_cost_deducted_usd,
                "elapsed_seconds": result.elapsed_seconds,
            })

    # Sort by capital desc
    leaderboard.sort(key=lambda x: x["total_capital"], reverse=True)
    for i, entry in enumerate(leaderboard):
        entry["rank"] = i + 1

    if ws_broadcast:
        await ws_broadcast("arena.leaderboard_updated", {"leaderboard": leaderboard})

    return {"date": str(tick_date or date.today()), "funds_run": len(traders), "leaderboard": leaderboard}
