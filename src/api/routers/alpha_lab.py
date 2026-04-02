"""
alpha_lab.py — API endpoints for the Alpha Lab autonomous strategy discovery.

All endpoints are prefixed with /api/alpha-lab/.
Fully isolated from production strategy/portfolio/trader endpoints.
Level 5 additions: /promote endpoint for one-click production promotion.
"""

import json
import math
from datetime import date, datetime
from pathlib import Path
from json.decoder import JSONDecodeError
from fastapi import APIRouter, Header, Depends, Request, BackgroundTasks, Body
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from src.alpha_lab.strategy_generator import generate_strategy, get_tier_info, combine_strategies
from src.alpha_lab.alpha_lab_store import (
    save_experiment,
    list_experiments,
    get_experiment,
    delete_experiment,
    get_equity_curve,
    update_experiment_code,
    update_experiment_name,
    save_editor_setting,
    get_editor_setting,
)
from src.alpha_lab.lab_backtester import run_lab_backtest, run_raw_backtest

router = APIRouter(prefix="/api/alpha-lab", tags=["alpha-lab"])


def _sanitize(obj):
    """Recursively make any object JSON-safe.

    Handles: NaN, Inf, datetime.date, datetime.datetime, numpy scalars.
    """
    if obj is None:
        return None
    # Handle date/datetime → ISO string
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    # Handle floats (including numpy float64)
    if isinstance(obj, (int, bool)):
        return obj
    try:
        # Works for float, np.float64, np.float32, etc.
        if isinstance(obj, float) or hasattr(obj, '__float__'):
            fval = float(obj)
            if math.isnan(fval) or math.isinf(fval):
                return None
            return fval
    except (TypeError, ValueError):
        pass
    if isinstance(obj, dict):
        return {str(k): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, str):
        return obj
    # Fallback: convert to string
    try:
        return str(obj)
    except Exception:
        return None


def _safe_response(data):
    """Return a JSONResponse with all values sanitized for JSON."""
    return JSONResponse(content=_sanitize(data))


@router.get("/aligned-profile")
async def get_aligned_profile():
    """Serves the semantic dictionary and statistical distribution of the feature store.

    Returns {total_rows, features: {col: {dtype, category, description, stats}}}
    from the joined aligned dataset — same data the backtester operates on.
    """
    try:
        from src.alpha_lab.stats_engine import generate_aligned_data_profile
        profile_data = generate_aligned_data_profile()
        if "error" in profile_data:
            return _safe_response({"error": profile_data["error"]})
        return _safe_response(profile_data)
    except Exception as e:
        return _safe_response({"error": f"Failed to generate profile: {e}"})


@router.get("/tiers")
async def get_model_tiers():
    """Return available model tiers and their pricing."""
    return get_tier_info()


@router.post("/generate")
async def generate_new_strategy(
    prompt: str = "",
    model_tier: str = "sonnet",
    strategy_style: str = "academic",
):
    """Generate a new strategy hypothesis using a single LLM call (1-shot)."""
    try:
        hypothesis = generate_strategy(prompt=prompt, model_tier=model_tier, strategy_style=strategy_style)

        # Save to store
        experiment_id = save_experiment(
            hypothesis=prompt or "(auto-generated)",
            strategy_code=hypothesis.code,
            strategy_name=hypothesis.name,
            model_tier=model_tier,
            rationale=hypothesis.rationale,
            input_tokens=hypothesis.input_tokens,
            output_tokens=hypothesis.output_tokens,
            cost_usd=hypothesis.cost_usd,
        )

        return _safe_response({
            "experiment_id": experiment_id,
            "strategy_name": hypothesis.name,
            "rationale": hypothesis.rationale,
            "code": hypothesis.code,
            "model_tier": model_tier,
            "input_tokens": hypothesis.input_tokens,
            "output_tokens": hypothesis.output_tokens,
            "cost_usd": hypothesis.cost_usd,
        })
    except ValueError as e:
        return _safe_response({"error": str(e)})
    except Exception as e:
        return _safe_response({"error": f"Generation failed: {type(e).__name__}: {e}"})


@router.post("/generate-swarm")
async def generate_swarm_strategy(
    prompt: str = "",
    model_tier: str = "sonnet",
    strategy_style: str = "academic",
):
    """Generate a strategy using the 3-agent swarm pipeline:
       Researcher → Risk Manager → Quantitative Developer.
    """
    try:
        from src.alpha_lab.swarm_generator import generate_strategy_swarm
        hypothesis = generate_strategy_swarm(prompt=prompt, model_tier=model_tier, strategy_style=strategy_style)

        experiment_id = save_experiment(
            hypothesis=prompt or "(swarm-generated)",
            strategy_code=hypothesis.code,
            strategy_name=hypothesis.name,
            model_tier=f"swarm/{model_tier}",
            rationale=hypothesis.rationale,
            input_tokens=hypothesis.input_tokens,
            output_tokens=hypothesis.output_tokens,
            cost_usd=hypothesis.cost_usd,
        )

        return _safe_response({
            "experiment_id": experiment_id,
            "strategy_name": hypothesis.name,
            "rationale": hypothesis.rationale,
            "code": hypothesis.code,
            "model_tier": f"swarm/{model_tier}",
            "input_tokens": hypothesis.input_tokens,
            "output_tokens": hypothesis.output_tokens,
            "cost_usd": hypothesis.cost_usd,
        })
    except ValueError as e:
        return _safe_response({"error": str(e)})
    except Exception as e:
        return _safe_response({"error": f"Swarm generation failed: {type(e).__name__}: {e}"})

@router.get("/generate-swarm-stream")
async def generate_swarm_strategy_stream(
    prompt: str = "",
    strategy_style: str = "academic",
    agent_tiers: str = "{}", # JSON string
    agent_notes: str = "{}"  # JSON string
):
    """SSE stream for the 3-agent swarm. Yields JSON events per agent step.

    The frontend connects via EventSource or fetch+ReadableStream.
    Closing the connection (Kill button) aborts the generator naturally.
    """
    import asyncio
    import json as _json
    import concurrent.futures
    from src.alpha_lab.swarm_generator import generate_strategy_swarm_stream

    parsed_tiers = _json.loads(agent_tiers)
    parsed_notes = _json.loads(agent_notes)

    gen = generate_strategy_swarm_stream(
        prompt=prompt,
        strategy_style=strategy_style,
        agent_tiers=parsed_tiers,
        agent_notes=parsed_notes,
    )

    async def _async_stream():
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            while True:
                try:
                    chunk = await loop.run_in_executor(executor, next, gen)
                    yield chunk
                except StopIteration:
                    break
        finally:
            executor.shutdown(wait=False)

    return StreamingResponse(
        _async_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


class SwarmSaveRequest(BaseModel):
    name: str
    hypothesis: str
    rationale: str
    code: str
    model_tier: str
    input_tokens: int
    output_tokens: int
    cost_usd: float

@router.post("/generate-swarm-save")
async def save_swarm_result(request: SwarmSaveRequest):
    """Save a completed swarm result. Called by the frontend after result event."""
    experiment_id = save_experiment(
        hypothesis=request.hypothesis or "(swarm-generated)",
        strategy_code=request.code,
        strategy_name=request.name,
        model_tier=f"swarm/{request.model_tier}",
        rationale=request.rationale,
        input_tokens=request.input_tokens,
        output_tokens=request.output_tokens,
        cost_usd=request.cost_usd,
    )
    return _safe_response({"experiment_id": experiment_id, "strategy_name": request.name})

class StandaloneSaveRequest(BaseModel):
    code: str

@router.post("/save-standalone")
async def save_standalone_result(request: StandaloneSaveRequest):
    """Save manually edited standalone code as a new experiment."""
    import re
    match = re.search(r"def\s+([a-zA-Z0-9_]+)\(df", request.code)
    strategy_name = match.group(1) if match else "standalone_strategy"
    
    experiment_id = save_experiment(
        hypothesis="(Manual Coding)",
        strategy_code=request.code,
        strategy_name=strategy_name,
        model_tier="human",
        rationale="(Coded via Standalone Editor)",
        input_tokens=0,
        output_tokens=0,
        cost_usd=0.0,
    )
    return _safe_response({"experiment_id": experiment_id, "strategy_name": strategy_name})


# ── Editor Settings ───────────────────────────────────────

@router.get("/settings/{key}")
async def get_setting(key: str):
    """Retrieve an arbitrary JSON dictionary from the editor defaults database."""
    val = get_editor_setting(key)
    if val is None:
        return _safe_response({"value": None})
    return _safe_response({"value": val})


@router.post("/settings/{key}")
async def set_setting(key: str, payload: dict = Body(...)):
    """Save an arbitrary JSON dictionary to the editor defaults database."""
    save_editor_setting(key, payload.get("value", {}))
    return _safe_response({"status": "ok"})


class UpdateCodeRequest(BaseModel):
    code: str

@router.patch("/{experiment_id}/code")
async def update_code(experiment_id: str, request: UpdateCodeRequest):
    """Update the strategy code for an experiment (human edits)."""
    exp = get_experiment(experiment_id)
    if not exp:
        return _safe_response({"error": f"Experiment {experiment_id} not found"})
    update_experiment_code(experiment_id, request.code)
    return _safe_response({"ok": True, "experiment_id": experiment_id})

class UpdateNameRequest(BaseModel):
    name: str

@router.patch("/{experiment_id}/name")
async def update_name(experiment_id: str, request: UpdateNameRequest):
    """Rename a strategy for an experiment."""
    exp = get_experiment(experiment_id)
    if not exp:
        return _safe_response({"error": f"Experiment {experiment_id} not found"})
    update_experiment_name(experiment_id, request.name)
    return _safe_response({"ok": True, "experiment_id": experiment_id})


@router.post("/{experiment_id}/backtest")
async def backtest_experiment(experiment_id: str):
    """Run a backtest for an existing experiment (works on any status)."""
    exp = get_experiment(experiment_id)
    if not exp:
        return _safe_response({"error": f"Experiment {experiment_id} not found"})

    result = run_lab_backtest(
        experiment_id=experiment_id,
        strategy_code=exp["strategy_code"],
    )

    return _safe_response(result)


class StandaloneBacktestRequest(BaseModel):
    code: str


@router.post("/standalone-backtest")
async def standalone_backtest(request: StandaloneBacktestRequest):
    """Run a backtest on raw strategy code without saving it to an experiment."""
    result = run_raw_backtest(
        strategy_code=request.code,
        starting_capital=10000.0,
        enable_self_healing=False,
    )
    
    # Remove _portfolio_df from response as it's not JSON serializable and not needed by client
    if "_portfolio_df" in result:
        del result["_portfolio_df"]
        
    return _safe_response(result)


@router.get("/experiments")
async def list_all_experiments():
    """List all experiments, newest first."""
    experiments = list_experiments()
    # Parse metrics_json for each experiment
    for exp in experiments:
        if exp.get("metrics_json"):
            try:
                exp["metrics"] = json.loads(exp["metrics_json"])
            except (json.JSONDecodeError, TypeError):
                exp["metrics"] = None
        else:
            exp["metrics"] = None
    return _safe_response(experiments)


@router.get("/{experiment_id}")
async def get_experiment_detail(experiment_id: str):
    """Get full experiment details including equity curve."""
    exp = get_experiment(experiment_id)
    if not exp:
        return _safe_response({"error": f"Experiment {experiment_id} not found"})

    # Parse metrics
    if exp.get("metrics_json"):
        try:
            exp["metrics"] = json.loads(exp["metrics_json"])
        except (json.JSONDecodeError, TypeError):
            exp["metrics"] = None
    else:
        exp["metrics"] = None

    # Load equity curve if available
    ec = get_equity_curve(experiment_id)
    if ec is not None:
        exp["equity_curve"] = ec.select(["date", "daily_return", "equity"]).to_dicts()
    else:
        exp["equity_curve"] = None

    return _safe_response(exp)


@router.delete("/{experiment_id}")
async def delete_experiment_endpoint(experiment_id: str):
    """Delete an experiment and its data."""
    success = delete_experiment(experiment_id)
    return _safe_response({"deleted": success})


# ═══════════════════════════════════════════════════════════════
# LEVEL 5: One-Click Production Promotion
# ═══════════════════════════════════════════════════════════════

CUSTOM_STRATEGIES_DIR = Path("src/ecs/strategies/custom")


@router.post("/{experiment_id}/promote")
async def promote_to_production(experiment_id: str):
    """Promote a passed experiment to production.

    1. Extract successful Python code from the store.
    2. Write it to src/ecs/strategies/custom/generated_{id}.py
    3. The strategy registry auto-discovers it on next startup.
    4. Mark the experiment as promoted.
    """
    exp = get_experiment(experiment_id)
    if not exp:
        return _safe_response({"error": f"Experiment {experiment_id} not found"})
    if exp.get("status") != "passed":
        return _safe_response({"error": "Only passed experiments can be promoted"})

    # Ensure custom directory exists
    CUSTOM_STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)

    # Write the strategy file
    strategy_name = exp.get("strategy_name", f"strategy_{experiment_id}")
    safe_id = experiment_id.replace("-", "_")
    filename = f"generated_{safe_id}.py"
    filepath = CUSTOM_STRATEGIES_DIR / filename

    # Parse metrics for the docstring
    metrics_str = "N/A"
    if exp.get("metrics_json"):
        try:
            metrics = json.loads(exp["metrics_json"]) if isinstance(exp["metrics_json"], str) else exp["metrics_json"]
            metrics_str = f"Sharpe: {metrics.get('sharpe', 'N/A')}"
        except (json.JSONDecodeError, TypeError):
            pass

    strategy_module = (
        f'"""\n'
        f'Auto-generated strategy from Alpha Lab experiment #{experiment_id}.\n'
        f'Experiment: {strategy_name}\n'
        f'Metrics: {metrics_str}\n'
        f'"""\n\n'
        f'import polars as pl\n'
        f'import numpy as np\n\n'
        f'STRATEGY_ID = "alphalab_{safe_id}"\n'
        f'STRATEGY_NAME = "{strategy_name}"\n\n'
        f'{exp["strategy_code"]}\n'
    )

    filepath.write_text(strategy_module)

    # Publish promotion event via Redis (for WebSocket broadcast)
    try:
        import redis as _redis
        from src.config import REDIS_URL
        r = _redis.from_url(REDIS_URL)
        r.publish("system_events", json.dumps({
            "event_type": "strategy_promoted",
            "payload": {
                "experiment_id": experiment_id,
                "strategy_id": f"alphalab_{safe_id}",
                "strategy_name": strategy_name,
            },
        }))
    except Exception:
        pass  # Redis not available — non-critical

    return _safe_response({
        "status": "promoted",
        "strategy_id": f"alphalab_{safe_id}",
        "file": str(filepath),
    })


# ═══════════════════════════════════════════════════════════════
# LEVEL 5: Combine / Evolve Strategies (Manual Genetic Prompting)
# ═══════════════════════════════════════════════════════════════

@router.post("/combine")
async def combine_experiments(
    experiment_ids: str,
    model_tier: str = "sonnet",
    guidance: str = "",
):
    """Combine multiple passed strategies into a new evolved strategy.

    experiment_ids: comma-separated experiment IDs to combine
    model_tier: LLM tier to use
    guidance: optional user guidance for the combination
    """
    ids = [eid.strip() for eid in experiment_ids.split(",") if eid.strip()]

    if len(ids) < 2:
        return _safe_response({"error": "Select at least 2 strategies to combine"})
    if len(ids) > 5:
        return _safe_response({"error": "Maximum 5 strategies can be combined"})

    # Fetch experiments and validate they're all passed
    strategy_codes = []
    strategy_names = []
    for eid in ids:
        exp = get_experiment(eid)
        if not exp:
            return _safe_response({"error": f"Experiment {eid} not found"})
        if exp.get("status") != "passed":
            return _safe_response({"error": f"Experiment '{exp.get('strategy_name', eid)}' has not passed backtesting"})
        strategy_codes.append(exp["strategy_code"])
        strategy_names.append(exp.get("strategy_name", eid))

    try:
        hypothesis = combine_strategies(
            strategy_codes=strategy_codes,
            strategy_names=strategy_names,
            model_tier=model_tier,
            user_guidance=guidance,
        )

        experiment_id = save_experiment(
            hypothesis=f"Combined from: {', '.join(strategy_names)}",
            strategy_code=hypothesis.code,
            strategy_name=hypothesis.name,
            model_tier=model_tier,
            rationale=hypothesis.rationale,
            input_tokens=hypothesis.input_tokens,
            output_tokens=hypothesis.output_tokens,
            cost_usd=hypothesis.cost_usd,
        )

        return _safe_response({
            "experiment_id": experiment_id,
            "strategy_name": hypothesis.name,
            "rationale": hypothesis.rationale,
            "code": hypothesis.code,
            "model_tier": model_tier,
            "parent_strategies": strategy_names,
            "input_tokens": hypothesis.input_tokens,
            "output_tokens": hypothesis.output_tokens,
            "cost_usd": hypothesis.cost_usd,
        })
    except ValueError as e:
        return _safe_response({"error": str(e)})
    except Exception as e:
        return _safe_response({"error": f"Combine failed: {type(e).__name__}: {e}"})


# ═══════════════════════════════════════════════════════════════
# LEVEL 5.5: Forensic AI Backtest Auditor
# ═══════════════════════════════════════════════════════════════

@router.get("/audit/models")
async def get_audit_models():
    """Return available Anthropic models for the Forensic Auditor."""
    from src.alpha_lab.forensic_auditor import get_available_models
    models = get_available_models()
    return _safe_response({"models": models})

class AuditOptions(BaseModel):
    model_id: str = "claude-sonnet-4-6"

@router.post("/{experiment_id}/audit")
async def run_audit(
    experiment_id: str,
    options: AuditOptions = Body(default=AuditOptions())
):
    """Trigger the Forensic AI Auditor for a completed backtest experiment.

    Samples top trades, compiles T-5 to T+5 evidence windows, calls Claude
    to classify errors against 3 taxonomies, persists and returns the verdict.
    """
    from src.alpha_lab.forensic_auditor import run_forensic_audit
    try:
        verdict = run_forensic_audit(experiment_id, model_id=options.model_id)
        return _safe_response(verdict)
    except Exception as e:
        return _safe_response({"error": f"Audit failed: {type(e).__name__}: {e}"})


@router.get("/{experiment_id}/trades")
async def get_experiment_trades(experiment_id: str):
    """Return the raw trade ledger for a completed experiment.

    Used by the Trade Inspector table in the Forensic Auditor UI.
    Returns a list of trade records sorted by date descending, with
    per-trade P/L computed via round-trip BUY->SELL price matching.
    Includes both pnl_pct (%) and pnl_usd ($ per share).
    """
    from src.alpha_lab.alpha_lab_store import get_trade_ledger
    ledger = get_trade_ledger(experiment_id)
    if ledger is None:
        return _safe_response({"trades": [], "message": "No trade ledger found — run backtest first"})

    # Sort chronologically ascending for P/L matching
    trades_asc = ledger.sort("date", descending=False).to_dicts()

    # Round-trip P/L: track last BUY price per ticker
    entry_prices: dict = {}
    for trade in trades_asc:
        ticker = trade.get("ticker") or f"entity_{trade.get('entity_id')}"
        price = trade.get("adj_close")
        action = trade.get("action", "")

        if action == "BUY" and price is not None:
            entry_prices[ticker] = float(price)
            trade["pnl_pct"] = None   # Open position — no realized P/L yet
            trade["pnl_usd"] = None
        elif action == "SELL" and price is not None:
            entry = entry_prices.get(ticker)
            if entry is not None and entry > 0:
                sell = float(price)
                trade["pnl_pct"] = (sell - entry) / entry  # realized return %
                trade["pnl_usd"] = sell - entry             # per-share $ gain/loss
            else:
                trade["pnl_pct"] = None
                trade["pnl_usd"] = None
        else:
            trade["pnl_pct"] = None
            trade["pnl_usd"] = None

    # Return sorted descending (newest first) for display
    trades_asc.sort(key=lambda t: str(t.get("date", "")), reverse=True)
    return _safe_response({"trades": trades_asc})

