"""
strategy_generator.py — LLM-powered strategy hypothesis generation using Anthropic Claude.

Generates Polars-based strategy code that follows the StrategyFn signature:
    def strategy_name(df: pl.DataFrame) -> pl.DataFrame

Supports three model tiers with cost tracking:
    - haiku:  claude-3-5-haiku-latest  (fast, cheap)
    - sonnet: claude-sonnet-4-20250514   (balanced)
    - opus:   claude-3-opus-latest    (highest quality)
"""

import ast
import os
import re
from dataclasses import dataclass
from collections import defaultdict

import anthropic
import polars as pl

from src.config import PROJECT_ROOT, INDICATOR_METADATA

# ── Model Configuration ─────────────────────────────────────
MODEL_TIERS = {
    "haiku": {
        "model_id": "claude-haiku-4-5-20251001",
        "label": "Haiku (Fast)",
        "input_cost_per_mtok": 1.00,    # $/M input tokens
        "output_cost_per_mtok": 5.00,   # $/M output tokens
    },
    "sonnet": {
        "model_id": "claude-sonnet-4-20250514",
        "label": "Sonnet (Balanced)",
        "input_cost_per_mtok": 3.00,
        "output_cost_per_mtok": 15.00,
    },
    "opus": {
        "model_id": "claude-opus-4-6",
        "label": "Opus (Premium)",
        "input_cost_per_mtok": 15.00,
        "output_cost_per_mtok": 75.00,
    },
}


@dataclass
class StrategyHypothesis:
    name: str
    rationale: str
    code: str
    model_tier: str
    input_tokens: int
    output_tokens: int
    cost_usd: float


# ═══════════════════════════════════════════════════════════════
# Dynamic Schema Discovery
# ═══════════════════════════════════════════════════════════════

def _build_dynamic_schema() -> str:
    """Read live parquet schemas and build categorized column context for the LLM.

    Uses pl.read_parquet_schema() which reads only file headers — zero data loaded.
    Cross-references INDICATOR_METADATA for semantic descriptions and categories.
    New columns added to any parquet are auto-discovered.
    """
    from src.core.duckdb_store import get_parquet_path

    columns = {}  # {name: (dtype_str, description, category)}

    for source in ["market_data", "feature", "macro", "fundamental"]:
        path = get_parquet_path(source)
        if os.path.exists(path):
            schema = pl.read_parquet_schema(path)
            for col_name, dtype in schema.items():
                if col_name not in columns:
                    meta = INDICATOR_METADATA.get(col_name, {})
                    category = meta.get("category", "other")
                    desc = meta.get("description", f"Numerical feature ({dtype}).")
                    columns[col_name] = (str(dtype), desc, category)

    # Group by category for organized output
    by_category = defaultdict(list)
    for name, (dtype, desc, category) in sorted(columns.items()):
        if category == "_internal":
            continue  # Skip entity_id, date — mentioned separately
        by_category[category].append(f"   - {name} ({dtype}): {desc}")

    # Build formatted output
    category_labels = {
        "market": "Market Data",
        "fundamental": "Fundamental",
        "statistical": "Statistical / Factor",
        "macro": "Macro",
        "other": "Other",
    }
    sections = []
    for cat_key in ["market", "fundamental", "statistical", "macro", "other"]:
        if cat_key in by_category:
            sections.append(f"   [{category_labels.get(cat_key, cat_key)}]")
            sections.extend(by_category[cat_key])

    return "\n".join(sections)


def _build_ticker_summary() -> str:
    """Build a truncated ticker summary: total count + small sample.

    Avoids injecting 3000+ tickers into the prompt at Russell 3000 scale.
    """
    from src.core.duckdb_store import PARQUET_DIR

    entity_map_path = os.path.join(PARQUET_DIR, "entity_map.parquet")
    if not os.path.exists(entity_map_path):
        # Fallback to config
        from src.config import DEFAULT_UNIVERSE
        tickers = DEFAULT_UNIVERSE
    else:
        emap = pl.read_parquet(entity_map_path)
        tickers = sorted(emap["ticker"].to_list()) if "ticker" in emap.columns else []

    if not tickers:
        return "Universe information unavailable."

    total = len(tickers)
    sample = tickers[:5]
    return (
        f"{total} stocks in the universe (e.g. {', '.join(sample)}, ...).\n"
        f"   You can use ticker for ticker-specific strategies, e.g.:\n"
        f'   pl.when(pl.col("ticker") == "AAPL").then(1.0).otherwise(0.0)'
    )


# Template with {dynamic_schema} and {ticker_summary} placeholders
_SYSTEM_PROMPT_TEMPLATE = """You are a quantitative strategist generating trading strategies for a backtesting platform.

You must generate a Python function that follows this EXACT signature:

```python
def strategy_name(df: pl.DataFrame) -> pl.DataFrame:
    # Your logic here
    return df.with_columns(...)
```

RULES:
1. The function takes a Polars DataFrame with these columns (auto-discovered from live data):
   - entity_id (int): unique stock identifier
   - ticker (str): stock ticker symbol
   - date (date): trading date
{dynamic_schema}

   Universe: {ticker_summary}

2. The function MUST add exactly ONE new column named `raw_weight_{{strategy_id}}` where strategy_id is a snake_case name
3. Weights should be float values. Positive = long, negative = short, 0 = no position
4. Only use `polars` (imported as `pl`) and `numpy` (imported as `np`) — no other imports
5. The function name must match the strategy_id in the column name
6. The function MUST return the FULL original DataFrame with the weight column ADDED (do NOT select a subset of columns)

CRITICAL POLARS API RULES (you MUST follow these exactly):
- `.fill_null(0)` or `.fill_null(value)` — use literal values, NOT pl.FillNullStrategy
- `.fill_null(strategy="forward")` — use string, NOT pl.FillNullStrategy.FORWARD
- For rolling operations per entity: `pl.col("x").rolling_mean(window_size=60).over("entity_id")` — rolling FIRST, then .over()
- For cross-sectional ranking: `pl.col("x").rank("ordinal").over("date").cast(pl.Float64)` — ALWAYS .cast(pl.Float64) after .rank()
- `.rank()` returns u32 (unsigned int) which CANNOT be negated or subtracted. You MUST cast to Float64 immediately after rank()
- `.count()` also returns u32. Cast to Float64 if using in arithmetic.
- Do NOT use `.over()` BEFORE rolling operations — always chain rolling FIRST, then .over()
- Use `pl.when(...).then(...).otherwise(...)` for conditionals
- Do NOT use `pl.FillNullStrategy` — it does not exist
- `.clip(lower, upper)` uses POSITIONAL args, NOT min_value/max_value kwargs. Example: `.clip(-3.0, 3.0)`
- Avoid `.select()` at the end — return the full df with the weight column added via `.with_columns()`
- NEVER divide by a value that could be zero or near-zero. Always clip the divisor: `/ pl.col("x").clip(0.01, None)` instead of `/ (pl.col("x") + 1e-6)`
- Use `.fill_null(0.0)` BEFORE arithmetic operations on columns that may have nulls
- T-1 LOOKAHEAD GUARDRAIL: All signal generation, trailing stops, trailing maximums, and price/volume filters MUST evaluate on strictly shifted T-1 data to make decisions for T0. Do NOT evaluate today's `adj_close`, `volume`, or calculated metrics mapped to today's row to trigger a trade today. Force a `.shift(1).over("entity_id")` on all price/volume references used in signal thresholds, stop-loss calculations, and conditionals.

RESPOND in this exact format:
STRATEGY_NAME: snake_case_name
RATIONALE: 1-2 sentence explanation of the hypothesis
CODE:
```python
def snake_case_name(df: pl.DataFrame) -> pl.DataFrame:
    ...
```"""


def _build_system_prompt() -> str:
    """Build the full system prompt with live schema and ticker data."""
    return _SYSTEM_PROMPT_TEMPLATE.format(
        dynamic_schema=_build_dynamic_schema(),
        ticker_summary=_build_ticker_summary(),
    )


def _build_data_profile_block() -> str:
    """Build the statistical profile reference block for the LLM.

    Kept separate from the core schema so the LLM treats it as a
    calibration reference (thresholds, ranges) rather than core rules.
    Uses JSON format for structured parsing by Claude.
    """
    try:
        from src.alpha_lab.stats_engine import build_profile_for_llm
        profile_json = build_profile_for_llm()
        if not profile_json:
            return ""
        return (
            "\n\n=== DYNAMIC DATA DICTIONARY & STATISTICAL PROFILE ===\n"
            "Use these statistics to calibrate your thresholds "
            "(e.g., setting Z-score cutoffs based on the actual distribution).\n"
            f"```json\n{profile_json}\n```"
        )
    except Exception:
        return ""  # Non-critical — degrade gracefully

# ── Style-specific addons ────────────────────────────────────
STYLE_ADDONS = {
    "academic": """

STRATEGY STYLE: Academic / Market-Neutral
- Build diversified, market-neutral long/short strategies
- Use cross-sectional ranking across the entire universe
- Assign weights to all stocks proportional to their signal strength
- Balance long and short sides for dollar neutrality
- Focus on Sharpe ratio and risk-adjusted returns over raw performance
""",
    "hedge_fund": """

STRATEGY STYLE: Hedge Fund / Concentrated Alpha
You MUST follow these hedge fund heuristics:

1. EXTREME CONCENTRATION: Do NOT assign weights to the entire universe.
   Rank stocks by your signal, but ONLY assign positive weights to the Top 10%
   (top decile). Set weight = 0 for everything else. Use:
   `pl.col("signal").rank("ordinal").over("date").cast(pl.Float64)` to get ranks,
   then `pl.col("signal").count().over("date").cast(pl.Float64)` for total count,
   and only assign weight when `rank >= count * 0.9` (top 10%).

2. REGIME-AWARE SHORTING: Do NOT build market-neutral long/short by default.
   Only allow negative weights (shorts) when a macro regime filter is triggered,
   such as SPY closing below its 200-day moving average:
   `pl.col("adj_close").filter(pl.col("ticker")=="SPY").rolling_mean(200).over("date")`.
   If SPY is above its 200-day MA, the strategy must be LONG-ONLY (weights 0.0 to 1.0).

3. MOMENTUM PRESERVATION: Do NOT trim winners just because their valuation
   gets expensive. If a stock has positive 60-day momentum AND is in the top decile,
   keep it. Only exit when momentum turns negative.

4. Weights should range from 0.0 to 1.0 in bull regimes (long-only),
   and can include shorts (-1.0 to 1.0) only in bear regimes.
""",
}


# ── Agent Swarm Prompts ───────────────────────────────────────

_RESEARCHER_SYS_PROMPT = """You are a Lead Quantitative Researcher at a top algorithmic trading firm.
Your job is to read the user's prompt and formulate a mathematical trading hypothesis based on the available data dictionary.

RULES:
1. Do NOT write any Python code. Write a clear, structured thesis in plain English.
2. Define clear entry conditions (e.g., "Buy when EV/Sales Z-score < -1.0").
3. Define signal combinations (e.g., Fundamental Value + Technical Momentum).
4. Outline the economic intuition behind the strategy.

AVAILABLE DATA DICTIONARY:
{dynamic_schema}

Universe: {ticker_summary}

Use these variables to propose a strategy idea.
"""

_RISK_MANAGER_SYS_PROMPT = """You are the Chief Risk Officer at a quantitative hedge fund.
Your job is to review the Quantitative Researcher's strategy hypothesis and enforce strict risk heuristics.

RULES:
1. Review the Researcher's logic.
2. Enforce these specific Hedge Fund / Academic Return constraints:
{style_addon}

3. Identify potential flaws like infinite hold periods, no stop losses, or excessive volatility exposure.
4. Output the FINALIZED strategy logic in plain English, incorporating the Researcher's alpha signals but strictly clamping it with your risk constraints. Make it explicitly sequential and clear.
5. Do NOT write any Python code.
"""

_DEVELOPER_SYS_PROMPT = """You are the Lead Quantitative Developer.
Your job is to translate the Risk Manager's finalized strategy rules into perfect Python Polars code.

You must generate a Python function that follows this EXACT signature:

```python
def strategy_name(df: pl.DataFrame) -> pl.DataFrame:
    # Your logic here
    return df.with_columns(...)
```

RULES:
1. The function takes a Polars DataFrame with these columns (auto-discovered from live data):
   - entity_id (int): unique stock identifier
   - ticker (str): stock ticker symbol
   - date (date): trading date
{dynamic_schema}

   Universe: {ticker_summary}

2. The function MUST add exactly ONE new column named `raw_weight_{{strategy_id}}` where strategy_id is a snake_case name
3. Weights should be float values. Positive = long, negative = short, 0 = no position
4. Only use `polars` (imported as `pl`) and `numpy` (imported as `np`) — no other imports
5. The function name must match the strategy_id in the column name
6. The function MUST return the FULL original DataFrame with the weight column ADDED (do NOT select a subset of columns)

CRITICAL POLARS API RULES (you MUST follow these exactly):
- `.fill_null(0)` or `.fill_null(value)` — use literal values, NOT pl.FillNullStrategy
- `.fill_null(strategy="forward")` — use string, NOT pl.FillNullStrategy.FORWARD
- For rolling operations per entity: `pl.col("x").rolling_mean(window_size=60).over("entity_id")` — rolling FIRST, then .over()
- For cross-sectional ranking: `pl.col("x").rank("ordinal").over("date").cast(pl.Float64)` — ALWAYS .cast(pl.Float64) after .rank()
- `.rank()` returns u32 (unsigned int) which CANNOT be negated or subtracted. You MUST cast to Float64 immediately after rank()
- `.count()` also returns u32. Cast to Float64 if using in arithmetic.
- Do NOT use `.over()` BEFORE rolling operations — always chain rolling FIRST, then .over()
- Use `pl.when(...).then(...).otherwise(...)` for conditionals
- Do NOT use `pl.FillNullStrategy` — it does not exist
- `.clip(lower, upper)` uses POSITIONAL args, NOT min_value/max_value kwargs. Example: `.clip(-3.0, 3.0)`
- Avoid `.select()` at the end — return the full df with the weight column added via `.with_columns()`
- NEVER divide by a value that could be zero or near-zero. Always clip the divisor: `/ pl.col("x").clip(0.01, None)` instead of `/ (pl.col("x") + 1e-6)`
- Use `.fill_null(0.0)` BEFORE arithmetic operations on columns that may have nulls
- T-1 LOOKAHEAD GUARDRAIL: All signal generation, trailing stops, trailing maximums, and price/volume filters MUST evaluate on strictly shifted T-1 data to make decisions for T0. Do NOT evaluate today's `adj_close`, `volume`, or calculated metrics mapped to today's row to trigger a trade today. Force a `.shift(1).over("entity_id")` on all price/volume references used in signal thresholds, stop-loss calculations, and conditionals.

RESPOND in this exact format:
STRATEGY_NAME: snake_case_name
RATIONALE: 1-2 sentence explanation of the final strategy
CODE:
```python
def snake_case_name(df: pl.DataFrame) -> pl.DataFrame:
    ...
```
"""

def generate_strategy_swarm(
    prompt: str = "",
    model_tier: str = "sonnet",
    strategy_style: str = "academic",
) -> StrategyHypothesis:
    """Generate a strategy hypothesis using Anthropic Claude via an Agent Swarm.

    Follows a 3-agent pipeline:
      1. Quantitative Researcher (Idea Generation)
      2. Risk Manager (Constraint Enforcement)
      3. Quantitative Developer (Polars Implementation)
    """
    if model_tier not in MODEL_TIERS:
        raise ValueError(f"Unknown model tier: {model_tier}. Use: {list(MODEL_TIERS.keys())}")
    if strategy_style not in STYLE_ADDONS:
        strategy_style = "academic"

    tier = MODEL_TIERS[model_tier]
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in .env.local")

    client = anthropic.Anthropic(api_key=api_key)

    user_msg = prompt if prompt else (
        "Generate an innovative trading strategy that combines at least two different "
        "signals (technical, fundamental, or statistical) in a novel way. "
        "Focus on strategies with clear economic intuition."
    )

    # ── AGENT 1: Quantitative Researcher ──
    researcher_sys = _RESEARCHER_SYS_PROMPT.format(
        dynamic_schema=_build_dynamic_schema(),
        ticker_summary=_build_ticker_summary()
    ) + _build_data_profile_block()

    resp_researcher = client.messages.create(
        model=tier["model_id"],
        max_tokens=2000,
        system=researcher_sys,
        messages=[{"role": "user", "content": user_msg}],
    )
    researcher_text = resp_researcher.content[0].text

    # ── AGENT 2: Risk Manager ──
    risk_sys = _RISK_MANAGER_SYS_PROMPT.format(
        style_addon=STYLE_ADDONS.get(strategy_style, STYLE_ADDONS["academic"])
    )

    resp_risk = client.messages.create(
        model=tier["model_id"],
        max_tokens=2000,
        system=risk_sys,
        messages=[
            {"role": "user", "content": f"Original prompt: {user_msg}"},
            {"role": "assistant", "content": researcher_text},
            {"role": "user", "content": "Please review this strategy and apply our risk management layers. Output the finalized logic."}
        ],
    )
    risk_text = resp_risk.content[0].text

    # ── AGENT 3: Quantitative Developer ──
    dev_sys = _DEVELOPER_SYS_PROMPT.format(
        dynamic_schema=_build_dynamic_schema(),
        ticker_summary=_build_ticker_summary()
    )

    resp_dev = client.messages.create(
        model=tier["model_id"],
        max_tokens=4000,
        system=dev_sys,
        messages=[
            {"role": "user", "content": f"Please implement the following finalized strategy logic into Polars code:\n\n{risk_text}"}
        ],
    )
    dev_text = resp_dev.content[0].text

    # Calculate total cost across the swarm
    input_tokens = (
        resp_researcher.usage.input_tokens +
        resp_risk.usage.input_tokens +
        resp_dev.usage.input_tokens
    )
    output_tokens = (
        resp_researcher.usage.output_tokens +
        resp_risk.usage.output_tokens +
        resp_dev.usage.output_tokens
    )
    cost_usd = (
        (input_tokens / 1_000_000) * tier["input_cost_per_mtok"]
        + (output_tokens / 1_000_000) * tier["output_cost_per_mtok"]
    )

    # Parse response
    name, dev_rationale, code = _parse_response(dev_text)

    combined_rationale = (
        f"**Researcher:** Proposed concept based on '{user_msg}'.\n"
        f"**Risk Manager:** Applied '{strategy_style}' heuristics.\n"
        f"**Final Rationale:** {dev_rationale}"
    )

    return StrategyHypothesis(
        name=name,
        rationale=combined_rationale,
        code=code,
        model_tier=model_tier,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=round(cost_usd, 6),
    )


import json as _json

def generate_strategy_swarm_stream(
    prompt: str = "",
    strategy_style: str = "academic",
    agent_tiers: dict[str, str] = None,
    agent_notes: dict[str, str] = None,
):
    """Synchronous generator version of generate_strategy_swarm.

    Yields SSE-formatted strings (data: <json>\\n\\n) after each agent step
    so the caller can stream them to the frontend via StreamingResponse.

    Event types emitted:
      {"type": "start",    "agent": ..., "label": ...}
      {"type": "done",     "agent": ..., "label": ..., "tokens": ..., "preview": ...}
      {"type": "result",   "name": ..., "rationale": ..., "cost_usd": ...}
      {"type": "error",    "message": ...}
    """
    if agent_tiers is None:
        agent_tiers = {"researcher": "haiku", "risk_manager": "haiku", "developer": "sonnet"}
    if agent_notes is None:
        agent_notes = {}

    def _sse(payload: dict) -> str:
        return f"data: {_json.dumps(payload)}\n\n"
    if strategy_style not in STYLE_ADDONS:
        strategy_style = "academic"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        yield _sse({"type": "error", "message": "ANTHROPIC_API_KEY not set"})
        return

    client = anthropic.Anthropic(api_key=api_key)

    user_msg = prompt if prompt else (
        "Generate an innovative trading strategy that combines at least two different "
        "signals (technical, fundamental, or statistical) in a novel way. "
        "Focus on strategies with clear economic intuition."
    )

    try:
        # ── AGENT 1: Researcher ──
        yield _sse({"type": "start", "agent": "researcher", "label": "🔬 Researcher is formulating hypothesis…"})
        r_tier_key = agent_tiers.get("researcher", "haiku")
        r_tier = MODEL_TIERS.get(r_tier_key, MODEL_TIERS["haiku"])
        r_note = agent_notes.get("researcher", "").strip()
        
        researcher_sys = _RESEARCHER_SYS_PROMPT.format(
            dynamic_schema=_build_dynamic_schema(),
            ticker_summary=_build_ticker_summary()
        ) + _build_data_profile_block()
        if r_note:
            researcher_sys += f"\n\nAdditional Instructions:\n{r_note}"

        resp_researcher = client.messages.create(
            model=r_tier["model_id"], max_tokens=2000,
            system=researcher_sys,
            messages=[{"role": "user", "content": user_msg}],
        )
        researcher_text = resp_researcher.content[0].text
        yield _sse({
            "type": "done", "agent": "researcher",
            "label": "✅ Researcher complete",
            "tokens": resp_researcher.usage.output_tokens,
            "preview": researcher_text[:300].strip(),
        })

        # ── AGENT 2: Risk Manager ──
        yield _sse({"type": "start", "agent": "risk_manager", "label": "🛡️ Risk Manager reviewing & applying constraints…"})
        rm_tier_key = agent_tiers.get("risk_manager", "haiku")
        rm_tier = MODEL_TIERS.get(rm_tier_key, MODEL_TIERS["haiku"])
        rm_note = agent_notes.get("risk_manager", "").strip()

        risk_sys = _RISK_MANAGER_SYS_PROMPT.format(
            style_addon=STYLE_ADDONS.get(strategy_style, STYLE_ADDONS["academic"])
        )
        if rm_note:
            risk_sys += f"\n\nAdditional Instructions:\n{rm_note}"

        resp_risk = client.messages.create(
            model=rm_tier["model_id"], max_tokens=2000,
            system=risk_sys,
            messages=[
                {"role": "user", "content": f"Original prompt: {user_msg}"},
                {"role": "assistant", "content": researcher_text},
                {"role": "user", "content": "Please review this strategy and apply our risk management layers. Output the finalized logic."},
            ],
        )
        risk_text = resp_risk.content[0].text
        yield _sse({
            "type": "done", "agent": "risk_manager",
            "label": "✅ Risk Manager complete",
            "tokens": resp_risk.usage.output_tokens,
            "preview": risk_text[:300].strip(),
        })

        # ── AGENT 3: Developer ──
        yield _sse({"type": "start", "agent": "developer", "label": "💻 Developer writing Polars implementation…"})
        d_tier_key = agent_tiers.get("developer", "sonnet")
        d_tier = MODEL_TIERS.get(d_tier_key, MODEL_TIERS["sonnet"])
        d_note = agent_notes.get("developer", "").strip()

        dev_sys = _DEVELOPER_SYS_PROMPT.format(
            dynamic_schema=_build_dynamic_schema(),
            ticker_summary=_build_ticker_summary()
        )
        if d_note:
            dev_sys += f"\n\nAdditional Instructions:\n{d_note}"

        resp_dev = client.messages.create(
            model=d_tier["model_id"], max_tokens=4000,
            system=dev_sys,
            messages=[{"role": "user", "content": f"Please implement the following finalized strategy logic into Polars code:\n\n{risk_text}"}],
        )
        dev_text = resp_dev.content[0].text
        yield _sse({
            "type": "done", "agent": "developer",
            "label": "✅ Developer complete",
            "tokens": resp_dev.usage.output_tokens,
            "preview": dev_text[:300].strip(),
        })

        # ── Parse & finalize ──
        input_tokens = (
            resp_researcher.usage.input_tokens +
            resp_risk.usage.input_tokens +
            resp_dev.usage.input_tokens
        )
        output_tokens = (
            resp_researcher.usage.output_tokens +
            resp_risk.usage.output_tokens +
            resp_dev.usage.output_tokens
        )
        cost_usd = (
            (resp_researcher.usage.input_tokens / 1_000_000) * r_tier["input_cost_per_mtok"] +
            (resp_researcher.usage.output_tokens / 1_000_000) * r_tier["output_cost_per_mtok"] +
            (resp_risk.usage.input_tokens / 1_000_000) * rm_tier["input_cost_per_mtok"] +
            (resp_risk.usage.output_tokens / 1_000_000) * rm_tier["output_cost_per_mtok"] +
            (resp_dev.usage.input_tokens / 1_000_000) * d_tier["input_cost_per_mtok"] +
            (resp_dev.usage.output_tokens / 1_000_000) * d_tier["output_cost_per_mtok"]
        )

        name, dev_rationale, code = _parse_response(dev_text)
        combined_rationale = (
            f"**Researcher:** Proposed concept based on '{user_msg}'.\n"
            f"**Risk Manager:** Applied '{strategy_style}' heuristics.\n"
            f"**Final Rationale:** {dev_rationale}"
        )

        hypothesis = StrategyHypothesis(
            name=name,
            rationale=combined_rationale,
            code=code,
            model_tier=f"{r_tier_key[:3]}-{rm_tier_key[:3]}-{d_tier_key[:3]}",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=round(cost_usd, 6),
        )


        yield _sse({
            "type": "result",
            "name": hypothesis.name,
            "rationale": hypothesis.rationale,
            "code": hypothesis.code,
            "model_tier": hypothesis.model_tier,
            "input_tokens": hypothesis.input_tokens,
            "output_tokens": hypothesis.output_tokens,
            "cost_usd": hypothesis.cost_usd,
        })

    except Exception as e:
        yield _sse({"type": "error", "message": f"{type(e).__name__}: {e}"})


def _parse_response(text: str) -> tuple[str, str, str]:
    """Parse the structured LLM response into (name, rationale, code)."""
    name = "unnamed_strategy"
    rationale = ""
    code = ""

    # Extract strategy name
    name_match = re.search(r"STRATEGY_NAME:\s*(.+)", text)
    if name_match:
        name = name_match.group(1).strip().lower().replace(" ", "_")

    # Extract rationale
    rationale_match = re.search(r"RATIONALE:\s*(.+?)(?=CODE:|```)", text, re.DOTALL)
    if rationale_match:
        rationale = rationale_match.group(1).strip()

    # Extract code block — try multiple fenced patterns
    for pattern in [
        r"```python\s*\n(.*?)```",    # ```python
        r"```py\s*\n(.*?)```",        # ```py
        r"```\s*\n(.*?)```",          # bare ```
    ]:
        code_match = re.search(pattern, text, re.DOTALL)
        if code_match:
            code = code_match.group(1).strip()
            break

    # Fallback: find raw function definition (no code fences)
    if not code:
        func_match = re.search(
            r"(def\s+\w+\s*\(df:\s*pl\.DataFrame\).*?)(?=\n\S|\Z)",
            text,
            re.DOTALL,
        )
        if func_match:
            code = func_match.group(1).strip()

    # If still no code, raise so the user gets a clear error
    if not code:
        raise ValueError(
            f"LLM did not return valid strategy code. Raw response:\n{text[:500]}"
        )

    # Apply strict AST-level guardrails to catch lookahead bias
    _enforce_ast_guardrails(code)

    return name, rationale, code


def _enforce_ast_guardrails(code: str) -> None:
    """Parse the generated code and look for illegal patterns such as lookahead bias.

    Guardrails enforced:
      G1. fill_null(strategy='backward')  — future lookahead via backward fill
      G2. .shift(-N) with negative N      — peeking at future rows
      G3. .pct_change(-N) with negative N — future return calculation
      G4. Fundamental columns used without filing_date staleness guard
          (filing_date must appear in code when any fundamental col is referenced)
    """
    try:
        tree = ast.parse(code)
    except Exception as e:
        # If it doesn't parse, let the execution fail naturally later,
        # or we could reject it here. We'll reject invalid syntax.
        raise ValueError(f"LLM generated invalid Python syntax: {e}")

    # ── Catalogue of fundamental columns that require staleness guards ──────
    FUNDAMENTAL_COLS = {
        "dcf_npv_gap", "ev_sales_zscore", "pe_ratio", "pb_ratio", "ps_ratio",
        "ebit", "ebitda", "net_income", "free_cash_flow", "total_debt",
        "total_equity", "revenue", "gross_margin", "operating_margin",
        "net_margin", "roe", "roa", "roic", "current_ratio", "quick_ratio",
        "debt_to_equity", "earnings_yield", "book_value", "enterprise_value",
        "ev_ebitda", "peg_ratio", "dividend_yield", "payout_ratio",
        "revenue_growth", "earnings_growth", "asset_turnover",
        "filing_date",  # itself — must be referenced alongside the guard
    }

    class GuardrailVisitor(ast.NodeVisitor):
        def __init__(self):
            self.referenced_fundamentals: set[str] = set()
            self.has_staleness_guard: bool = False  # filing_date check present

        def visit_Call(self, node):
            # ── G1: fill_null(strategy='backward') ─────────────────────────
            if isinstance(node.func, ast.Attribute) and node.func.attr == "fill_null":
                for kw in node.keywords:
                    if kw.arg == "strategy":
                        val = None
                        if isinstance(kw.value, ast.Constant):
                            val = kw.value.value
                        elif isinstance(kw.value, ast.Str):
                            val = kw.value.s
                        if val == "backward":
                            raise ValueError(
                                "AST Guardrail Violation [G1]: fill_null(strategy='backward') "
                                "is strictly prohibited — introduces future lookahead bias."
                            )

            # ── G2: .shift(-N) lookahead ────────────────────────────────────
            if isinstance(node.func, ast.Attribute) and node.func.attr == "shift":
                if node.args:
                    arg = node.args[0]
                    # shift(-N) as direct negative literal
                    if isinstance(arg, ast.UnaryOp) and isinstance(arg.op, ast.USub):
                        if isinstance(arg.operand, ast.Constant) and isinstance(arg.operand.value, (int, float)):
                            raise ValueError(
                                f"AST Guardrail Violation [G2]: .shift({-arg.operand.value}) "
                                "uses a negative offset — this peeks at future rows (lookahead bias). "
                                "Use .shift(1) or positive offsets only."
                            )
                for kw in node.keywords:
                    if kw.arg in ("n", "periods"):
                        if isinstance(kw.value, ast.UnaryOp) and isinstance(kw.value.op, ast.USub):
                            raise ValueError(
                                "AST Guardrail Violation [G2]: .shift(n=negative) "
                                "peeks at future rows (lookahead bias). Use positive offsets only."
                            )

            # ── G3: .pct_change(-N) lookahead ──────────────────────────────
            if isinstance(node.func, ast.Attribute) and node.func.attr == "pct_change":
                if node.args:
                    arg = node.args[0]
                    if isinstance(arg, ast.UnaryOp) and isinstance(arg.op, ast.USub):
                        if isinstance(arg.operand, ast.Constant) and isinstance(arg.operand.value, (int, float)):
                            raise ValueError(
                                f"AST Guardrail Violation [G3]: .pct_change({-arg.operand.value}) "
                                "computes future returns (lookahead bias). "
                                "Use positive period arguments only."
                            )
                for kw in node.keywords:
                    if kw.arg == "n":
                        if isinstance(kw.value, ast.UnaryOp) and isinstance(kw.value.op, ast.USub):
                            raise ValueError(
                                "AST Guardrail Violation [G3]: .pct_change(n=negative) "
                                "computes future returns (lookahead bias). Use positive n only."
                            )

            self.generic_visit(node)

        def visit_Constant(self, node):
            # ── G4: Track fundamental column string references ───────────────
            if isinstance(node.value, str) and node.value in FUNDAMENTAL_COLS:
                if node.value != "filing_date":
                    self.referenced_fundamentals.add(node.value)
                else:
                    self.has_staleness_guard = True
            self.generic_visit(node)

    visitor = GuardrailVisitor()
    visitor.visit(tree)

    # ── G4 post-visit: fundamental usage without staleness guard ────────────
    if visitor.referenced_fundamentals and not visitor.has_staleness_guard:
        cols = ", ".join(sorted(visitor.referenced_fundamentals))
        raise ValueError(
            f"AST Guardrail Violation [G4]: Strategy references fundamental column(s) "
            f"[{cols}] without a filing_date staleness guard."
        )


def get_tier_info() -> dict:
    """Return model tier info for the frontend."""
    return {
        tier_id: {
            "label": tier["label"],
            "input_cost_per_mtok": tier["input_cost_per_mtok"],
            "output_cost_per_mtok": tier["output_cost_per_mtok"],
        }
        for tier_id, tier in MODEL_TIERS.items()
    }


# ── Combine / Evolve Strategies ──────────────────────────────

COMBINE_STYLE_ADDON = """

STRATEGY STYLE: Evolutionary Combination
You are being given code from multiple top-performing strategies. Your task is to:
1. Analyze the signals and logic used in each parent strategy
2. Combine the BEST elements from each into a SINGLE new strategy
3. Introduce at least ONE novel mutation or improvement (e.g. new window size, different weighting, additional signal)
4. The new strategy must be meaningfully different from any one parent — not a trivial copy
5. Aim to reduce max drawdown while maintaining or improving Sharpe ratio
6. Name the strategy to reflect its combined heritage (e.g. 'momentum_value_hybrid')
"""


def combine_strategies(
    strategy_codes: list[str],
    strategy_names: list[str],
    model_tier: str = "sonnet",
    user_guidance: str = "",
) -> StrategyHypothesis:
    """Combine multiple passed strategies into a new evolved strategy.

    This is a manual version of genetic prompting — the user selects
    top strategies and the LLM creates a novel combination.
    """
    if model_tier not in MODEL_TIERS:
        raise ValueError(f"Unknown model tier: {model_tier}. Use: {list(MODEL_TIERS.keys())}")

    tier = MODEL_TIERS[model_tier]
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in .env.local")

    client = anthropic.Anthropic(api_key=api_key)

    # Build the user message with parent strategy code
    parents_section = ""
    for i, (name, code) in enumerate(zip(strategy_names, strategy_codes), 1):
        parents_section += f"\n--- PARENT STRATEGY {i}: {name} ---\n```python\n{code}\n```\n\n"

    user_msg = (
        f"Here are {len(strategy_codes)} of our best-performing strategies:\n"
        f"{parents_section}\n"
        f"Combine the best elements from these strategies into ONE new, evolved strategy. "
        f"Introduce at least one novel mutation or improvement.\n"
    )
    if user_guidance:
        user_msg += f"\nAdditional guidance: {user_guidance}\n"

    response = client.messages.create(
        model=tier["model_id"],
        max_tokens=4000,
        system=_build_system_prompt() + COMBINE_STYLE_ADDON + _build_data_profile_block(),
        messages=[{"role": "user", "content": user_msg}],
    )

    text = response.content[0].text
    input_tokens = response.usage.input_tokens
    output_tokens = response.usage.output_tokens
    cost_usd = (
        (input_tokens / 1_000_000) * tier["input_cost_per_mtok"]
        + (output_tokens / 1_000_000) * tier["output_cost_per_mtok"]
    )

    name, rationale, code = _parse_response(text)

    return StrategyHypothesis(
        name=name,
        rationale=f"[Combined from: {', '.join(strategy_names)}] {rationale}",
        code=code,
        model_tier=model_tier,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=round(cost_usd, 6),
    )
