"""
src/arena/schemas.py — Inter-Agent Communication Schemas (Plan 1)

Production-grade Pydantic V2 models that form the strict communication protocol
between every agent in the Hedge Fund Swarm. These schemas guarantee type-safe,
hallucination-proof inter-agent payloads.

Key guarantees:
  - StrategyKey Literal type rejects any hallucinated strategy names at deserialization
  - CommanderDirective enforces cash_reserve_pct >= 5%
  - PMDecision.confirmation must be "LOCKED" (forces explicit acknowledgment)
  - TokenUsage is embedded in every agent output for cost tracking
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


# ═══════════════════════════════════════════════════════════════
# STRATEGY KEY — Hallucination Guard
# ═══════════════════════════════════════════════════════════════

# Derived from STRATEGY_REGISTRY keys in src/ecs/strategy_registry.py
# EXCLUDES buy_hold (benchmark) and ev_sales (superseded by ls_zscore)
StrategyKey = Literal[
    "ls_zscore",
    "fortress",
    "low_beta",
    "xgboost",
    "dcf_value",
    "momentum",
    "pullback_rsi",
    "sma_crossover",
    "macro_regime",
    "macro_v2",
]

# Human-readable descriptions for Strategist prompt injection
STRATEGY_DESCRIPTIONS: dict[str, str] = {
    "ls_zscore":     "Long cheapest 2, short expensive 2 by EV/Sales z-score",
    "fortress":      "Top 10% by net cash (cash minus debt)",
    "low_beta":      "Bottom 20% by beta_spy (low-volatility anomaly)",
    "xgboost":       "ML-based factor model using XGBoost predictions",
    "dcf_value":     "Top 5 by DCF NPV gap (most undervalued)",
    "momentum":      "Top 20% by 6-month trailing return",
    "pullback_rsi":  "Buy when RSI(3) < 20 AND price > SMA(200)",
    "sma_crossover": "Buy when SMA(50) > SMA(200) (golden cross)",
    "macro_regime":  "VIX-regime-scaled equal weight",
    "macro_v2":      "VIX term structure (contango=risk-on, backwardation=risk-off)",
}

ALL_STRATEGY_KEYS: list[str] = list(STRATEGY_DESCRIPTIONS.keys())


# ═══════════════════════════════════════════════════════════════
# TOKEN USAGE — Embedded in every agent output
# ═══════════════════════════════════════════════════════════════

class TokenUsage(BaseModel):
    """Track LLM API token consumption per agent call."""
    input_tokens: int = 0
    output_tokens: int = 0
    model_id: str = ""
    estimated_cost_usd: float = 0.0
    agent_name: str = ""

    def add(self, other: "TokenUsage") -> "TokenUsage":
        """Combine two TokenUsage records (for retry accumulation)."""
        return TokenUsage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            model_id=self.model_id or other.model_id,
            estimated_cost_usd=self.estimated_cost_usd + other.estimated_cost_usd,
            agent_name=self.agent_name or other.agent_name,
        )


# ═══════════════════════════════════════════════════════════════
# C-SUITE CONSULTANT BRIEFS
# ═══════════════════════════════════════════════════════════════

class MacroBrief(BaseModel):
    """Market Consultant output. Source: GET /api/risk/summary."""
    agent_name: str = "market_consultant"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    macro_regime: Literal["Risk-On", "Risk-Off", "Caution"]
    vix_level: float = Field(ge=0.0)
    ten_year_yield: float
    risk_assessment: str = Field(max_length=500)
    token_cost: TokenUsage = Field(default_factory=TokenUsage)


class AuditBrief(BaseModel):
    """Data Integrity Auditor output. Source: GET /api/diagnostics/pipeline-coverage."""
    agent_name: str = "auditor"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data_freshness_ok: bool
    stale_tickers: list[str] = Field(default_factory=list)
    risk_breaches: list[str] = Field(default_factory=list)
    overall_health: Literal["HEALTHY", "WARNING", "CRITICAL"]
    token_cost: TokenUsage = Field(default_factory=TokenUsage)


class ScoutBrief(BaseModel):
    """Competitive Intelligence Scout output. Source: GET /api/traders."""
    agent_name: str = "scout"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    competitor_count: int = Field(ge=0)
    top_threat: dict = Field(default_factory=dict)  # {name, sharpe, strategy}
    avg_sharpe: float
    strategic_summary: str = Field(max_length=500)
    token_cost: TokenUsage = Field(default_factory=TokenUsage)


# ═══════════════════════════════════════════════════════════════
# COMMANDER DIRECTIVE — CEO → Trading Desks
# ═══════════════════════════════════════════════════════════════

class DeskAllocation(BaseModel):
    """Capital allocation for one trading desk."""
    desk_id: int = Field(ge=1, le=3)
    tickers: list[str] = Field(min_length=1, max_length=20)
    capital_budget_usd: float = Field(gt=0)
    strategic_directive: str = Field(max_length=500)
    risk_tolerance: Literal["conservative", "moderate", "aggressive"]


class CommanderDirective(BaseModel):
    """Commander (CEO) output — allocates budgets and directives to 3 desks."""
    commander_reasoning: str = Field(max_length=800)
    desk_allocations: list[DeskAllocation] = Field(min_length=1, max_length=3)
    total_deployed_pct: float = Field(ge=0.0, le=1.0)
    cash_reserve_pct: float = Field(ge=0.05)  # Must keep ≥5% cash
    token_cost: TokenUsage = Field(default_factory=TokenUsage)

    @model_validator(mode="after")
    def validate_allocations(self) -> "CommanderDirective":
        """Enforce that deployed + reserve <= 100%."""
        if self.total_deployed_pct + self.cash_reserve_pct > 1.01:  # 1% tolerance
            raise ValueError(
                f"total_deployed_pct ({self.total_deployed_pct:.2f}) + "
                f"cash_reserve_pct ({self.cash_reserve_pct:.2f}) > 1.0"
            )
        return self


# ═══════════════════════════════════════════════════════════════
# DESK-LEVEL SCHEMAS — Analyst → Strategist → PM
# ═══════════════════════════════════════════════════════════════

class AnalystInsight(BaseModel):
    """Research Analyst output — data summary for Strategist consumption."""
    desk_id: int = Field(ge=1, le=3)
    tickers_analyzed: list[str]
    key_findings: str = Field(max_length=600)
    bullish_tickers: list[str] = Field(default_factory=list)
    bearish_tickers: list[str] = Field(default_factory=list)
    token_cost: TokenUsage = Field(default_factory=TokenUsage)


class StrategistRecommendation(BaseModel):
    """Strategist output — strategy recommendation for PM.
    
    The `recommended_strategy` field is typed as StrategyKey (Literal),
    which is the primary hallucination guard — any invalid strategy name
    causes a Pydantic ValidationError at deserialization.
    """
    desk_id: int = Field(ge=1, le=3)
    recommended_strategy: StrategyKey  # THIS is the hallucination guard
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(max_length=400)
    alternative_strategy: StrategyKey | None = None
    token_cost: TokenUsage = Field(default_factory=TokenUsage)


class PMDecision(BaseModel):
    """Portfolio Manager output — locks the strategy and capital allocation.
    
    confirmation must be "LOCKED" — this forces explicit acknowledgment
    of the irreversible nature of the PM's daily decision.
    """
    desk_id: int = Field(ge=1, le=3)
    portfolio_id: int
    strategy_id: StrategyKey  # Validated against registry
    allocated_capital: float = Field(gt=0)
    confirmation: Literal["LOCKED"]  # Forces explicit acknowledgment
    token_cost: TokenUsage = Field(default_factory=TokenUsage)


# ═══════════════════════════════════════════════════════════════
# AGGREGATED RESULT — Full Daily Tick Output
# ═══════════════════════════════════════════════════════════════

class DailyTickResult(BaseModel):
    """Aggregated output of one complete daily game loop."""
    tick_date: date
    trader_id: int
    commander_directive: CommanderDirective
    desk_results: list[PMDecision]
    total_token_cost: TokenUsage
    api_cost_deducted_usd: float
    elapsed_seconds: float
    macro_brief: MacroBrief | None = None
    audit_brief: AuditBrief | None = None
    scout_brief: ScoutBrief | None = None
