# Plan 1: Inter-Agent Communication Schemas (`src/arena/schemas.py`)

## Objective
Define production-grade Pydantic V2 models that form the strict communication protocol between every agent in the Hedge Fund Swarm. These schemas guarantee type-safe, hallucination-proof inter-agent payloads. The Strategist cannot hallucinate a strategy name (enforced via `Literal`), and the PM cannot malformat the API payload (enforced via Pydantic validators).

## Architecture Context

```
┌──────────────────────────────────────────────────────────────────┐
│                    STATE FLOW (DAG)                               │
│                                                                  │
│  Morning Ingestion (API reads)                                   │
│       ├── Market Consultant → MacroBrief                         │
│       ├── Auditor → AuditBrief                                   │
│       └── Scout → ScoutBrief                                     │
│             ↓                                                    │
│       Commander → CommanderDirective (per-desk budgets)          │
│             ↓                                                    │
│  ┌──── Desk 1 ─────┐  ┌──── Desk 2 ─────┐  ┌──── Desk 3 ─────┐│
│  │ Analyst→Insight  │  │ Analyst→Insight  │  │ Analyst→Insight  ││
│  │ Strategist→Rec   │  │ Strategist→Rec   │  │ Strategist→Rec   ││
│  │ PM→PMDecision    │  │ PM→PMDecision    │  │ PM→PMDecision    ││
│  └──────────────────┘  └──────────────────┘  └──────────────────┘│
│             ↓                                                    │
│  Back Office (deterministic Python):                             │
│    ingestion_system → risk_system → Accountant                   │
└──────────────────────────────────────────────────────────────────┘
```

## Files to Create

### `src/arena/__init__.py`
Empty package init.

### `src/arena/schemas.py`

#### 1. Strategy Enum / Literal Type

```python
from typing import Literal

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
```

**Why `Literal` over `Enum`:** LLM structured outputs produce raw strings. `Literal` validates at deserialization without requiring `.value` extraction. Pydantic V2 natively validates `Literal` fields.

#### 2. Consultant Briefs (C-Suite Input Layer)

| Model | Source Endpoint | Key Fields |
|-------|----------------|------------|
| `MacroBrief` | `GET /api/risk/summary` | `macro_regime`, `vix_level`, `ten_year_yield`, `risk_assessment` |
| `AuditBrief` | `GET /api/diagnostics/pipeline-coverage` | `data_freshness_ok`, `stale_tickers[]`, `risk_breaches[]`, `overall_health` |
| `ScoutBrief` | `GET /api/traders` | `competitor_count`, `top_threat`, `avg_sharpe`, `strategic_summary` |

Each brief includes:
- `agent_name: str` — identity tag
- `timestamp: datetime` — when brief was generated
- `token_cost: TokenUsage` — embedded token tracking

#### 3. TokenUsage (embedded in every agent output)

```python
class TokenUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    model_id: str = ""
    estimated_cost_usd: float = 0.0
```

#### 4. CommanderDirective (CEO → Desks)

```python
class DeskAllocation(BaseModel):
    desk_id: int = Field(ge=1, le=3)
    tickers: list[str] = Field(min_length=1, max_length=20)
    capital_budget_usd: float = Field(gt=0)
    strategic_directive: str = Field(max_length=500)
    risk_tolerance: Literal["conservative", "moderate", "aggressive"]

class CommanderDirective(BaseModel):
    commander_reasoning: str = Field(max_length=800)
    desk_allocations: list[DeskAllocation] = Field(min_length=1, max_length=3)
    total_deployed_pct: float = Field(ge=0, le=1.0)
    cash_reserve_pct: float = Field(ge=0.05)  # Must keep ≥5% cash
    token_cost: TokenUsage
```

**Critical validator:** `total_deployed_pct + cash_reserve_pct <= 1.0`

#### 5. StrategistRecommendation (Strategist → PM)

```python
class StrategistRecommendation(BaseModel):
    desk_id: int
    recommended_strategy: StrategyKey  # THIS is the hallucination guard
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(max_length=400)
    alternative_strategy: StrategyKey | None = None
    token_cost: TokenUsage
```

#### 6. PMDecision (PM → API `PUT /api/portfolios/{id}/strategy`)

```python
class PMDecision(BaseModel):
    desk_id: int
    portfolio_id: int
    strategy_id: StrategyKey  # Validated against registry
    allocated_capital: float = Field(gt=0)
    confirmation: Literal["LOCKED"]  # Forces explicit acknowledgment
    token_cost: TokenUsage
```

#### 7. AnalystInsight (Research Analyst → Strategist)

```python
class AnalystInsight(BaseModel):
    desk_id: int
    tickers_analyzed: list[str]
    key_findings: str = Field(max_length=600)
    bullish_tickers: list[str] = Field(default_factory=list)
    bearish_tickers: list[str] = Field(default_factory=list)
    token_cost: TokenUsage
```

#### 8. DailyTickResult (aggregated output)

```python
class DailyTickResult(BaseModel):
    tick_date: date
    trader_id: int
    commander_directive: CommanderDirective
    desk_results: list[PMDecision]
    total_token_cost: TokenUsage
    api_cost_deducted_usd: float
    elapsed_seconds: float
```

## Validation Rules (Pydantic `model_validator`)

1. **Commander budget conservation**: `sum(desk.capital_budget_usd) <= total_capital * total_deployed_pct`
2. **Cash buffer enforcement**: `cash_reserve_pct >= 0.05` (matches `CASH_BUFFER` in `src/config.py`)
3. **Strategy registry validation**: `StrategyKey` is derived at module load from `STRATEGY_REGISTRY.keys()` — if a custom strategy is loaded via `discover_custom_strategies()`, the Literal type won't include it. **Decision:** Keep static Literal for core strategies; custom strategies are NOT available to LLM agents (they're Alpha Lab outputs, not swarm inputs).

## Integration Points

| Schema | Consumed By | Produced By |
|--------|------------|-------------|
| `MacroBrief` | Commander | Market Consultant agent |
| `AuditBrief` | Commander | Auditor agent |
| `ScoutBrief` | Commander | Scout agent |
| `CommanderDirective` | 3x Trading Desks | Commander agent |
| `AnalystInsight` | Strategist | Research Analyst agent |
| `StrategistRecommendation` | PM | Strategist agent |
| `PMDecision` | Back Office / API | PM agent |
| `DailyTickResult` | WebSocket / Frontend | Orchestrator |

## Dependencies
- `pydantic>=2.0` (already in requirements via FastAPI)
- No new dependencies required

## Verification
- Unit tests with `pytest` validating:
  - Invalid strategy keys are rejected by `StrategistRecommendation`
  - Cash buffer constraint is enforced on `CommanderDirective`
  - `PMDecision.confirmation` must equal `"LOCKED"`
  - Token costs aggregate correctly in `DailyTickResult`
