# Plan 3: Daily Game Loop Orchestrator (`src/arena/orchestrator.py`)

## Objective
Implement the core asynchronous game loop that runs one complete "trading day" for a single hedge fund. This is the central nervous system of the Swarm — it coordinates all agent calls in the correct topological order, enforces the hierarchy, tracks token costs, and triggers the deterministic Back Office.

## Execution DAG (Topological Order)

```
Phase 0: Morning Data Fetch
    ├── fetch_risk_summary()         ─┐
    ├── fetch_pipeline_coverage()     │ concurrent (asyncio.gather)
    └── fetch_competitors()          ─┘

Phase 1: C-Suite Consultants (concurrent)
    ├── run_market_consultant(risk_data)    ─┐
    ├── run_auditor(coverage_data)           │ asyncio.gather
    └── run_scout(competitor_data)          ─┘

Phase 2: Commander (sequential — depends on Phase 1)
    └── run_commander(macro_brief, audit_brief, scout_brief, positions)

Phase 3: Trading Desks (concurrent per desk, sequential within desk)
    For each desk_allocation in commander_directive:
        ├── run_analyst(tickers)                        │ sequential
        ├── run_strategist(analyst_insight, directive)   │ within desk
        └── run_pm(budget, strategy_rec, portfolio_id)  │

Phase 4: Back Office (deterministic Python, NO LLMs)
    ├── trigger ingestion_system (validate data freshness)
    ├── trigger risk_system (scale down correlated allocations)
    └── run_accountant(total_tokens) → deduct from P/L

Phase 5: Publish Results
    └── broadcast via WebSocket + return DailyTickResult
```

## Core Function Signatures

### `async def run_daily_tick(trader_id: int) -> DailyTickResult`

The master orchestrator function. Accepts a trader ID and executes the full daily cycle.

### Data Fetch Layer (Phase 0)

```python
async def _fetch_risk_summary() -> dict:
    """GET /api/risk/summary — VIX, yields, macro regime."""

async def _fetch_pipeline_coverage() -> dict:
    """GET /api/diagnostics/pipeline-coverage — data staleness."""

async def _fetch_competitors(exclude_trader_id: int) -> list[dict]:
    """GET /api/traders — all traders except self."""

async def _fetch_positions(trader_id: int) -> dict:
    """GET /api/traders/{id}/positions — current holdings."""
```

**Implementation:** Use `httpx.AsyncClient` for non-blocking HTTP. Target `localhost:8000` (configurable via env var `ARENA_API_BASE_URL`).

### Agent Execution Layer (Phases 1-3)

```python
async def _call_llm(
    system_prompt: str,
    user_message: str,
    response_model: type[BaseModel],
    model_id: str = "claude-sonnet-4-6",
) -> tuple[BaseModel, TokenUsage]:
    """Generic LLM call wrapper with structured output parsing."""
```

**Key design decisions:**
1. Uses Anthropic's native API (already in `requirements.txt`)
2. Parses response into Pydantic model with retry logic (max 2 retries on JSON parse failure)
3. Returns both the parsed model AND token usage for Accountant tracking
4. Model ID is configurable per-run (supports model A/B testing per user rule #16)

### Agent Runner Functions

```python
async def _run_market_consultant(risk_data: dict) -> MacroBrief:
async def _run_auditor(coverage_data: dict) -> AuditBrief:
async def _run_scout(competitor_data: list[dict]) -> ScoutBrief:
async def _run_commander(
    macro: MacroBrief, audit: AuditBrief, scout: ScoutBrief,
    positions: dict, total_capital: float,
) -> CommanderDirective:
async def _run_analyst(desk_id: int, tickers: list[str]) -> AnalystInsight:
async def _run_strategist(
    desk_id: int, insight: AnalystInsight, directive: str, risk_tolerance: str,
) -> StrategistRecommendation:
async def _run_pm(
    desk_id: int, budget: float, strategy_rec: StrategistRecommendation,
    portfolio_id: int,
) -> PMDecision:
```

### Back Office Layer (Phase 4)

```python
def _run_risk_bouncer(pm_decisions: list[PMDecision]) -> None:
    """Execute risk_system.py to scale down correlated allocations.
    
    This runs AFTER PMs lock strategies, so the Commander cannot
    bypass the Risk Bouncer. The Python engine reads the DB state
    set by PMs and applies constraints.
    """

def _run_accountant(token_ledger: list[TokenUsage]) -> float:
    """Calculate total API cost and deduct from fund P/L.
    
    Pricing:
    - Claude Opus 4.6:   $15.00/MTok in, $75.00/MTok out
    - Claude Sonnet 4.6: $3.00/MTok in,  $15.00/MTok out
    - Claude Haiku 4.5:  $0.25/MTok in,  $1.25/MTok out
    
    Returns the total USD deducted.
    """
```

## Concurrency Model

```python
# Phase 0: Concurrent data fetch
risk_data, coverage_data, competitor_data = await asyncio.gather(
    _fetch_risk_summary(),
    _fetch_pipeline_coverage(),
    _fetch_competitors(trader_id),
)

# Phase 1: Concurrent consultant calls
macro_brief, audit_brief, scout_brief = await asyncio.gather(
    _run_market_consultant(risk_data),
    _run_auditor(coverage_data),
    _run_scout(competitor_data),
)

# Phase 2: Sequential commander call (depends on all briefs)
positions = await _fetch_positions(trader_id)
directive = await _run_commander(macro_brief, audit_brief, scout_brief, positions, total_capital)

# Phase 3: Concurrent desks (sequential within each desk)
async def _run_desk(alloc: DeskAllocation) -> PMDecision:
    insight = await _run_analyst(alloc.desk_id, alloc.tickers)
    strategy = await _run_strategist(alloc.desk_id, insight, alloc.strategic_directive, alloc.risk_tolerance)
    pm_decision = await _run_pm(alloc.desk_id, alloc.capital_budget_usd, strategy, portfolio_id)
    return pm_decision

desk_results = await asyncio.gather(
    *[_run_desk(alloc) for alloc in directive.desk_allocations]
)

# Phase 4: Deterministic back office (sync, runs in executor)
await asyncio.get_event_loop().run_in_executor(None, _run_risk_bouncer, desk_results)
cost = _run_accountant(all_token_usages)
```

## Error Handling Strategy

| Failure Mode | Handling |
|---|---|
| LLM returns invalid JSON | Retry up to 2x with error feedback. If still fails, use fallback (previous day's directive) |
| LLM hallucinates strategy key | Pydantic `StrategistRecommendation` raises `ValidationError` → caught, retried with explicit error in user message |
| API endpoint unreachable | `httpx.TimeoutException` → abort tick, log error, keep previous positions |
| Risk Bouncer finds critical breach | Scales allocations to 0 for breaching assets (existing behavior in `risk_system.py`) |
| Token budget exceeded | Accountant deducts full cost; if cost > 1% of fund NAV, log WARNING |

## Configuration

```python
# Environment variables
ARENA_API_BASE_URL = os.getenv("ARENA_API_BASE_URL", "http://localhost:8000")
ARENA_LLM_MODEL = os.getenv("ARENA_LLM_MODEL", "claude-sonnet-4-6")
ARENA_MAX_RETRIES = int(os.getenv("ARENA_MAX_RETRIES", "2"))
ARENA_TIMEOUT_SECONDS = int(os.getenv("ARENA_TIMEOUT_SECONDS", "30"))
```

## API Route for External Triggering

A new endpoint in `src/api/routers/arena.py`:

```python
@router.post("/api/arena/tick/{trader_id}")
async def trigger_daily_tick(trader_id: int):
    """Trigger one daily game loop for a specific trader/fund."""
```

## Dependencies

- `httpx` (already used in `forensic_auditor.py`)
- `anthropic` (already in `requirements.txt`)
- `asyncio` (stdlib)
- All schemas from Plan 1
- All prompts from Plan 2

## Verification

1. Unit test: mock all API calls and LLM responses, verify DAG order
2. Integration test: run against live API with a test trader, verify PM decisions hit DB
3. Token accounting test: verify Accountant deduction matches sum of all agent token costs
4. Hierarchy test: verify Commander output cannot contain strategy execution calls
