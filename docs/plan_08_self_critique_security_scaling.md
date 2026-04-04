# Plan 8: Self-Critique, Security & Multi-Fund Scaling (`src/arena/safety.py`)

## Objective
This plan addresses the **verification layer** — the critical self-critique questions that must be answered before the swarm is production-ready, plus the security model and the path to scaling from 1 fund to N competing funds in the arena.

---

## Part A: Self-Critique & Verification Matrix

### Q1: Can the Commander accidentally bypass the Risk Bouncer?

**Answer: NO.** The architecture guarantees this via three mechanisms:

1. **Schema isolation:** `CommanderDirective` has no fields for trade execution. It only allocates budgets and sets directives. The Commander literally cannot express "execute trade X" in its output schema.

2. **DAG ordering:** The orchestrator enforces:
   ```
   Phase 2 (Commander) → Phase 3 (PMs write to DB) → Phase 4 (Risk Bouncer reads DB)
   ```
   Phase 4 is triggered by `orchestrator.py` AFTER `asyncio.gather` in Phase 3 resolves. There is no code path that skips Phase 4.

3. **DB-level enforcement:** PMs write strategies via `PUT /api/portfolios/{id}/strategy`, which calls `assign_strategy()` in `trader_manager.py`. The Risk Bouncer then reads the resulting `target_portfolio.parquet` and applies `iterative_mcr_scale()`. Even if a PM writes an aggressive allocation, the Bouncer scales it down.

**Residual risk:** If the API server crashes between PM writes and Bouncer execution, the PM's unfiltered allocation persists. **Mitigation:** Add a `risk_validated: bool` flag to the portfolio table. Execution should refuse to run unless `risk_validated = true`.

### Q2: Are API costs actively tracked and deducted in the Python loop?

**Answer: YES.** The Accountant (Plan 4) is called in Phase 4 of the orchestrator:

```python
# Every TokenUsage from every agent is collected
all_usages = [brief.token_cost for brief in briefs] + [directive.token_cost] + ...

# Phase 4: Accountant deducts from P/L
summary = aggregate_tick_cost(all_usages)
deduct_from_pnl(trader_id, summary.total_cost_usd)
```

**Edge case:** If the tick crashes mid-way (e.g., Desk 2's strategist times out), the Accountant still runs on whatever tokens were consumed. This is enforced via `try/finally` in the orchestrator:

```python
async def run_daily_tick(trader_id: int) -> DailyTickResult:
    token_ledger: list[TokenUsage] = []
    try:
        # ... agent calls, each appending to token_ledger
        pass
    finally:
        # ALWAYS deduct costs, even on failure
        if token_ledger:
            summary = aggregate_tick_cost(token_ledger)
            deduct_from_pnl(trader_id, summary.total_cost_usd)
```

### Q3: Can the Strategist hallucinate a strategy name?

**Answer: NO.** The `StrategistRecommendation.recommended_strategy` field is typed as `StrategyKey`, which is a `Literal` type:

```python
StrategyKey = Literal["ls_zscore", "fortress", "low_beta", ...]
```

If the LLM outputs `"rsi_divergence"` (a non-existent strategy), Pydantic V2 raises `ValidationError` at deserialization. The `call_llm()` retry logic then re-prompts the LLM with the error, explicitly listing the valid keys.

### Q4: Can a PM exceed the Commander's budget?

**Answer: NO.** Enforced at two levels:

1. **Pydantic validator on PMDecision:**
   ```python
   @model_validator(mode='after')
   def budget_check(self) -> 'PMDecision':
       # PM's allocated_capital is validated against Commander's desk allocation
       # This requires the Commander's budget to be passed as context
   ```

2. **Orchestrator-level enforcement:**
   ```python
   pm_decision = await _run_pm(desk_id, alloc.capital_budget_usd, ...)
   if pm_decision.allocated_capital > alloc.capital_budget_usd:
       pm_decision.allocated_capital = alloc.capital_budget_usd  # Hard clamp
       logger.warning(f"PM desk {desk_id} tried to exceed budget. Clamped.")
   ```

### Q5: What happens when competing agents see each other's strategies?

**Answer: The Scout can observe competitor performance (Sharpe, drawdown) but NOT their internal strategy selections.** The `GET /api/traders` endpoint returns aggregate metrics, not portfolio-level strategy assignments. This prevents strategy copying.

### Q6: Can token costs cause a "death spiral"?

**Scenario:** A fund's NAV drops → it still pays token costs → NAV drops further → eventually goes to zero from API costs alone.

**Mitigation:**
1. Estimated cost per tick is ~$0.13 (see Plan 4). Even with 250 ticks/year, that's $32.50 out of a typical $10,000 fund (0.33%).
2. Add a **cost circuit breaker:** If cumulative API costs exceed 2% of initial NAV, switch all agents to Haiku (cheapest model).
3. If costs exceed 5% of initial NAV, halt the fund with a warning.

---

## Part B: Security Model

### LLM Sandbox Constraints

| Constraint | Enforcement |
|---|---|
| Agents cannot access filesystem | No tool_use / function_calling enabled |
| Agents cannot make network calls | Pure text-in-text-out via Anthropic API |
| Agents cannot see other funds' internals | Scout reads only public `GET /api/traders` |
| Agents cannot modify their own prompts | System prompts are hardcoded in `prompts.py` |
| Agents cannot bypass hierarchy | Schema validation + DAG ordering |
| Output is always sanitized | Pydantic V2 validates all LLM output before consumption |

### API Key Security

```python
# API key is read from .env.local (server-side only, per user rule #3)
# Never exposed to frontend or LLM agents
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
```

### Prompt Injection Defense

Agents receive data as structured JSON in the `user` message, not as uncontrolled text. Even if market data contained adversarial strings (e.g., a ticker named `"IGNORE PREVIOUS INSTRUCTIONS"`), the schema parser would reject it because ticker symbols are validated against the entity map.

---

## Part C: Multi-Fund Scaling Architecture

### Phase 1: Single Fund (Current Plan)

```
1 Fund → 1 Commander → 3 Desks → 13 agents total
Token cost: ~$0.13/tick
```

### Phase 2: Arena Mode (N Funds Competing)

```
Arena Controller
├── Fund 1 (LLM Team A) → run_daily_tick(trader_id=1)
├── Fund 2 (LLM Team B) → run_daily_tick(trader_id=2)
├── Fund 3 (Human Trader) → manual API calls
└── Fund 4 (Baseline) → buy_hold strategy (no LLM, no cost)
```

**Implementation:**
```python
async def run_arena_tick(date: date) -> ArenaTickResult:
    """Run a daily tick for ALL funds concurrently."""
    traders = list_traders()  # All registered funds
    
    results = await asyncio.gather(
        *[run_daily_tick(t["id"]) for t in traders],
        return_exceptions=True,
    )
    
    # Rank funds by cumulative return
    leaderboard = compute_leaderboard(traders, results)
    await manager.broadcast("arena.leaderboard_updated", leaderboard)
    
    return ArenaTickResult(date=date, results=results, leaderboard=leaderboard)
```

### Key Scaling Considerations

| Factor | Single Fund | N Funds |
|---|---|---|
| Concurrent API calls | 5 (semaphore) | 5 × N (need higher semaphore or sequential funds) |
| Token costs | ~$0.13/tick | ~$0.13N/tick |
| Data fetch | 1x (shared) | 1x (shared — all funds read same market data) |
| Risk Bouncer | Per-fund | Per-fund (each fund has its own covariance matrix) |
| Execution | Per-fund | Per-fund (isolated portfolios) |

### Leaderboard Schema

```python
class LeaderboardEntry(BaseModel):
    rank: int
    trader_id: int
    fund_name: str
    total_return: float
    sharpe: float
    max_drawdown: float
    api_cost_total: float
    net_return: float  # total_return - api_cost_impact
    active_strategies: list[str]
```

---

## Part D: Testing & Verification Plan

### Unit Tests (`tests/arena/`)

```
tests/arena/
├── test_schemas.py        — Pydantic validation edge cases
├── test_prompts.py        — Prompt template formatting
├── test_llm_client.py     — Mock API calls, retry logic
├── test_accountant.py     — Token cost calculations
├── test_back_office.py    — Risk Bouncer integration
└── test_orchestrator.py   — Full DAG ordering verification
```

### Key Test Scenarios

1. **Hallucination rejection test:**
   ```python
   def test_invalid_strategy_rejected():
       with pytest.raises(ValidationError):
           StrategistRecommendation(
               desk_id=1,
               recommended_strategy="fibonacci_expansion",  # Invalid!
               confidence=0.9,
               reasoning="test",
           )
   ```

2. **Budget conservation test:**
   ```python
   def test_commander_budget_sums_to_deployed():
       directive = CommanderDirective(...)
       total = sum(d.capital_budget_usd for d in directive.desk_allocations)
       assert total <= capital * directive.total_deployed_pct
   ```

3. **Risk Bouncer ordering test:**
   ```python
   async def test_bouncer_runs_after_pms():
       # Verify that _run_back_office is called AFTER all PM decisions complete
       # by checking that PM decisions exist in DB before Bouncer reads them
   ```

4. **Token deduction test:**
   ```python
   def test_accountant_deducts_from_nav():
       initial = get_trader(1)["total_capital"]
       run_accountant(1, today, token_ledger)
       after = get_trader(1)["total_capital"]
       assert after == initial - expected_cost
   ```

### Integration Test (End-to-End)

```python
async def test_full_daily_tick():
    """Run a complete daily tick with a test trader.
    
    Uses Claude Haiku for all agents (cheapest) to minimize test costs.
    Verifies:
    1. All 13 agents are called
    2. Commander produces valid directive
    3. PMs lock strategies in DB
    4. Risk Bouncer scales appropriately
    5. Accountant deducts correct amount
    6. WebSocket events are broadcast
    """
```

---

## File Structure Summary (All Plans Combined)

```
src/arena/
├── __init__.py              (Plan 1)
├── schemas.py               (Plan 1)  — Pydantic V2 inter-agent models
├── prompts.py               (Plan 2)  — System prompts for all 7 agent roles
├── orchestrator.py          (Plan 3)  — Async game loop DAG
├── accountant.py            (Plan 4)  — Token cost tracking & P/L deduction
├── back_office.py           (Plan 6)  — Risk Bouncer + Data Integrity
├── llm_client.py            (Plan 7)  — Structured output LLM wrapper
└── safety.py                (Plan 8)  — Circuit breakers & scaling config

src/api/routers/
└── arena.py                 (Plan 5)  — REST endpoints & WebSocket events

tests/arena/
├── test_schemas.py
├── test_llm_client.py
├── test_accountant.py
├── test_back_office.py
└── test_orchestrator.py
```

## Implementation Priority

| Order | Plan | File | Depends On | Estimated LOC |
|---|---|---|---|---|
| 1 | Plan 1 | `schemas.py` | None | ~200 |
| 2 | Plan 4 | `accountant.py` | Plan 1 | ~120 |
| 3 | Plan 7 | `llm_client.py` | Plan 1, 4 | ~180 |
| 4 | Plan 2 | `prompts.py` | Plan 1 | ~250 |
| 5 | Plan 6 | `back_office.py` | Plan 1, 4 | ~200 |
| 6 | Plan 3 | `orchestrator.py` | Plans 1-7 | ~350 |
| 7 | Plan 5 | `arena.py` (router) | Plan 3 | ~200 |
| 8 | Plan 8 | `safety.py` + tests | All | ~150 + tests |
| **Total** | | | | **~1,650** |
