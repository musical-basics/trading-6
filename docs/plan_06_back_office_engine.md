# Plan 6: Back Office Deterministic Engine (`src/arena/back_office.py`)

## Objective
Implement the three deterministic Back Office roles — Data Integrity Analyst, Risk Analyst (The Bouncer), and The Accountant — as pure Python functions with ZERO LLM involvement. These functions run AFTER the LLM agents have made their decisions and provide the safety rails that prevent catastrophic outcomes.

## Critical Ordering Guarantee

```
PM locks strategy → Back Office runs → Execution happens
                    ↑ THIS IS INVIOLABLE ↑
```

The Commander CANNOT bypass the Risk Bouncer because:
1. PMs write strategy selections to the DB via `PUT /api/portfolios/{id}/strategy`
2. **Only after all PMs complete** does the orchestrator invoke the Back Office
3. The Risk Bouncer reads the DB state and applies constraints
4. If the Bouncer scales down, it overwrites the PM's weights in the target portfolio

## Components

### 1. Data Integrity Analyst

**Source:** Wraps `src/ecs/ingestion_system.py` (existing code).

```python
def run_data_integrity_check() -> DataIntegrityReport:
    """Validate data freshness before positions are executed.
    
    Checks:
    1. market_data.parquet: is the latest date within 2 trading days?
    2. fundamental.parquet: are any tickers stale (>540 days since last filing)?
    3. macro.parquet: is VIX/TNX data current?
    4. feature.parquet: are computed features aligned with latest market data?
    
    Returns a report with:
    - freshness_ok: bool
    - stale_components: list of component names with stale data
    - stale_tickers: list of tickers with outdated fundamentals
    - recommendation: "PROCEED" | "HALT" | "PROCEED_WITH_WARNING"
    """
```

**Integration with existing code:**
- Reuses the staleness detection logic from `ingestion_system.py` lines 269-296
- Reads the same Parquet files via `get_parquet_path()`
- Does NOT trigger new data fetches (that's `ingestion_system.py`'s job during morning ingestion)

### 2. Risk Analyst (The Bouncer)

**Source:** Wraps `src/ecs/risk_system.py` (existing code).

```python
def run_risk_bouncer(
    pm_decisions: list[PMDecision],
    trader_id: int,
) -> RiskBouncerReport:
    """Apply variance-based constraints to PM allocations.
    
    Process:
    1. Read the PM decisions (strategy + capital per desk)
    2. Evaluate each strategy against market data (via strategy_registry)
    3. Feed raw weights into risk_system.apply_risk_constraints()
    4. Scale down correlated allocations using iterative MCR
    5. Write adjusted target_weights to target_portfolio.parquet
    
    Returns:
    - original_allocations: dict[desk_id, float]
    - adjusted_allocations: dict[desk_id, float]
    - scaling_factor: float (how much total was scaled down)
    - max_mcr: float
    - portfolio_vol: float
    - iterations: int
    - breaching_assets: list[str]
    """
```

**Key interaction with existing risk_system.py:**

```python
from src.ecs.risk_system import apply_risk_constraints, iterative_mcr_scale

# The Bouncer uses the SAME math as the tournament system
# but applies it to the LIVE portfolio, not a backtest
def _apply_to_live_portfolio(strategy_weights: pl.DataFrame) -> pl.DataFrame:
    market_df = pl.read_parquet(get_parquet_path("market_data"))
    return apply_risk_constraints(strategy_weights, market_df)
```

**Correlation detection across desks:**

If Desk 1 runs `momentum` and Desk 3 runs `xgboost`, but both heavily weight NVDA, the Bouncer detects this cross-desk concentration and scales down the combined exposure. This is what makes the Bouncer essential — individual PMs cannot see each other's allocations.

### 3. The Accountant

**Source:** `src/arena/accountant.py` (Plan 4).

```python
def run_accountant(
    trader_id: int,
    tick_date: date,
    token_ledger: list[TokenUsage],
) -> AccountingReport:
    """Calculate and deduct API costs from fund P/L.
    
    Process:
    1. Sum all token costs across all agents
    2. Apply model-specific pricing
    3. Deduct total from trader's total_capital in SQLite
    4. Persist per-agent costs to api_cost_ledger.parquet
    5. Return detailed accounting report
    
    Returns:
    - total_cost_usd: float
    - cost_by_agent: dict
    - cost_as_pct_of_nav: float
    - nav_before: float
    - nav_after: float
    """
```

## Data Integrity Report Schema

```python
class DataIntegrityReport(BaseModel):
    freshness_ok: bool
    checked_at: datetime
    components_checked: int
    stale_components: list[str]
    stale_tickers: list[str]
    latest_market_date: date | None
    latest_macro_date: date | None
    days_since_market_update: int
    recommendation: Literal["PROCEED", "HALT", "PROCEED_WITH_WARNING"]
```

## Risk Bouncer Report Schema

```python
class RiskBouncerReport(BaseModel):
    timestamp: datetime
    trader_id: int
    original_total_weight: float
    adjusted_total_weight: float
    scaling_factor: float  # adjusted / original
    portfolio_vol_annualized: float
    max_mcr: float
    iterations: int
    breaching_assets: list[dict]  # [{ticker, original_weight, adjusted_weight, mcr}]
    cross_desk_concentration: list[dict]  # [{ticker, desk_ids, combined_weight}]
```

## Orchestrator Integration

```python
# In orchestrator.py, Phase 4:

async def _run_back_office(
    trader_id: int,
    pm_decisions: list[PMDecision], 
    token_ledger: list[TokenUsage],
    tick_date: date,
) -> tuple[RiskBouncerReport, AccountingReport]:
    """Run Back Office as deterministic Python (no LLMs).
    
    Uses run_in_executor to avoid blocking the async event loop since
    the risk calculations involve heavy NumPy/Polars operations.
    """
    loop = asyncio.get_event_loop()
    
    # Data integrity check (fast, ~50ms)
    integrity = await loop.run_in_executor(None, run_data_integrity_check)
    
    if integrity.recommendation == "HALT":
        raise BackOfficeHaltError(f"Data integrity check failed: {integrity.stale_components}")
    
    # Risk Bouncer (heavy computation, ~200-500ms)
    risk_report = await loop.run_in_executor(
        None, run_risk_bouncer, pm_decisions, trader_id
    )
    
    # Accountant (fast, ~10ms)
    accounting = await loop.run_in_executor(
        None, run_accountant, trader_id, tick_date, token_ledger
    )
    
    return risk_report, accounting
```

## Safety Invariants

| Invariant | Enforcement |
|---|---|
| Risk Bouncer runs AFTER all PMs | Orchestrator DAG ordering (sequential) |
| Bouncer cannot be skipped | `run_daily_tick()` always calls back office |
| Commander cannot execute trades | Commander's output schema has no trade fields |
| PM cannot exceed Commander's budget | Pydantic validator on `PMDecision.allocated_capital` |
| Token costs are always deducted | Accountant runs even if tick fails mid-way |
| Retry costs are additive | Each `_call_llm` invocation records tokens independently |

## File Structure

```
src/arena/
├── __init__.py
├── schemas.py          (Plan 1)
├── prompts.py          (Plan 2)
├── orchestrator.py     (Plan 3)
├── accountant.py       (Plan 4)
└── back_office.py      (Plan 6 — THIS FILE)
```

## Dependencies
- `src/ecs/risk_system.py` (existing)
- `src/ecs/ingestion_system.py` (existing)
- `src/ecs/strategy_registry.py` (existing)
- `src/core/duckdb_store.py` (existing)
- Schemas from Plan 1
- Accountant from Plan 4
