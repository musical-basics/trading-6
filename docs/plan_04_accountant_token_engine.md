# Plan 4: The Accountant — Token Cost Engine (`src/arena/accountant.py`)

## Objective
Build a deterministic Python engine that tracks every LLM API call across all agents in the swarm, calculates the real USD cost, and **deducts it directly from the fund's P/L**. This is the core game mechanic that prevents "over-thinking" — agents that are too verbose literally lose money for their fund.

## Why This Matters (Game Theory)
Without the Accountant, there is no cost to LLM verbosity. Agents would produce 2000-token responses when 200 tokens suffice, burning API costs that provide zero alpha. The Accountant creates a natural selection pressure: funds with tighter, more efficient agents will outperform funds with chatty ones, even if both have the same strategy quality.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              Token Ledger (per tick)                 │
│                                                     │
│  agent_name  │ model_id       │ in_tok │ out_tok    │
│  ───────────────────────────────────────────────────│
│  consultant  │ sonnet-4-6     │  1200  │   180      │
│  auditor     │ sonnet-4-6     │   800  │   120      │
│  scout       │ sonnet-4-6     │  1500  │   200      │
│  commander   │ opus-4-6       │  2800  │   450      │
│  analyst_d1  │ haiku-4-5      │  1000  │   300      │
│  analyst_d2  │ haiku-4-5      │  1000  │   280      │
│  analyst_d3  │ haiku-4-5      │  1000  │   310      │
│  strat_d1    │ sonnet-4-6     │  1200  │   200      │
│  strat_d2    │ sonnet-4-6     │  1200  │   180      │
│  strat_d3    │ sonnet-4-6     │  1200  │   190      │
│  pm_d1       │ haiku-4-5      │   400  │    80      │
│  pm_d2       │ haiku-4-5      │   400  │    75      │
│  pm_d3       │ haiku-4-5      │   400  │    85      │
│  ───────────────────────────────────────────────────│
│  TOTAL                        │ 14100  │  2650      │
│  TOTAL COST                   │        │  $0.0847   │
└─────────────────────────────────────────────────────┘
```

## Pricing Table

```python
MODEL_PRICING: dict[str, tuple[float, float]] = {
    # model_id_prefix: (input_cost_per_mtok, output_cost_per_mtok)
    "claude-opus-4":    (15.00, 75.00),
    "claude-sonnet-4":  (3.00, 15.00),
    "claude-haiku-4":   (0.25, 1.25),
    # Fallback for unknown models
    "default":          (3.00, 15.00),
}
```

**Pricing is resolved by prefix matching** to handle model version suffixes (e.g., `claude-sonnet-4-6` matches `claude-sonnet-4`).

## Core Functions

### `calculate_cost(usage: TokenUsage) -> float`
Compute the USD cost for a single agent interaction.

```python
def calculate_cost(usage: TokenUsage) -> float:
    prefix = _match_model_prefix(usage.model_id)
    in_rate, out_rate = MODEL_PRICING.get(prefix, MODEL_PRICING["default"])
    return (usage.input_tokens / 1_000_000 * in_rate) + \
           (usage.output_tokens / 1_000_000 * out_rate)
```

### `aggregate_tick_cost(token_ledger: list[TokenUsage]) -> TickCostSummary`

```python
class TickCostSummary(BaseModel):
    total_input_tokens: int
    total_output_tokens: int
    total_cost_usd: float
    cost_by_agent: dict[str, float]
    cost_by_model: dict[str, float]
    cost_as_pct_of_nav: float
```

### `deduct_from_pnl(trader_id: int, cost_usd: float) -> None`
Write the deduction to the trader's P/L ledger. This is a deterministic DB operation (not an LLM call).

```python
def deduct_from_pnl(trader_id: int, cost_usd: float) -> None:
    """Deduct API costs from the trader's total equity.
    
    Implementation:
    1. Read current total_capital from traders table
    2. Subtract cost_usd
    3. Write updated capital back
    4. Insert a record into api_cost_ledger for audit trail
    """
```

### `persist_cost_ledger(trader_id: int, tick_date: date, entries: list[TokenUsage]) -> None`
Write individual agent costs to a Parquet file for historical analysis.

```python
# Output: data/components/api_cost_ledger.parquet
# Schema: [trader_id, tick_date, agent_name, model_id, input_tokens, output_tokens, cost_usd]
```

## Model Selection Strategy (per agent tier)

| Agent Tier | Recommended Model | Rationale |
|---|---|---|
| Commander (CEO) | `claude-opus-4-6` | Highest stakes decision (capital allocation). Worth the extra cost. |
| Strategist × 3 | `claude-sonnet-4-6` | Needs to understand strategy semantics. Mid-tier cost. |
| Consultants × 3 | `claude-sonnet-4-6` | Macro analysis requires reasoning ability. |
| Research Analyst × 3 | `claude-haiku-4-5` | Data summarization task. Cheapest model suffices. |
| PM × 3 | `claude-haiku-4-5` | Simple slot-filling (strategy + budget → JSON). Cheapest. |

### Estimated Cost Per Daily Tick

| Component | Agents | Model | Est. Tokens (in+out) | Unit Cost | Total |
|---|---|---|---|---|---|
| Consultants | 3 | Sonnet | ~1500+200 each | ~$0.0075 | $0.0225 |
| Commander | 1 | Opus | ~3000+500 | ~$0.0825 | $0.0825 |
| Analysts | 3 | Haiku | ~1000+300 each | ~$0.0006 | $0.0018 |
| Strategists | 3 | Sonnet | ~1200+200 each | ~$0.0066 | $0.0198 |
| PMs | 3 | Haiku | ~400+80 each | ~$0.0002 | $0.0006 |
| **Total** | **13** | | | | **~$0.1272** |

At ~250 trading days/year: **~$31.80/year** per fund for one daily tick.

## Integration with Orchestrator

The Accountant is called at the END of `run_daily_tick()`:

```python
# Collect all token usages from every agent call
all_usages: list[TokenUsage] = [
    macro_brief.token_cost,
    audit_brief.token_cost,
    scout_brief.token_cost,
    directive.token_cost,
    *[desk.insight.token_cost for desk in desks],
    *[desk.strategy.token_cost for desk in desks],
    *[desk.pm.token_cost for desk in desks],
]

# Phase 4: Accountant
summary = aggregate_tick_cost(all_usages)
deduct_from_pnl(trader_id, summary.total_cost_usd)
persist_cost_ledger(trader_id, tick_date, all_usages)
```

## Cost Visualization Endpoint

New API endpoint for the frontend to display cost analytics:

```python
@router.get("/api/arena/costs/{trader_id}")
async def get_cost_history(trader_id: int):
    """Return API cost history for a trader fund."""
    # Reads from api_cost_ledger.parquet
    # Returns: daily costs, cumulative costs, cost breakdown by agent/model
```

## Anti-Gaming Measures

1. **Token costs are non-refundable:** Even if the Strategist recommends a bad strategy that the Risk Bouncer overrides, the tokens are still billed.
2. **Retry costs are additive:** If a PM's output fails Pydantic validation and requires a retry, BOTH calls are billed.
3. **Minimum cost floor:** Each agent call has a minimum 100-token charge (prevents gaming via empty responses).

## Dependencies
- No new dependencies (pure math + Polars for Parquet I/O)
- Uses `TokenUsage` from `src/arena/schemas.py`
