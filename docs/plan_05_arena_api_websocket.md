# Plan 5: Arena API Router & WebSocket Integration (`src/api/routers/arena.py`)

## Objective
Expose the Hedge Fund Swarm orchestration layer via REST endpoints and real-time WebSocket events. This allows the Next.js frontend to trigger game ticks, monitor agent activity in real-time, view cost analytics, and display the complete state of the arena.

## Endpoints

### 1. Trigger Daily Tick

```
POST /api/arena/tick/{trader_id}
```

**Request Body (optional):**
```json
{
  "model_overrides": {
    "commander": "claude-opus-4-6",
    "strategist": "claude-sonnet-4-6",
    "analyst": "claude-haiku-4-5",
    "pm": "claude-haiku-4-5"
  },
  "dry_run": false
}
```

**Response:**
```json
{
  "status": "started",
  "tick_id": "uuid-v4",
  "trader_id": 1,
  "message": "Daily tick started for fund 'Alpha Prime'"
}
```

**Behavior:**
- Runs the orchestrator in a background `asyncio.Task` (non-blocking)
- Returns immediately with a `tick_id` for polling
- Uses the existing `_run_in_background` pattern from `pipeline.py` for log capture
- If `dry_run=true`, runs all agents but does NOT execute DB writes (PM decisions are computed but not committed)

### 2. Tick Status & Results

```
GET /api/arena/tick/{tick_id}/status
```

**Response (while running):**
```json
{
  "status": "running",
  "phase": "desks",
  "current_agent": "strategist_desk_2",
  "elapsed_seconds": 12.4,
  "completed_agents": ["consultant", "auditor", "scout", "commander", "analyst_d1", "analyst_d2", "analyst_d3", "strategist_d1"],
  "pending_agents": ["strategist_d2", "strategist_d3", "pm_d1", "pm_d2", "pm_d3"]
}
```

**Response (complete):**
```json
{
  "status": "complete",
  "result": { /* DailyTickResult */ },
  "elapsed_seconds": 28.7
}
```

### 3. Arena Logs (Live)

```
GET /api/arena/logs?since=0
```

Mirrors the pattern from `pipeline.py`:
```json
{
  "logs": [
    {"ts": "14:30:05", "level": "INFO", "agent": "commander", "msg": "Deploying 85% across 3 desks"},
    {"ts": "14:30:12", "level": "INFO", "agent": "strategist_d1", "msg": "Recommending 'momentum' (confidence: 0.87)"}
  ],
  "total": 42,
  "running": true
}
```

### 4. Cost Analytics

```
GET /api/arena/costs/{trader_id}
```

**Response:**
```json
{
  "trader_id": 1,
  "total_cost_usd": 3.42,
  "ticks_run": 27,
  "avg_cost_per_tick": 0.127,
  "cost_as_pct_of_initial_capital": 0.034,
  "daily_breakdown": [
    {"date": "2024-01-15", "cost_usd": 0.132, "agents": 13, "total_tokens": 16750}
  ],
  "cost_by_agent_role": {
    "commander": 2.23,
    "strategist": 0.53,
    "consultant": 0.61,
    "analyst": 0.03,
    "pm": 0.02
  }
}
```

### 5. Arena State (Dashboard Summary)

```
GET /api/arena/state/{trader_id}
```

**Response:**
```json
{
  "trader_id": 1,
  "fund_name": "Alpha Prime",
  "last_tick_date": "2024-01-15",
  "last_tick_result": { /* abbreviated DailyTickResult */ },
  "active_strategies": ["momentum", "fortress", "low_beta"],
  "total_api_cost_usd": 3.42,
  "current_equity": 9650.00,
  "hierarchy": {
    "commander": { "model": "opus-4-6", "avg_tokens_per_tick": 3500 },
    "desks": [
      { "desk_id": 1, "strategy": "momentum", "capital": 3400 },
      { "desk_id": 2, "strategy": "fortress", "capital": 2550 },
      { "desk_id": 3, "strategy": "low_beta", "capital": 2550 }
    ]
  }
}
```

### 6. Available Models

```
GET /api/arena/models
```

Uses the same Anthropic model discovery pattern from `forensic_auditor.py`:
```python
# Pings https://api.anthropic.com/v1/models
# Returns live model list with display names
# Supports user rule #13: never hardcode model names
```

## WebSocket Events

Extend the existing `/api/ws/telemetry` WebSocket with new arena event types:

| Event Type | Payload | When |
|---|---|---|
| `arena.tick_started` | `{trader_id, tick_id}` | Tick begins |
| `arena.agent_started` | `{agent_name, model_id, phase}` | Agent LLM call begins |
| `arena.agent_completed` | `{agent_name, tokens, cost, elapsed_ms}` | Agent returns |
| `arena.phase_completed` | `{phase_name, agents_completed}` | Phase finishes |
| `arena.tick_completed` | `{tick_id, total_cost, elapsed_seconds}` | Full tick done |
| `arena.error` | `{agent_name, error_type, message}` | Agent failure |

**Integration with existing `ws_manager.py`:**
```python
from src.api.ws_manager import manager

# Inside orchestrator, after each agent completes:
await manager.broadcast("arena.agent_completed", {
    "agent_name": "strategist_d1",
    "tokens": {"input": 1200, "output": 200},
    "cost_usd": 0.0066,
    "elapsed_ms": 2340,
})
```

## Server Registration

In `src/api/server.py`, add:
```python
from src.api.routers import arena
app.include_router(arena.router)
```

## Background Task Pattern

The tick runs as a background task to avoid blocking the API. Pattern follows `pipeline.py`:

```python
# Global state for tracking active ticks
_active_ticks: dict[str, dict] = {}

@router.post("/api/arena/tick/{trader_id}")
async def trigger_tick(trader_id: int, req: TickRequest = None):
    tick_id = str(uuid.uuid4())
    _active_ticks[tick_id] = {"status": "running", "phase": "init"}
    
    asyncio.create_task(_execute_tick(tick_id, trader_id, req))
    
    return {"status": "started", "tick_id": tick_id}

async def _execute_tick(tick_id: str, trader_id: int, req: TickRequest):
    try:
        result = await run_daily_tick(trader_id, model_overrides=req.model_overrides)
        _active_ticks[tick_id] = {"status": "complete", "result": result}
    except Exception as e:
        _active_ticks[tick_id] = {"status": "error", "error": str(e)}
```

## Pydantic Request/Response Models

```python
class TickRequest(BaseModel):
    model_overrides: dict[str, str] | None = None
    dry_run: bool = False

class TickStatusResponse(BaseModel):
    status: Literal["running", "complete", "error"]
    phase: str | None = None
    current_agent: str | None = None
    elapsed_seconds: float = 0.0
    result: DailyTickResult | None = None
    error: str | None = None
```

## Dependencies
- All existing middleware and CORS config from `server.py`
- `uuid` (stdlib)
- WebSocket manager from `ws_manager.py`
- Orchestrator from Plan 3
- Schemas from Plan 1
