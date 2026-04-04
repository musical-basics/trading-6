"""
server.py — Level 5 FastAPI Application

The central REST + WebSocket API server for the trading terminal.
Serves the Next.js frontend with endpoints for:
  - Strategy tournaments & backtesting
  - X-Ray diagnostic inspection
  - Risk matrix & macro regime data
  - Execution ledger management
  - Alpha Lab autonomous strategy discovery
  - Real-time WebSocket telemetry (Level 5)

Run with: uvicorn src.api.server:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from src.core.duckdb_store import init_store
from src.api.routers import tournament, xray, risk, execution, traders, portfolios
from src.api.ws_manager import manager


# ── Lifespan: initialize DuckDB + Redis listener on startup ──
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Initialize the DuckDB store and start the Redis event listener."""
    conn = init_store()
    app.state.db = conn
    print("✓ DuckDB store initialized")

    # Start Redis pub/sub listener for WebSocket broadcasting
    redis_task = asyncio.create_task(_redis_listener())
    print("✓ Redis WebSocket listener started")

    yield

    redis_task.cancel()
    conn.close()
    print("✓ DuckDB store closed")


async def _redis_listener():
    """Subscribe to Redis channels and forward events to WebSocket clients."""
    try:
        import redis.asyncio as aioredis
        from src.config import REDIS_URL

        r = aioredis.from_url(REDIS_URL)
        pubsub = r.pubsub()
        await pubsub.subscribe("execution_events", "pnl_ticks", "system_events")

        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await manager.broadcast(
                        data.get("event_type", "unknown"),
                        data.get("payload", {}),
                    )
                except (json.JSONDecodeError, TypeError):
                    pass
    except Exception as e:
        # Redis not available — WebSocket push degrades gracefully
        print(f"  ⚠ Redis listener not available: {e}")
        print(f"  ⚠ WebSocket push will be disabled (REST polling still works)")


# ── FastAPI App ──────────────────────────────────────────────
app = FastAPI(
    title="QuantPrime Level 5 API",
    description="ECS Data-Oriented Trading Pipeline — The God Engine",
    version="5.0.0",
    lifespan=lifespan,
)

# ── CORS: Allow Next.js frontend ────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",           # Next.js dev server (default)
        "http://localhost:3001",           # Next.js dev server (port bumped)
        "http://localhost:3002",           # Next.js dev server (port bumped)
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
        "http://127.0.0.1:3002",
        "http://web:3000",                 # Docker service name
        "https://trading-5.vercel.app",    # Vercel production
        "https://*.vercel.app",            # Vercel preview deploys
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Include REST Routers ────────────────────────────────────
app.include_router(tournament.router)
app.include_router(xray.router)
app.include_router(risk.router)
app.include_router(execution.router)
app.include_router(traders.router)
app.include_router(portfolios.router)

from src.api.routers import indicators
app.include_router(indicators.router)

from src.api.routers import alpha_lab
app.include_router(alpha_lab.router)

from src.api.routers import pipeline
app.include_router(pipeline.router)

from src.api.routers import arena
app.include_router(arena.router)


# ── WebSocket Endpoint — Real-Time Telemetry ────────────────
@app.websocket("/api/ws/telemetry")
async def telemetry_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time execution and PnL telemetry."""
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text('{"type": "pong"}')
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ── Health Check ─────────────────────────────────────────────
@app.get("/api/health")
async def health_check():
    """Basic health check endpoint."""
    return {
        "status": "healthy",
        "version": "5.0.0",
        "engine": "polars+duckdb",
        "websocket_clients": len(manager.active_connections),
    }
