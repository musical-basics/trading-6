# Trading-4 (QuantPrime)

## Quick Start

### One Command (both servers)

```bash
./start.sh
```

Runs both the API server (:8000) and frontend (:3000) concurrently. Press `Ctrl+C` to stop both.

Logs are prefixed `[API]` and `[WEB]` so you can tell them apart. For heavier debugging, run them in separate terminals (see below).

### Or run separately:

### 1. Backend API Server

```bash
source .venv/bin/activate
python3 -m uvicorn src.api.server:app --reload --port 8000
```

### 2. Frontend Dashboard (Next.js)

```bash
cd frontend
pnpm dev
```

Opens at [http://localhost:3000](http://localhost:3000). Requires the backend API running on port 8000.

### 3. Streamlit Dashboard (Legacy)

```bash
source .venv/bin/activate
streamlit run ui/app.py
```

### 4. Run Full Pipeline

```bash
source .venv/bin/activate
python3 main.py
```

---

## Project Structure

```
trading-4/
├── src/
│   ├── api/              # FastAPI backend
│   │   ├── server.py     # App entrypoint + CORS
│   │   └── routers/      # tournament, xray, risk, execution, traders, portfolios
│   ├── core/             # Business logic
│   │   ├── trader_manager.py      # Trader/portfolio CRUD
│   │   ├── duckdb_store.py        # DuckDB parquet views
│   │   └── migrate_sqlite_to_parquet.py
│   ├── ecs/              # Strategy engine
│   │   ├── strategy_registry.py   # All 12 strategies
│   │   └── tournament_system.py   # Vectorized backtester
│   └── pipeline/         # Data pipeline + execution
│       ├── core/         # DB init, rebalance scheduler
│       └── execution/    # Order router, portfolio state, simulation
├── frontend/             # Next.js 16 + Recharts + Radix UI
│   ├── components/       # Dashboard shell, strategy studio, trader manager, etc.
│   └── lib/api.ts        # Typed API client
├── ui/                   # Streamlit dashboard (legacy)
├── data/                 # SQLite DB + parquet files
└── docs/                 # Implementation plans, bug reports
```

## Key Features

- **Strategy Studio** — Run tournament backtests across 12 strategies with equity curves
- **X-Ray Inspector** — Deep-dive into individual strategy signals and weights
- **Risk War Room** — Macro & covariance risk analysis
- **Execution Ledger** — Paper trading order management
- **Traders & Portfolios** — Hierarchical capital management with isolated sub-portfolios
- **Trader Backtest** — Portfolio-weighted combined equity curves with per-strategy breakdown

## Environment Variables

Create `frontend/.env.local`:

```
NEXT_PUBLIC_API_URL=http://localhost:8000
```

API keys (if using Alpaca live trading) go in `.env.local` at the project root.
