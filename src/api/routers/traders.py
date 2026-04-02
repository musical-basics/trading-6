"""
traders.py — FastAPI Router for Trader management.

Endpoints:
  GET  /api/traders              — List all traders
  POST /api/traders              — Create a new trader (auto-generates 10 portfolios)
  GET  /api/traders/{id}         — Get a single trader with constraints
  PUT  /api/traders/{id}/constraints — Update risk constraints
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.core.trader_manager import (
    create_trader,
    get_trader,
    list_traders,
    update_constraints as update_trader_constraints,
    load_trader_constraints,
)
from src.api.routers.models import (
    TraderCreate,
    TraderResponse,
    TraderConstraintUpdate,
    TraderConstraintResponse,
)
from src.core.database import get_db

router = APIRouter(prefix="/api/traders", tags=["traders"])


@router.get("/")
async def api_list_traders():
    """List all traders with aggregate info."""
    traders = list_traders()
    results = []
    for t in traders:
        constraints = load_trader_constraints(t["id"])
        results.append({
            **t,
            "constraints": constraints,
            "portfolios_count": 10,
        })
    return results


@router.post("/", status_code=201)
async def api_create_trader(req: TraderCreate):
    """Create a new trader with auto-allocated portfolios."""
    try:
        trader_id = create_trader(
            name=req.name,
            capital=req.total_capital,
            num_portfolios=req.num_portfolios,
            capital_per_portfolio=req.capital_per_portfolio,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    trader = get_trader(trader_id)
    return {
        **trader,
        "portfolios_count": 10,
    }


@router.get("/{trader_id}")
async def api_get_trader(trader_id: int):
    """Get a single trader with constraints."""
    trader = get_trader(trader_id)
    if not trader:
        raise HTTPException(status_code=404, detail=f"Trader {trader_id} not found")
    trader["portfolios_count"] = 10
    return trader


@router.put("/{trader_id}/constraints")
async def api_update_constraints(trader_id: int, req: TraderConstraintUpdate):
    """Update risk constraints for a trader."""
    trader = get_trader(trader_id)
    if not trader:
        raise HTTPException(status_code=404, detail=f"Trader {trader_id} not found")

    try:
        update_trader_constraints(
            trader_id=trader_id,
            max_drawdown_pct=req.max_drawdown_pct,
            max_open_positions=req.max_open_positions,
            max_capital_per_trade=req.max_capital_per_trade,
            halt_trading_flag=req.halt_trading_flag,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"status": "updated", "trader_id": trader_id}


@router.get("/{trader_id}/positions")
async def api_trader_positions(trader_id: int):
    """Get real-time positions and PnL for a trader.

    Calls get_trader_state() to reconstruct holdings from paper_executions,
    then fetches the latest adj_close from daily_bars to compute market value
    and unrealized PnL.
    """
    import sqlite3
    import pandas as pd
    from src.config import DB_PATH
    from src.pipeline.execution.portfolio_state import get_trader_state

    trader = get_trader(trader_id)
    if not trader:
        raise HTTPException(status_code=404, detail=f"Trader {trader_id} not found")

    try:
        total_equity, all_holdings = get_trader_state(trader_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch trader state: {e}")

    positions = []
    total_invested = 0.0

    if all_holdings:
        conn = sqlite3.connect(DB_PATH)
        try:
            for ticker, info in all_holdings.items():
                shares = info["shares"]
                avg_price = info["avg_price"]
                cost_basis = shares * avg_price

                # Fetch latest price from daily_bars
                try:
                    price_row = pd.read_sql_query(
                        "SELECT adj_close FROM daily_bars WHERE ticker = ? ORDER BY date DESC LIMIT 1",
                        conn, params=(ticker,)
                    )
                    current_price = float(price_row["adj_close"].iloc[0]) if not price_row.empty else avg_price
                except Exception:
                    current_price = avg_price

                market_value = shares * current_price
                unrealized_pnl_usd = market_value - cost_basis
                unrealized_pnl_pct = (unrealized_pnl_usd / cost_basis * 100) if cost_basis > 0 else 0.0

                positions.append({
                    "ticker": ticker,
                    "shares": shares,
                    "avg_entry": round(avg_price, 4),
                    "current_price": round(current_price, 4),
                    "market_value": round(market_value, 2),
                    "cost_basis": round(cost_basis, 2),
                    "unrealized_pnl_usd": round(unrealized_pnl_usd, 2),
                    "unrealized_pnl_pct": round(unrealized_pnl_pct, 4),
                    "strategies": info.get("strategies", []),
                })
                total_invested += market_value
        finally:
            conn.close()

    total_cash = total_equity - total_invested
    total_unrealized_pnl = sum(p["unrealized_pnl_usd"] for p in positions)

    return {
        "trader_id": trader_id,
        "trader_name": trader["name"],
        "total_equity": round(total_equity, 2),
        "total_cash": round(total_cash, 2),
        "total_invested": round(total_invested, 2),
        "total_unrealized_pnl": round(total_unrealized_pnl, 2),
        "positions": sorted(positions, key=lambda p: p["market_value"], reverse=True),
    }


@router.get("/{trader_id}/executions")
async def api_trader_executions(trader_id: int):
    """Get recent execution history (tickets) for a trader."""
    import sqlite3
    import pandas as pd
    from src.config import DB_PATH

    trader = get_trader(trader_id)
    if not trader:
        raise HTTPException(status_code=404, detail=f"Trader {trader_id} not found")

    conn = sqlite3.connect(DB_PATH)
    try:
        # Fetch up to 200 most recent executions for this trader
        executions = pd.read_sql_query("""
            SELECT 
                e.id, e.timestamp, e.ticker, e.action, e.quantity, 
                e.simulated_price, e.strategy_id, e.portfolio_id, p.name as portfolio_name
            FROM paper_executions e
            LEFT JOIN portfolios p ON e.portfolio_id = p.id
            WHERE e.trader_id = ?
            ORDER BY e.timestamp DESC
            LIMIT 200
        """, conn, params=(trader_id,))

        if executions.empty:
            return []

        # Convert timestamps to string format natively avoiding NaT issues
        executions["timestamp"] = executions["timestamp"].astype(str)
        return executions.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch execution history: {e}")
    finally:
        conn.close()


@router.delete("/{trader_id}")
async def api_delete_trader(trader_id: int, db: Session = Depends(get_db)):
    """Delete a trader and cascade delete constraints and portfolios."""
    from src.core.models import Trader
    
    trader = db.query(Trader).filter(Trader.id == trader_id).first()
    if not trader:
        raise HTTPException(status_code=404, detail=f"Trader {trader_id} not found")
    
    db.delete(trader)
    db.commit()
    return {"status": "deleted", "trader_id": trader_id}


class TraderBacktestRequest(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@router.post("/{trader_id}/backtest")
async def api_trader_backtest(trader_id: int, req: TraderBacktestRequest):
    """Run a portfolio-weighted backtest for a trader.

    Each assigned portfolio runs its strategy at its allocated capital.
    Returns per-portfolio curves + a blended combined curve.
    """
    from src.core.trader_manager import get_trader, get_portfolios
    from src.ecs.tournament_system import run_tournament

    trader = get_trader(trader_id)
    if not trader:
        raise HTTPException(status_code=404, detail=f"Trader {trader_id} not found")

    portfolios = get_portfolios(trader_id)
    assigned = [p for p in portfolios if p.get("strategy_id")]

    if not assigned:
        raise HTTPException(
            status_code=400,
            detail="No strategies assigned to any portfolios. Assign strategies first.",
        )

    # Collect unique strategy IDs and portfolio capital map
    strategy_ids = list({p["strategy_id"] for p in assigned})
    # Map: strategy_id → allocated capital (for weighting)
    capital_map = {}
    for p in assigned:
        sid = p["strategy_id"]
        capital_map[sid] = p["allocated_capital"]

    total_capital = sum(capital_map.values())

    # Run the tournament with only the assigned strategies
    results = run_tournament(
        strategy_ids=strategy_ids,
        start_date=req.start_date,
        end_date=req.end_date,
        starting_capital=10000.0,  # normalised base for each strategy
    )

    # Scale each strategy's equity curve by its capital weight
    # and build a combined curve
    combined_by_date = {}
    portfolio_results = {}

    for sid, data in results.get("strategies", {}).items():
        alloc = capital_map.get(sid, 1000.0)
        weight = alloc / total_capital  # e.g. $2500 / $10000 = 0.25

        curve = data["equity_curve"]
        scaled_curve = []
        for point in curve:
            scaled_val = round(point["value"] * weight, 2)
            scaled_curve.append({"date": point["date"], "value": scaled_val})

            if point["date"] not in combined_by_date:
                combined_by_date[point["date"]] = 0.0
            combined_by_date[point["date"]] += scaled_val

        # Find the portfolio(s) using this strategy
        portfolio_name = next(
            (p["name"] for p in assigned if p["strategy_id"] == sid), sid
        )

        portfolio_results[sid] = {
            "name": data["name"],
            "portfolio_name": portfolio_name,
            "allocated_capital": alloc,
            "weight": round(weight, 4),
            "metrics": data["metrics"],
            "equity_curve": scaled_curve,
        }

    # Build combined equity curve
    combined_curve = sorted(
        [{"date": d, "value": round(v, 2)} for d, v in combined_by_date.items()],
        key=lambda x: x["date"],
    )

    # Compute combined metrics from the blended curve
    import numpy as np
    if len(combined_curve) > 1:
        equity_arr = np.array([p["value"] for p in combined_curve])
        daily_rets = np.diff(equity_arr) / equity_arr[:-1]

        sharpe = float(daily_rets.mean() / daily_rets.std() * np.sqrt(252)) if daily_rets.std() > 0 else 0.0
        running_max = np.maximum.accumulate(equity_arr)
        drawdown = 1 - equity_arr / running_max
        max_dd = float(np.max(drawdown))
        total_ret = float(equity_arr[-1] / equity_arr[0] - 1)
        n = len(daily_rets)
        cagr = float((equity_arr[-1] / equity_arr[0]) ** (252 / max(n, 1)) - 1)

        combined_metrics = {
            "sharpe": round(sharpe, 3),
            "max_drawdown": round(max_dd, 4),
            "cagr": round(cagr, 4),
            "total_return": round(total_ret, 4),
            "trading_days": n,
        }
    else:
        combined_metrics = {"sharpe": 0, "max_drawdown": 0, "cagr": 0, "total_return": 0, "trading_days": 0}

    return {
        "trader": {"id": trader_id, "name": trader["name"], "total_capital": total_capital},
        "portfolios": portfolio_results,
        "combined": {
            "name": f"{trader['name']} — Combined",
            "metrics": combined_metrics,
            "equity_curve": combined_curve,
        },
        "benchmark": results.get("benchmark"),
    }

