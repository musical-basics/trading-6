"""
execution.py — FastAPI router for the Execution Ledger.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime

from fastapi import APIRouter
import polars as pl

from src.config import DB_PATH
from src.core.duckdb_store import get_parquet_path, PARQUET_DIR

router = APIRouter(prefix="/api/execution", tags=["execution"])


@router.get("/pending")
async def get_pending_orders():
    """Return pending execution orders from target portfolio vs current state.

    In paper mode, "current state" is from the paper_executions ledger.
    """
    try:
        target = pl.read_parquet(get_parquet_path("target_portfolio"))
        emap = pl.read_parquet(os.path.join(PARQUET_DIR, "entity_map.parquet"))

        latest_date = target["date"].max()
        orders = (
            target.filter(
                (pl.col("date") == latest_date)
                & (pl.col("target_weight").abs() > 0.001)
            )
            .sort("target_weight", descending=True)
            .join(emap, on="entity_id", how="left")
        )

        return {
            "date": str(latest_date),
            "orders": [
                {
                    "id": i + 1,
                    "ticker": row["ticker"],
                    "action": "BUY" if row["target_weight"] > 0 else "SELL",
                    "target_weight": round(row["target_weight"] * 100, 2),
                    "mcr": round(abs(row.get("mcr", 0)) * 100, 2),
                    "status": "pending",
                }
                for i, row in enumerate(orders.to_dicts())
            ],
        }
    except Exception as e:
        return {"date": None, "orders": [], "error": str(e)}


@router.post("/route")
async def route_paper_trades():
    """Lock pending orders into the paper execution ledger (SQLite)."""
    try:
        target = pl.read_parquet(get_parquet_path("target_portfolio"))
        emap = pl.read_parquet(os.path.join(PARQUET_DIR, "entity_map.parquet"))
        market = pl.read_parquet(get_parquet_path("market_data"))

        latest_date = target["date"].max()
        orders = (
            target.filter(
                (pl.col("date") == latest_date)
                & (pl.col("target_weight").abs() > 0.001)
            )
            .join(emap, on="entity_id", how="left")
        )

        # Get latest prices for simulated execution
        latest_prices = (
            market.filter(pl.col("date") == latest_date)
            .select(["entity_id", "adj_close"])
        )
        orders = orders.join(latest_prices, on="entity_id", how="left")

        # Write to SQLite paper_executions
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        count = 0

        for row in orders.to_dicts():
            cursor.execute("""
                INSERT INTO paper_executions (ticker, action, quantity, simulated_price, strategy_id)
                VALUES (?, ?, ?, ?, ?)
            """, (
                row["ticker"],
                "BUY" if row["target_weight"] > 0 else "SELL",
                1,  # Quantity placeholder (weight-based system)
                row.get("adj_close", 0),
                row.get("strategy_id", "ecs_pipeline"),
            ))
            count += 1

        conn.commit()
        conn.close()

        return {
            "status": "routed",
            "orders_routed": count,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

from fastapi import Request

@router.post("/alpaca-webhook")
async def alpaca_webhook(request: Request):
    """
    Webhook receiver for Alpaca Trade Updates.
    Listens for 'fill' or 'partial_fill' events, retrieves pending 
    intents from Redis, and distributes fills to the portfolio ledgers.
    """
    try:
        payload = await request.json()
        event = payload.get("event")
        
        if event not in ("fill", "partial_fill"):
            return {"status": "ignored", "reason": f"Event '{event}' not actionable"}
            
        order_data = payload.get("order", {})
        order_id = order_data.get("id")
        if not order_id:
            return {"status": "error", "message": "Missing order ID in payload"}
            
        # Retrieve pending intents from Redis
        import redis.asyncio as aioredis
        import json
        from src.config import REDIS_URL
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        
        pending_json = await r.get(f"alpaca_order:{order_id}")
        if not pending_json:
            return {"status": "ignored", "reason": "Order ID not found in pending cache"}
            
        pending_data = json.loads(pending_json)
        ticker = pending_data["ticker"]
        intents = pending_data["intents"]
        
        filled_qty = float(payload.get("qty", order_data.get("filled_qty", 0)))
        price = float(payload.get("price", order_data.get("filled_avg_price", 0)))
        
        if filled_qty <= 0:
            return {"status": "ignored", "reason": "Filled qty is zero"}
            
        net_fills = {
            ticker: {
                "filled_qty": filled_qty,
                "avg_price": price,
                "order_id": order_id,
            }
        }
        
        # Distribute fills internally
        from src.pipeline.execution.net_delta import distribute_fills
        from src.pipeline.execution.order_router import _log_execution_orm, _publish_execution_event
        
        executions = distribute_fills(intents, net_fills)
        for exec_record in executions:
            _log_execution_orm(exec_record)
            
            if "price" not in exec_record and "simulated_price" in exec_record:
                exec_record["price"] = exec_record["simulated_price"]
                
            _publish_execution_event({
                "ticker": exec_record["ticker"],
                "action": exec_record["side"],
                "quantity": exec_record["quantity"],
                "price": exec_record["price"],
                "timestamp": payload.get("timestamp", datetime.now().isoformat()),
                "trader_id": exec_record.get("trader_id"),
                "portfolio_id": exec_record.get("portfolio_id"),
                "strategy_id": exec_record.get("strategy_id", "default_ml"),
            })
            
        if event == "fill":
            await r.delete(f"alpaca_order:{order_id}")
            
        return {"status": "success", "distributed_executions": len(executions)}
        
    except Exception as e:
        print(f"  ⚠ Error in Alpaca webhook: {e}")
        return {"status": "error", "message": str(e)}
