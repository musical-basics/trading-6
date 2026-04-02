"""
order_router.py — Level 5 Execution Routing

Routes approved orders through a multi-layer execution pipeline:
  1. Sub-portfolio intents → Net-Delta aggregation
  2. Order pacing (TWAP/VWAP slicing for large orders)
  3. Broker submission (Alpaca Live or Paper, with dry-run fallback)
  4. Execution logging via SQLAlchemy ORM (Postgres or SQLite)
  5. Redis pub/sub for real-time WebSocket broadcasting

Supports both paper and live trading based on ALPACA_PAPER env flag.
"""

import os
import math
import time
from datetime import datetime

from src.config import DB_PATH


def _get_alpaca_client():
    """
    Attempt to create an Alpaca API client using alpaca-py.
    Returns None if keys are not configured or connection fails.
    Supports both paper and live mode based on ALPACA_PAPER env var.
    """
    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()
    is_paper = os.getenv("ALPACA_PAPER", "true").lower() in ("true", "1", "yes")

    if not api_key or not secret_key:
        return None

    try:
        from alpaca.trading.client import TradingClient
        client = TradingClient(api_key=api_key, secret_key=secret_key, paper=is_paper)
        client.get_account()
        return client
    except Exception as e:
        print(f"  ⚠ Alpaca API connection failed: {e}")
        print(f"  ⚠ Falling back to dry-run mode.")
        return None


# ── TWAP/VWAP Order Pacing ──────────────────────────────────────

def _should_pace(order: dict, adv: float = 0) -> bool:
    """Determine if an order is large enough to require slicing."""
    notional = order["quantity"] * order["price"]
    # Pace orders > $50k notional or > 1% of ADV
    if notional > 50_000:
        return True
    if adv > 0 and order["quantity"] > (adv * 0.01):
        return True
    return False


def _twap_slices(order: dict, num_slices: int = 5) -> list[dict]:
    """Split a large order into TWAP slices for even execution."""
    base_qty = order["quantity"] // num_slices
    remainder = order["quantity"] % num_slices

    slices = []
    for i in range(num_slices):
        qty = base_qty + (1 if i < remainder else 0)
        if qty > 0:
            slices.append({
                **order,
                "quantity": qty,
                "slice_index": i + 1,
                "total_slices": num_slices,
            })

    return slices


# ── Execution Engine ────────────────────────────────────────────

def _publish_execution_event(event: dict):
    """Broadcast an execution event via Redis pub/sub."""
    try:
        import redis
        from src.config import REDIS_URL
        r = redis.from_url(REDIS_URL)
        import json
        r.publish("execution_events", json.dumps({
            "event_type": "execution",
            "payload": event,
        }))
    except Exception:
        pass  # Redis not available — non-critical


def _log_execution_orm(exec_record: dict):
    """Log execution to Postgres/SQLite via SQLAlchemy ORM."""
    try:
        from src.core.database import SessionLocal
        from src.core.models import PaperExecution

        session = SessionLocal()
        execution = PaperExecution(
            ticker=exec_record["ticker"],
            action=exec_record["side"],
            quantity=exec_record["quantity"],
            simulated_price=exec_record["price"],
            strategy_id=exec_record.get("strategy_id", "sma_crossover"),
            trader_id=exec_record.get("trader_id"),
            portfolio_id=exec_record.get("portfolio_id"),
        )
        session.add(execution)
        session.commit()
        session.close()
    except Exception:
        # Fallback to raw SQLite if ORM not available
        _log_execution_sqlite(exec_record)


def _log_execution_sqlite(exec_record: dict):
    """Fallback: log execution via raw SQLite (backward compat)."""
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO paper_executions
        (timestamp, ticker, action, quantity, simulated_price,
         strategy_id, trader_id, portfolio_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        exec_record["ticker"], exec_record["side"], exec_record["quantity"],
        exec_record["price"], exec_record.get("strategy_id", "sma_crossover"),
        exec_record.get("trader_id"), exec_record.get("portfolio_id"),
    ))
    conn.commit()
    conn.close()


def route_orders(intents: list[dict]):
    """
    Route approved sub-portfolio intents to Alpaca (Paper/Live) or dry-run.
    Implements true Level 5 Net-Delta aggregation:
      1. Aggregates all long/short intents per ticker to a single net order.
      2. Executes bulk net order via Alpaca.
      3. Fractionally ledger fills back to sub-portfolios.

    Args:
        intents: List of dicts (ticker, side, quantity, price, portfolio_id, trader_id, strategy_id)
    """
    from src.pipeline.execution.net_delta import calculate_net_delta, distribute_fills

    print("=" * 60)
    print("PHASE 4: Execution Routing (Net-Delta)")
    print("=" * 60)

    if not intents:
        print("  No intents to route. Pipeline complete.")
        print()
        return

    today_str = datetime.now().strftime("%Y-%m-%d")

    # ── Idempotency Check ────────────────────────────────────
    already_executed = set()
    try:
        from src.core.database import SessionLocal
        from src.core.models import PaperExecution
        from sqlalchemy import func
        session = SessionLocal()
        # Find all tickers executed today
        results = session.query(PaperExecution.ticker).filter(
            func.date(PaperExecution.timestamp) == today_str,
        ).all()
        already_executed = {r.ticker for r in results}
        session.close()
    except Exception:
        # Fallback to SQLite
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM paper_executions WHERE DATE(timestamp) = ?", (today_str,))
        already_executed = {row[0] for row in cursor.fetchall()}
        conn.close()

    if already_executed:
        print(f"  ⚠ Skipped execution for {len(already_executed)} tickers due to idempotency.")
        # Filter out intents for tickers already executed today to avoid duplicate trades
        intents = [i for i in intents if i["ticker"] not in already_executed]

    if not intents:
        print("  No net intents remaining after idempotency check. Pipeline complete.\n")
        return

    # ── 1. Calculate Net-Delta ───────────────────────────────
    net_orders_df = calculate_net_delta(intents)
    if net_orders_df.is_empty():
        print("  Net-Delta perfectly neutral. No broker orders required.\n")
        return

    print("  Aggregated sub-portfolio intents into Net-Delta orders:")
    for row in net_orders_df.iter_rows(named=True):
        print(f"    {row['net_side']} {row['net_quantity']} x {row['ticker']}")
    print()

    # ── 2. Route Net Orders to Broker ────────────────────────
    alpaca = _get_alpaca_client()
    is_live = alpaca is not None
    is_paper = os.getenv("ALPACA_PAPER", "true").lower() in ("true", "1", "yes")

    if is_live:
        mode = "PAPER" if is_paper else "LIVE"
        print(f"  ✓ Connected to Alpaca {mode} Trading API")
    else:
        print("  ℹ Running in DRY-RUN mode (no Alpaca API keys configured)")

    net_fills = {}
    
    # We need a quick map to get the intent 'price' for dry-run
    # Just take the first price for each ticker as reference
    price_map = {}
    for i in intents:
        if i["ticker"] not in price_map:
            price_map[i["ticker"]] = i["price"]

    for row in net_orders_df.iter_rows(named=True):
        ticker = row["ticker"]
        side = row["net_side"]
        quantity = row["net_quantity"]
        fallback_price = price_map.get(ticker, 0.0)

        # Basic TWAP for huge net orders (placeholder for logic)
        slices = _twap_slices({"ticker": ticker, "quantity": quantity, "price": fallback_price}) if _should_pace({"ticker": ticker, "quantity": quantity, "price": fallback_price}, 0) else [{"quantity": quantity}]
        
        total_filled_qty = 0
        total_fill_cost = 0.0
        
        for sl in slices:
            sl_qty = sl["quantity"]
            try:
                if is_live:
                    from alpaca.trading.requests import MarketOrderRequest
                    from alpaca.trading.enums import OrderSide, TimeInForce
                    
                    req = MarketOrderRequest(
                        symbol=ticker,
                        qty=sl_qty,
                        side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
                        time_in_force=TimeInForce.DAY,
                    )
                    
                    order = alpaca.submit_order(order_data=req)
                    print(f"  ✓ ROUTED Net {side} {sl_qty} x {ticker} → Alpaca (ID: {order.id})")
                    
                    # Store pending intents in Redis for the async Webhook/WebSocket to fulfill later 
                    try:
                        import redis
                        import json
                        from src.config import REDIS_URL
                        r = redis.from_url(REDIS_URL)
                        
                        pending_payload = {
                            "ticker": ticker,
                            "intents": [i for i in intents if i["ticker"] == ticker]
                        }
                        # Store with 24h expiry
                        r.setex(f"alpaca_order:{order.id}", 86400, json.dumps(pending_payload))
                        print(f"    (Pending Intents registered to Webhook via Redis)")
                    except Exception as redis_e:
                        print(f"    ⚠ Could not cache pending intents in Redis: {redis_e}")

                else:
                    print(f"  ✓ DRY-RUN Net {side} {sl_qty} x {ticker} @ ~${fallback_price:.2f}")
                    total_filled_qty += sl_qty
                    total_fill_cost += sl_qty * fallback_price
                    
                if len(slices) > 1:
                    time.sleep(0.1)
            except Exception as e:
                print(f"  ✗ FAILED to route {side} {sl_qty} x {ticker}: {e}")

        # Record the net fill for this ticker ONLY in DRY-RUN mode
        if not is_live and total_filled_qty > 0:
            avg_price = total_fill_cost / total_filled_qty if total_filled_qty > 0 else fallback_price
            net_fills[ticker] = {
                "filled_qty": total_filled_qty, 
                "avg_price": avg_price
            }

    # ── 3. Internal Ledger (Distribute Fills) ────────────────
    # Only run synchronously for DRY-RUN. Live/Paper fills handles asynchronously via Webhook/WebSocket
    if not is_live:
        print("\n  Distributing fractional fills back to sub-portfolios...")
        executions = distribute_fills(intents, net_fills)
        
        for exec_record in executions:
            _log_execution_orm(exec_record)
            
            # Ensure price is carried over for the event payload
            if "price" not in exec_record and "simulated_price" in exec_record:
                exec_record["price"] = exec_record["simulated_price"]
                
            _publish_execution_event({
                "ticker": exec_record["ticker"],
                "action": exec_record["side"],
                "quantity": exec_record["quantity"],
                "price": exec_record["price"],
                "timestamp": datetime.now().isoformat(),
                "trader_id": exec_record.get("trader_id"),
                "portfolio_id": exec_record.get("portfolio_id"),
                "strategy_id": exec_record.get("strategy_id", "default_ml"),
            })

        print(f"  ✓ Distributed and logged {len(executions)} fractional executions.\n")
    else:
        print("\n  ✓ Asynchronous execution mode enabled. Master orders routed successfully.")
        print("  ✓ PaperExecution models will naturally update via TradeUpdate Webhook/WebSocket.")
        
    return net_fills


if __name__ == "__main__":
    # Test intent payload
    test_intents = [
        {"ticker": "AAPL", "side": "BUY", "quantity": 5, "price": 400.50, "portfolio_id": 1, "strategy_id": "SMA_5_20"},
        {"ticker": "AAPL", "side": "SELL", "quantity": 2, "price": 400.50, "portfolio_id": 2, "strategy_id": "MACD_DIV"},
    ]
    route_orders(test_intents)
