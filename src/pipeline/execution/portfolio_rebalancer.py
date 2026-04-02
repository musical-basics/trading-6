"""
portfolio_rebalancer.py — Level 3 Phase 4: Target Weight Portfolio Rebalancer

Translates target portfolio weights from target_portfolio (risk-adjusted) into
physical buy/sell orders by computing the delta between desired and
actual positions. Applies:
  - Concentration limits (max 10% per stock)
  - Cash buffer (5% minimum cash)
  - Liquidity gating (1% of 30-day ADV max order size)
"""

import sqlite3
import math
import pandas as pd
from datetime import datetime, timedelta
from src.config import (
    DB_PATH, MAX_SINGLE_WEIGHT, CASH_BUFFER, ADV_LOOKBACK, ADV_MAX_PCT
)
from src.pipeline.execution.portfolio_state import get_portfolio_state_by_id
from src.core.database import SessionLocal
from src.core.models import Portfolio, Trader


def extract_portfolio_intents():
    """
    Calculate rebalance orders by comparing target weights against
    current holdings per sub-portfolio. Returns a list of discrete intents.

    Returns:
        list of {ticker, side, quantity, price, target_weight, portfolio_id, trader_id, strategy_id}
    """
    print("=" * 60)
    print("PHASE 4: Hierarchical Portfolio Rebalancer")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    session = SessionLocal()

    # ── Step 1: Load mapping and full feature history ────────
    import os
    import math
    import polars as pl
    from src.config import PROJECT_ROOT
    from src.core.duckdb_store import get_parquet_path

    em_path = os.path.join(PROJECT_ROOT, "data", "components", "entity_map.parquet")
    if not os.path.exists(em_path):
        print("⚠ No entity map found. Run Phase 2 first.")
        conn.close()
        session.close()
        return []

    em_df = pl.read_parquet(em_path)
    entity_to_ticker = dict(zip(em_df["entity_id"], em_df["ticker"]))

    print("  Loading combined ECS data for strategy evaluation...", end=" ")
    from src.ecs.tournament_system import _prepare_data
    df_full = _prepare_data()
    if df_full.is_empty():
        print("⚠ ECS Data empty. Run pipeline first.")
        conn.close()
        session.close()
        return []

    latest_date_expr = df_full.select(pl.max("date")).item()
    print(f"✓ {len(df_full):,} rows (latest date: {latest_date_expr})")

    # ── Step 2: Get current prices ───────────────────────────
    prices = pd.read_sql_query("""
        SELECT ticker, adj_close as price
        FROM daily_bars
        WHERE date = (SELECT MAX(date) FROM daily_bars)
    """, conn)
    price_map = dict(zip(prices["ticker"], prices["price"]))

    # ── Step 3: Extract intents per Portfolio ────────────────
    print("  Calculating sub-portfolio intents...")
    intents = []
    
    portfolios = session.query(Portfolio).all()
    if not portfolios:
        print("  ⚠ No active portfolios found in database.")
    
    from src.ecs.strategy_registry import evaluate_strategies, STRATEGY_REGISTRY
    from src.ecs.risk_system import apply_risk_constraints

    # Find unique valid strategy IDs across all portfolios
    active_sids = set()
    for port in portfolios:
        sid = port.strategy_id or "xgboost"
        if sid in STRATEGY_REGISTRY:
            active_sids.add(sid)
            
    # Evaluate all needed strategies simultaneously on full history
    if active_sids:
        for sid in active_sids:
            try:
                fn = STRATEGY_REGISTRY[sid]
                df_full = fn(df_full)
            except Exception as e:
                print(f"      ⚠ Global evaluation failed for strategy {sid}: {e}")

    df_latest = df_full.filter(pl.col("date") == latest_date_expr)

    for port in portfolios:
        print(f"\n    Portfolio: {port.name} (ID: {port.id})")
        
        strategy_id = port.strategy_id or "xgboost"
        if strategy_id not in STRATEGY_REGISTRY:
            print(f"      ⚠ Strategy {strategy_id} not found in registry. Skipping.")
            continue
            
        weight_col = f"raw_weight_{strategy_id}"
        if weight_col not in df_latest.columns:
            print(f"      ⚠ Strategy {strategy_id} result missing.")
            continue
            
        print(f"      Evaluating strategy: {strategy_id}...")
        
        # Prepare strategy output for risk constraints
        strat_out = df_latest.select(["entity_id", "date", weight_col]).rename({weight_col: "raw_weight"})
        
        try:
            # Apply APT risk constraints to get final risk-adjusted target_weight
            target_df = apply_risk_constraints(strat_out, strategy_col="raw_weight")
        except Exception as e:
            print(f"      ⚠ Error applying risk constraints to {strategy_id}: {e}")
            continue

        if target_df.is_empty():
            print(f"      ⚠ Strategy {strategy_id} generated no target weights.")
            continue

        # Use allocated_capital strictly, NOT drifting total_equity
        allocated_capital = port.allocated_capital
        _, holdings = get_portfolio_state_by_id(port.id)
        print(f"      Allocated Capital=${allocated_capital:,.2f}, {len(holdings)} positions")
        
        # Build dictionary of target weights by ticker
        target_map = {}
        for row in target_df.iter_rows(named=True):
            ticker = entity_to_ticker.get(row["entity_id"])
            if ticker:
                target_map[ticker] = row["target_weight"]

        port_intents = []
        for ticker, target_weight in target_map.items():
            if target_weight <= 0:
                continue
                
            price = price_map.get(ticker)
            if price is None or price <= 0:
                continue

            target_shares = math.floor((allocated_capital * target_weight) / price)
            current_shares = holdings.get(ticker, {}).get("shares", 0)
            delta = target_shares - current_shares

            if delta == 0:
                continue

            side = "BUY" if delta > 0 else "SELL"
            quantity = abs(delta)

            port_intents.append({
                "ticker": ticker,
                "side": side,
                "quantity": quantity,
                "price": price,
                "target_weight": target_weight,
                "portfolio_id": port.id,
                "trader_id": port.trader_id,
                "strategy_id": strategy_id,
            })

        # Liquidate positions not in targets for this portfolio
        for ticker, info in holdings.items():
            if ticker not in target_map or target_map[ticker] <= 0:
                if info["shares"] > 0:
                    price = price_map.get(ticker, info.get("avg_price", 0))
                    port_intents.append({
                        "ticker": ticker,
                        "side": "SELL",
                        "quantity": info["shares"],
                        "price": price,
                        "target_weight": 0.0,
                        "portfolio_id": port.id,
                        "trader_id": port.trader_id,
                        "strategy_id": strategy_id,
                    })
        
        print(f"      ✓ Generated {len(port_intents)} intent(s)")
        intents.extend(port_intents)

    # ── Step 5: Apply ADV liquidity gating globally ──────────
    if intents:
        print("\n  Applying global ADV liquidity gating...", end=" ")
        gated = 0
        cutoff_date = (datetime.now() - timedelta(days=ADV_LOOKBACK + 10)).strftime("%Y-%m-%d")

        # Group intents by ticker to gate the TOTAL quantity
        # Since we are pacing based on global volume, we need to reduce proportional to intent
        import polars as pl
        intent_df = pl.DataFrame(intents)
        
        if not intent_df.is_empty():
            sum_by_ticker = intent_df.group_by("ticker").agg(pl.col("quantity").sum().alias("total_qty"))
            
            for row in sum_by_ticker.iter_rows(named=True):
                ticker = row["ticker"]
                total_qty = row["total_qty"]
                
                adv_result = pd.read_sql_query("""
                    SELECT AVG(volume) as adv
                    FROM (
                        SELECT volume FROM daily_bars
                        WHERE ticker = ? AND date >= ?
                        ORDER BY date DESC
                        LIMIT ?
                    )
                """, conn, params=(ticker, cutoff_date, ADV_LOOKBACK))

                if not adv_result.empty and adv_result["adv"].iloc[0] is not None:
                    adv = adv_result["adv"].iloc[0]
                    max_trade = math.floor(adv * ADV_MAX_PCT)

                    if total_qty > max_trade and max_trade > 0:
                        # Scale down all intents for this ticker proportionally
                        scale = max_trade / total_qty
                        for intent in intents:
                            if intent["ticker"] == ticker:
                                original = intent["quantity"]
                                intent["quantity"] = math.floor(original * scale)
                                gated += 1
                        print(f"\n    ⚠ {ticker}: global intent {total_qty} → {max_trade} (1% ADV)", end="")

        print(f"\n  ✓ {gated} intent items scaled down")

    # ── Sort: SELLs first ────────────────────────────────────
    intents.sort(key=lambda x: (0 if x["side"] == "SELL" else 1, x["ticker"], x.get("portfolio_id", 0)))

    conn.close()
    session.close()

    # ── Print summary ────────────────────────────────────────
    if intents:
        print()
        print(f"  {'SIDE':<6} {'TICKER':<8} {'QTY':>6} {'PRICE':>10} {'PORTFOLIO':>10}")
        print(f"  {'─' * 6} {'─' * 8} {'─' * 6} {'─' * 10} {'─' * 10}")
        for i in intents:
            print(f"  {i['side']:<6} {i['ticker']:<8} {i['quantity']:>6} "
                  f"${i['price']:>9.2f} {i['portfolio_id']:>10}")
    else:
        print("  ✓ All portfolios already at target. No intents needed.")

    print()
    print(f"  ✓ {len(intents)} discrete intents extracted across all portfolios")
    print()

    return intents


if __name__ == "__main__":
    intents = extract_portfolio_intents()
