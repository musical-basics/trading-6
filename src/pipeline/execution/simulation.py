"""
simulation.py — Level 1 Phase 3: Simulation & Risk Limits

Part 1: Calculates historical strategy vs buy-and-hold PnL.
Part 2: Filters today's BUY signals based on risk limits.
"""

import sqlite3
import pandas as pd
import math
from datetime import datetime
from src.config import DB_PATH

# ── Configurable Risk Limits ────────────────────────────────
MAX_OPEN_POSITIONS = 5
CAPITAL_PER_TRADE = 1000.0


def simulate_and_filter():
    """
    Run historical simulation and filter today's signals through risk limits.
    Returns a list of approved orders.
    """
    print("=" * 60)
    print("PHASE 3: Simulation & Risk Limits")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── PART 1: Historical Simulation ────────────────────────
    tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM strategy_signals", conn
    )["ticker"].tolist()

    print("\n  ── Historical Strategy vs Buy & Hold ──")

    for ticker in tickers:
        try:
            df = pd.read_sql_query("""
                SELECT s.date, s.signal, b.adj_close
                FROM strategy_signals s
                JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
                WHERE s.ticker = ?
                ORDER BY s.date
            """, conn, params=(ticker,))

            if df.empty or len(df) < 2:
                continue

            df["daily_return"] = df["adj_close"].pct_change()

            # Build position state
            position = 0
            positions = []
            for sig in df["signal"]:
                if sig == 1:
                    position = 1
                elif sig == -1:
                    position = 0
                positions.append(position)

            df["position"] = positions
            df["strategy_return"] = df["daily_return"] * pd.Series(positions).shift(1).values

            strat_cum = (1 + df["strategy_return"].fillna(0)).cumprod().iloc[-1] - 1
            bh_cum = (1 + df["daily_return"].fillna(0)).cumprod().iloc[-1] - 1

            print(f"  {ticker}: Strategy {strat_cum:+.1%}  |  Buy&Hold {bh_cum:+.1%}")

        except Exception as e:
            print(f"  {ticker}: FAILED — {e}")
            continue

    # ── PART 2: Today's Signal Filter ────────────────────────
    print(f"\n  ── Today's Signal Filter (Max {MAX_OPEN_POSITIONS} positions, ${CAPITAL_PER_TRADE:,.0f}/trade) ──")

    today_str = datetime.now().strftime("%Y-%m-%d")

    buy_signals = pd.read_sql_query("""
        SELECT s.ticker, b.adj_close as price
        FROM strategy_signals s
        JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
        WHERE s.signal = 1 AND s.date = ?
    """, conn, params=(today_str,))

    # Check current open positions
    try:
        positions_df = pd.read_sql_query("""
            SELECT ticker,
                   SUM(CASE WHEN action = 'BUY' THEN quantity ELSE -quantity END) as net_shares
            FROM paper_executions
            GROUP BY ticker
            HAVING net_shares > 0
        """, conn)
        current_open = len(positions_df) if not positions_df.empty else 0
    except Exception:
        current_open = 0

    approved_orders = []

    if buy_signals.empty:
        print(f"  ℹ No BUY signals for today ({today_str}).")
    else:
        for _, row in buy_signals.iterrows():
            ticker = row["ticker"]
            price = row["price"]

            # Limit 1: Max open positions
            if current_open >= MAX_OPEN_POSITIONS:
                print(f"  ✗ REJECTED {ticker}: Max {MAX_OPEN_POSITIONS} positions reached")
                continue

            # Limit 2: Position sizing
            quantity = math.floor(CAPITAL_PER_TRADE / price)
            if quantity < 1:
                print(f"  ✗ REJECTED {ticker}: Price ${price:.2f} exceeds ${CAPITAL_PER_TRADE:,.0f} capital")
                continue

            approved_orders.append({
                "ticker": ticker,
                "action": "BUY",
                "quantity": quantity,
                "price": price,
            })
            current_open += 1
            print(f"  ✓ APPROVED BUY {quantity} x {ticker} @ ${price:.2f}")

    conn.close()

    print(f"\n  ✓ {len(approved_orders)} orders approved for execution.")
    print()

    return approved_orders


if __name__ == "__main__":
    simulate_and_filter()
