"""
strategy.py — Level 1 Phase 2: SMA Crossover Signal Generation

Calculates 50-day and 200-day SMAs on adj_close prices and detects
crossover signals (BUY/SELL) using vectorized Pandas operations.
"""

import sqlite3
import pandas as pd
from src.config import DB_PATH


def compute_signals():
    """
    Compute SMA 50/200 crossover signals for all tickers in daily_bars.
    Saves results to the strategy_signals table.
    """
    print("=" * 60)
    print("PHASE 2: Signal Generation (SMA Crossover)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM daily_bars", conn
    )["ticker"].tolist()

    total_signals = 0

    for ticker in tickers:
        try:
            df = pd.read_sql_query(
                "SELECT date, adj_close FROM daily_bars WHERE ticker = ? ORDER BY date",
                conn, params=(ticker,)
            )

            if len(df) < 200:
                print(f"  {ticker}: Skipping — only {len(df)} rows (need 200)")
                continue

            # Calculate SMAs
            df["sma_50"] = df["adj_close"].rolling(window=50).mean()
            df["sma_200"] = df["adj_close"].rolling(window=200).mean()

            # Detect crossovers using .shift(1)
            curr_above = df["sma_50"] > df["sma_200"]
            prev_above = df["sma_50"].shift(1) > df["sma_200"].shift(1)

            df["signal"] = 0
            df.loc[(curr_above) & (~prev_above), "signal"] = 1   # Golden Cross (BUY)
            df.loc[(~curr_above) & (prev_above), "signal"] = -1  # Death Cross (SELL)

            # Drop NaN rows and save
            df = df.dropna(subset=["sma_50", "sma_200"])
            df["ticker"] = ticker

            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO strategy_signals (ticker, date, sma_50, sma_200, signal)
                    VALUES (?, ?, ?, ?, ?)
                """, (ticker, row["date"], row["sma_50"], row["sma_200"], int(row["signal"])))

            total_signals += len(df)

            buy_count = (df["signal"] == 1).sum()
            sell_count = (df["signal"] == -1).sum()
            print(f"  {ticker}: ✓ {len(df)} rows | {buy_count} BUY crossovers, {sell_count} SELL crossovers")

        except Exception as e:
            print(f"  {ticker}: FAILED — {e}")
            continue

    conn.commit()
    conn.close()

    print()
    print(f"  ✓ Total signal rows saved: {total_signals}")
    print()


if __name__ == "__main__":
    compute_signals()
