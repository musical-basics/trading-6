"""
pullback_strategy.py — Level 1: "Hello World" Trend Pullback Strategy

Strategy ID: solo_trend_pullback
Type: SOLO

Logic:
  1. Macro Trend Filter: close > 200-day SMA (institutions defending)
  2. Micro Panic Trigger: 3-day RSI < 20 (extreme short-term oversold)
  3. Liquidity Gate: 30-day ADV > 1,000,000 shares

Entry Signal: All 3 conditions met → signal = +1.0
Exit: RSI(3) crosses above 70 (bounce) OR close drops below 200 SMA (thesis broken)

Uses pure Pandas vectorized math. No external TA libraries needed.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH

# ── Strategy Parameters ──────────────────────────────────────
SMA_PERIOD = 200
RSI_PERIOD = 3
RSI_ENTRY_THRESHOLD = 20
RSI_EXIT_THRESHOLD = 70
ADV_PERIOD = 30
ADV_MIN_VOLUME = 1_000_000


def _compute_rsi(series, period):
    """
    Calculate Relative Strength Index using Pandas.
    Uses exponential moving average (Wilder's smoothing) for accuracy.
    """
    delta = series.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def compute_pullback_signals():
    """
    Compute the Trend Pullback strategy signals for all tickers in daily_bars.
    Saves results to the pullback_signals table.
    """
    print("=" * 60)
    print("PHASE 2B: Pullback Strategy Signal Generation")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM daily_bars", conn
    )["ticker"].tolist()

    total_signals = 0

    for ticker in tickers:
        try:
            df = pd.read_sql_query(
                "SELECT date, close, volume FROM daily_bars WHERE ticker = ? ORDER BY date",
                conn, params=(ticker,)
            )

            if len(df) < SMA_PERIOD:
                print(f"  {ticker}: Skipping — only {len(df)} rows (need {SMA_PERIOD})")
                continue

            # ── Step 1: Macro Trend Filter (200 SMA) ─────────
            df["sma_200"] = df["close"].rolling(window=SMA_PERIOD).mean()
            df["above_trend"] = df["close"] > df["sma_200"]

            # ── Step 2: Micro Panic Trigger (3-day RSI) ──────
            df["rsi_3"] = _compute_rsi(df["close"], RSI_PERIOD)
            df["rsi_oversold"] = df["rsi_3"] < RSI_ENTRY_THRESHOLD

            # ── Step 3: Liquidity Gate (30-day ADV) ──────────
            df["adv_30"] = df["volume"].rolling(window=ADV_PERIOD).mean()
            df["liquid"] = df["adv_30"] > ADV_MIN_VOLUME

            # ── Entry Signal: All 3 conditions met ───────────
            df["signal"] = 0.0
            entry_mask = df["above_trend"] & df["rsi_oversold"] & df["liquid"]
            df.loc[entry_mask, "signal"] = 1.0

            # ── Exit Signals ─────────────────────────────────
            df["exit_signal"] = None
            rsi_exit = (df["rsi_3"] > RSI_EXIT_THRESHOLD) & (df["rsi_3"].shift(1) <= RSI_EXIT_THRESHOLD)
            df.loc[rsi_exit, "exit_signal"] = "TAKE_PROFIT"
            trend_break = (~df["above_trend"]) & (df["above_trend"].shift(1))
            df.loc[trend_break, "exit_signal"] = "STOP_LOSS"

            # Drop rows where SMA is NaN
            df = df.dropna(subset=["sma_200", "rsi_3"])
            df["ticker"] = ticker

            # ── Save to pullback_signals ─────────────────────
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO pullback_signals 
                    (ticker, date, close, sma_200, rsi_3, adv_30, signal, exit_signal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker, row["date"], row["close"], row["sma_200"],
                    row["rsi_3"], row["adv_30"], row["signal"], row["exit_signal"]
                ))

            total_signals += len(df)

            entries = (df["signal"] == 1.0).sum()
            exits = df["exit_signal"].notna().sum()
            print(f"  {ticker}: ✓ {len(df)} rows | {entries} entries, {exits} exits")

        except Exception as e:
            print(f"  {ticker}: FAILED — {e}")
            continue

    conn.commit()
    conn.close()

    print()
    print(f"  ✓ Total pullback signal rows saved: {total_signals}")
    print()


def simulate_pullback(ticker, conn):
    """
    Simulate the pullback strategy for a given ticker.
    Returns a DataFrame with equity curve data.
    """
    df = pd.read_sql_query("""
        SELECT p.date, p.close, p.sma_200, p.rsi_3, p.signal, p.exit_signal,
               b.adj_close
        FROM pullback_signals p
        JOIN daily_bars b ON p.ticker = b.ticker AND p.date = b.date
        WHERE p.ticker = ?
        ORDER BY p.date
    """, conn, params=(ticker,))

    if df.empty:
        return df

    df["daily_return"] = df["adj_close"].pct_change()

    # Build position state: enter on signal=1, exit on exit_signal
    position = 0
    positions = []
    for _, row in df.iterrows():
        if row["signal"] == 1.0 and position == 0:
            position = 1
        elif row["exit_signal"] is not None and position == 1:
            position = 0
        positions.append(position)

    df["position"] = positions
    df["strategy_return"] = df["daily_return"] * pd.Series(positions).shift(1).values

    return df


if __name__ == "__main__":
    compute_pullback_signals()
