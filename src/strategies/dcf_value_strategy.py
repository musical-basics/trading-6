"""
dcf_value_strategy.py — Deep Value DCF Arbitrage (Absolute Valuation)

Strategy ID: dcf_deep_value
Type: PORTFOLIO (equal-weight, daily signal)

Logic:
  1. Each day, check each stock's dcf_npv_gap from ml_features table.
  2. If dcf_npv_gap > 0.15 (stock is 15%+ cheaper than DCF intrinsic value),
     signal = BUY. If gap closes to <= 0.15, signal = EXIT.
  3. All active positions get equal weight.
  4. Use YESTERDAY's signal for TODAY's trade (no look-ahead).

Why: Unlike EV/Sales (which is relative — cheapest in the room), this is
     ABSOLUTE — if the entire market is overvalued, no stocks pass the
     threshold and the strategy holds 100% cash, acting as a natural
     crash defense mechanism.

Data Source: ml_features (dcf_npv_gap) + daily_bars
"""

import pandas as pd
import numpy as np
from src.config import DB_PATH

# ── Strategy Parameters ──────────────────────────────────────
GAP_THRESHOLD = 0.15  # 15% discount to intrinsic value required


def run_dcf_value_portfolio(conn, starting_capital=10000, gap_threshold=GAP_THRESHOLD):
    """
    Long equal-weight any stock where dcf_npv_gap > threshold
    (trading at 15%+ discount to intrinsic value).
    Exit when gap closes to <= threshold.
    """
    features = pd.read_sql_query("""
        SELECT ticker, date, dcf_npv_gap FROM ml_features
        ORDER BY date, ticker
    """, conn, parse_dates=["date"])

    if features.empty:
        return pd.DataFrame(), {}

    # Build daily signal: 1 if gap > threshold, 0 otherwise
    features["signal"] = (features["dcf_npv_gap"] > gap_threshold).astype(int)

    # Get daily returns
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE ticker != 'SPY' ORDER BY ticker, date
    """, conn, parse_dates=["date"])
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()

    # Merge signals with prices
    merged = prices.merge(features[["ticker", "date", "signal"]], on=["ticker", "date"], how="left")
    merged["signal"] = merged["signal"].fillna(0)

    # Use YESTERDAY's signal (no look-ahead)
    merged = merged.sort_values(["ticker", "date"])
    merged["position"] = merged.groupby("ticker")["signal"].shift(1).fillna(0)

    # Count active positions per day for equal-weighting
    active_per_day = merged[merged["position"] == 1].groupby("date")["ticker"].count()
    merged["n_active"] = merged["date"].map(active_per_day).fillna(0)

    # Weighted return: only from active positions, equal-weighted
    merged["weighted_return"] = np.where(
        (merged["position"] == 1) & (merged["n_active"] > 0),
        merged["daily_return"].fillna(0) / merged["n_active"],
        0.0
    )

    portfolio = merged.groupby("date")["weighted_return"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"].fillna(0)).cumprod()

    return portfolio
