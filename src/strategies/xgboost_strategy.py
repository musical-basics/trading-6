"""
xgboost_strategy.py — XGBoost AI Meta-Model

Strategy ID: xgboost_ai
Type: PORTFOLIO (ML-weighted)

Logic:
  Uses the risk-adjusted ML weights from target_portfolio (Phase 3a/3b).
  XGBoost WFO trains on 6 features (ev_sales_zscore, dcf_npv_gap, beta_spy,
  beta_10y, beta_vix, dynamic_discount_rate) to predict 20-day forward returns.
  Risk APT then adjusts weights for variance constraints.

  IMPORTANT: Returns are computed on a CONTINUOUS price timeline to avoid
  sparse-data pct_change() teleportation bugs. Weights are merged onto
  the continuous timeline, not the other way around.

  Caps individual stock weights at MAX_SINGLE_WEIGHT (10%).
  Uses YESTERDAY's weight for TODAY's return (no look-ahead bias).

Data Source: target_portfolio + daily_bars (continuous prices)
"""

import pandas as pd
from src.config import DB_PATH, MAX_SINGLE_WEIGHT


def run_xgboost_portfolio(conn, starting_capital=10000):
    """
    Simulates the XGBoost portfolio without look-ahead bias or sparse-data bugs.
    Uses a continuous price timeline to compute true daily returns.
    """
    # 1. Load the CONTINUOUS price matrix for the entire universe
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close
        FROM daily_bars
        ORDER BY ticker, date
    """, conn, parse_dates=["date"])

    if prices.empty:
        return pd.DataFrame(), {}

    # Calculate true daily returns on the contiguous dataset
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()

    # 2. Load the target weights
    weights = pd.read_sql_query("""
        SELECT ticker, date, target_weight
        FROM target_portfolio
    """, conn, parse_dates=["date"])

    if weights.empty:
        return pd.DataFrame(), {}

    # 3. Merge weights onto the continuous price timeline
    df = pd.merge(prices, weights, on=["ticker", "date"], how="left")

    # Fill missing weights with 0 (days we don't hold the stock)
    df["target_weight"] = df["target_weight"].fillna(0)

    # 4. Cap and normalize weights per day
    df["capped_weight"] = df["target_weight"].clip(upper=MAX_SINGLE_WEIGHT)
    daily_cap_sum = df.groupby("date")["capped_weight"].transform("sum")

    # Avoid division by zero on days with no positions
    df["norm_weight"] = df["capped_weight"] / daily_cap_sum.replace(0, 1)

    # 5. CRITICAL: Shift weights by 1 day per ticker
    # The weight computed on Monday's close is traded to capture Tuesday's return
    df = df.sort_values(["ticker", "date"])
    df["actual_weight"] = df.groupby("ticker")["norm_weight"].shift(1).fillna(0)

    # 6. Calculate weighted portfolio return
    df["weighted_return"] = df["actual_weight"] * df["daily_return"].fillna(0)

    # Aggregate to portfolio level
    portfolio = df.groupby("date")["weighted_return"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)

    # Build equity curve
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"]).cumprod()

    return portfolio
