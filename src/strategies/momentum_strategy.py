"""
momentum_strategy.py — Cross-Sectional Price Momentum (6-Month)

Strategy ID: momentum_6m
Type: PORTFOLIO (equal-weight, monthly rebalance)

Logic:
  1. On the last trading day of each month, compute trailing 126-day
     (6-month) price return for every stock in the universe.
  2. Rank all stocks by that return (percentile).
  3. LONG the top 20% (biggest winners), equal-weight.
  4. Use PREVIOUS month's ranking for current month (no look-ahead).

Why: Momentum is the opposite of Value — it buys what is going up.
     Historically negatively correlated with EV/Sales strategies.

Data Source: daily_bars (adj_close)
"""

import pandas as pd
from src.config import DB_PATH

# ── Strategy Parameters ──────────────────────────────────────
LOOKBACK_DAYS = 126       # 6-month trailing return period
TOP_PCT = 0.20            # Long top 20% of stocks


def run_momentum_portfolio(conn, starting_capital=10000, lookback=LOOKBACK_DAYS, top_pct=TOP_PCT):
    """
    Monthly rebalance: long the top 20% of stocks ranked by
    trailing 6-month (126-day) price return. Equal-weight.
    """
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE ticker != 'SPY'
        ORDER BY ticker, date
    """, conn, parse_dates=["date"])

    if prices.empty:
        return pd.DataFrame(), {}

    # Compute trailing returns
    prices = prices.sort_values(["ticker", "date"])
    prices["ret_126"] = prices.groupby("ticker")["adj_close"].pct_change(periods=lookback)
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()
    prices["month"] = prices["date"].dt.to_period("M")

    # Monthly ranking: on last day of each month, rank by trailing return
    month_ends = prices.groupby(["ticker", "month"]).tail(1).copy()
    month_ends = month_ends.dropna(subset=["ret_126"])
    month_ends["rank_pct"] = month_ends.groupby("month")["ret_126"].rank(pct=True)
    # Top 20% = momentum winners
    month_ends["in_portfolio"] = (month_ends["rank_pct"] >= (1 - top_pct)).astype(int)

    # Build a month→set of tickers map
    portfolio_map = {}
    for _, row in month_ends[month_ends["in_portfolio"] == 1].iterrows():
        m = row["month"]
        if m not in portfolio_map:
            portfolio_map[m] = []
        portfolio_map[m].append(row["ticker"])

    # Compute daily portfolio returns
    prices = prices.dropna(subset=["daily_return"])
    daily_returns = []
    for _, row in prices.iterrows():
        # Use PREVIOUS month's selection (no look-ahead)
        prev_month = row["month"] - 1
        tickers_in = portfolio_map.get(prev_month, [])
        if row["ticker"] in tickers_in:
            daily_returns.append({"date": row["date"], "ret": row["daily_return"] / len(tickers_in)})

    if not daily_returns:
        return pd.DataFrame(), {}

    portfolio = pd.DataFrame(daily_returns).groupby("date")["ret"].sum().reset_index()
    portfolio.columns = ["date", "daily_return"]
    portfolio = portfolio.sort_values("date").reset_index(drop=True)
    portfolio["equity"] = starting_capital * (1 + portfolio["daily_return"].fillna(0)).cumprod()

    return portfolio
