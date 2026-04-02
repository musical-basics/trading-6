"""
low_beta_strategy.py — Low-Beta Anomaly (Defensive Smart Beta)

Strategy ID: low_beta
Type: PORTFOLIO (equal-weight, monthly rebalance)

Logic:
  1. Rank universe cross-sectionally by beta_spy (from factor_betas table).
  2. LONG the bottom 20% (lowest market sensitivity). Monthly rebalance.
  3. Use PREVIOUS month's ranking for current month (no look-ahead).

Why: Exploits the academic behavioral anomaly where boring, low-volatility
     stocks historically generate better risk-adjusted returns (higher Sharpe)
     than flashy, highly volatile stocks. Naturally loads up on consumer
     staples and healthcare — incredibly smooth, low-drawdown equity curve.

Data Source: factor_betas (beta_spy) + daily_bars
"""

import pandas as pd
from src.config import DB_PATH

# ── Strategy Parameters ──────────────────────────────────────
BOTTOM_PCT = 0.20  # Long bottom 20% by beta


def run_low_beta_portfolio(conn, starting_capital=10000, bottom_pct=BOTTOM_PCT):
    """
    Monthly rebalance: long the bottom 20% of stocks ranked by
    beta_spy (lowest market sensitivity). Equal-weight.
    """
    betas = pd.read_sql_query("""
        SELECT ticker, date, beta_spy FROM factor_betas
        ORDER BY date, ticker
    """, conn, parse_dates=["date"])

    if betas.empty:
        return pd.DataFrame(), {}

    betas["month"] = betas["date"].dt.to_period("M")

    # Monthly ranking: use last available beta per month
    month_ends = betas.groupby(["ticker", "month"]).tail(1).copy()
    month_ends = month_ends.dropna(subset=["beta_spy"])
    month_ends["rank_pct"] = month_ends.groupby("month")["beta_spy"].rank(pct=True)
    # Bottom 20% = lowest beta
    month_ends["in_portfolio"] = (month_ends["rank_pct"] <= bottom_pct).astype(int)

    portfolio_map = {}
    for _, row in month_ends[month_ends["in_portfolio"] == 1].iterrows():
        m = row["month"]
        if m not in portfolio_map:
            portfolio_map[m] = []
        portfolio_map[m].append(row["ticker"])

    # Get daily returns
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE ticker != 'SPY' ORDER BY ticker, date
    """, conn, parse_dates=["date"])
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()
    prices["month"] = prices["date"].dt.to_period("M")
    prices = prices.dropna(subset=["daily_return"])

    daily_returns = []
    for _, row in prices.iterrows():
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
