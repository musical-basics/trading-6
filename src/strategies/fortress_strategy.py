"""
fortress_strategy.py — Fortress Balance Sheet (Fundamental Quality)

Strategy ID: fortress_bs
Type: PORTFOLIO (equal-weight, monthly rebalance)

Logic:
  1. Calculate Net Cash = cash_and_equivalents - total_debt
  2. Net Cash Yield = Net Cash / Market Cap
  3. Rank cross-sectionally, LONG top 10%. Monthly rebalance.
  4. Use PREVIOUS month's ranking for current month (no look-ahead).

  EXCLUDES financial sector tickers (banks, brokers) because their
  "cash and equivalents" includes customer deposits — not real
  fortress-grade cash on hand.

Why: This is a "Quality" factor — defensive shield in high-rate
     environments where indebted companies get crushed.

Data Source: cross_sectional_scores (market_value) + quarterly_fundamentals
"""

import pandas as pd
from src.config import DB_PATH

# ── Strategy Parameters ──────────────────────────────────────
TOP_PCT = 0.10  # Long top 10% by net cash yield

# Exclude financials — their "cash" is customer deposits
FINANCIALS = {'GS', 'MS', 'BAC', 'JPM', 'WFC', 'C', 'BLK', 'SCHW'}


def run_fortress_portfolio(conn, starting_capital=10000, top_pct=TOP_PCT):
    """
    Monthly rebalance: long the top 10% of stocks ranked by
    Net Cash Yield = (cash - debt) / market_cap. Equal-weight.
    Excludes financial sector tickers.
    """
    scores = pd.read_sql_query("""
        SELECT ticker, date, market_value FROM cross_sectional_scores
        WHERE market_value > 0
        ORDER BY ticker, date
    """, conn, parse_dates=["date"])

    if scores.empty:
        return pd.DataFrame(), {}

    # Exclude financials
    scores = scores[~scores["ticker"].isin(FINANCIALS)]

    fundies = pd.read_sql_query("""
        SELECT ticker, filing_date, cash_and_equivalents, total_debt
        FROM quarterly_fundamentals
        ORDER BY ticker, filing_date
    """, conn, parse_dates=["filing_date"])

    # For each ticker, find the latest fundamental as of each date
    merged_parts = []
    for ticker in scores["ticker"].unique():
        s = scores[scores["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        f = fundies[fundies["ticker"] == ticker].sort_values("filing_date").reset_index(drop=True)
        if f.empty:
            continue
        m = pd.merge_asof(s, f.drop(columns=["ticker"]), left_on="date", right_on="filing_date", direction="backward")
        merged_parts.append(m)

    if not merged_parts:
        return pd.DataFrame(), {}
    merged = pd.concat(merged_parts, ignore_index=True)

    merged["net_cash"] = merged["cash_and_equivalents"].fillna(0) - merged["total_debt"].fillna(0)
    merged["net_cash_yield"] = merged["net_cash"] / merged["market_value"]
    merged["month"] = merged["date"].dt.to_period("M")

    # Drop rows without fundamentals
    merged = merged.dropna(subset=["cash_and_equivalents"])

    # Monthly ranking: last available day per month per ticker
    month_ends = merged.groupby(["ticker", "month"]).tail(1).copy()
    month_ends["rank_pct"] = month_ends.groupby("month")["net_cash_yield"].rank(pct=True)
    month_ends["in_portfolio"] = (month_ends["rank_pct"] >= (1 - top_pct)).astype(int)

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
