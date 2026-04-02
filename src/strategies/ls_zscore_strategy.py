"""
ls_zscore_strategy.py — Long/Short EV/Sales Z-Score Strategy

Strategy rules:
  1. At the start of each rebalance period, rank the universe by EV/Sales Z-score
  2. LONG the N lowest Z-score stocks (cheapest relative to peers)
  3. SHORT the N highest Z-score stocks (most expensive relative to peers)
  4. Hold for the rebalance period, then close all positions and re-rank
  5. Equal weight within each leg

Rebalance frequencies: Weekly, Biweekly, Monthly (default), Quarterly.

This is a dollar-neutral long/short equity strategy — portfolio return
comes from the spread between cheap and expensive stocks, not market direction.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH, SLIPPAGE_BPS


# Supported rebalance frequencies
REBALANCE_OPTIONS = ["Weekly", "Biweekly", "Monthly", "Quarterly"]


def _assign_rebalance_period(dates, freq="Monthly"):
    """Assign a rebalance period label to each date."""
    if freq == "Monthly":
        return dates.dt.to_period("M")
    elif freq == "Quarterly":
        return dates.dt.to_period("Q")
    elif freq == "Weekly":
        # ISO week: year-week
        return dates.dt.isocalendar().year.astype(str) + "-W" + dates.dt.isocalendar().week.astype(str).str.zfill(2)
    elif freq == "Biweekly":
        # Group into 2-week blocks based on day-of-year
        return dates.dt.year.astype(str) + "-BW" + ((dates.dt.isocalendar().week - 1) // 2).astype(str).str.zfill(2)
    else:
        return dates.dt.to_period("M")


def simulate_ls_zscore(n_long=2, n_short=2, starting_capital=10000, rebalance_freq="Monthly"):
    """
    Simulate the long/short rebalance strategy using
    cross_sectional_scores data.

    Args:
        n_long: Number of stocks to go long (lowest Z-scores)
        n_short: Number of stocks to go short (highest Z-scores)
        starting_capital: Initial portfolio value
        rebalance_freq: One of 'Weekly', 'Biweekly', 'Monthly', 'Quarterly'

    Returns:
        equity_df: DataFrame with [date, equity, long_tickers, short_tickers]
        trades_log: list of dicts describing each rebalance
    """
    conn = sqlite3.connect(DB_PATH)

    # Load Z-scores + prices
    df = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_sales_zscore,
               db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        ORDER BY cs.date, cs.ticker
    """, conn, parse_dates=["date"])
    conn.close()

    if df.empty:
        return pd.DataFrame(), []

    # Group by rebalance period
    df["rebal_period"] = _assign_rebalance_period(df["date"], rebalance_freq)

    # Get all unique rebalance periods
    periods = sorted(df["rebal_period"].unique())

    if len(periods) < 2:
        return pd.DataFrame(), []

    # For each period, get the FIRST trading day's Z-scores to select positions
    # Then track daily returns through that period
    trades_log = []
    all_daily_returns = []

    for i, period in enumerate(periods):
        period_data = df[df["rebal_period"] == period].copy()
        if period_data.empty:
            continue

        # Get first day of this period to rank and select
        first_day = period_data["date"].min()
        ranking_day = period_data[period_data["date"] == first_day]

        if len(ranking_day) < (n_long + n_short):
            continue  # Not enough tickers to fill both legs

        # Rank: lowest Z-score = LONG, highest Z-score = SHORT
        sorted_rank = ranking_day.sort_values("ev_sales_zscore")
        long_tickers = sorted_rank.head(n_long)["ticker"].tolist()
        short_tickers = sorted_rank.tail(n_short)["ticker"].tolist()

        # Get daily returns for all days in this period
        all_dates = sorted(period_data["date"].unique())

        for j, date in enumerate(all_dates):
            if j == 0:
                continue  # Skip first day (entry day, no return yet)

            prev_date = all_dates[j - 1]
            day_return = 0.0

            # Long leg: profit from price increases
            for ticker in long_tickers:
                curr = period_data[(period_data["date"] == date) & (period_data["ticker"] == ticker)]
                prev = period_data[(period_data["date"] == prev_date) & (period_data["ticker"] == ticker)]
                if not curr.empty and not prev.empty:
                    ret = (curr["adj_close"].iloc[0] / prev["adj_close"].iloc[0]) - 1
                    day_return += ret / n_long  # Equal weight

            # Short leg: profit from price decreases
            for ticker in short_tickers:
                curr = period_data[(period_data["date"] == date) & (period_data["ticker"] == ticker)]
                prev = period_data[(period_data["date"] == prev_date) & (period_data["ticker"] == ticker)]
                if not curr.empty and not prev.empty:
                    ret = (curr["adj_close"].iloc[0] / prev["adj_close"].iloc[0]) - 1
                    day_return -= ret / n_short  # Invert return for short

            # Deduct friction on rebalance day (first trading day of month)
            if j == 1:
                # Slippage for entire portfolio turnover (all positions change)
                day_return -= SLIPPAGE_BPS * 2  # Both legs rebalance

            all_daily_returns.append({
                "date": date,
                "daily_return": day_return,
                "long_tickers": ", ".join(long_tickers),
                "short_tickers": ", ".join(short_tickers),
            })

        trades_log.append({
            "month": str(period),
            "long": long_tickers,
            "short": short_tickers,
            "long_zscores": sorted_rank.head(n_long)["ev_sales_zscore"].tolist(),
            "short_zscores": sorted_rank.tail(n_short)["ev_sales_zscore"].tolist(),
        })

    if not all_daily_returns:
        return pd.DataFrame(), trades_log

    equity_df = pd.DataFrame(all_daily_returns)
    equity_df["equity"] = starting_capital * (1 + equity_df["daily_return"]).cumprod()

    return equity_df, trades_log


if __name__ == "__main__":
    eq, trades = simulate_ls_zscore()
    if not eq.empty:
        print(f"L/S Z-Score Strategy: {len(eq)} days")
        print(f"Final equity: ${eq['equity'].iloc[-1]:,.2f}")
        total_ret = eq['equity'].iloc[-1] / 10000 - 1
        print(f"Total return: {total_ret:+.2%}")
        print()
        for t in trades:
            print(f"  {t['month']}: LONG {t['long']} | SHORT {t['short']}")
    else:
        print("No data. Run Phase 2 first.")
