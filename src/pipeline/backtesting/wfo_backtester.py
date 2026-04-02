"""
wfo_backtester.py — Level 2 Phase 3: Walk-Forward Optimization (WFO) Backtester

Evaluates the cross-sectional EV/Sales strategy through rolling Train/Test
windows with mandatory transaction cost friction (slippage + commissions).

The WFO loop:
  1. Train: Find optimal Z-score threshold on lookback window
  2. Test: Apply that threshold on forward window with friction
  3. Roll: Shift forward and repeat
  4. Stitch: Combine all OOS (test) blocks into one valid equity curve
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import (
    DB_PATH, SLIPPAGE_BPS, COMMISSION_PER_SHARE,
    MAX_SINGLE_WEIGHT, CASH_BUFFER,
    WFO_TRAIN_YEARS, WFO_TEST_YEARS, WFO_STEP_YEARS,
    WFO_ZSCORE_CANDIDATES,
)


def run_wfo_tournament(strategy_id="ev_sales_zscore"):
    """
    Run the Walk-Forward Optimization tournament on historical
    cross_sectional_scores data.

    Saves metrics per test window to wfo_results table.
    Returns the stitched OOS equity curve DataFrame.
    """
    print("=" * 60)
    print("PHASE 3: Walk-Forward Optimization (WFO) Tournament")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Load scored data with prices ─────────────────────────
    print("  Loading cross-sectional scores + prices...", end=" ")
    df = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_to_sales, cs.ev_sales_zscore,
               db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        ORDER BY cs.date, cs.ticker
    """, conn, parse_dates=["date"])

    if df.empty:
        print("⚠ No scored data. Run Phase 2 first.")
        conn.close()
        return pd.DataFrame()

    print(f"✓ {len(df):,} rows")

    # ── Compute daily returns per ticker ─────────────────────
    df = df.sort_values(["ticker", "date"])
    df["daily_return"] = df.groupby("ticker")["adj_close"].pct_change()

    # ── Define WFO windows ───────────────────────────────────
    all_dates = sorted(df["date"].unique())
    min_date = all_dates[0]
    max_date = all_dates[-1]

    train_days = int(WFO_TRAIN_YEARS * 252)
    test_days = int(WFO_TEST_YEARS * 252)
    step_days = int(WFO_STEP_YEARS * 252)

    windows = []
    window_start_idx = 0

    while True:
        train_end_idx = window_start_idx + train_days
        test_end_idx = train_end_idx + test_days

        if train_end_idx >= len(all_dates):
            break  # Not enough data for a full train window

        # Get actual dates
        train_start = all_dates[window_start_idx]
        train_end = all_dates[min(train_end_idx, len(all_dates) - 1)]
        test_start = train_end
        test_end = all_dates[min(test_end_idx, len(all_dates) - 1)]

        if train_end_idx >= len(all_dates) - 10:
            break  # Not enough test data

        windows.append({
            "train_start": train_start,
            "train_end": train_end,
            "test_start": test_start,
            "test_end": test_end,
        })

        window_start_idx += step_days
        if window_start_idx >= len(all_dates) - train_days:
            break

    if not windows:
        # If we don't have enough data for a proper train/test split,
        # run with whatever we have as a single window
        print(f"  ⚠ Only {len(all_dates)} days available. Running single-window backtest.")
        mid_idx = len(all_dates) * 2 // 3
        windows = [{
            "train_start": all_dates[0],
            "train_end": all_dates[mid_idx],
            "test_start": all_dates[mid_idx],
            "test_end": all_dates[-1],
        }]

    print(f"  WFO Windows: {len(windows)} (Train={WFO_TRAIN_YEARS}yr, Test={WFO_TEST_YEARS}yr, Step={WFO_STEP_YEARS}yr)")

    # ── WFO Loop ─────────────────────────────────────────────
    cursor = conn.cursor()
    # Clear previous results for this strategy
    cursor.execute("DELETE FROM wfo_results WHERE strategy_id = ?", (strategy_id,))

    oos_equity_parts = []
    cumulative_equity = 1.0

    for i, window in enumerate(windows):
        print(f"\n  ── Window {i+1}/{len(windows)} ──")
        print(f"     Train: {window['train_start'].strftime('%Y-%m-%d')} → {window['train_end'].strftime('%Y-%m-%d')}")
        print(f"     Test:  {window['test_start'].strftime('%Y-%m-%d')} → {window['test_end'].strftime('%Y-%m-%d')}")

        # ── TRAIN: Find optimal Z-score threshold ────────────
        train_data = df[
            (df["date"] >= window["train_start"]) &
            (df["date"] < window["train_end"])
        ].copy()

        best_threshold = WFO_ZSCORE_CANDIDATES[2]  # Default: -1.0
        best_sharpe = -np.inf

        for threshold in WFO_ZSCORE_CANDIDATES:
            sharpe = _simulate_sharpe(train_data, threshold)
            if sharpe > best_sharpe:
                best_sharpe = sharpe
                best_threshold = threshold

        print(f"     Train best: threshold={best_threshold}, Sharpe={best_sharpe:.3f}")

        # ── TEST: Apply threshold with friction ──────────────
        test_data = df[
            (df["date"] >= window["test_start"]) &
            (df["date"] <= window["test_end"])
        ].copy()

        if test_data.empty:
            print("     ⚠ No test data, skipping window")
            continue

        test_equity, test_metrics = _simulate_with_friction(
            test_data, best_threshold, cumulative_equity
        )

        # Save metrics to wfo_results
        cursor.execute("""
            INSERT INTO wfo_results
            (strategy_id, test_window_start, test_window_end,
             sharpe_ratio, max_drawdown, cagr)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            strategy_id,
            window["test_start"].strftime("%Y-%m-%d"),
            window["test_end"].strftime("%Y-%m-%d"),
            test_metrics["sharpe"],
            test_metrics["max_drawdown"],
            test_metrics["cagr"],
        ))

        oos_equity_parts.append(test_equity)
        cumulative_equity = test_equity["equity"].iloc[-1]

        print(f"     Test: Sharpe={test_metrics['sharpe']:.3f}, "
              f"MaxDD={test_metrics['max_drawdown']:.2%}, "
              f"CAGR={test_metrics['cagr']:.2%}")

    conn.commit()

    # ── Stitch OOS equity curve ──────────────────────────────
    if oos_equity_parts:
        stitched = pd.concat(oos_equity_parts, ignore_index=True)
        final_equity = stitched["equity"].iloc[-1]
        total_return = final_equity - 1.0
        print(f"\n  ✓ Stitched OOS equity curve: {len(stitched)} days")
        print(f"  ✓ Total OOS return: {total_return:+.2%}")
    else:
        stitched = pd.DataFrame()
        print("\n  ⚠ No OOS equity data generated")

    conn.close()

    print(f"  ✓ {len(windows)} window results saved to wfo_results")
    print()

    return stitched


def _simulate_sharpe(data, threshold):
    """
    Quick in-sample Sharpe calculation for a given Z-score threshold.
    No friction applied during training (avoids double-fitting on costs).
    """
    data = data.copy()

    # Generate weights: equal weight among stocks with z-score below threshold
    data["weight"] = 0.0
    buy_mask = data["ev_sales_zscore"] < threshold

    if not buy_mask.any():
        return -np.inf

    buy_counts = data.loc[buy_mask].groupby("date")["ticker"].transform("count")
    raw_weight = 1.0 / buy_counts
    data.loc[buy_mask, "weight"] = np.minimum(raw_weight.values, MAX_SINGLE_WEIGHT)

    # Portfolio daily returns (weighted sum per day)
    data["weighted_return"] = data["weight"] * data["daily_return"]
    portfolio_returns = data.groupby("date")["weighted_return"].sum()

    if portfolio_returns.std() == 0:
        return -np.inf

    sharpe = portfolio_returns.mean() / portfolio_returns.std() * np.sqrt(252)
    return sharpe


def _simulate_with_friction(data, threshold, starting_equity=1.0):
    """
    Simulate the test window with friction penalties.

    Returns:
        equity_df: DataFrame with columns [date, equity]
        metrics: dict with sharpe, max_drawdown, cagr
    """
    data = data.copy()

    # Generate target weights
    data["target_weight"] = 0.0
    buy_mask = data["ev_sales_zscore"] < threshold

    if buy_mask.any():
        buy_counts = data.loc[buy_mask].groupby("date")["ticker"].transform("count")
        raw_weight = 1.0 / buy_counts
        data.loc[buy_mask, "target_weight"] = np.minimum(raw_weight.values, MAX_SINGLE_WEIGHT)

        # Enforce cash buffer
        max_total = 1.0 - CASH_BUFFER
        daily_sums = data.groupby("date")["target_weight"].transform("sum")
        over = daily_sums > max_total
        if over.any():
            scale = max_total / daily_sums
            data.loc[over, "target_weight"] *= scale[over]

    # Compute weighted portfolio returns per day
    data["weighted_return"] = data["target_weight"] * data["daily_return"]
    daily_portfolio = data.groupby("date").agg(
        portfolio_return=("weighted_return", "sum"),
        total_weight=("target_weight", "sum"),
    ).reset_index().sort_values("date")

    # ── Friction: detect trade days and deduct costs ──────────
    # Track previous day's weight allocation per ticker to detect trades
    dates = sorted(data["date"].unique())
    prev_weights = {}
    friction_per_day = {}

    for d in dates:
        day_data = data[data["date"] == d]
        current_weights = dict(zip(day_data["ticker"], day_data["target_weight"]))

        # Total absolute weight change
        all_tickers = set(list(prev_weights.keys()) + list(current_weights.keys()))
        total_delta = sum(
            abs(current_weights.get(t, 0) - prev_weights.get(t, 0))
            for t in all_tickers
        )

        # Slippage: proportional to trade value
        slippage_cost = total_delta * SLIPPAGE_BPS

        # Commissions: approximate shares traded
        # Use average price from day_data to estimate share counts
        commission_cost = 0.0
        for t in all_tickers:
            weight_delta = abs(current_weights.get(t, 0) - prev_weights.get(t, 0))
            if weight_delta > 0.0001:
                price_row = day_data[day_data["ticker"] == t]
                if not price_row.empty:
                    price = price_row["adj_close"].iloc[0]
                    # Assume $100k portfolio for commission estimation
                    approx_shares = (weight_delta * 100000) / price
                    commission_cost += approx_shares * COMMISSION_PER_SHARE

        # Normalize commission to portfolio return terms
        friction = slippage_cost + (commission_cost / 100000)
        friction_per_day[d] = friction
        prev_weights = current_weights

    # Apply friction to daily returns
    daily_portfolio["friction"] = daily_portfolio["date"].map(friction_per_day).fillna(0)
    daily_portfolio["net_return"] = daily_portfolio["portfolio_return"] - daily_portfolio["friction"]

    # Build equity curve
    daily_portfolio["equity"] = starting_equity * (1 + daily_portfolio["net_return"]).cumprod()

    # ── Compute metrics ──────────────────────────────────────
    returns = daily_portfolio["net_return"]
    trading_days = len(returns)

    # Sharpe
    if returns.std() > 0:
        sharpe = returns.mean() / returns.std() * np.sqrt(252)
    else:
        sharpe = 0.0

    # Max Drawdown
    equity = daily_portfolio["equity"]
    running_max = equity.expanding().max()
    drawdown = 1 - equity / running_max
    max_dd = drawdown.max()

    # CAGR
    if trading_days > 0 and equity.iloc[0] > 0:
        total_return = equity.iloc[-1] / equity.iloc[0]
        cagr = total_return ** (252 / max(trading_days, 1)) - 1
    else:
        cagr = 0.0

    metrics = {
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "cagr": cagr,
    }

    equity_df = daily_portfolio[["date", "equity"]].copy()
    return equity_df, metrics


if __name__ == "__main__":
    stitched = run_wfo_tournament()
    if not stitched.empty:
        print(f"Stitched equity curve: {len(stitched)} rows")
        print(f"Final equity: {stitched['equity'].iloc[-1]:.4f}")
