"""
tournament_system.py — Level 4 ECS System 5: Vectorized Tournament Backtester

Runs multiple strategies in parallel using Polars column operations:
  1. Load market_data + feature components ONCE.
  2. Apply all requested strategy functions (adds raw_weight columns).
  3. Compute equity curves per strategy using vectorized operations.
  4. Calculate metrics (Sharpe, MaxDD, CAGR, Total Return).
  5. Apply vectorized friction (slippage as weight_change * SLIPPAGE_BPS).

Because Polars evaluates column expressions in parallel using SIMD,
running 12 strategies takes virtually the same time as running 1.
"""

from __future__ import annotations

import os
from datetime import datetime

import numpy as np
import polars as pl

from src.config import SLIPPAGE_BPS
from src.core.duckdb_store import get_parquet_path
from src.ecs.strategy_registry import (
    STRATEGY_REGISTRY, STRATEGY_NAMES,
    evaluate_strategies, get_all_strategy_ids,
)


def _compute_metrics(equity: np.ndarray, daily_returns: np.ndarray) -> dict:
    """Compute comprehensive strategy performance metrics from equity curve.
    
    Includes:
    - Basic returns: total return, CAGR
    - Risk-adjusted returns: Sharpe, Sortino, Calmar, recovery factor
    - Consistency: win rate, consecutive wins/losses
    - Drawdown analytics: max drawdown, average drawdown, duration
    - Statistics: volatility, skewness, kurtosis, VaR
    """
    n = len(daily_returns)
    
    # ──────────────────────────────────────────────────────────
    # 1. RETURN METRICS
    # ──────────────────────────────────────────────────────────
    total_return = float(equity[-1] / equity[0] - 1) if equity[0] > 0 else 0.0
    
    # CAGR: Compound Annual Growth Rate
    if n > 0 and equity[0] > 0:
        total_factor = equity[-1] / equity[0]
        years = n / 252.0
        cagr = float(total_factor ** (1 / years) - 1) if years > 0 else 0.0
    else:
        cagr = 0.0
    
    # ──────────────────────────────────────────────────────────
    # 2. VOLATILITY METRICS
    # ──────────────────────────────────────────────────────────
    daily_vol = float(daily_returns.std()) if len(daily_returns) > 1 else 0.0
    annual_volatility = daily_vol * np.sqrt(252)
    
    # Downside volatility (only negative returns)
    downside_returns = daily_returns[daily_returns < 0]
    downside_vol = float(downside_returns.std()) if len(downside_returns) > 1 else 0.0
    annual_downside_vol = downside_vol * np.sqrt(252)
    
    # ──────────────────────────────────────────────────────────
    # 3. SHARPE RATIO & RISK-ADJUSTED RETURNS
    # ──────────────────────────────────────────────────────────
    if daily_vol > 0:
        sharpe = float(daily_returns.mean() / daily_vol * np.sqrt(252))
    else:
        sharpe = 0.0
    
    # Sortino ratio (uses downside volatility)
    if annual_downside_vol > 0:
        sortino = float(cagr / annual_downside_vol)
    else:
        sortino = 0.0
    
    # ──────────────────────────────────────────────────────────
    # 4. DRAWDOWN ANALYTICS
    # ──────────────────────────────────────────────────────────
    running_max = np.maximum.accumulate(equity)
    drawdown = 1 - equity / running_max
    
    max_dd = float(np.max(drawdown)) if len(drawdown) > 0 else 0.0
    avg_dd = float(np.mean(drawdown[drawdown > 0])) if np.any(drawdown > 0) else 0.0
    
    # Calmar ratio (CAGR / abs(max drawdown))
    calmar = float(cagr / abs(max_dd)) if max_dd != 0 else 0.0
    
    # Recovery factor (total return / max drawdown in dollars)
    max_dd_dollars = equity[0] * max_dd  # Dollar amount of max drawdown
    recovery_factor = float(total_return * equity[0] / max_dd_dollars) if max_dd_dollars > 0 else 0.0
    
    # Drawdown duration (longest consecutive days in drawdown)
    in_drawdown = drawdown > 0
    dd_durations = []
    current_duration = 0
    for in_dd in in_drawdown:
        if in_dd:
            current_duration += 1
        else:
            if current_duration > 0:
                dd_durations.append(current_duration)
            current_duration = 0
    if current_duration > 0:
        dd_durations.append(current_duration)
    
    max_dd_duration = max(dd_durations) if dd_durations else 0
    avg_dd_duration = float(np.mean(dd_durations)) if dd_durations else 0.0
    
    # ──────────────────────────────────────────────────────────
    # 5. CONSISTENCY METRICS
    # ──────────────────────────────────────────────────────────
    positive_days = np.sum(daily_returns > 0)
    negative_days = np.sum(daily_returns < 0)
    zero_days = np.sum(daily_returns == 0)
    
    win_rate = float(positive_days / max(n, 1))
    
    # Best/Worst day
    best_day = float(np.max(daily_returns)) if len(daily_returns) > 0 else 0.0
    worst_day = float(np.min(daily_returns)) if len(daily_returns) > 0 else 0.0
    
    # Consecutive metrics
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    current_wins = 0
    current_losses = 0
    
    for ret in daily_returns:
        if ret > 0:
            current_wins += 1
            max_consecutive_wins = max(max_consecutive_wins, current_wins)
            current_losses = 0
        elif ret < 0:
            current_losses += 1
            max_consecutive_losses = max(max_consecutive_losses, current_losses)
            current_wins = 0
        else:
            current_wins = 0
            current_losses = 0
    
    # ──────────────────────────────────────────────────────────
    # 6. PROFIT FACTOR & EXPECTANCY
    # ──────────────────────────────────────────────────────────
    gain_sum = np.sum(daily_returns[daily_returns > 0])
    loss_sum = np.sum(daily_returns[daily_returns < 0])
    
    profit_factor = float(gain_sum / abs(loss_sum)) if loss_sum != 0 else 0.0
    avg_gain = float(gain_sum / max(positive_days, 1))
    avg_loss = float(loss_sum / max(negative_days, 1)) if negative_days > 0 else 0.0
    expectancy = float((win_rate * avg_gain) + ((1 - win_rate) * avg_loss))
    
    # ──────────────────────────────────────────────────────────
    # 7. MONTHLY ANALYTICS
    # ──────────────────────────────────────────────────────────
    # Group returns by month (simplified: every 21 trading days ≈ 1 month)
    monthly_returns = []
    for i in range(0, len(daily_returns), 21):
        month_end_idx = min(i + 21, len(daily_returns))
        month_rets = daily_returns[i:month_end_idx]
        if len(month_rets) > 0:
            monthly_return = float(np.prod(1 + month_rets) - 1)
            monthly_returns.append(monthly_return)
    
    positive_months = sum(1 for mr in monthly_returns if mr > 0)
    negative_months = sum(1 for mr in monthly_returns if mr < 0)
    
    best_month = float(np.max(monthly_returns)) if monthly_returns else 0.0
    worst_month = float(np.min(monthly_returns)) if monthly_returns else 0.0
    
    # ──────────────────────────────────────────────────────────
    # 8. DISTRIBUTION METRICS
    # ──────────────────────────────────────────────────────────
    from scipy import stats
    
    # Skewness (tail risk)
    skewness = float(stats.skew(daily_returns)) if len(daily_returns) > 2 else 0.0
    
    # Kurtosis (frequency of extreme returns)
    kurtosis = float(stats.kurtosis(daily_returns)) if len(daily_returns) > 3 else 0.0
    
    # Value at Risk (95% confidence)
    var_95 = float(np.percentile(daily_returns, 5))
    
    # Conditional Value at Risk (Expected Shortfall)
    cvar_95 = float(np.mean(daily_returns[daily_returns <= var_95])) if np.any(daily_returns <= var_95) else var_95
    
    # ──────────────────────────────────────────────────────────
    # 9. RETURN/RISK RATIO
    # ──────────────────────────────────────────────────────────
    return_risk_ratio = float(total_return / annual_volatility) if annual_volatility > 0 else 0.0
    
    # ──────────────────────────────────────────────────────────
    # RETURN COMPREHENSIVE METRICS DICT
    # ──────────────────────────────────────────────────────────
    return {
        # Basic returns
        "total_return": round(total_return, 4),
        "cagr": round(cagr, 4),
        "trading_days": n,
        
        # Volatility
        "volatility": round(annual_volatility, 4),
        "downside_volatility": round(annual_downside_vol, 4),
        
        # Risk-adjusted returns
        "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3),
        "calmar": round(calmar, 3),
        "return_risk_ratio": round(return_risk_ratio, 3),
        
        # Drawdown analytics
        "max_drawdown": round(max_dd, 4),
        "avg_drawdown": round(avg_dd, 4),
        "max_drawdown_duration": max_dd_duration,
        "avg_drawdown_duration": round(avg_dd_duration, 1),
        "recovery_factor": round(recovery_factor, 3),
        
        # Consistency
        "win_rate": round(win_rate, 3),
        "positive_days": int(positive_days),
        "negative_days": int(negative_days),
        "best_day": round(best_day, 4),
        "worst_day": round(worst_day, 4),
        "max_consecutive_wins": int(max_consecutive_wins),
        "max_consecutive_losses": int(max_consecutive_losses),
        
        # Profit metrics
        "profit_factor": round(profit_factor, 3),
        "avg_gain": round(avg_gain, 4),
        "avg_loss": round(avg_loss, 4),
        "expectancy": round(expectancy, 4),
        
        # Monthly analytics
        "positive_months": int(positive_months),
        "negative_months": int(negative_months),
        "best_month": round(best_month, 4),
        "worst_month": round(worst_month, 4),
        
        # Distribution metrics
        "skewness": round(skewness, 3),
        "kurtosis": round(kurtosis, 3),
        "var_95": round(var_95, 4),
        "cvar_95": round(cvar_95, 4),
    }


def _prepare_data(tickers: list[str] | None = None) -> pl.DataFrame:
    """Load and merge all components needed for strategy evaluation.
    
    Args:
        tickers: Optional list of tickers to filter by. If None, uses all non-benchmark tickers.
    """
    from src.ecs.alignment_system import align_fundamentals
    df = align_fundamentals()

    # Load optional components
    feature_path = get_parquet_path("feature")
    macro_path = get_parquet_path("macro")

    # Merge features if available
    if os.path.exists(feature_path):
        features = pl.read_parquet(feature_path)
        feature_cols = [c for c in features.columns if c not in ("entity_id", "date")]
        # Avoid duplicates
        for c in feature_cols:
            if c in df.columns:
                df = df.drop(c)
        df = df.join(features, on=["entity_id", "date"], how="left")

    # Merge macro if available
    if os.path.exists(macro_path):
        macro = pl.read_parquet(macro_path)
        macro_cols = [c for c in macro.columns if c != "date"]
        for c in macro_cols:
            if c in df.columns:
                df = df.drop(c)
        df = df.join(macro, on="date", how="left")

    # Load entity map for ticker names
    entity_map_path = os.path.join(os.path.dirname(get_parquet_path("market_data")), "entity_map.parquet")
    if os.path.exists(entity_map_path):
        emap = pl.read_parquet(entity_map_path)
        
        # Exclude benchmark tickers (SPY, QQQ) from strategies
        benchmark_ids = emap.filter(pl.col("ticker").is_in(["SPY", "QQQ"]))["entity_id"].to_list()
        df = df.filter(~pl.col("entity_id").is_in(benchmark_ids))
        
        # Apply ticker filter if provided
        if tickers is not None:
            ticker_ids = emap.filter(pl.col("ticker").is_in(tickers))["entity_id"].to_list()
            df = df.filter(pl.col("entity_id").is_in(ticker_ids))

    return df


def run_tournament(
    strategy_ids: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    starting_capital: float = 10000.0,
    tickers: list[str] | None = None,
) -> dict:
    """Run a tournament of strategies and return comprehensive results.

    Args:
        strategy_ids: List of strategy IDs to evaluate (all if None)
        start_date: Filter start date (None = all data)
        end_date: Filter end date (None = all data)
        starting_capital: Initial portfolio value
        tickers: Optional list of tickers to run strategies on (all if None)

    Returns:
        {
            "strategies": {
                "strategy_id": {
                    "name": "Human Name",
                    "metrics": {
                        # Returns
                        "total_return": float,
                        "cagr": float,
                        "trading_days": int,
                        
                        # Volatility & Risk
                        "volatility": float (annualized),
                        "downside_volatility": float,
                        "max_drawdown": float,
                        "avg_drawdown": float,
                        "max_drawdown_duration": int (days),
                        
                        # Risk-Adjusted Returns
                        "sharpe": float,
                        "sortino": float,
                        "calmar": float,
                        "recovery_factor": float,
                        "return_risk_ratio": float,
                        
                        # Consistency
                        "win_rate": float,
                        "positive_days": int,
                        "negative_days": int,
                        "best_day": float,
                        "worst_day": float,
                        "max_consecutive_wins": int,
                        "max_consecutive_losses": int,
                        
                        # Profit Metrics
                        "profit_factor": float,
                        "avg_gain": float,
                        "avg_loss": float,
                        "expectancy": float,
                        
                        # Monthly Analytics
                        "positive_months": int,
                        "negative_months": int,
                        "best_month": float,
                        "worst_month": float,
                        
                        # Distribution (risk) Metrics
                        "skewness": float,
                        "kurtosis": float,
                        "var_95": float,
                        "cvar_95": float,
                    },
                    "equity_curve": [ { date, value }, ... ],
                }
            },
            "benchmark": {
                "name": "SPY",
                "metrics": {...same as above...},
                "equity_curve": [ { date, value }, ... ]
            }
        }
    """
    start_time = datetime.now()

    if strategy_ids is None:
        strategy_ids = get_all_strategy_ids()

    print("=" * 60)
    print(f"ECS TOURNAMENT: {len(strategy_ids)} strategies")
    if tickers:
        print(f"  Universe: {len(tickers)} tickers")
    print("=" * 60)

    # Load data ONCE
    df = _prepare_data()

    if start_date:
        df = df.filter(pl.col("date") >= pl.lit(start_date).str.to_date())
    if end_date:
        df = df.filter(pl.col("date") <= pl.lit(end_date).str.to_date())

    print(f"  Data: {len(df):,} rows, {df['entity_id'].n_unique()} entities")

    # Apply all strategies (adds raw_weight columns)
    df = evaluate_strategies(df, strategy_ids)

    # Get unique sorted dates
    dates = df["date"].unique().sort().to_list()

    results: dict = {"strategies": {}, "benchmark": None}

    # Compute equity curves per strategy
    for sid in strategy_ids:
        weight_col = f"raw_weight_{sid}"
        if weight_col not in df.columns:
            print(f"  ⚠ {sid}: weight column not found")
            continue

        # Portfolio return = sum(weight_i * return_i) per date
        portfolio_df = (
            df.filter(pl.col(weight_col) != 0)
            .with_columns(
                (pl.col(weight_col) * pl.col("daily_return")).alias("_weighted_return")
            )
            .group_by("date")
            .agg(pl.col("_weighted_return").sum().alias("daily_return"))
            .sort("date")
        )

        if portfolio_df.is_empty():
            print(f"  ⚠ {sid}: no active positions")
            continue

        # Apply friction (vectorized)
        portfolio_df = portfolio_df.with_columns(
            (pl.col("daily_return") - SLIPPAGE_BPS).alias("net_return")
        )

        # Compute equity curve
        net_returns = portfolio_df["net_return"].fill_null(0).to_numpy()
        equity = starting_capital * np.cumprod(1 + net_returns)
        portfolio_dates = portfolio_df["date"].to_list()

        metrics = _compute_metrics(equity, net_returns)

        display_name = STRATEGY_NAMES.get(sid, sid)
        print(f"  {display_name:30s} | Return: {metrics['total_return']:+.2%} | "
              f"Sharpe: {metrics['sharpe']:.2f} | MaxDD: {metrics['max_drawdown']:.2%}")

        results["strategies"][sid] = {
            "name": display_name,
            "metrics": metrics,
            "equity_curve": [
                {"date": str(d), "value": round(float(v), 2)}
                for d, v in zip(portfolio_dates, equity)
            ],
        }

    # Compute SPY benchmark
    market = pl.read_parquet(get_parquet_path("market_data"))
    entity_map_path = os.path.join(os.path.dirname(get_parquet_path("market_data")), "entity_map.parquet")
    if os.path.exists(entity_map_path):
        emap = pl.read_parquet(entity_map_path)
        spy_ids = emap.filter(pl.col("ticker") == "SPY")["entity_id"].to_list()
        if spy_ids:
            spy_df = market.filter(pl.col("entity_id") == spy_ids[0]).sort("date")
            if start_date:
                spy_df = spy_df.filter(pl.col("date") >= pl.lit(start_date).str.to_date())
            if end_date:
                spy_df = spy_df.filter(pl.col("date") <= pl.lit(end_date).str.to_date())

            spy_returns = spy_df["daily_return"].fill_null(0).to_numpy()
            spy_equity = starting_capital * np.cumprod(1 + spy_returns)
            spy_dates = spy_df["date"].to_list()

            results["benchmark"] = {
                "name": "SPY",
                "metrics": _compute_metrics(spy_equity, spy_returns),
                "equity_curve": [
                    {"date": str(d), "value": round(float(v), 2)}
                    for d, v in zip(spy_dates, spy_equity)
                ],
            }

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n  ✓ Tournament complete in {elapsed:.1f}s")
    print(f"  {len(results['strategies'])} strategies evaluated")

    return results


if __name__ == "__main__":
    results = run_tournament()
    
    # Print comprehensive metrics table
    print("\n" + "=" * 160)
    print(f"{'Strategy':25s} {'Return':>10} {'CAGR':>10} {'Sharpe':>8} {'Sortino':>8} {'Calmar':>8} "
          f"{'Vol':>8} {'MaxDD':>8} {'WR':>7} {'FF':>6} {'RecFac':>8}")
    print("-" * 160)
    
    for sid, data in results["strategies"].items():
        m = data["metrics"]
        print(f"{data['name']:25s} {m['total_return']:+9.2%} {m['cagr']:+9.2%} "
              f"{m['sharpe']:8.2f} {m['sortino']:8.2f} {m['calmar']:8.2f} "
              f"{m['volatility']:8.2%} {m['max_drawdown']:8.2%} "
              f"{m['win_rate']:7.1%} {m['profit_factor']:6.2f} {m['recovery_factor']:8.2f}")
    
    # Print benchmark if available
    if results.get("benchmark"):
        print("-" * 160)
        bm = results["benchmark"]["metrics"]
        print(f"{'SPY (Benchmark)':25s} {bm['total_return']:+9.2%} {bm['cagr']:+9.2%} "
              f"{bm['sharpe']:8.2f} {bm['sortino']:8.2f} {bm['calmar']:8.2f} "
              f"{bm['volatility']:8.2%} {bm['max_drawdown']:8.2%} "
              f"{bm['win_rate']:7.1%} {bm['profit_factor']:6.2f} {bm['recovery_factor']:8.2f}")
    
    # Extended metrics detail section
    print("\n" + "=" * 160)
    print("EXTENDED METRICS DETAIL")
    print("=" * 160)
    
    for sid, data in results["strategies"].items():
        m = data["metrics"]
        print(f"\n{data['name']} ({sid})")
        print(f"  Returns        → Total: {m['total_return']:+.2%} | CAGR: {m['cagr']:+.2%}")
        print(f"  Risk-Adjusted  → Sharpe: {m['sharpe']:.3f} | Sortino: {m['sortino']:.3f} | "
              f"Calmar: {m['calmar']:.3f} | Return/Risk: {m['return_risk_ratio']:.3f}")
        print(f"  Volatility     → Annual: {m['volatility']:.2%} | Downside: {m['downside_volatility']:.2%}")
        print(f"  Drawdown       → Max: {m['max_drawdown']:.2%} | Avg: {m['avg_drawdown']:.2%} | "
              f"Duration: {m['max_drawdown_duration']}d (avg {m['avg_drawdown_duration']:.0f}d)")
        print(f"  Win Rate       → {m['win_rate']:.1%} | Pos Days: {m['positive_days']} | Neg Days: {m['negative_days']}")
        print(f"  Streaks        → Max Wins: {m['max_consecutive_wins']} | Max Losses: {m['max_consecutive_losses']}")
        print(f"  P&L Metrics    → Profit Factor: {m['profit_factor']:.3f} | Avg Gain: {m['avg_gain']:.4f} | "
              f"Avg Loss: {m['avg_loss']:.4f} | Expectancy: {m['expectancy']:.4f}")
        print(f"  Months         → Positive: {m['positive_months']} | Negative: {m['negative_months']} | "
              f"Best: {m['best_month']:+.2%} | Worst: {m['worst_month']:+.2%}")
        print(f"  Distribution   → Skewness: {m['skewness']:.3f} | Kurtosis: {m['kurtosis']:.3f}")
        print(f"  Risk Measures  → VaR(95%): {m['var_95']:.4f} | CVaR(95%): {m['cvar_95']:.4f}")
