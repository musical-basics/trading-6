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
    """Compute strategy performance metrics from equity curve."""
    n = len(daily_returns)

    # Sharpe ratio
    if daily_returns.std() > 0:
        sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252))
    else:
        sharpe = 0.0

    # Max drawdown
    running_max = np.maximum.accumulate(equity)
    drawdown = 1 - equity / running_max
    max_dd = float(np.max(drawdown)) if len(drawdown) > 0 else 0.0

    # CAGR
    if n > 0 and equity[0] > 0:
        total_factor = equity[-1] / equity[0]
        cagr = float(total_factor ** (252 / max(n, 1)) - 1)
    else:
        cagr = 0.0

    # Total return
    total_return = float(equity[-1] / equity[0] - 1) if equity[0] > 0 else 0.0

    return {
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(max_dd, 4),
        "cagr": round(cagr, 4),
        "total_return": round(total_return, 4),
        "trading_days": n,
    }


def _prepare_data() -> pl.DataFrame:
    """Load and merge all components needed for strategy evaluation."""
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

    return df


def run_tournament(
    strategy_ids: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    starting_capital: float = 10000.0,
) -> dict:
    """Run a tournament of strategies and return results.

    Args:
        strategy_ids: List of strategy IDs to evaluate (all if None)
        start_date: Filter start date (None = all data)
        end_date: Filter end date (None = all data)
        starting_capital: Initial portfolio value

    Returns:
        {
            "strategies": {
                "strategy_id": {
                    "name": "Human Name",
                    "metrics": { sharpe, max_drawdown, cagr, total_return },
                    "equity_curve": [ { date, value }, ... ],
                }
            },
            "benchmark": {
                "equity_curve": [ { date, value }, ... ]
            }
        }
    """
    start_time = datetime.now()

    if strategy_ids is None:
        strategy_ids = get_all_strategy_ids()

    print("=" * 60)
    print(f"ECS TOURNAMENT: {len(strategy_ids)} strategies")
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
    print(f"\n{'Strategy':30s} {'Return':>10} {'Sharpe':>10} {'MaxDD':>10} {'CAGR':>10}")
    print("-" * 70)
    for sid, data in results["strategies"].items():
        m = data["metrics"]
        print(f"{data['name']:30s} {m['total_return']:+9.2%} {m['sharpe']:10.2f} "
              f"{m['max_drawdown']:9.2%} {m['cagr']:9.2%}")
