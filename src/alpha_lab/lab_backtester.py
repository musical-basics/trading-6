"""
lab_backtester.py — Sandboxed backtester for Alpha Lab experiments.

Uses the same metrics computation as the production tournament (Sharpe, MaxDD,
CAGR, Total Return) but reads only from parquet files and writes only to the
isolated alpha_lab directory.
"""

import os
from typing import Optional

import polars as pl
import numpy as np

from src.core.duckdb_store import get_parquet_path
from src.alpha_lab.sandbox_executor import execute_strategy
from src.alpha_lab.alpha_lab_store import (
    save_equity_curve, save_trade_ledger, update_experiment_status, update_experiment_code,
)


def _load_aligned_data() -> pl.DataFrame:
    """Load market + feature data aligned for strategy evaluation.

    Returns a DataFrame with entity_id, date, adj_close, volume, and
    all feature columns — read-only from existing parquet files.
    """
    market_path = get_parquet_path("market_data")
    feature_path = get_parquet_path("feature")

    if not os.path.exists(market_path):
        raise FileNotFoundError("market_data.parquet not found — run the pipeline first")

    market = pl.read_parquet(market_path).select([
        "entity_id", "date", "adj_close", "volume"
    ]).sort(["entity_id", "date"])

    # Join entity_map to add ticker names (enables ticker-specific strategies)
    entity_map_path = os.path.join(os.path.dirname(market_path), "entity_map.parquet")
    if os.path.exists(entity_map_path):
        emap = pl.read_parquet(entity_map_path)
        if "ticker" in emap.columns and "entity_id" in emap.columns:
            market = market.join(
                emap.select(["entity_id", "ticker"]),
                on="entity_id",
                how="left",
            )

    if os.path.exists(feature_path):
        features = pl.read_parquet(feature_path)
        # Join features onto market data
        join_cols = ["entity_id", "date"]
        market = market.join(features, on=join_cols, how="left")

    # Also try to join macro data for VIX/TNX columns
    macro_path = get_parquet_path("macro")
    if os.path.exists(macro_path):
        macro = pl.read_parquet(macro_path)
        # Macro has date-level data (not entity-level), so cross-join by date
        macro_cols = [c for c in macro.columns if c != "date"]
        if macro_cols:
            macro_select = ["date"] + macro_cols
            market = market.join(
                macro.select(macro_select),
                on="date",
                how="left",
            )

    # Also try fundamental data (uses filing_date instead of date)
    fund_path = get_parquet_path("fundamental")
    if os.path.exists(fund_path):
        fund = pl.read_parquet(fund_path)
        if "filing_date" in fund.columns and "entity_id" in fund.columns:
            # Drop date column from fundamental if it somehow exists to prevent conflicts
            if "date" in fund.columns:
                fund = fund.drop("date")
            
            market = market.sort(["entity_id", "date"])
            fund = fund.sort(["entity_id", "filing_date"])
            
            market = market.join_asof(
                fund,
                left_on="date",
                right_on="filing_date",
                by="entity_id",
                strategy="backward",
            )

    return market


def _compute_metrics(equity: pl.DataFrame) -> dict:
    """Compute backtest metrics from equity curve DataFrame.

    Expects columns: date, daily_return, equity
    """
    returns = equity["daily_return"].drop_nulls().to_numpy()
    eq_vals = equity["equity"].to_numpy()

    trading_days = len(returns)

    # Sharpe
    if returns.std() > 0:
        sharpe = float(returns.mean() / returns.std() * np.sqrt(252))
    else:
        sharpe = 0.0

    # Max Drawdown
    running_max = np.maximum.accumulate(eq_vals)
    drawdown = 1 - eq_vals / running_max
    max_dd = float(drawdown.max())

    # CAGR
    if trading_days > 0 and eq_vals[0] > 0:
        total_factor = eq_vals[-1] / eq_vals[0]
        cagr = float(total_factor ** (252 / max(trading_days, 1)) - 1)
    else:
        cagr = 0.0

    # Total Return
    total_return = float(eq_vals[-1] / eq_vals[0] - 1) if eq_vals[0] > 0 else 0.0

    return {
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(max_dd, 4),
        "cagr": round(cagr, 4),
        "total_return": round(total_return, 4),
        "trading_days": trading_days,
    }


def run_raw_backtest(
    strategy_code: str,
    starting_capital: float = 10000.0,
    enable_self_healing: bool = False,
) -> dict:
    """Run backtest on raw strategy code without saving to database.

    Returns dict with 'metrics', 'equity_curve', 'final_code', and '_portfolio_df',
    or 'error'.
    """
    try:
        data = _load_aligned_data()

        result_df = None
        error = None
        max_retries = 2 if enable_self_healing else 0
        current_code = strategy_code

        for attempt in range(max_retries + 1):
            result_df, error = execute_strategy(current_code, data)
            if error is None:
                break

            if attempt < max_retries and "Runtime error" in error:
                try:
                    from src.alpha_lab.strategy_generator import generate_strategy
                    print(f"  🔧 Self-healing attempt {attempt + 1}: {error[:100]}...")
                    fix_prompt = (
                        f"The following strategy code failed with this error:\n"
                        f"```python\n{current_code}\n```\n\n"
                        f"Error:\n{error}\n\n"
                        f"Fix the code so it runs correctly. Return only the corrected function."
                    )
                    fixed = generate_strategy(prompt=fix_prompt, model_tier="haiku")
                    current_code = fixed.code
                except Exception:
                    break
            else:
                break

        if error:
            return {"error": error, "final_code": current_code}

        weight_col = [c for c in result_df.columns if c.startswith("raw_weight_")][0]

        result_df = (
            result_df
            .with_columns([
                pl.col(weight_col).filter(pl.col(weight_col) > 0).sum().over("date").alias("_long_sum"),
                pl.col(weight_col).filter(pl.col(weight_col) < 0).abs().sum().over("date").alias("_short_sum"),
            ])
            .with_columns(
                pl.when(pl.col(weight_col) > 0)
                .then(pl.col(weight_col) / pl.col("_long_sum").clip(1e-8, None))
                .when(pl.col(weight_col) < 0)
                .then(-pl.col(weight_col).abs() / pl.col("_short_sum").clip(1e-8, None))
                .otherwise(0.0)
                .alias("_norm_weight")
            )
            .drop("_long_sum", "_short_sum")
        )

        portfolio = (
            result_df
            .sort(["entity_id", "date"])
            .with_columns(
                (pl.col("adj_close") / pl.col("adj_close").shift(1).over("entity_id") - 1).alias("_daily_ret"),
                (pl.col("_norm_weight") - pl.col("_norm_weight").shift(1).over("entity_id").fill_null(0.0)).abs().alias("_weight_turnover")
            )
            .with_columns(
                # Asset return based on previous day's intended weight, minus 5 bps transaction cost on turnover
                (
                    (pl.col("_norm_weight").shift(1).over("entity_id").fill_null(0.0) * pl.col("_daily_ret").fill_null(0.0))
                    - (pl.col("_weight_turnover") * 0.0005)
                ).alias("_weighted_ret")
            )
            .group_by("date")
            .agg(pl.col("_weighted_ret").sum().alias("daily_return"))
            .sort("date")
            .with_columns(
                (starting_capital * (1 + pl.col("daily_return").fill_null(0)).cum_prod()).alias("equity")
            )
        )

        # ── Trade Ledger Extraction ─────────────────────────────────
        # Compute weight_delta per entity (shifted by 1 day) BEFORE the
        # group_by aggregation that collapses entities into a portfolio return.
        result_with_delta = (
            result_df
            .sort(["entity_id", "date"])
            .with_columns(
                (
                    pl.col("_norm_weight")
                    - pl.col("_norm_weight").shift(1).over("entity_id")
                ).alias("weight_delta")
            )
            .filter(pl.col("weight_delta").abs() > 0.001)
        )

        # Build trade ledger columns
        ledger_cols = ["date", "entity_id", "weight_delta", "_norm_weight"]
        if "ticker" in result_with_delta.columns:
            ledger_cols.append("ticker")
        if "adj_close" in result_with_delta.columns:
            ledger_cols.append("adj_close")
        if "volume" in result_with_delta.columns:
            ledger_cols.append("volume")

        trade_ledger = (
            result_with_delta
            .select([c for c in ledger_cols if c in result_with_delta.columns])
            .rename({"_norm_weight": "norm_weight"})
            .with_columns(
                pl.when(pl.col("weight_delta") > 0)
                .then(pl.lit("BUY"))
                .otherwise(pl.lit("SELL"))
                .alias("action")
            )
        )

        metrics = _compute_metrics(portfolio)

        return {
            "metrics": metrics,
            "equity_curve": portfolio.select(["date", "daily_return", "equity"]).to_dicts(),
            "_portfolio_df": portfolio,
            "_trade_ledger": trade_ledger,
            "final_code": current_code,
        }

    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "final_code": strategy_code}


def run_lab_backtest(
    experiment_id: str,
    strategy_code: str,
    starting_capital: float = 10000.0,
) -> dict:
    """Run a full backtest for an Alpha Lab experiment with self-healing and persistence."""
    update_experiment_status(experiment_id, "backtesting")

    res = run_raw_backtest(
        strategy_code=strategy_code,
        starting_capital=starting_capital,
        enable_self_healing=True,
    )

    if res.get("final_code") != strategy_code:
        update_experiment_code(experiment_id, res["final_code"])

    if "error" in res:
        update_experiment_status(experiment_id, "error", {"error": res["error"]})
        return {"error": res["error"]}

    status = "passed" if res["metrics"]["sharpe"] > 0 else "failed"
    save_equity_curve(experiment_id, res["_portfolio_df"])
    if "_trade_ledger" in res and res["_trade_ledger"] is not None:
        try:
            save_trade_ledger(experiment_id, res["_trade_ledger"])
        except Exception as e:
            print(f"  ⚠️  Trade ledger save failed (non-critical): {e}")
    update_experiment_status(experiment_id, status, res["metrics"])

    return {
        "metrics": res["metrics"],
        "equity_curve": res["equity_curve"],
        "status": status,
    }
