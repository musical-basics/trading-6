"""
lab_backtester.py — Sandboxed backtester for Alpha Lab experiments.

Uses the same metrics computation as the production tournament (Sharpe, MaxDD,
CAGR, Total Return) but reads only from parquet files and writes only to the
isolated alpha_lab directory.
"""

import os

import polars as pl
import numpy as np

from src.config import SLIPPAGE_BPS
from src.core.duckdb_store import get_parquet_path
from src.core.cache_manifest import get_manifest
from src.alpha_lab.sandbox_executor import execute_strategy
from src.alpha_lab.alpha_lab_store import (
    save_equity_curve, save_trade_ledger, update_experiment_status, update_experiment_code,
)


def _load_aligned_data() -> pl.DataFrame:
    """Load market + feature data aligned for strategy evaluation.

    Returns a DataFrame with entity_id, date, adj_close, volume, and
    all feature columns — read-only from existing parquet files.
    
    Uses incremental caching to avoid re-reading unchanged Parquet files.
    """
    from src.ecs.alignment_system import align_fundamentals

    manifest = get_manifest()
    
    # Check if we can use cached aligned data
    cached_result = manifest.load_cached("_aligned_data_full")
    if cached_result is not None:
        return cached_result

    # Keep data prep identical to Strategy Studio tournament system.
    df = align_fundamentals()

    feature_path = get_parquet_path("feature")
    macro_path = get_parquet_path("macro")

    if os.path.exists(feature_path):
        features = pl.read_parquet(feature_path)
        feature_cols = [c for c in features.columns if c not in ("entity_id", "date")]
        for c in feature_cols:
            if c in df.columns:
                df = df.drop(c)
        df = df.join(features, on=["entity_id", "date"], how="left")

    if os.path.exists(macro_path):
        macro = pl.read_parquet(macro_path)
        macro_cols = [c for c in macro.columns if c != "date"]
        for c in macro_cols:
            if c in df.columns:
                df = df.drop(c)
        df = df.join(macro, on="date", how="left")

    entity_map_path = os.path.join(os.path.dirname(get_parquet_path("market_data")), "entity_map.parquet")
    if os.path.exists(entity_map_path):
        emap = pl.read_parquet(entity_map_path)
        benchmark_ids = emap.filter(pl.col("ticker").is_in(["SPY", "QQQ"]))["entity_id"].to_list()
        df = df.filter(~pl.col("entity_id").is_in(benchmark_ids))

        if "ticker" in emap.columns and "entity_id" in emap.columns:
            df = df.join(emap.select(["entity_id", "ticker"]), on="entity_id", how="left")

    # Cache the full aligned result
    manifest.save_cached("_aligned_data_full", df)
    
    return df


def _compute_metrics(equity: np.ndarray, daily_returns: np.ndarray) -> dict:
    """Compute backtest metrics from equity and daily return arrays."""
    trading_days = len(daily_returns)

    # Sharpe
    if daily_returns.std() > 0:
        sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252))
    else:
        sharpe = 0.0

    # Max Drawdown
    running_max = np.maximum.accumulate(equity)
    drawdown = 1 - equity / running_max
    max_dd = float(drawdown.max())

    # CAGR
    if trading_days > 0 and equity[0] > 0:
        total_factor = equity[-1] / equity[0]
        cagr = float(total_factor ** (252 / max(trading_days, 1)) - 1)
    else:
        cagr = 0.0

    # Total Return
    total_return = float(equity[-1] / equity[0] - 1) if equity[0] > 0 else 0.0

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

        if "daily_return" not in result_df.columns:
            result_df = result_df.sort(["entity_id", "date"]).with_columns(
                (pl.col("adj_close") / pl.col("adj_close").shift(1).over("entity_id") - 1).alias("daily_return")
            )

        portfolio = (
            result_df
            .filter(pl.col(weight_col) != 0)
            .with_columns((pl.col(weight_col) * pl.col("daily_return")).alias("_weighted_return"))
            .group_by("date")
            .agg(pl.col("_weighted_return").sum().alias("daily_return"))
            .sort("date")
        )

        if portfolio.is_empty():
            return {"error": "Strategy generated no active positions", "final_code": current_code}

        portfolio = portfolio.with_columns(
            (pl.col("daily_return") - SLIPPAGE_BPS).alias("net_return")
        ).with_columns(
            (starting_capital * (1 + pl.col("net_return").fill_null(0)).cum_prod()).alias("equity"),
            pl.col("net_return").alias("daily_return"),
        )

        # ── Trade Ledger Extraction ─────────────────────────────────
        # Compute weight_delta per entity (shifted by 1 day) BEFORE the
        # group_by aggregation that collapses entities into a portfolio return.
        result_with_delta = (
            result_df
            .sort(["entity_id", "date"])
            .with_columns(
                (
                    pl.col(weight_col)
                    - pl.col(weight_col).shift(1).over("entity_id")
                ).alias("weight_delta")
            )
            .filter(pl.col("weight_delta").abs() > 0.001)
        )

        # Build trade ledger columns
        ledger_cols = ["date", "entity_id", "weight_delta", weight_col]
        if "ticker" in result_with_delta.columns:
            ledger_cols.append("ticker")
        if "adj_close" in result_with_delta.columns:
            ledger_cols.append("adj_close")
        if "volume" in result_with_delta.columns:
            ledger_cols.append("volume")

        trade_ledger = (
            result_with_delta
            .select([c for c in ledger_cols if c in result_with_delta.columns])
            .rename({weight_col: "norm_weight"})
            .with_columns(
                pl.when(pl.col("weight_delta") > 0)
                .then(pl.lit("BUY"))
                .otherwise(pl.lit("SELL"))
                .alias("action")
            )
        )

        metrics = _compute_metrics(
            portfolio["equity"].to_numpy(),
            portfolio["daily_return"].fill_null(0).to_numpy(),
        )

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
