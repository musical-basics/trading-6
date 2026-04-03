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
    
    # Check if we can use cached aligned data.
    # Newer cache manifest validation may reject synthetic keys; fail open.
    try:
        cached_result = manifest.load_cached("_aligned_data_full")
        if cached_result is not None:
            return cached_result
    except KeyError:
        cached_result = None

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

    # Cache the full aligned result when manifest supports this key.
    try:
        manifest.save_cached("_aligned_data_full", df)
    except KeyError:
        pass
    
    return df


def _compute_metrics(equity: np.ndarray, daily_returns: np.ndarray) -> dict:
    """Compute comprehensive strategy performance metrics from equity curve."""
    n = len(daily_returns)

    # 1) Basic returns
    total_return = float(equity[-1] / equity[0] - 1) if equity[0] > 0 else 0.0
    if n > 0 and equity[0] > 0:
        total_factor = equity[-1] / equity[0]
        years = n / 252.0
        cagr = float(total_factor ** (1 / years) - 1) if years > 0 else 0.0
    else:
        cagr = 0.0

    # 2) Volatility metrics
    daily_vol = float(daily_returns.std()) if len(daily_returns) > 1 else 0.0
    annual_volatility = daily_vol * np.sqrt(252)

    downside_returns = daily_returns[daily_returns < 0]
    downside_vol = float(downside_returns.std()) if len(downside_returns) > 1 else 0.0
    annual_downside_vol = downside_vol * np.sqrt(252)

    # 3) Risk-adjusted returns
    if daily_vol > 0:
        sharpe = float(daily_returns.mean() / daily_vol * np.sqrt(252))
    else:
        sharpe = 0.0

    if annual_downside_vol > 0:
        sortino = float(cagr / annual_downside_vol)
    else:
        sortino = 0.0

    # 4) Drawdown analytics
    running_max = np.maximum.accumulate(equity)
    drawdown = 1 - equity / running_max

    max_dd = float(np.max(drawdown)) if len(drawdown) > 0 else 0.0
    avg_dd = float(np.mean(drawdown[drawdown > 0])) if np.any(drawdown > 0) else 0.0

    calmar = float(cagr / abs(max_dd)) if max_dd != 0 else 0.0

    max_dd_dollars = equity[0] * max_dd
    recovery_factor = (
        float(total_return * equity[0] / max_dd_dollars)
        if max_dd_dollars > 0
        else 0.0
    )

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

    # 5) Consistency metrics
    positive_days = np.sum(daily_returns > 0)
    negative_days = np.sum(daily_returns < 0)
    win_rate = float(positive_days / max(n, 1))

    best_day = float(np.max(daily_returns)) if len(daily_returns) > 0 else 0.0
    worst_day = float(np.min(daily_returns)) if len(daily_returns) > 0 else 0.0

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

    # 6) Profit metrics
    gain_sum = np.sum(daily_returns[daily_returns > 0])
    loss_sum = np.sum(daily_returns[daily_returns < 0])

    profit_factor = float(gain_sum / abs(loss_sum)) if loss_sum != 0 else 0.0
    avg_gain = float(gain_sum / max(positive_days, 1))
    avg_loss = float(loss_sum / max(negative_days, 1)) if negative_days > 0 else 0.0
    expectancy = float((win_rate * avg_gain) + ((1 - win_rate) * avg_loss))

    # 7) Monthly analytics (approximate: 21 trading days per month)
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

    # 8) Distribution metrics
    from scipy import stats

    skewness = float(stats.skew(daily_returns)) if len(daily_returns) > 2 else 0.0
    kurtosis = float(stats.kurtosis(daily_returns)) if len(daily_returns) > 3 else 0.0

    var_95 = float(np.percentile(daily_returns, 5))
    cvar_95 = (
        float(np.mean(daily_returns[daily_returns <= var_95]))
        if np.any(daily_returns <= var_95)
        else var_95
    )

    # 9) Return/risk ratio
    return_risk_ratio = (
        float(total_return / annual_volatility) if annual_volatility > 0 else 0.0
    )

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
