"""
strategy_registry.py — Level 4 ECS System 3: Strategy Registry & Executor

Each strategy is a function that takes a LazyFrame/DataFrame of aligned data
and returns a new column of raw weights. The registry maps strategy names to
these functions so the tournament system can evaluate them in parallel.

Strategies are categorized:
  - Heuristic: Pure expression-based (EV/Sales, SMA, Pullback, Momentum, etc.)
  - ML: Requires model loading (XGBoost)
  - Macro: Uses macro regime signals (Regime, Regime V2)
"""

from __future__ import annotations

from typing import Callable

import polars as pl
import numpy as np

from src.config import (
    ZSCORE_BUY_THRESHOLD, MAX_SINGLE_WEIGHT, CASH_BUFFER,
    VIX_RISK_ON_THRESHOLD, VIX_RISK_OFF_THRESHOLD,
)


# ── Type alias for strategy functions ────────────────────────
StrategyFn = Callable[[pl.DataFrame], pl.DataFrame]


# ═══════════════════════════════════════════════════════════════
# STRATEGY IMPLEMENTATIONS
# ═══════════════════════════════════════════════════════════════

def buy_hold(df: pl.DataFrame) -> pl.DataFrame:
    """Buy & Hold: Equal-weight all entities, constant allocation."""
    return df.with_columns(
        (1.0 / pl.col("entity_id").n_unique().over("date"))
        .cast(pl.Float32)
        .alias("raw_weight_buy_hold")
    )


def ev_sales(df: pl.DataFrame) -> pl.DataFrame:
    """EV/Sales Long-Only: Buy stocks with Z-score < threshold, equal-weight."""
    return df.with_columns(
        pl.when(pl.col("ev_sales_zscore") < ZSCORE_BUY_THRESHOLD)
        .then(
            1.0 / pl.col("ev_sales_zscore")
            .filter(pl.col("ev_sales_zscore") < ZSCORE_BUY_THRESHOLD)
            .count()
            .over("date")
            .cast(pl.Float64)
            .clip(1.0, None)
        )
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_ev_sales")
    )


def ls_zscore(df: pl.DataFrame) -> pl.DataFrame:
    """L/S Z-Score: Long cheapest 2, short most expensive 2 per date."""
    return df.with_columns(
        pl.when(
            pl.col("ev_sales_zscore").rank("ordinal").over("date") <= 2
        ).then(0.25)  # Long 25% each
        .when(
            pl.col("ev_sales_zscore").rank("ordinal", descending=True).over("date") <= 2
        ).then(-0.25)  # Short 25% each
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_ls_zscore")
    )


def sma_crossover(df: pl.DataFrame) -> pl.DataFrame:
    """SMA 50/200 Crossover: Buy when SMA50 > SMA200."""
    return df.with_columns([
        pl.col("adj_close").rolling_mean(50).over("entity_id").alias("_sma50"),
        pl.col("adj_close").rolling_mean(200).over("entity_id").alias("_sma200"),
    ]).with_columns(
        pl.when(pl.col("_sma50") > pl.col("_sma200"))
        .then(
            1.0 / pl.col("_sma50")
            .filter(pl.col("_sma50") > pl.col("_sma200"))
            .count()
            .over("date")
            .cast(pl.Float64)
            .clip(1.0, None)
        )
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_sma_crossover")
    ).drop(["_sma50", "_sma200"])


def pullback_rsi(df: pl.DataFrame) -> pl.DataFrame:
    """Pullback RSI: Buy when RSI(3) < 20 and price > SMA(200)."""
    # Compute RSI(3) inline
    delta = pl.col("adj_close") - pl.col("adj_close").shift(1).over("entity_id")
    gain = pl.when(delta > 0).then(delta).otherwise(0.0)
    loss = pl.when(delta < 0).then(-delta).otherwise(0.0)

    return df.with_columns([
        gain.rolling_mean(3).over("entity_id").alias("_avg_gain"),
        loss.rolling_mean(3).over("entity_id").alias("_avg_loss"),
        pl.col("adj_close").rolling_mean(200).over("entity_id").alias("_sma200"),
    ]).with_columns(
        pl.when(pl.col("_avg_loss") > 0)
        .then(100.0 - 100.0 / (1.0 + pl.col("_avg_gain") / pl.col("_avg_loss")))
        .otherwise(100.0)
        .alias("_rsi3")
    ).with_columns(
        pl.when(
            (pl.col("_rsi3") < 20) & (pl.col("adj_close") > pl.col("_sma200"))
        )
        .then(
            1.0 / pl.col("_rsi3")
            .filter((pl.col("_rsi3") < 20) & (pl.col("adj_close") > pl.col("_sma200")))
            .count()
            .over("date")
            .cast(pl.Float64)
            .clip(1.0, None)
        )
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_pullback_rsi")
    ).drop(["_avg_gain", "_avg_loss", "_sma200", "_rsi3"])


def momentum(df: pl.DataFrame) -> pl.DataFrame:
    """6-Month Momentum: Top 20% by trailing 126-day return."""
    return df.with_columns(
        (pl.col("adj_close") / pl.col("adj_close").shift(126).over("entity_id") - 1)
        .alias("_mom_6m")
    ).with_columns(
        pl.when(
            pl.col("_mom_6m").rank("ordinal", descending=True).over("date")
            <= (pl.col("entity_id").n_unique().over("date") * 0.2).cast(pl.Int32).clip(1, None)
        )
        .then(
            1.0 / (pl.col("entity_id").n_unique().over("date") * 0.2)
            .cast(pl.Float64).clip(1.0, None)
        )
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_momentum")
    ).drop("_mom_6m")


def low_beta(df: pl.DataFrame) -> pl.DataFrame:
    """Low-Beta Anomaly: Bottom 20% by beta_spy."""
    if "beta_spy" not in df.columns:
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_low_beta"))

    return df.with_columns(
        pl.when(
            pl.col("beta_spy").rank("ordinal").over("date")
            <= (pl.col("entity_id").n_unique().over("date") * 0.2).cast(pl.Int32).clip(1, None)
        )
        .then(
            1.0 / (pl.col("entity_id").n_unique().over("date") * 0.2)
            .cast(pl.Float64).clip(1.0, None)
        )
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_low_beta")
    )


def dcf_value(df: pl.DataFrame) -> pl.DataFrame:
    """Deep Value DCF: Buy stocks with highest NPV gap (undervalued)."""
    if "dcf_npv_gap" not in df.columns:
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_dcf_value"))

    return df.with_columns(
        pl.when(
            (pl.col("dcf_npv_gap") > 0)
            & (pl.col("dcf_npv_gap").rank("ordinal", descending=True).over("date") <= 5)
        )
        .then(0.2)
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_dcf_value")
    )


def fortress(df: pl.DataFrame) -> pl.DataFrame:
    """Fortress Balance Sheet: Low debt-to-equity, high cash."""
    has_cols = all(c in df.columns for c in ["total_debt", "cash", "shares_out"])
    if not has_cols:
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_fortress"))

    return df.with_columns(
        (pl.col("cash").fill_null(0) - pl.col("total_debt").fill_null(0))
        .alias("_net_cash")
    ).with_columns(
        pl.when(
            pl.col("_net_cash").rank("ordinal", descending=True).over("date")
            <= (pl.col("entity_id").n_unique().over("date") * 0.1).cast(pl.Int32).clip(1, None)
        )
        .then(
            1.0 / (pl.col("entity_id").n_unique().over("date") * 0.1)
            .cast(pl.Float64).clip(1.0, None)
        )
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_fortress")
    ).drop("_net_cash")


def macro_regime(df: pl.DataFrame) -> pl.DataFrame:
    """Macro Regime: Scale equal-weight by VIX regime."""
    if "vix" not in df.columns:
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_macro_regime"))

    n_entities = df.select(pl.col("entity_id").n_unique()).item()

    return df.with_columns(
        pl.when(pl.col("vix") < VIX_RISK_ON_THRESHOLD)
        .then(1.0 / max(n_entities, 1))  # Full exposure
        .when(pl.col("vix") > VIX_RISK_OFF_THRESHOLD)
        .then(0.0)  # Risk-off: zero exposure
        .otherwise(0.5 / max(n_entities, 1))  # Caution: half exposure
        .cast(pl.Float32)
        .alias("raw_weight_macro_regime")
    )


def macro_regime_v2(df: pl.DataFrame) -> pl.DataFrame:
    """Macro Regime V2 (VIX Term Structure): Backwardation = risk-off."""
    if "vix" not in df.columns or "vix3m" not in df.columns:
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_macro_v2"))

    n_entities = df.select(pl.col("entity_id").n_unique()).item()

    return df.with_columns(
        # Contango (VIX < VIX3M) = risk-on, Backwardation = risk-off
        pl.when(pl.col("vix") < pl.col("vix3m"))
        .then(1.0 / max(n_entities, 1))
        .otherwise(0.0)
        .cast(pl.Float32)
        .alias("raw_weight_macro_v2")
    )


def xgboost_ai(df: pl.DataFrame) -> pl.DataFrame:
    """XGBoost AI: Use pre-computed predictions from action_intent component.

    For the tournament, this reads from the already-migrated action_intent.parquet
    rather than re-running XGBoost (which requires a trained model).
    """
    import os
    from src.core.duckdb_store import get_parquet_path

    intent_path = get_parquet_path("action_intent")
    if not os.path.exists(intent_path):
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_xgboost"))

    intent = pl.read_parquet(intent_path).filter(pl.col("strategy_id") == "xgboost")

    if intent.is_empty():
        return df.with_columns(pl.lit(0.0).cast(pl.Float32).alias("raw_weight_xgboost"))

    # Join intent weights onto the main frame
    result = df.join(
        intent.select(["entity_id", "date", "raw_weight"]),
        on=["entity_id", "date"],
        how="left",
    ).with_columns(
        pl.col("raw_weight").fill_null(0.0).cast(pl.Float32).alias("raw_weight_xgboost")
    )

    if "raw_weight" in result.columns and "raw_weight_xgboost" in result.columns:
        result = result.drop("raw_weight")

    return result


# ═══════════════════════════════════════════════════════════════
# STRATEGY REGISTRY
# ═══════════════════════════════════════════════════════════════

STRATEGY_REGISTRY: dict[str, StrategyFn] = {
    "buy_hold":        buy_hold,
    "ev_sales":        ev_sales,
    "ls_zscore":       ls_zscore,
    "sma_crossover":   sma_crossover,
    "pullback_rsi":    pullback_rsi,
    "momentum":        momentum,
    "low_beta":        low_beta,
    "dcf_value":       dcf_value,
    "fortress":        fortress,
    "macro_regime":    macro_regime,
    "macro_v2":        macro_regime_v2,
    "xgboost":         xgboost_ai,
}

# Human-readable display names
STRATEGY_NAMES: dict[str, str] = {
    "buy_hold":        "Buy & Hold (EW)",
    "ev_sales":        "EV/Sales Long-Only",
    "ls_zscore":       "L/S Z-Score",
    "sma_crossover":   "SMA Crossover (EW)",
    "pullback_rsi":    "Pullback RSI (EW)",
    "momentum":        "Momentum 6M (Top 20%)",
    "low_beta":        "Low-Beta (Bot 20%)",
    "dcf_value":       "DCF Deep Value",
    "fortress":        "Fortress B/S (Top 10%)",
    "macro_regime":    "Macro Regime (EW)",
    "macro_v2":        "Macro V2 Term (EW)",
    "xgboost":         "XGBoost AI (Risk-Adj)",
}


def get_all_strategy_ids() -> list[str]:
    """Return all registered strategy IDs."""
    return list(STRATEGY_REGISTRY.keys())


def evaluate_single_strategy(
    strategy_id: str,
    df: pl.DataFrame,
    portfolio_id: int = None,
    trader_id: int = None,
) -> pl.DataFrame:
    """Evaluate exactly one strategy for a scoped portfolio.

    Returns DataFrame with [entity_id, date, raw_weight]
    and optionally portfolio_id / trader_id columns.
    """
    if strategy_id not in STRATEGY_REGISTRY:
        raise KeyError(f"Unknown strategy: {strategy_id}")

    fn = STRATEGY_REGISTRY[strategy_id]
    result = fn(df)

    col_name = f"raw_weight_{strategy_id}"
    out = result.select(["entity_id", "date", col_name]).rename({col_name: "raw_weight"})

    if portfolio_id is not None:
        out = out.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
    if trader_id is not None:
        out = out.with_columns(pl.lit(trader_id).alias("trader_id"))

    return out


def evaluate_strategies(
    df: pl.DataFrame,
    strategy_ids: list[str] | None = None,
) -> pl.DataFrame:
    """Apply requested strategies to the data, adding raw_weight columns.

    Each strategy adds a column named `raw_weight_{strategy_id}`.
    """
    if strategy_ids is None:
        strategy_ids = get_all_strategy_ids()

    for sid in strategy_ids:
        if sid not in STRATEGY_REGISTRY:
            raise KeyError(f"Unknown strategy: {sid}")
        fn = STRATEGY_REGISTRY[sid]
        df = fn(df)

    return df


# ═══════════════════════════════════════════════════════════════
# LEVEL 5: Dynamic Custom Strategy Discovery
# ═══════════════════════════════════════════════════════════════

def discover_custom_strategies():
    """Dynamically scan src/ecs/strategies/custom/ for promoted Alpha Lab strategies.

    Each module must define STRATEGY_ID and a callable strategy function
    (any function whose name starts with 'strategy_' or matches the
    pattern used by generate_signals).
    """
    import importlib
    import pkgutil
    from pathlib import Path

    custom_dir = Path(__file__).parent / "strategies" / "custom"
    if not custom_dir.exists():
        return

    # Ensure __init__.py exists for the custom package
    init_file = custom_dir / "__init__.py"
    if not init_file.exists():
        init_file.write_text("")

    for module_info in pkgutil.iter_modules([str(custom_dir)]):
        try:
            module = importlib.import_module(
                f"src.ecs.strategies.custom.{module_info.name}"
            )
            strategy_id = getattr(module, "STRATEGY_ID", module_info.name)
            strategy_name = getattr(module, "STRATEGY_NAME", strategy_id)

            # Find the strategy function — look for common patterns
            strategy_fn = None
            for fn_name in ["generate_signals", "strategy", module_info.name]:
                fn = getattr(module, fn_name, None)
                if callable(fn):
                    strategy_fn = fn
                    break

            # Fallback: find any callable that isn't a builtin
            if strategy_fn is None:
                for name, obj in vars(module).items():
                    if callable(obj) and not name.startswith("_") and name not in ("pl", "np"):
                        strategy_fn = obj
                        break

            if strategy_fn:
                STRATEGY_REGISTRY[strategy_id] = strategy_fn
                STRATEGY_NAMES[strategy_id] = f"🤖 {strategy_name}"
                print(f"  ✓ Loaded custom strategy: {strategy_id} ({strategy_name})")

        except Exception as e:
            print(f"  ⚠ Failed to load custom strategy {module_info.name}: {e}")


# Run discovery on import
discover_custom_strategies()


