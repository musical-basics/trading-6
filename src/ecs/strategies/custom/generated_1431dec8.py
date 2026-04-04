"""
Auto-generated strategy from Alpha Lab experiment #1431dec8.
Experiment: relaxed_quality_momentum
Metrics: Sharpe: 0.389
"""

import polars as pl
import numpy as np

STRATEGY_ID = "alphalab_1431dec8"
STRATEGY_NAME = "relaxed_quality_momentum"

def relaxed_quality_momentum(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        # Calculate SPY 200-day moving average for regime detection
        pl.col("spy").rolling_mean(200).over("date").alias("spy_200ma"),
        
        # Calculate 60-day price momentum (shifted for T-1 guardrail)
        (pl.col("adj_close") / pl.col("adj_close").shift(60).over("entity_id") - 1.0)
        .shift(1).over("entity_id").fill_null(0.0).alias("momentum_60d"),
        
        # Quality signal: DCF NPV gap (positive = undervalued)
        pl.when(
            pl.col("filing_date").is_null() | 
            ((pl.col("date") - pl.col("filing_date").cast(pl.Date)).dt.total_days() < 45) |
            ((pl.col("date") - pl.col("filing_date").cast(pl.Date)).dt.total_days() > 540)
        ).then(0.0).otherwise(pl.col("dcf_npv_gap")).alias("quality_signal"),
    ]).with_columns([
        # Combined signal: quality + momentum (additive instead of multiplicative)
        (pl.col("quality_signal") + pl.col("momentum_60d") * 10.0).alias("combined_signal"),
    ]).with_columns([
        # Cross-sectional ranking
        pl.col("combined_signal").rank("ordinal").over("date").cast(pl.Float64).alias("signal_rank"),
        pl.col("combined_signal").count().over("date").cast(pl.Float64).alias("total_count"),
        
        # Regime filters - more relaxed
        (pl.col("spy") > pl.col("spy_200ma")).alias("bull_regime"),
    ]).with_columns([
        # Top decile filter (only top 10% get weights)
        (pl.col("signal_rank") >= pl.col("total_count") * 0.9).alias("top_decile"),
    ]).with_columns([
        # Final weight calculation with filing_date circuit-breaker
        pl.when(
            pl.col("filing_date").is_null() | 
            ((pl.col("date") - pl.col("filing_date").cast(pl.Date)).dt.total_days() > 540) | 
            ((pl.col("date") - pl.col("filing_date").cast(pl.Date)).dt.total_days() < 45)
        ).then(0.0).otherwise(
            pl.when(
                # Relaxed conditions: just top decile OR positive momentum
                pl.col("top_decile") & 
                (pl.col("momentum_60d") > -0.05) &  # Allow slight negative momentum
                (pl.col("quality_signal") > -1.0)    # Allow some overvaluation
            ).then(
                # Weight based on signal rank within top decile
                ((pl.col("signal_rank") - pl.col("total_count") * 0.9) / (pl.col("total_count") * 0.1)).clip(0.0, 1.0)
            ).when(
                # Allow shorts only in clear bear regime
                ~pl.col("bull_regime") & 
                pl.col("top_decile") &
                (pl.col("momentum_60d") < -0.05)  # Relaxed negative momentum threshold
            ).then(
                # Moderate short weight
                -0.3
            ).otherwise(0.0)
        ).alias("raw_weight_relaxed_quality_momentum")
    ])
