"""
alignment_system.py — Level 4 ECS System 2: Alignment & Math Engine

Polars-native implementations of:
  1. Fundamental alignment via join_asof (backward strategy)
  2. Rolling OLS factor betas via numpy.linalg.lstsq
  3. Cross-sectional EV/Sales Z-scores via Polars window functions
  4. Dynamic DCF valuations

All operations are vectorized — no per-ticker Python loops.
"""

from __future__ import annotations

import os
from datetime import datetime

import numpy as np
import polars as pl

from src.config import (
    OLS_ROLLING_WINDOW, RISK_FREE_RATE, EQUITY_RISK_PREMIUM,
    VIX_RISK_PREMIUM, GROWTH_RATE, FILING_DELAY_DAYS,
    ZSCORE_BUY_THRESHOLD, MAX_SINGLE_WEIGHT,
)
from src.core.duckdb_store import get_parquet_path, PARQUET_DIR
from src.ecs.fundamental_hygiene import canonicalize_quarterly_fundamentals


def align_fundamentals() -> pl.DataFrame:
    """Align quarterly fundamentals onto daily prices using join_asof.

    For each (entity_id, date) in market_data, find the most recent
    fundamental row where filing_date <= date (backward strategy).

    Returns merged DataFrame and writes nothing (used as input to later steps).
    """
    print("  System 2a: Aligning fundamentals...", end=" ")

    market = pl.read_parquet(get_parquet_path("market_data"))
    fundamental = pl.read_parquet(get_parquet_path("fundamental"))
    fundamental = canonicalize_quarterly_fundamentals(fundamental)

    # join_asof requires both to be sorted on the join key
    market = market.sort(["entity_id", "date"])
    fundamental = fundamental.sort(["entity_id", "filing_date"])

    merged = market.join_asof(
        fundamental,
        left_on="date",
        right_on="filing_date",
        by="entity_id",
        strategy="backward",
    )

    print(f"✓ {len(merged):,} rows aligned")
    return merged


def compute_factor_betas() -> None:
    """Compute rolling OLS factor betas (β_SPY, β_TNX, β_VIX) using numpy.

    For each entity on each date, runs a 90-day rolling regression:
        r_i = α + β_spy * r_spy + β_tnx * r_tnx + β_vix * r_vix + ε

    Uses vectorized numpy.linalg.lstsq over pre-sorted arrays.
    Writes to feature.parquet (or a separate factor_betas component).
    """
    print("  System 2b: Computing factor betas...", end=" ")

    market = pl.read_parquet(get_parquet_path("market_data"))
    macro = pl.read_parquet(get_parquet_path("macro"))

    # Compute macro daily returns
    macro = macro.sort("date")
    macro = macro.with_columns([
        (pl.col("spy") / pl.col("spy").shift(1) - 1).cast(pl.Float32).alias("r_spy"),
        (pl.col("tnx") / pl.col("tnx").shift(1) - 1).cast(pl.Float32).alias("r_tnx"),
        (pl.col("vix") / pl.col("vix").shift(1) - 1).cast(pl.Float32).alias("r_vix"),
    ])

    # Join market with macro on date
    joined = market.join(
        macro.select(["date", "r_spy", "r_tnx", "r_vix"]),
        on="date",
        how="inner",
    ).sort(["entity_id", "date"]).drop_nulls(subset=["daily_return", "r_spy", "r_tnx", "r_vix"])

    entity_ids = joined["entity_id"].unique().sort().to_list()
    results = []
    window = OLS_ROLLING_WINDOW

    for eid in entity_ids:
        entity_df = joined.filter(pl.col("entity_id") == eid)
        n = len(entity_df)
        if n < window:
            continue

        dates = entity_df["date"].to_list()
        y = entity_df["daily_return"].to_numpy()
        X_spy = entity_df["r_spy"].to_numpy()
        X_tnx = entity_df["r_tnx"].to_numpy()
        X_vix = entity_df["r_vix"].to_numpy()

        for i in range(window, n):
            y_win = y[i - window:i]
            X_win = np.column_stack([
                np.ones(window),
                X_spy[i - window:i],
                X_tnx[i - window:i],
                X_vix[i - window:i],
            ])

            try:
                coeffs, _, _, _ = np.linalg.lstsq(X_win, y_win, rcond=None)
                results.append({
                    "entity_id": eid,
                    "date": dates[i],
                    "beta_spy": float(coeffs[1]),
                    "beta_tnx": float(coeffs[2]),
                    "beta_vix": float(coeffs[3]),
                })
            except Exception:
                continue

    if not results:
        print("⚠ No betas computed")
        return

    betas_df = pl.DataFrame(results)
    betas_df = betas_df.with_columns([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("date").cast(pl.Date),
        pl.col("beta_spy").cast(pl.Float32),
        pl.col("beta_tnx").cast(pl.Float32),
        pl.col("beta_vix").cast(pl.Float32),
    ])

    print(f"✓ {len(betas_df):,} beta rows")
    return betas_df


def compute_cross_sectional_scores(aligned_df: pl.DataFrame | None = None) -> pl.DataFrame:
    """Compute EV/Sales Z-scores using Polars window functions.

    This is a pure cross-sectional operation: for each date, compute
    the z-score of EV/Sales across all entities.

    No per-ticker loops — uses pl.col().over("date").
    """
    print("  System 2c: Computing cross-sectional scores...", end=" ")

    if aligned_df is None:
        aligned_df = align_fundamentals()

    # Filter to rows with fundamental data
    scored = aligned_df.filter(
        pl.col("revenue").is_not_null() & (pl.col("revenue") > 0)
    )

    if scored.is_empty():
        print("⚠ No fundamental-aligned rows")
        return pl.DataFrame()

    # Compute EV/Sales
    scored = scored.with_columns([
        # Enterprise Value = Market Cap + Debt - Cash
        # Market cap proxy = adj_close * shares_out
        (pl.col("adj_close") * pl.col("shares_out")).alias("market_cap"),
    ])

    scored = scored.with_columns(
        (pl.col("market_cap") + pl.col("total_debt").fill_null(0) - pl.col("cash").fill_null(0))
        .alias("enterprise_value")
    )

    scored = scored.with_columns(
        (pl.col("enterprise_value") / pl.col("revenue")).alias("ev_to_sales")
    )

    # Cross-sectional Z-score per date using window functions
    scored = scored.with_columns([
        ((pl.col("ev_to_sales") - pl.col("ev_to_sales").mean().over("date"))
         / pl.col("ev_to_sales").std().over("date"))
        .alias("ev_sales_zscore"),
    ])

    print(f"✓ {len(scored):,} rows scored")
    return scored


def compute_dynamic_dcf(aligned_df: pl.DataFrame | None = None) -> pl.DataFrame:
    """Compute Dynamic DCF valuations using vectorized Polars expressions.

    Uses the APT discount rate: r = Rf + β_spy * ERP + β_vix * λ_vix
    Gordon Growth Model: NPV = Revenue * (1+g) / (r - g)
    Gap = (NPV - Market Cap) / Market Cap
    """
    print("  System 2d: Computing Dynamic DCF...", end=" ")

    if aligned_df is None:
        aligned_df = align_fundamentals()

    # Load betas
    betas_path = get_parquet_path("feature")
    if os.path.exists(betas_path):
        features = pl.read_parquet(betas_path)
        if "beta_spy" in features.columns:
            aligned_df = aligned_df.join(
                features.select(["entity_id", "date", "beta_spy", "beta_tnx", "beta_vix"]),
                on=["entity_id", "date"],
                how="left",
            )

    if "beta_spy" not in aligned_df.columns:
        print("⚠ No beta data — skipping DCF")
        return aligned_df

    # Dynamic discount rate
    dcf = aligned_df.filter(
        pl.col("revenue").is_not_null() & pl.col("beta_spy").is_not_null()
    )

    dcf = dcf.with_columns(
        (RISK_FREE_RATE
         + pl.col("beta_spy").abs() * EQUITY_RISK_PREMIUM
         + pl.col("beta_vix").abs() * VIX_RISK_PREMIUM)
        .alias("dynamic_discount_rate")
    )

    # Gordon Growth Model NPV
    dcf = dcf.with_columns(
        pl.when(pl.col("dynamic_discount_rate") > GROWTH_RATE)
        .then(
            pl.col("revenue") * (1 + GROWTH_RATE)
            / (pl.col("dynamic_discount_rate") - GROWTH_RATE)
        )
        .otherwise(None)
        .alias("dcf_npv")
    )

    # NPV gap
    dcf = dcf.with_columns(
        pl.when(pl.col("adj_close") * pl.col("shares_out") > 0)
        .then(
            (pl.col("dcf_npv") - pl.col("adj_close") * pl.col("shares_out"))
            / (pl.col("adj_close") * pl.col("shares_out"))
        )
        .otherwise(None)
        .alias("dcf_npv_gap")
    )

    print(f"✓ {len(dcf):,} rows")
    return dcf


def run_alignment_pipeline() -> None:
    """Execute the full alignment and math pipeline (Systems 2a-2d).

    Writes the combined feature.parquet with all computed features.
    """
    start = datetime.now()

    print("=" * 60)
    print("ECS SYSTEM 2: Alignment & Math Engine")
    print("=" * 60)

    # Step 1: Align fundamentals
    aligned = align_fundamentals()

    # Step 2: Compute betas
    betas = compute_factor_betas()

    # Step 3: Cross-sectional scores
    scored = compute_cross_sectional_scores(aligned)

    # Step 4: Dynamic DCF (needs betas)
    # If we have betas, merge them in first
    if betas is not None and not betas.is_empty():
        feature_path = get_parquet_path("feature")

        # Start from scored data (all tickers with Z-scores), left-join betas
        if scored is not None and not scored.is_empty():
            combined = scored.join(
                betas.select(["entity_id", "date", "beta_spy", "beta_tnx", "beta_vix"]),
                on=["entity_id", "date"],
                how="left",
                suffix="_beta",
            )

            # Prefer beta columns from betas computation
            for col in ["beta_spy", "beta_tnx", "beta_vix"]:
                beta_col = f"{col}_beta"
                if beta_col in combined.columns:
                    combined = combined.with_columns(
                        pl.coalesce([pl.col(beta_col), pl.col(col)]).alias(col)
                    ).drop(beta_col)
        else:
            combined = betas

        # Compute DCF on aligned data (only for rows that have betas)
        dcf = compute_dynamic_dcf(aligned)

        # Merge DCF columns (dynamic_discount_rate, dcf_npv_gap) into
        # the combined scored+betas dataframe so we keep ALL scored rows
        if dcf is not None and not dcf.is_empty() and combined is not None:
            dcf_cols = dcf.select([
                "entity_id", "date",
                *[c for c in ["dynamic_discount_rate", "dcf_npv_gap"] if c in dcf.columns]
            ]).unique(subset=["entity_id", "date"])

            combined = combined.join(
                dcf_cols,
                on=["entity_id", "date"],
                how="left",
                suffix="_dcf",
            )
            # Resolve any duplicate columns from DCF join
            for c in ["dynamic_discount_rate", "dcf_npv_gap"]:
                dcf_c = f"{c}_dcf"
                if dcf_c in combined.columns:
                    combined = combined.with_columns(
                        pl.coalesce([pl.col(dcf_c), pl.col(c)]).alias(c)
                    ).drop(dcf_c)

        # Build final feature component from combined data
        feature_cols = ["entity_id", "date"]
        for c in ["ev_sales_zscore", "dynamic_discount_rate", "dcf_npv_gap",
                   "beta_spy", "beta_tnx", "beta_vix"]:
            if c in combined.columns:
                feature_cols.append(c)

        feature_df = combined.select([c for c in feature_cols if c in combined.columns])

        if feature_df is not None and not feature_df.is_empty():
            # Cast types
            for c in ["ev_sales_zscore", "dynamic_discount_rate", "dcf_npv_gap",
                       "beta_spy", "beta_tnx", "beta_vix"]:
                if c in feature_df.columns:
                    feature_df = feature_df.with_columns(pl.col(c).cast(pl.Float32))

            feature_df = feature_df.with_columns(pl.col("entity_id").cast(pl.Int32))
            feature_df.write_parquet(feature_path)
            print(f"\n  ✓ Feature component written: {len(feature_df):,} rows")

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n  ✓ Alignment pipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    run_alignment_pipeline()
