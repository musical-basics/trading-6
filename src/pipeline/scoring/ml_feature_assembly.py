"""
ml_feature_assembly.py — Level 3 Phase 2D: ML Feature Matrix Assembly

The master merger: joins all computed features into a single ml_features
table ready for XGBoost consumption.

Columns assembled:
  - ev_sales_zscore (from cross_sectional_scores)
  - dynamic_discount_rate, dcf_npv_gap (from _dcf_staging)
  - beta_spy, beta_10y (beta_tnx), beta_vix (from factor_betas)
  - fwd_return_20d (calculated from daily_bars, shifted -20 days)
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH, FWD_RETURN_DAYS


def assemble_features():
    """
    Merge all Level 3 feature sources into the ml_features table.
    Forward returns are calculated as the ML label.
    """
    print("=" * 60)
    print("PHASE 2D: ML Feature Assembly")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load EV/Sales Z-scores ───────────────────────
    print("  Loading cross_sectional_scores...", end=" ")
    scores_df = pd.read_sql_query(
        "SELECT ticker, date, ev_sales_zscore FROM cross_sectional_scores",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(scores_df):,} rows")

    # ── Step 2: Load Factor Betas ────────────────────────────
    print("  Loading factor_betas...", end=" ")
    betas_df = pd.read_sql_query(
        "SELECT ticker, date, beta_spy, beta_vix, beta_tnx FROM factor_betas",
        conn,
        parse_dates=["date"],
    )
    # Rename beta_tnx → beta_10y for the ML feature matrix
    betas_df = betas_df.rename(columns={"beta_tnx": "beta_10y"})
    print(f"✓ {len(betas_df):,} rows")

    # ── Step 3: Load DCF Staging ─────────────────────────────
    print("  Loading _dcf_staging...", end=" ")
    dcf_df = pd.read_sql_query(
        "SELECT ticker, date, dynamic_discount_rate, dcf_npv_gap FROM _dcf_staging",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(dcf_df):,} rows")

    # ── Step 4: Load prices for forward returns ──────────────
    print("  Loading daily_bars for forward returns...", end=" ")
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(prices_df):,} rows")

    if scores_df.empty or betas_df.empty or dcf_df.empty or prices_df.empty:
        print("  ⚠ Missing input data. Ensure Phases 1–2C are complete.")
        conn.close()
        return

    # ── Step 5: Calculate forward 20-day returns (the ML label) ─
    print("  Calculating forward returns...", end=" ")
    prices_df = prices_df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Shift price forward by FWD_RETURN_DAYS within each ticker group
    prices_df["future_price"] = prices_df.groupby("ticker")["adj_close"].shift(-FWD_RETURN_DAYS)
    prices_df["fwd_return_20d"] = (
        (prices_df["future_price"] - prices_df["adj_close"]) / prices_df["adj_close"]
    )

    fwd_returns = prices_df[["ticker", "date", "fwd_return_20d"]]
    labeled_count = fwd_returns["fwd_return_20d"].notna().sum()
    null_count = fwd_returns["fwd_return_20d"].isna().sum()
    print(f"✓ {labeled_count:,} labeled, {null_count:,} unlabeled (last {FWD_RETURN_DAYS} days per ticker)")

    # ── Step 6: Inner-join all features ──────────────────────
    print("  Merging all features...", end=" ")

    # Start with scores as base
    merged = scores_df.merge(betas_df, on=["ticker", "date"], how="inner")
    merged = merged.merge(dcf_df, on=["ticker", "date"], how="inner")
    merged = merged.merge(fwd_returns, on=["ticker", "date"], how="inner")

    # Drop rows where any FEATURE is NaN (labels can be NaN — that's expected)
    feature_cols = [
        "ev_sales_zscore", "dynamic_discount_rate", "dcf_npv_gap",
        "beta_spy", "beta_10y", "beta_vix",
    ]
    pre_drop = len(merged)
    merged = merged.dropna(subset=feature_cols)
    print(f"✓ {len(merged):,} rows ({pre_drop - len(merged):,} dropped for NaN features)")

    if merged.empty:
        print("  ⚠ No valid feature rows after merge. Check upstream phases.")
        conn.close()
        return

    # ── Step 7: Write to ml_features table ───────────────────
    print("  Saving to ml_features...", end=" ")

    output_df = merged[["ticker", "date"] + feature_cols + ["fwd_return_20d"]].copy()
    output_df["date"] = output_df["date"].dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.executemany(
        """INSERT OR REPLACE INTO ml_features
           (ticker, date, ev_sales_zscore, dynamic_discount_rate, dcf_npv_gap,
            beta_spy, beta_10y, beta_vix, fwd_return_20d)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        output_df.values.tolist()
    )
    conn.commit()
    conn.close()

    unique_tickers = output_df["ticker"].nunique()
    unique_dates = output_df["date"].nunique()
    labeled = output_df["fwd_return_20d"].notna().sum()
    unlabeled = output_df["fwd_return_20d"].isna().sum()

    print(f"✓ {len(output_df):,} rows")
    print()
    print(f"  ✓ ML features assembled: {unique_tickers} tickers × {unique_dates:,} dates")
    print(f"  ✓ Labeled (trainable): {labeled:,} | Unlabeled (predict-only): {unlabeled:,}")
    print(f"  ✓ Feature columns: {', '.join(feature_cols)}")
    print()


if __name__ == "__main__":
    assemble_features()
