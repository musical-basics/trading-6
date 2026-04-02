"""
xgb_wfo_engine.py — Level 3 Phase 3a: XGBoost Walk-Forward Optimization

Implements a Purged Expanding-Window WFO that trains XGBoost Regressors
to predict forward 20-day relative returns. Predictions are ranked
cross-sectionally to assign raw_weight to the top N stocks.

Key features:
  - Embargo (Purge): removes training rows whose forward labels
    overlap with the test window to prevent data leakage.
  - Expanding window: grows with each step (not sliding).
  - Feature importance tracking per WFO window.
  - In-memory only — no model serialization (per stack.md).
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta
from xgboost import XGBRegressor
from src.config import (
    DB_PATH,
    WFO_TRAIN_YEARS, WFO_TEST_YEARS, WFO_STEP_YEARS,
    EMBARGO_DAYS, TOP_N_HOLDINGS,
    XGB_N_ESTIMATORS, XGB_MAX_DEPTH, XGB_LEARNING_RATE, XGB_SUBSAMPLE,
)

# Feature columns used by XGBoost
FEATURE_COLS = [
    "ev_sales_zscore",
    "dynamic_discount_rate",
    "dcf_npv_gap",
    "beta_spy",
    "beta_10y",
    "beta_vix",
]

TARGET_COL = "fwd_return_20d"


def run_xgb_wfo():
    """
    Run the Purged Expanding-Window WFO with XGBoost.
    Writes predictions and raw weights to ml_predictions table.
    Returns feature importance log for UI visualization.
    """
    print("=" * 60)
    print("PHASE 3a: XGBoost Walk-Forward Optimization")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load ml_features ─────────────────────────────
    print("  Loading ml_features...", end=" ")
    features_df = pd.read_sql_query(
        "SELECT * FROM ml_features ORDER BY date, ticker",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(features_df):,} rows")

    if features_df.empty:
        print("  ⚠ No ML features. Run Phase 2D first.")
        conn.close()
        return []

    # ── Step 2: Determine WFO windows ────────────────────────
    min_date = features_df["date"].min()
    max_date = features_df["date"].max()

    print(f"  Data range: {min_date.date()} → {max_date.date()}")

    # Build WFO windows: expanding train, fixed test
    windows = []
    train_start = min_date
    train_end = min_date + pd.DateOffset(years=WFO_TRAIN_YEARS)

    while train_end + pd.DateOffset(years=WFO_TEST_YEARS) <= max_date + pd.Timedelta(days=30):
        test_start = train_end
        test_end = train_end + pd.DateOffset(years=WFO_TEST_YEARS)
        windows.append({
            "train_start": train_start,
            "train_end": train_end,
            "test_start": test_start,
            "test_end": min(test_end, max_date),
        })
        # Expand: roll forward by STEP_YEARS
        train_end = train_end + pd.DateOffset(years=WFO_STEP_YEARS)

    print(f"  WFO windows: {len(windows)}")

    if not windows:
        print("  ⚠ Insufficient data for even one WFO window.")
        conn.close()
        return []

    # ── Step 3: Run WFO loop ─────────────────────────────────
    all_predictions = []
    importance_log = []

    for i, w in enumerate(windows, 1):
        print(f"\n  ── Window {i}/{len(windows)} ──")
        print(f"     Train: {w['train_start'].date()} → {w['train_end'].date()}")
        print(f"     Test:  {w['test_start'].date()} → {w['test_end'].date()}")

        # EMBARGO: remove training rows whose labels overlap with test window
        embargo_cutoff = w["train_end"] - pd.Timedelta(days=EMBARGO_DAYS)

        # Training data: before embargo cutoff, with valid labels
        train_mask = (
            (features_df["date"] >= w["train_start"])
            & (features_df["date"] < embargo_cutoff)
            & (features_df[TARGET_COL].notna())
        )
        train_df = features_df[train_mask].copy()

        # Test data: in the test window (labels may be NaN for prediction)
        test_mask = (
            (features_df["date"] >= w["test_start"])
            & (features_df["date"] < w["test_end"])
        )
        test_df = features_df[test_mask].copy()

        print(f"     Train samples: {len(train_df):,} (embargo: {EMBARGO_DAYS}d purged)")
        print(f"     Test samples:  {len(test_df):,}")

        if len(train_df) < 50:
            print("     ⚠ Insufficient training data. Skipping window.")
            continue

        if test_df.empty:
            print("     ⚠ No test data. Skipping window.")
            continue

        # Prepare X, Y
        X_train = train_df[FEATURE_COLS].values
        Y_train = train_df[TARGET_COL].values
        X_test = test_df[FEATURE_COLS].values

        # Train XGBoost
        model = XGBRegressor(
            n_estimators=XGB_N_ESTIMATORS,
            max_depth=XGB_MAX_DEPTH,
            learning_rate=XGB_LEARNING_RATE,
            subsample=XGB_SUBSAMPLE,
            random_state=42,
            verbosity=0,
            n_jobs=1,
        )
        model.fit(X_train, Y_train)

        # Predict
        test_df = test_df.copy()
        test_df["xgb_prediction"] = model.predict(X_test)

        # Cross-sectional ranking: per date, assign weights to top N
        test_df["raw_weight"] = 0.0
        for date, group in test_df.groupby("date"):
            if len(group) < TOP_N_HOLDINGS:
                # Not enough tickers — equal weight all
                test_df.loc[group.index, "raw_weight"] = 1.0 / len(group)
            else:
                # Top N by prediction get equal weight
                top_n_idx = group.nlargest(TOP_N_HOLDINGS, "xgb_prediction").index
                test_df.loc[top_n_idx, "raw_weight"] = 1.0 / TOP_N_HOLDINGS

        all_predictions.append(
            test_df[["ticker", "date", "xgb_prediction", "raw_weight"]]
        )

        # Feature importance tracking
        importances = dict(zip(FEATURE_COLS, model.feature_importances_))
        importance_log.append({
            "window": i,
            "train_end": w["train_end"].strftime("%Y-%m-%d"),
            "importances": importances,
        })
        top_feature = max(importances, key=importances.get)
        print(f"     ✓ Trained. Top feature: {top_feature} ({importances[top_feature]:.3f})")

        # Keep reference to the last trained model for carry-forward
        last_model = model
        last_test_end = w["test_end"]

    if not all_predictions:
        print("\n  ⚠ No predictions generated. Check data availability.")
        conn.close()
        return []

    # ── Step 3b: Carry-Forward — score remaining unseen dates ─
    # In a live system, you keep using the last trained model until retrained.
    remaining_mask = features_df["date"] >= last_test_end
    remaining_df = features_df[remaining_mask].copy()

    if not remaining_df.empty:
        print(f"\n  ── Carry-Forward ──")
        print(f"     Scoring {len(remaining_df):,} unseen rows "
              f"({remaining_df['date'].min().date()} → {remaining_df['date'].max().date()}) "
              f"with last trained model")

        X_remaining = remaining_df[FEATURE_COLS].values
        remaining_df["xgb_prediction"] = last_model.predict(X_remaining)

        # Same cross-sectional ranking
        remaining_df["raw_weight"] = 0.0
        for date, group in remaining_df.groupby("date"):
            if len(group) < TOP_N_HOLDINGS:
                remaining_df.loc[group.index, "raw_weight"] = 1.0 / len(group)
            else:
                top_n_idx = group.nlargest(TOP_N_HOLDINGS, "xgb_prediction").index
                remaining_df.loc[top_n_idx, "raw_weight"] = 1.0 / TOP_N_HOLDINGS

        all_predictions.append(
            remaining_df[["ticker", "date", "xgb_prediction", "raw_weight"]]
        )
        print(f"     ✓ {len(remaining_df):,} carry-forward predictions added")

    # ── Step 4: Save to ml_predictions ───────────────────────
    print("\n  Saving predictions to ml_predictions...", end=" ")
    predictions_df = pd.concat(all_predictions, ignore_index=True)

    # De-duplicate: if overlapping windows, keep the latest prediction
    predictions_df = predictions_df.sort_values(["ticker", "date"])
    predictions_df = predictions_df.drop_duplicates(
        subset=["ticker", "date"], keep="last"
    )

    predictions_df["date"] = predictions_df["date"].dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.executemany(
        """INSERT OR REPLACE INTO ml_predictions
           (ticker, date, xgb_prediction, raw_weight)
           VALUES (?, ?, ?, ?)""",
        predictions_df[["ticker", "date", "xgb_prediction", "raw_weight"]].values.tolist()
    )
    conn.commit()
    conn.close()

    total_weighted = (predictions_df["raw_weight"] > 0).sum()
    unique_dates = predictions_df["date"].nunique()

    print(f"✓ {len(predictions_df):,} rows")
    print()
    print(f"  ✓ XGBoost WFO complete:")
    print(f"    • {len(windows)} WFO windows processed")
    print(f"    • {len(predictions_df):,} predictions across {unique_dates:,} dates")
    print(f"    • {total_weighted:,} weighted entries (top {TOP_N_HOLDINGS} per date)")
    print()

    # Print feature importance summary
    print("  Feature Importance Summary:")
    for entry in importance_log:
        imps = entry["importances"]
        sorted_imps = sorted(imps.items(), key=lambda x: x[1], reverse=True)
        top_3 = ", ".join([f"{k}: {v:.3f}" for k, v in sorted_imps[:3]])
        print(f"    Window {entry['window']} (→{entry['train_end']}): {top_3}")
    print()

    return importance_log


if __name__ == "__main__":
    run_xgb_wfo()
