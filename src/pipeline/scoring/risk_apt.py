"""
risk_apt.py — Level 3 Phase 3b: Risk APT (The Defensive Shield)

Takes XGBoost raw weights from ml_predictions and applies
variance-based constraints:
  1. 90-day rolling Covariance Matrix (Σ) from daily returns
  2. Marginal Contribution to Risk (MCR) per asset
  3. Scale down weights that breach MAX_MCR_THRESHOLD
  4. Ensure total portfolio vol stays under MAX_PORTFOLIO_VOL

Writes final risk-adjusted weights to target_portfolio.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import (
    DB_PATH, COVARIANCE_WINDOW, MAX_MCR_THRESHOLD,
    MAX_PORTFOLIO_VOL, CASH_BUFFER
)


def apply_risk_constraints():
    """
    Load raw weights from ml_predictions, calculate covariance + MCR,
    scale down excessive risk contributors, and save to target_portfolio.
    """
    print("=" * 60)
    print("PHASE 3b: Risk APT (Variance Constraint)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load raw weights ─────────────────────────────
    print("  Loading ml_predictions...", end=" ")
    predictions_df = pd.read_sql_query(
        "SELECT ticker, date, xgb_prediction, raw_weight FROM ml_predictions",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(predictions_df):,} rows")

    # ── Step 2: Load daily prices for covariance ─────────────
    print("  Loading daily_bars...", end=" ")
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(prices_df):,} rows")

    if predictions_df.empty or prices_df.empty:
        print("  ⚠ Missing input data.")
        conn.close()
        return

    # Get unique dates where we have portfolio weights
    weighted_dates = predictions_df[predictions_df["raw_weight"] > 0]["date"].unique()
    weighted_dates = sorted(weighted_dates)

    print(f"  Processing {len(weighted_dates):,} dates with active portfolios...")

    all_results = []
    skipped = 0

    for date in weighted_dates:
        day_preds = predictions_df[
            (predictions_df["date"] == date) & (predictions_df["raw_weight"] > 0)
        ].copy()

        if day_preds.empty:
            continue

        tickers = day_preds["ticker"].tolist()
        weights = day_preds["raw_weight"].values.copy()

        # Get trailing returns for covariance
        lookback_start = date - pd.Timedelta(days=int(COVARIANCE_WINDOW * 1.5))
        returns_data = prices_df[
            (prices_df["ticker"].isin(tickers))
            & (prices_df["date"] >= lookback_start)
            & (prices_df["date"] <= date)
        ].copy()

        # Pivot to get returns matrix
        pivot = returns_data.pivot(index="date", columns="ticker", values="adj_close")
        pivot = pivot.dropna(axis=1, how="any")

        # If missing tickers from pivot, skip those
        available_tickers = [t for t in tickers if t in pivot.columns]
        if len(available_tickers) < 2:
            # Can't compute covariance with < 2 assets
            for _, row in day_preds.iterrows():
                all_results.append({
                    "ticker": row["ticker"],
                    "date": date,
                    "target_weight": row["raw_weight"],
                    "mcr": 0.0,
                })
            skipped += 1
            continue

        # Daily log returns
        returns = np.log(pivot[available_tickers] / pivot[available_tickers].shift(1)).dropna()

        if len(returns) < 30:
            # Not enough data for reliable covariance
            for _, row in day_preds.iterrows():
                all_results.append({
                    "ticker": row["ticker"],
                    "date": date,
                    "target_weight": row["raw_weight"],
                    "mcr": 0.0,
                })
            skipped += 1
            continue

        # Use only the last COVARIANCE_WINDOW days
        returns = returns.tail(COVARIANCE_WINDOW)

        # Annualized covariance matrix
        cov_matrix = returns.cov().values * 252

        # Align weights to available tickers
        w = np.array([
            day_preds[day_preds["ticker"] == t]["raw_weight"].values[0]
            if t in day_preds["ticker"].values else 0.0
            for t in available_tickers
        ])

        # Normalize weights to sum to 1
        if w.sum() > 0:
            w = w / w.sum()

        # ── Calculate MCR ────────────────────────────────────
        # Portfolio variance: σ²_p = w^T Σ w
        port_var = w @ cov_matrix @ w
        port_vol = np.sqrt(max(port_var, 1e-10))

        # MCR_i = (Σw)_i / √(w^T Σ w)
        sigma_w = cov_matrix @ w
        mcr = sigma_w / port_vol

        # ── Apply MCR scaling ────────────────────────────────
        adjusted_w = w.copy()
        for j in range(len(available_tickers)):
            if abs(mcr[j]) > MAX_MCR_THRESHOLD:
                scale = MAX_MCR_THRESHOLD / abs(mcr[j])
                adjusted_w[j] *= scale

        # Re-normalize to (1 - CASH_BUFFER)
        total = adjusted_w.sum()
        if total > 0:
            adjusted_w = adjusted_w * (1 - CASH_BUFFER) / total

        # ── Portfolio vol check ──────────────────────────────
        new_port_var = adjusted_w @ cov_matrix @ adjusted_w
        new_port_vol = np.sqrt(max(new_port_var, 1e-10))

        if new_port_vol > MAX_PORTFOLIO_VOL and new_port_vol > 0:
            vol_scale = MAX_PORTFOLIO_VOL / new_port_vol
            adjusted_w *= vol_scale

        # ── Build results ────────────────────────────────────
        for j, ticker in enumerate(available_tickers):
            all_results.append({
                "ticker": ticker,
                "date": date,
                "target_weight": float(adjusted_w[j]),
                "mcr": float(mcr[j]),
            })

    if not all_results:
        print("  ⚠ No portfolio weights computed.")
        conn.close()
        return

    # ── Step 3: Save to target_portfolio ─────────────────────
    print("  Saving to target_portfolio...", end=" ")
    results_df = pd.DataFrame(all_results)
    results_df["date"] = pd.to_datetime(results_df["date"]).dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.executemany(
        """INSERT OR REPLACE INTO target_portfolio
           (ticker, date, target_weight, mcr)
           VALUES (?, ?, ?, ?)""",
        results_df[["ticker", "date", "target_weight", "mcr"]].values.tolist()
    )
    conn.commit()
    conn.close()

    unique_tickers = results_df["ticker"].nunique()
    unique_dates = results_df["date"].nunique()

    print(f"✓ {len(results_df):,} rows")
    print()
    print(f"  ✓ Risk APT complete:")
    print(f"    • {unique_tickers} tickers across {unique_dates:,} dates")
    print(f"    • {skipped} dates skipped (insufficient covariance data)")
    print(f"    • MCR range: {results_df['mcr'].min():.4f} – {results_df['mcr'].max():.4f}")
    print(f"    • Weight range: {results_df['target_weight'].min():.4f} – {results_df['target_weight'].max():.4f}")
    print()


if __name__ == "__main__":
    apply_risk_constraints()
