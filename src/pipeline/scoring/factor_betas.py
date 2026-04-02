"""
factor_betas.py — Level 3 Phase 2A: Return APT (Rolling OLS Betas)

For each stock, calculates 90-day rolling OLS regression coefficients
against macro factors (SPY, VIX, TNX) using statsmodels.

    r_stock = α + β_spy * r_spy + β_vix * r_vix + β_tnx * r_tnx + ε

These betas feed into:
  1. The Dynamic DCF discount rate (Phase 2C)
  2. XGBoost feature matrix (Phase 2D)
"""

import sqlite3
import pandas as pd
import numpy as np
import statsmodels.api as sm
from src.config import DB_PATH, OLS_ROLLING_WINDOW, DEFAULT_UNIVERSE


def compute_factor_betas():
    """
    Calculate 90-day rolling OLS regression betas for every stock
    against SPY, VIX, and TNX returns. Results stored in factor_betas table.
    """
    print("=" * 60)
    print("PHASE 2A: Rolling OLS Factor Betas (Return APT)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    window = OLS_ROLLING_WINDOW

    # ── Step 1: Load macro factor returns ────────────────────
    print("  Loading macro_factors...", end=" ")
    macro_df = pd.read_sql_query(
        "SELECT date, vix_close, tnx_close, spy_close FROM macro_factors ORDER BY date",
        conn,
        parse_dates=["date"],
    )
    if macro_df.empty:
        print("⚠ No macro data. Run Phase 1c first.")
        conn.close()
        return
    print(f"✓ {len(macro_df):,} rows")

    # Compute daily log returns for macro factors
    macro_df = macro_df.sort_values("date").reset_index(drop=True)
    macro_df["r_spy"] = np.log(macro_df["spy_close"] / macro_df["spy_close"].shift(1))
    macro_df["r_vix"] = np.log(macro_df["vix_close"] / macro_df["vix_close"].shift(1))
    macro_df["r_tnx"] = np.log(macro_df["tnx_close"] / macro_df["tnx_close"].shift(1))
    macro_df = macro_df.dropna(subset=["r_spy", "r_vix", "r_tnx"])

    # ── Step 2: Load stock prices ────────────────────────────
    print("  Loading daily_bars...", end=" ")
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(prices_df):,} rows")

    if prices_df.empty:
        print("  ⚠ No price data. Run Phase 1a first.")
        conn.close()
        return

    # Filter to universe (exclude SPY/QQQ — they are benchmarks, not tradeable here)
    tradeable = [t for t in DEFAULT_UNIVERSE if t not in ("SPY", "QQQ")]
    prices_df = prices_df[prices_df["ticker"].isin(tradeable)]

    # ── Step 3: Rolling OLS per ticker ───────────────────────
    all_betas = []
    tickers = prices_df["ticker"].unique()
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        stock = prices_df[prices_df["ticker"] == ticker].copy()
        stock = stock.sort_values("date").reset_index(drop=True)

        # Compute daily log returns
        stock["r_stock"] = np.log(stock["adj_close"] / stock["adj_close"].shift(1))
        stock = stock.dropna(subset=["r_stock"])

        # Merge with macro returns on date
        merged = stock[["date", "r_stock"]].merge(
            macro_df[["date", "r_spy", "r_vix", "r_tnx"]],
            on="date",
            how="inner",
        )

        if len(merged) < window:
            print(f"  [{i}/{total}] {ticker}: ⚠ Only {len(merged)} rows, need {window}. Skipping.")
            continue

        # Rolling OLS: slide a window of size `window` across the data
        ticker_betas = []
        for end_idx in range(window, len(merged) + 1):
            start_idx = end_idx - window
            w = merged.iloc[start_idx:end_idx]

            Y = w["r_stock"].values
            X = w[["r_spy", "r_vix", "r_tnx"]].values
            X = sm.add_constant(X)

            try:
                model = sm.OLS(Y, X).fit()
                # Coefficients: [const, beta_spy, beta_vix, beta_tnx]
                ticker_betas.append({
                    "ticker": ticker,
                    "date": w["date"].iloc[-1],
                    "beta_spy": model.params[1],
                    "beta_vix": model.params[2],
                    "beta_tnx": model.params[3],
                })
            except Exception:
                # Singular matrix or other OLS failure — skip this window
                continue

        if ticker_betas:
            all_betas.extend(ticker_betas)
            print(f"  [{i}/{total}] {ticker}: ✓ {len(ticker_betas):,} windows computed")
        else:
            print(f"  [{i}/{total}] {ticker}: ⚠ No valid OLS windows")

    if not all_betas:
        print("  ⚠ No beta data computed. Check input data.")
        conn.close()
        return

    # ── Step 4: Save to factor_betas ─────────────────────────
    betas_df = pd.DataFrame(all_betas)
    betas_df["date"] = pd.to_datetime(betas_df["date"]).dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.executemany(
        """INSERT OR REPLACE INTO factor_betas
           (ticker, date, beta_spy, beta_vix, beta_tnx)
           VALUES (?, ?, ?, ?, ?)""",
        betas_df[["ticker", "date", "beta_spy", "beta_vix", "beta_tnx"]].values.tolist()
    )
    conn.commit()
    conn.close()

    unique_tickers = betas_df["ticker"].nunique()
    print()
    print(f"  ✓ Factor betas computed: {len(betas_df):,} rows for {unique_tickers} tickers")
    print(f"  ✓ Saved to factor_betas table")
    print()


if __name__ == "__main__":
    compute_factor_betas()
