"""
cross_sectional_scoring.py — Level 2 Phase 2: The Math Desk

Fuses daily prices with quarterly fundamentals using pd.merge_asof
(direction='backward') to prevent look-ahead bias, then computes:
  1. Enterprise Value (EV)
  2. EV/Sales ratio
  3. Cross-sectional Z-scores (daily ranking across the universe)
  4. Target portfolio weights for undervalued stocks

All math operations are fully vectorized (no for-loops).
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import (
    DB_PATH, ZSCORE_BUY_THRESHOLD, MAX_SINGLE_WEIGHT, CASH_BUFFER
)


def compute_cross_sectional_scores():
    """
    Run the full cross-sectional scoring pipeline:
      1. Load daily_bars and quarterly_fundamentals from SQLite
      2. merge_asof to align fundamentals to prices (bias-free)
      3. Compute EV, EV/Sales, daily Z-scores
      4. Generate target weights for undervalued tickers
      5. Upsert results to cross_sectional_scores table
    """
    print("=" * 60)
    print("PHASE 2: Cross-Sectional Scoring (The Math Desk)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load raw data ────────────────────────────────
    print("  Loading daily_bars...", end=" ")
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close, volume FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(prices_df):,} rows")

    print("  Loading quarterly_fundamentals...", end=" ")
    fundamentals_df = pd.read_sql_query(
        """SELECT ticker, filing_date, revenue, total_debt,
                  cash_and_equivalents, shares_outstanding
           FROM quarterly_fundamentals
           ORDER BY ticker, filing_date""",
        conn,
        parse_dates=["filing_date"],
    )
    print(f"✓ {len(fundamentals_df):,} rows")

    if prices_df.empty:
        print("  ⚠ No price data. Run Phase 1 first.")
        conn.close()
        return

    if fundamentals_df.empty:
        print("  ⚠ No fundamental data. Run fundamental ingestion first.")
        conn.close()
        return

    # ── Step 2: merge_asof (CRITICAL — prevents look-ahead bias) ─
    print("  Aligning fundamentals to prices via merge_asof...", end=" ")

    # merge_asof requires left_on key to be globally sorted
    prices_df = prices_df.sort_values("date")
    fundamentals_df = fundamentals_df.sort_values("filing_date")

    # Drop rows with missing critical fundamental fields
    fundamentals_df = fundamentals_df.dropna(
        subset=["revenue", "shares_outstanding"]
    )

    merged = pd.merge_asof(
        prices_df,
        fundamentals_df,
        left_on="date",
        right_on="filing_date",
        by="ticker",
        direction="backward",
    )

    # Drop rows where no fundamental data has been filed yet
    pre_merge_len = len(merged)
    merged = merged.dropna(subset=["revenue", "shares_outstanding"])
    print(f"✓ {len(merged):,} rows aligned ({pre_merge_len - len(merged):,} pre-filing rows dropped)")

    if merged.empty:
        print("  ⚠ No aligned data. Ensure fundamentals have been ingested.")
        conn.close()
        return

    # ── Step 3: Compute EV, EV/Sales (vectorized) ───────────
    print("  Computing Enterprise Value and EV/Sales...", end=" ")

    # Market Value (Market Cap) = price × shares outstanding
    merged["market_value"] = merged["adj_close"] * merged["shares_outstanding"]

    # EV = Market Cap + total_debt - cash_and_equivalents
    merged["enterprise_value"] = (
        merged["market_value"]
        + merged["total_debt"].fillna(0)
        - merged["cash_and_equivalents"].fillna(0)
    )

    # EV/Sales = EV / (quarterly_revenue * 4) — annualized
    merged["ev_to_sales"] = merged["enterprise_value"] / (merged["revenue"] * 4)

    # Handle edge cases: infinite or NaN EV/Sales
    merged["ev_to_sales"] = merged["ev_to_sales"].replace(
        [np.inf, -np.inf], np.nan
    )
    merged = merged.dropna(subset=["ev_to_sales"])

    print(f"✓ {len(merged):,} rows with valid EV/Sales")

    # ── Step 4: Cross-sectional Z-scores (vectorized groupby) ─
    print("  Computing daily cross-sectional Z-scores...", end=" ")

    # Z = (X - mean) / std, computed per day across all tickers
    merged["ev_sales_zscore"] = merged.groupby("date")["ev_to_sales"].transform(
        lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0.0
    )

    # Drop days where Z-score couldn't be computed (single-ticker days)
    merged = merged.dropna(subset=["ev_sales_zscore"])

    unique_dates = merged["date"].nunique()
    unique_tickers = merged["ticker"].nunique()
    print(f"✓ {unique_dates:,} dates × {unique_tickers:,} tickers")

    # ── Step 5: Generate target weights ──────────────────────
    print("  Generating target portfolio weights...", end=" ")

    merged["target_weight"] = 0.0

    # Buy candidates: Z-score below threshold (statistically undervalued)
    buy_mask = merged["ev_sales_zscore"] < ZSCORE_BUY_THRESHOLD

    if buy_mask.any():
        # Count BUY candidates per day
        buy_counts_per_day = merged.loc[buy_mask].groupby("date")["ticker"].transform("count")

        # Equal weight, capped at MAX_SINGLE_WEIGHT
        raw_weight = 1.0 / buy_counts_per_day
        capped_weight = np.minimum(raw_weight, MAX_SINGLE_WEIGHT)

        merged.loc[buy_mask, "target_weight"] = capped_weight.values

        # Enforce cash buffer: scale down if sum > (1 - CASH_BUFFER)
        max_total = 1.0 - CASH_BUFFER
        daily_weight_sums = merged.groupby("date")["target_weight"].transform("sum")
        over_budget = daily_weight_sums > max_total
        if over_budget.any():
            scale_factor = max_total / daily_weight_sums
            merged.loc[over_budget, "target_weight"] *= scale_factor[over_budget]

    buy_days = merged.loc[buy_mask, "date"].nunique() if buy_mask.any() else 0
    total_buy_signals = buy_mask.sum()
    print(f"✓ {total_buy_signals:,} BUY signals across {buy_days:,} days")

    # ── Step 6: Upsert to SQLite ─────────────────────────────
    print("  Saving to cross_sectional_scores table...", end=" ")

    output_df = merged[["ticker", "date", "market_value", "enterprise_value", "ev_to_sales",
                         "ev_sales_zscore", "target_weight"]].copy()
    output_df["date"] = output_df["date"].dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()

    # Batch insert using executemany for performance
    cursor.executemany(
        """INSERT OR REPLACE INTO cross_sectional_scores
           (ticker, date, market_value, enterprise_value, ev_to_sales, ev_sales_zscore, target_weight)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        output_df.values.tolist()
    )
    conn.commit()

    total_rows = len(output_df)
    print(f"✓ {total_rows:,} rows saved")

    conn.close()

    # ── Summary ──────────────────────────────────────────────
    print()
    print(f"  ✓ Cross-sectional scoring complete:")
    print(f"    • {unique_tickers} tickers scored across {unique_dates:,} trading days")
    print(f"    • {total_buy_signals:,} BUY signals (Z < {ZSCORE_BUY_THRESHOLD})")
    print(f"    • {total_rows:,} rows written to cross_sectional_scores")
    print()


if __name__ == "__main__":
    compute_cross_sectional_scores()
