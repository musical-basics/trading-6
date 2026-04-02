"""
dynamic_dcf.py — Level 3 Phase 2C: Dynamic DCF Valuations

Uses APT-derived factor betas to calculate a Dynamic Discount Rate
per stock per day, then runs a simplified Gordon Growth Model to
compute Intrinsic Value and the NPV Gap (over/undervaluation).

Math:
  1. r_i = R_f + (β_spy × ERP) + (β_vix × λ_VIX)
  2. V_0 = Revenue_annualized × (1 + g) / (r_i - g)
  3. dcf_npv_gap = (V_0 / shares - price) / price
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import (
    DB_PATH, RISK_FREE_RATE, EQUITY_RISK_PREMIUM,
    VIX_RISK_PREMIUM, GROWTH_RATE
)


def compute_dynamic_dcf():
    """
    Calculate Dynamic DCF valuations for all stocks using APT betas.
    Writes dynamic_discount_rate and dcf_npv_gap columns that will
    be consumed by the ML Feature Assembly phase.
    """
    print("=" * 60)
    print("PHASE 2C: Dynamic DCF Valuations")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load daily prices ────────────────────────────
    print("  Loading daily_bars...", end=" ")
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(prices_df):,} rows")

    # ── Step 2: Load factor betas ────────────────────────────
    print("  Loading factor_betas...", end=" ")
    betas_df = pd.read_sql_query(
        "SELECT ticker, date, beta_spy, beta_vix, beta_tnx FROM factor_betas",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(betas_df):,} rows")

    # ── Step 3: Load quarterly fundamentals ──────────────────
    print("  Loading quarterly_fundamentals...", end=" ")
    fundamentals_df = pd.read_sql_query(
        """SELECT ticker, filing_date, revenue, shares_outstanding
           FROM quarterly_fundamentals
           ORDER BY ticker, filing_date""",
        conn,
        parse_dates=["filing_date"],
    )
    print(f"✓ {len(fundamentals_df):,} rows")

    if prices_df.empty or betas_df.empty or fundamentals_df.empty:
        print("  ⚠ Missing input data. Ensure Phases 1–2A are complete.")
        conn.close()
        return

    # ── Step 4: merge_asof fundamentals to prices (bias-free) ─
    print("  Aligning fundamentals via merge_asof...", end=" ")
    prices_df = prices_df.sort_values("date")
    fundamentals_df = fundamentals_df.sort_values("filing_date")

    # Drop rows missing critical fields
    fundamentals_df = fundamentals_df.dropna(
        subset=["revenue", "shares_outstanding"]
    )
    fundamentals_df = fundamentals_df[fundamentals_df["revenue"] > 0]
    fundamentals_df = fundamentals_df[fundamentals_df["shares_outstanding"] > 0]

    merged = pd.merge_asof(
        prices_df,
        fundamentals_df,
        left_on="date",
        right_on="filing_date",
        by="ticker",
        direction="backward",
    )
    merged = merged.dropna(subset=["revenue", "shares_outstanding"])
    print(f"✓ {len(merged):,} rows aligned")

    # ── Step 5: Join betas ───────────────────────────────────
    print("  Joining factor betas...", end=" ")
    merged = merged.merge(
        betas_df,
        on=["ticker", "date"],
        how="inner",
    )
    print(f"✓ {len(merged):,} rows with betas")

    if merged.empty:
        print("  ⚠ No overlapping data between prices, fundamentals, and betas.")
        conn.close()
        return

    # ── Step 6: Calculate Dynamic Discount Rate (Return APT) ─
    print("  Computing Dynamic Discount Rates...", end=" ")

    # r_i = R_f + (β_spy × ERP) + (β_vix × λ_VIX)
    merged["dynamic_discount_rate"] = (
        RISK_FREE_RATE
        + (merged["beta_spy"] * EQUITY_RISK_PREMIUM)
        + (merged["beta_vix"] * VIX_RISK_PREMIUM)
    )

    # Clamp: if r_i <= g, the Gordon Growth Model breaks
    min_discount = GROWTH_RATE + 0.01  # at least 1% spread above g
    merged["dynamic_discount_rate"] = merged["dynamic_discount_rate"].clip(lower=min_discount)

    print("✓")

    # ── Step 7: Gordon Growth DCF Proxy ──────────────────────
    print("  Computing Intrinsic Values (Gordon Growth)...", end=" ")

    # V_0 = Revenue_annualized * (1 + g) / (r_i - g)
    revenue_annualized = merged["revenue"] * 4
    merged["intrinsic_value_total"] = (
        revenue_annualized * (1 + GROWTH_RATE)
        / (merged["dynamic_discount_rate"] - GROWTH_RATE)
    )

    # Per-share intrinsic value
    merged["intrinsic_value_per_share"] = (
        merged["intrinsic_value_total"] / merged["shares_outstanding"]
    )

    # NPV Gap = (intrinsic - current price) / current price
    merged["dcf_npv_gap"] = (
        (merged["intrinsic_value_per_share"] - merged["adj_close"])
        / merged["adj_close"]
    )

    # Handle edge cases (inf, extreme values)
    merged["dcf_npv_gap"] = merged["dcf_npv_gap"].replace([np.inf, -np.inf], np.nan)
    merged = merged.dropna(subset=["dcf_npv_gap"])

    print(f"✓ {len(merged):,} rows with valid DCF")

    # ── Store result for downstream (ml_feature_assembly will read from DB) ─
    # Save DCF results back to a staging approach: we'll write key columns
    # to the ml_features table in Phase 2D. For now, persist to a temp
    # table or let Phase 2D do the merge. We'll store in factor_betas-adjacent
    # fashion by updating the factor_betas with DCF columns... 
    # 
    # Actually, per the architecture: dynamic_dcf returns a DataFrame
    # that Phase 2D (ml_feature_assembly) will consume. Let's save to
    # a lightweight "dcf_valuations" staging approach via the DB.
    
    print("  Saving DCF staging data...", end=" ")
    
    # Create a staging table for DCF results
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _dcf_staging (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            dynamic_discount_rate REAL,
            dcf_npv_gap REAL,
            PRIMARY KEY (ticker, date)
        )
    """)
    
    output_df = merged[["ticker", "date", "dynamic_discount_rate", "dcf_npv_gap"]].copy()
    output_df["date"] = output_df["date"].dt.strftime("%Y-%m-%d")
    
    cursor.executemany(
        """INSERT OR REPLACE INTO _dcf_staging
           (ticker, date, dynamic_discount_rate, dcf_npv_gap)
           VALUES (?, ?, ?, ?)""",
        output_df.values.tolist()
    )
    conn.commit()
    conn.close()

    unique_tickers = merged["ticker"].nunique()
    print(f"✓ {len(output_df):,} rows")
    print()
    print(f"  ✓ Dynamic DCF computed: {len(output_df):,} rows for {unique_tickers} tickers")
    print(f"  ✓ Discount rates range: {merged['dynamic_discount_rate'].min():.3f} – {merged['dynamic_discount_rate'].max():.3f}")
    print(f"  ✓ NPV Gap range: {merged['dcf_npv_gap'].min():.2f} – {merged['dcf_npv_gap'].max():.2f}")
    print()


if __name__ == "__main__":
    compute_dynamic_dcf()
