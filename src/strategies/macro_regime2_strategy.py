"""
macro_regime2_strategy.py — Macro Regime V2: VIX Term Structure

Strategy ID: macro_regime2_vix_term
Type: PORTFOLIO (exposure scaling)

Logic:
  Uses the VIX / VIX3M ratio (term structure) as a leading indicator.
  When near-term VIX exceeds 3-month VIX (backwardation), it signals
  imminent fear — often BEFORE a VIX spike.

  Term Ratio = VIX / VIX3M

  Regimes:
    1. Risk-On:  ratio < 0.90 (contango, calm markets)
       → 100% equity exposure
    2. Caution:  ratio 0.90–1.0 (flattening, tension building)
       → 50% equity exposure
    3. Risk-Off: ratio > 1.0 (backwardation, fear imminent)
       → 0% exposure (all cash)

  Additionally, we use a 10-day SMA of the ratio to smooth noise.
  The regime is based on the smoothed ratio.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH


# ── Regime Thresholds ────────────────────────────────────────
TERM_RISK_ON = 0.90       # ratio < 0.90 → contango → risk-on
TERM_RISK_OFF = 1.0       # ratio > 1.0  → backwardation → risk-off
TERM_SMA_WINDOW = 10      # Smoothing window for the ratio


def _classify_term_regime(row):
    """Classify a single day based on smoothed VIX term ratio."""
    ratio = row["term_sma_10"]

    if pd.isna(ratio):
        return "caution", 0.5

    if ratio > TERM_RISK_OFF:
        return "risk_off", 0.0

    if ratio < TERM_RISK_ON:
        return "risk_on", 1.0

    # In between: caution
    return "caution", 0.5


def compute_macro_regime2_signals():
    """
    Read VIX and VIX3M from macro_factors, compute term structure ratio,
    classify regime, and save to macro_regime2_signals.
    """
    print("=" * 60)
    print("MACRO REGIME V2: VIX Term Structure Signal Generation")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # Load macro data
    print("  Loading macro_factors...", end=" ")
    macro_df = pd.read_sql_query(
        "SELECT date, vix_close, vix3m_close FROM macro_factors ORDER BY date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(macro_df):,} rows")

    if macro_df.empty:
        print("  ⚠ No macro data. Run Phase 1c first.")
        conn.close()
        return

    # Drop rows where VIX3M is missing
    before = len(macro_df)
    macro_df = macro_df.dropna(subset=["vix_close", "vix3m_close"])
    if len(macro_df) < before:
        print(f"  Dropped {before - len(macro_df)} rows with missing VIX3M data")

    if macro_df.empty:
        print("  ⚠ No VIX3M data available. Re-run Phase 1c to fetch ^VIX3M.")
        conn.close()
        return

    # Compute term structure ratio
    print("  Computing VIX term structure...", end=" ")
    macro_df["term_ratio"] = macro_df["vix_close"] / macro_df["vix3m_close"]
    macro_df["term_sma_10"] = macro_df["term_ratio"].rolling(window=TERM_SMA_WINDOW).mean()
    print("✓")

    # Drop warm-up
    macro_df = macro_df.dropna(subset=["term_sma_10"])

    # Classify each day
    print("  Classifying regimes...", end=" ")
    classifications = macro_df.apply(_classify_term_regime, axis=1, result_type="expand")
    macro_df["regime"] = classifications[0]
    macro_df["exposure"] = classifications[1]
    print("✓")

    # Save to DB
    print("  Saving to macro_regime2_signals...", end=" ")
    output_df = macro_df[["date", "vix_close", "vix3m_close", "term_ratio",
                           "term_sma_10", "regime", "exposure"]].copy()
    output_df["date"] = output_df["date"].dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.execute("DELETE FROM macro_regime2_signals")
    cursor.executemany(
        """INSERT OR REPLACE INTO macro_regime2_signals
           (date, vix_close, vix3m_close, term_ratio, term_sma_10, regime, exposure)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        output_df.values.tolist()
    )
    conn.commit()
    conn.close()

    # Summary
    regime_counts = macro_df["regime"].value_counts()
    print(f"✓ {len(output_df):,} rows")
    print()
    print(f"  ✓ Regime breakdown (VIX Term Structure):")
    for regime, count in regime_counts.items():
        pct = count / len(macro_df) * 100
        print(f"    • {regime}: {count:,} days ({pct:.1f}%)")
    print(f"  ✓ Term ratio range: {macro_df['term_ratio'].min():.3f} – {macro_df['term_ratio'].max():.3f}")
    print()


if __name__ == "__main__":
    compute_macro_regime2_signals()
