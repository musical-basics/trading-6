"""
macro_regime_strategy.py — Macro Regime Rotation Strategy

Strategy ID: macro_regime_rotation
Type: PORTFOLIO (exposure scaling)

Logic:
  Uses VIX levels and rate-of-change signals to classify the macro
  environment into three regimes:

  1. Risk-On:  VIX < 20 AND VIX SMA(50) trending down
     → 100% equity exposure (equal-weight universe)
  2. Caution:  VIX 20-30 OR 10Y yield ROC(50) > 10%
     → 50% equity exposure
  3. Risk-Off: VIX > 30 OR VIX ROC(20) > 40%
     → 0% exposure (all cash)

  The exposure scalar is applied to the equal-weight daily return
  of the full stock universe.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import (
    DB_PATH,
    VIX_RISK_ON_THRESHOLD,
    VIX_RISK_OFF_THRESHOLD,
    VIX_SPIKE_ROC_PCT,
    TNX_RATE_SHOCK_ROC_PCT,
)


def _classify_regime(row):
    """
    Classify a single day's macro environment.
    Returns (regime_label, exposure_scalar).
    """
    vix = row["vix_close"]
    vix_roc_20 = row["vix_roc_20"]
    tnx_roc_50 = row["tnx_roc_50"]
    vix_sma_50 = row["vix_sma_50"]

    # Risk-Off: VIX > 30 or VIX spiking
    if vix > VIX_RISK_OFF_THRESHOLD or vix_roc_20 > VIX_SPIKE_ROC_PCT:
        return "risk_off", 0.0

    # Risk-On: VIX < 20 and VIX trend declining
    if vix < VIX_RISK_ON_THRESHOLD and vix < vix_sma_50:
        return "risk_on", 1.0

    # Caution: everything else (VIX 20-30, or rate shock)
    if tnx_roc_50 > TNX_RATE_SHOCK_ROC_PCT:
        return "caution", 0.5

    # Default caution for VIX in 20-30 range
    if vix >= VIX_RISK_ON_THRESHOLD:
        return "caution", 0.5

    # VIX < 20 but trending up — still risk-on but cautious
    return "risk_on", 1.0


def compute_macro_regime_signals():
    """
    Read macro_factors, compute regime classification per day,
    and save to macro_regime_signals table.
    """
    print("=" * 60)
    print("MACRO REGIME: Signal Generation")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # Load macro data
    print("  Loading macro_factors...", end=" ")
    macro_df = pd.read_sql_query(
        "SELECT date, vix_close, tnx_close, spy_close FROM macro_factors ORDER BY date",
        conn,
        parse_dates=["date"],
    )
    print(f"✓ {len(macro_df):,} rows")

    if macro_df.empty:
        print("  ⚠ No macro data. Run Phase 1c first.")
        conn.close()
        return

    # Compute indicators
    print("  Computing regime indicators...", end=" ")
    macro_df["vix_sma_50"] = macro_df["vix_close"].rolling(window=50).mean()
    macro_df["vix_roc_20"] = macro_df["vix_close"].pct_change(periods=20)
    macro_df["tnx_roc_50"] = macro_df["tnx_close"].pct_change(periods=50)
    print("✓")

    # Drop warm-up rows
    macro_df = macro_df.dropna(subset=["vix_sma_50", "vix_roc_20", "tnx_roc_50"])

    # Classify each day
    print("  Classifying regimes...", end=" ")
    classifications = macro_df.apply(_classify_regime, axis=1, result_type="expand")
    macro_df["regime"] = classifications[0]
    macro_df["exposure"] = classifications[1]
    print("✓")

    # Save to DB
    print("  Saving to macro_regime_signals...", end=" ")
    output_df = macro_df[["date", "vix_close", "vix_sma_50", "vix_roc_20",
                           "tnx_roc_50", "regime", "exposure"]].copy()
    output_df["date"] = output_df["date"].dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.execute("DELETE FROM macro_regime_signals")  # Full refresh
    cursor.executemany(
        """INSERT OR REPLACE INTO macro_regime_signals
           (date, vix_close, vix_sma_50, vix_roc_20, tnx_roc_50, regime, exposure)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        output_df.values.tolist()
    )
    conn.commit()
    conn.close()

    # Summary
    regime_counts = macro_df["regime"].value_counts()
    print(f"✓ {len(output_df):,} rows")
    print()
    print(f"  ✓ Regime breakdown:")
    for regime, count in regime_counts.items():
        pct = count / len(macro_df) * 100
        print(f"    • {regime}: {count:,} days ({pct:.1f}%)")
    print()


if __name__ == "__main__":
    compute_macro_regime_signals()
