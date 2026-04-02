"""
macro_ingestion.py — Level 3 Phase 1c: Macro Factor Harvester

Fetches historical daily closes for:
  - ^VIX   (Volatility Index)
  - ^VIX3M (3-Month Volatility Index)
  - ^TNX   (10-Year Treasury Yield)
  - SPY    (Market proxy / Beta benchmark)

Upserts into the macro_factors table (date PK).
"""

import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from src.config import DB_PATH, MACRO_TICKERS


def ingest_macro_factors():
    """
    Download macro factor data and merge into a single DataFrame
    keyed by date, then upsert to macro_factors.
    """
    print("=" * 60)
    print("PHASE 1c: Macro Data Ingestion")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1825)  # ~5 years

    # Download each factor individually
    frames = {}
    for label, symbol in MACRO_TICKERS.items():
        print(f"  Fetching {symbol}...", end=" ")
        try:
            df = yf.download(
                symbol,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=False,
            )
            if df.empty:
                print(f"WARNING: No data for {symbol}")
                continue

            # Flatten multi-level columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df = df.rename(columns={"Date": "date", "Close": f"{label}_close"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            frames[label] = df[["date", f"{label}_close"]]
            print(f"✓ {len(df)} rows")
        except Exception as e:
            print(f"FAILED: {e}")
            continue

    # Merge on date (inner join — only dates with all factors)
    required_labels = ["vix", "tnx", "spy"]
    if not all(l in frames for l in required_labels):
        print("  ⚠ Could not fetch required macro factors (VIX, TNX, SPY). Aborting.")
        conn.close()
        return

    merged = frames["vix"]
    for label in ["vix3m", "tnx", "irx", "spy"]:
        if label in frames:
            merged = merged.merge(frames[label], on="date", how="left")

    # Upsert into macro_factors
    cursor = conn.cursor()
    cursor.executemany(
        """INSERT OR REPLACE INTO macro_factors
           (date, vix_close, vix3m_close, tnx_close, irx_close, spy_close)
           VALUES (?, ?, ?, ?, ?, ?)""",
        merged[["date", "vix_close", "vix3m_close", "tnx_close", "irx_close", "spy_close"]].values.tolist()
    )
    conn.commit()
    conn.close()

    print(f"\n  ✓ {len(merged):,} macro factor rows saved to macro_factors\n")


if __name__ == "__main__":
    ingest_macro_factors()
