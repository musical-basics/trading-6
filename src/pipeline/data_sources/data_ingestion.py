"""
data_ingestion.py — Level 1 Phase 1: Data Ingestion

Fetches 5 years of EOD market data from yfinance for the configured
ticker universe and upserts into the daily_bars SQLite table.
"""

import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE

# Mutable universe (can be overridden by the UI)
UNIVERSE = list(DEFAULT_UNIVERSE)


def ingest():
    """
    Fetch 5 years of daily EOD data for each ticker in the universe
    and upsert into the daily_bars table.
    """
    print("=" * 60)
    print("PHASE 1: Data Ingestion")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=1825)  # ~5 years

    total_inserted = 0
    failed_tickers = []

    for ticker in UNIVERSE:
        try:
            print(f"  Fetching {ticker}...", end=" ")

            df = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                progress=False,
                auto_adjust=False,
            )

            if df.empty:
                print(f"WARNING: No data returned for {ticker}. Skipping.")
                failed_tickers.append(ticker)
                continue

            # Flatten multi-level columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Prepare DataFrame for SQLite insertion
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            })
            df["ticker"] = ticker
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

            # Select only the columns we need
            df = df[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]]

            # Count before insert
            rows_before = pd.read_sql_query(
                "SELECT COUNT(*) as cnt FROM daily_bars WHERE ticker = ?",
                conn, params=(ticker,)
            )["cnt"][0]

            # Direct INSERT OR IGNORE using cursor
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute(
                    """INSERT OR IGNORE INTO daily_bars 
                       (ticker, date, open, high, low, close, adj_close, volume)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row["ticker"], row["date"], row["open"], row["high"],
                     row["low"], row["close"], row["adj_close"], row["volume"])
                )
            conn.commit()

            rows_after = pd.read_sql_query(
                "SELECT COUNT(*) as cnt FROM daily_bars WHERE ticker = ?",
                conn, params=(ticker,)
            )["cnt"][0]

            new_rows = rows_after - rows_before
            total_inserted += new_rows
            print(f"✓ {len(df)} rows fetched, {new_rows} new rows inserted.")

        except Exception as e:
            print(f"FAILED: {e}")
            failed_tickers.append(ticker)
            continue

    conn.commit()
    conn.close()

    print()
    print(f"  ✓ Total new rows inserted: {total_inserted}")
    if failed_tickers:
        print(f"  ⚠ Failed tickers: {', '.join(failed_tickers)}")
    else:
        print(f"  ✓ All {len(UNIVERSE)} tickers fetched successfully.")
    print()


if __name__ == "__main__":
    ingest()
