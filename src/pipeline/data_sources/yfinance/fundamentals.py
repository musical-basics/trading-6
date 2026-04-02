"""
fundamental_ingestion.py — Level 2 Phase 1b: Quarterly Fundamental Ingestion

Fetches quarterly financial statements from yfinance and stores them
in the quarterly_fundamentals table with a proxy filing_date calculated
as period_end_date + 45 days to prevent look-ahead bias.

This module is designed to work alongside the existing data_ingestion.py
which handles daily bars (Phase 1a).
"""

import sqlite3
import pandas as pd
import yfinance as yf
from datetime import timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE, FILING_DELAY_DAYS


def ingest_fundamentals(tickers=None):
    """
    Fetch quarterly financials for each ticker and upsert into
    the quarterly_fundamentals table.

    Args:
        tickers: Optional list of tickers. Defaults to DEFAULT_UNIVERSE.
    """
    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for ticker in tickers:
        try:
            print(f"  Fetching fundamentals for {ticker}...", end=" ")

            yf_ticker = yf.Ticker(ticker)

            # ── Pull quarterly financial statements ──────────────
            income_stmt = yf_ticker.quarterly_financials
            balance_sheet = yf_ticker.quarterly_balance_sheet

            if income_stmt is None or income_stmt.empty:
                print("WARNING: No income statement data. Skipping.")
                failed_tickers.append(ticker)
                continue

            if balance_sheet is None or balance_sheet.empty:
                print("WARNING: No balance sheet data. Skipping.")
                failed_tickers.append(ticker)
                continue

            # ── Extract data for each quarter ────────────────────
            # Columns are period_end_dates (as datetime index)
            quarters = income_stmt.columns
            ticker_rows = 0

            for period_end in quarters:
                try:
                    period_end_date = pd.Timestamp(period_end)

                    # Revenue — try multiple keys
                    revenue = _safe_get(income_stmt, period_end, [
                        "Total Revenue", "Revenue", "Operating Revenue"
                    ])

                    # Total Debt — from balance sheet
                    total_debt = _safe_get(balance_sheet, period_end, [
                        "Total Debt", "Long Term Debt",
                        "Long Term Debt And Capital Lease Obligation"
                    ])

                    # Cash — from balance sheet
                    cash = _safe_get(balance_sheet, period_end, [
                        "Cash And Cash Equivalents",
                        "Cash Cash Equivalents And Short Term Investments",
                        "Cash And Short Term Investments",
                        "Cash Financial",
                    ])

                    # Shares Outstanding — from balance sheet
                    shares = _safe_get(balance_sheet, period_end, [
                        "Ordinary Shares Number",
                        "Share Issued",
                        "Common Stock Shares Outstanding",
                    ])

                    # Calculate proxy filing date
                    filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)

                    # Upsert into database
                    cursor.execute("""
                        INSERT OR REPLACE INTO quarterly_fundamentals
                        (ticker, period_end_date, filing_date, revenue,
                         total_debt, cash_and_equivalents, shares_outstanding)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker,
                        period_end_date.strftime("%Y-%m-%d"),
                        filing_date.strftime("%Y-%m-%d"),
                        revenue,
                        total_debt,
                        cash,
                        shares,
                    ))
                    ticker_rows += 1

                except Exception as e:
                    # Skip individual quarters that fail
                    continue

            conn.commit()
            total_inserted += ticker_rows
            print(f"✓ {ticker_rows} quarters stored.")

        except Exception as e:
            print(f"FAILED: {e}")
            failed_tickers.append(ticker)
            continue

    conn.commit()
    conn.close()

    print()
    print(f"  ✓ Total quarterly records upserted: {total_inserted}")
    if failed_tickers:
        print(f"  ⚠ Failed tickers: {', '.join(failed_tickers)}")
    else:
        print(f"  ✓ All {len(tickers)} tickers fetched successfully.")
    print()


def _safe_get(df, column, keys):
    """
    Attempt to extract a value from a DataFrame column using multiple
    possible row keys (yfinance labels vary across tickers/versions).

    Returns the first non-null value found, or None if all keys miss.
    """
    for key in keys:
        try:
            val = df.loc[key, column]
            if pd.notna(val):
                return float(val)
        except (KeyError, TypeError):
            continue
    return None


if __name__ == "__main__":
    # Quick smoke test with 2 tickers
    ingest_fundamentals(tickers=["AAPL", "MSFT"])
