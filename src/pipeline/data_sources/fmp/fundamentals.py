"""
fundamental_ingestion_fmp.py — Quarterly Fundamental Ingestion via FMP API.

Uses Financial Modeling Prep (financialmodelingprep.com) stable endpoints
to fetch quarterly income statements and balance sheets.

Free tier: 5 quarters per ticker.
Paid tier: 10+ years (50+ quarters) per ticker.

Falls back to yfinance if no FMP_API_KEY is set.

Same output schema as fundamental_ingestion.py — writes to quarterly_fundamentals table.
"""

import os
import sqlite3
import requests
import pandas as pd
from datetime import timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE, FILING_DELAY_DAYS

FMP_BASE_URL = "https://financialmodelingprep.com/stable"
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")


def _fmp_get(endpoint, ticker):
    """Fetch JSON from FMP stable API."""
    url = f"{FMP_BASE_URL}/{endpoint}"
    params = {"symbol": ticker, "period": "quarter", "apikey": FMP_API_KEY}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def ingest_fundamentals_fmp(tickers=None):
    """
    Fetch quarterly fundamentals from FMP and upsert into quarterly_fundamentals.

    Auto-falls back to yfinance if no FMP_API_KEY is set.
    """
    if not FMP_API_KEY:
        print("  ⚠ FMP_API_KEY not set. Falling back to yfinance ingestion.")
        from src.pipeline.fundamental_ingestion import ingest_fundamentals
        return ingest_fundamentals(tickers)

    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion (FMP)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for ticker in tickers:
        try:
            print(f"  Fetching {ticker}...", end=" ")

            income = _fmp_get("income-statement", ticker)
            balance = _fmp_get("balance-sheet-statement", ticker)

            if not income:
                print("WARNING: No income data. Skipping.")
                failed_tickers.append(ticker)
                continue

            # Index balance sheet by date for fast lookup
            balance_by_date = {item["date"]: item for item in balance} if balance else {}

            ticker_rows = 0
            for stmt in income:
                try:
                    period_end_str = stmt.get("date")
                    if not period_end_str:
                        continue

                    period_end_date = pd.Timestamp(period_end_str)

                    # Revenue from income statement
                    revenue = stmt.get("revenue")

                    # Get matching balance sheet data
                    bs = balance_by_date.get(period_end_str, {})
                    total_debt = bs.get("totalDebt") or bs.get("longTermDebt")
                    cash = (bs.get("cashAndCashEquivalents")
                            or bs.get("cashAndShortTermInvestments"))
                    shares = bs.get("commonStock")

                    # Calculate proxy filing date
                    filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)

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

                except Exception:
                    continue

            conn.commit()
            total_inserted += ticker_rows
            print(f"✓ {ticker_rows} quarters stored.")

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                print("FAILED: API key invalid or rate limited")
            else:
                print(f"FAILED: {e}")
            failed_tickers.append(ticker)
        except Exception as e:
            print(f"FAILED: {e}")
            failed_tickers.append(ticker)

    conn.commit()
    conn.close()

    print()
    print(f"  ✓ Total quarterly records upserted: {total_inserted}")
    if failed_tickers:
        print(f"  ⚠ Failed tickers: {', '.join(failed_tickers)}")
    else:
        print(f"  ✓ All {len(tickers)} tickers fetched successfully.")
    print()


if __name__ == "__main__":
    ingest_fundamentals_fmp(tickers=["AAPL", "MSFT"])
