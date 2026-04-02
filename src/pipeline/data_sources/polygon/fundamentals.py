"""
Polygon.io — Quarterly Fundamental Ingestion.

Uses Polygon's financials API (vX/reference/financials) for quarterly data.
Free tier: 5 API calls/min. Paid ($29/mo): unlimited + 10+ years history.

Requires POLYGON_API_KEY in .env.local.
Falls back to yfinance if no key is set.
"""

import os
import sqlite3
import time
import requests
import pandas as pd
from datetime import timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE, FILING_DELAY_DAYS

POLYGON_BASE = "https://api.polygon.io/vX/reference/financials"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")


def _polygon_get(ticker):
    """Fetch quarterly financials from Polygon."""
    params = {
        "ticker": ticker,
        "timeframe": "quarterly",
        "limit": 50,
        "apiKey": POLYGON_API_KEY,
    }
    resp = requests.get(POLYGON_BASE, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def ingest_fundamentals_polygon(tickers=None):
    """
    Fetch quarterly fundamentals from Polygon.io and upsert into quarterly_fundamentals.
    """
    if not POLYGON_API_KEY:
        print("  ⚠ POLYGON_API_KEY not set. Falling back to yfinance ingestion.")
        from src.pipeline.fundamental_ingestion import ingest_fundamentals
        return ingest_fundamentals(tickers)

    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion (Polygon.io)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for i, ticker in enumerate(tickers):
        # Polygon free tier: 5 calls/min — wait 13s between calls
        if i > 0:
            time.sleep(13)

        try:
            print(f"  Fetching {ticker} ({i+1}/{len(tickers)})...", end=" ")
            results = _polygon_get(ticker)

            if not results:
                print("WARNING: No data. Skipping.")
                failed_tickers.append(ticker)
                continue

            ticker_rows = 0
            for filing in results:
                try:
                    period_end_str = filing.get("end_date", "")[:10]
                    if not period_end_str:
                        continue

                    period_end_date = pd.Timestamp(period_end_str)
                    filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)

                    # Polygon nests financials under 'financials' key
                    fin = filing.get("financials", {})
                    income = fin.get("income_statement", {})
                    balance = fin.get("balance_sheet", {})

                    revenue = income.get("revenues", {}).get("value")
                    total_debt = balance.get("long_term_debt", {}).get("value")
                    cash = balance.get("cash", {}).get("value")
                    if cash is None:
                        cash = balance.get("cash_and_cash_equivalents", {}).get("value")
                    shares = (balance.get("common_stock_shares_outstanding", {}).get("value")
                              or income.get("basic_average_shares", {}).get("value"))

                    cursor.execute("""
                        INSERT OR REPLACE INTO quarterly_fundamentals
                        (ticker, period_end_date, filing_date, revenue,
                         total_debt, cash_and_equivalents, shares_outstanding)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker,
                        period_end_date.strftime("%Y-%m-%d"),
                        filing_date.strftime("%Y-%m-%d"),
                        revenue, total_debt, cash, shares,
                    ))
                    ticker_rows += 1
                except Exception:
                    continue

            conn.commit()
            total_inserted += ticker_rows
            print(f"✓ {ticker_rows} quarters stored.")

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print("RATE LIMITED — waiting 60s and retrying...")
                time.sleep(60)
                try:
                    results = _polygon_get(ticker)
                    ticker_rows = 0
                    for filing in results:
                        try:
                            period_end_str = filing.get("end_date", "")[:10]
                            if not period_end_str:
                                continue
                            period_end_date = pd.Timestamp(period_end_str)
                            filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)
                            fin = filing.get("financials", {})
                            income = fin.get("income_statement", {})
                            balance = fin.get("balance_sheet", {})
                            revenue = income.get("revenues", {}).get("value")
                            total_debt = balance.get("long_term_debt", {}).get("value")
                            cash = balance.get("cash", {}).get("value") or balance.get("cash_and_cash_equivalents", {}).get("value")
                            shares = balance.get("common_stock_shares_outstanding", {}).get("value") or income.get("basic_average_shares", {}).get("value")
                            cursor.execute("""INSERT OR REPLACE INTO quarterly_fundamentals
                                (ticker, period_end_date, filing_date, revenue, total_debt, cash_and_equivalents, shares_outstanding)
                                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                (ticker, period_end_date.strftime("%Y-%m-%d"), filing_date.strftime("%Y-%m-%d"), revenue, total_debt, cash, shares))
                            ticker_rows += 1
                        except Exception:
                            continue
                    conn.commit()
                    total_inserted += ticker_rows
                    print(f"  ✓ {ticker_rows} quarters stored (after retry).")
                except Exception as e2:
                    print(f"  Still failing: {e2}")
                    failed_tickers.append(ticker)
            elif e.response is not None and e.response.status_code == 403:
                print("FAILED: API key invalid")
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
    ingest_fundamentals_polygon(tickers=["AAPL", "MSFT"])
