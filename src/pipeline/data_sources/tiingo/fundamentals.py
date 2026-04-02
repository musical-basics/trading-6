"""
fundamental_ingestion_tiingo.py — Quarterly Fundamental Ingestion via Tiingo.

Uses Tiingo's fundamentals API for quarterly income statements and balance sheets.
Free tier: 3 years of data for Dow 30 tickers, 500 req/day.
Paid tier: 20+ years, all tickers.

Requires TIINGO_API_KEY in .env.local (free at tiingo.com).
Falls back to yfinance if no key is set.

Same output schema as fundamental_ingestion.py — writes to quarterly_fundamentals table.
"""

import os
import sqlite3
import requests
import pandas as pd
from datetime import timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE, FILING_DELAY_DAYS

TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/fundamentals"
TIINGO_API_KEY = os.environ.get("TIINGO_API_KEY", "")


def _tiingo_get(ticker):
    """Fetch quarterly statements from Tiingo."""
    url = f"{TIINGO_BASE_URL}/{ticker}/statements"
    headers = {"Content-Type": "application/json"}
    params = {
        "token": TIINGO_API_KEY,
        "startDate": "2010-01-01",
        "statementType": "quarterly",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest_fundamentals_tiingo(tickers=None):
    """
    Fetch quarterly fundamentals from Tiingo and upsert into quarterly_fundamentals.

    Auto-falls back to yfinance if no TIINGO_API_KEY is set.
    """
    if not TIINGO_API_KEY:
        print("  ⚠ TIINGO_API_KEY not set. Falling back to yfinance ingestion.")
        from src.pipeline.fundamental_ingestion import ingest_fundamentals
        return ingest_fundamentals(tickers)

    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion (Tiingo)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for ticker in tickers:
        try:
            print(f"  Fetching {ticker}...", end=" ")

            data = _tiingo_get(ticker)
            if not data:
                print("WARNING: No data. Skipping.")
                failed_tickers.append(ticker)
                continue

            ticker_rows = 0
            for statement in data:
                try:
                    period_end_str = statement.get("date", "")[:10]
                    if not period_end_str:
                        continue

                    period_end_date = pd.Timestamp(period_end_str)
                    filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)

                    # Tiingo nests data in statementData dict
                    sd = statement.get("statementData", {})

                    # Income statement fields
                    income = sd.get("incomeStatement", {})
                    revenue = income.get("revenue", {}).get("value")

                    # Balance sheet fields
                    balance = sd.get("balanceSheet", {})
                    total_debt = balance.get("totalDebt", {}).get("value")
                    if total_debt is None:
                        total_debt = balance.get("totalLiabilities", {}).get("value")
                    cash = balance.get("cashAndEquiv", {}).get("value")
                    if cash is None:
                        cash = balance.get("cashAndST", {}).get("value")
                    shares = balance.get("sharesBasic", {}).get("value")

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
            if e.response is not None and e.response.status_code == 403:
                print("FAILED: API key invalid or fundamentals not included in plan")
            elif e.response is not None and e.response.status_code == 429:
                print("FAILED: Rate limited")
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
    ingest_fundamentals_tiingo(tickers=["AAPL", "MSFT"])
