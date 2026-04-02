"""
EODHD — Quarterly Fundamental Ingestion.

Uses EODHD's fundamentals API for quarterly balance sheets and income statements.
Free tier: 20 API calls/day for US tickers. Paid ($20/mo): unlimited.

Requires EODHD_API_KEY in .env.local (free at eodhistoricaldata.com).
Falls back to yfinance if no key is set.
"""

import os
import sqlite3
import requests
import pandas as pd
from datetime import timedelta
from src.config import DB_PATH, DEFAULT_UNIVERSE, FILING_DELAY_DAYS

EODHD_BASE = "https://eodhd.com/api/fundamentals"
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")


def _eodhd_get(ticker):
    """Fetch fundamentals from EODHD."""
    url = f"{EODHD_BASE}/{ticker}.US"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
        "filter": "Financials",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest_fundamentals_eodhd(tickers=None):
    """
    Fetch quarterly fundamentals from EODHD and upsert into quarterly_fundamentals.
    """
    if not EODHD_API_KEY:
        print("  ⚠ EODHD_API_KEY not set. Falling back to yfinance ingestion.")
        from src.pipeline.fundamental_ingestion import ingest_fundamentals
        return ingest_fundamentals(tickers)

    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion (EODHD)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for ticker in tickers:
        try:
            print(f"  Fetching {ticker}...", end=" ")
            data = _eodhd_get(ticker)

            if not data or not isinstance(data, dict):
                print("WARNING: No data. Skipping.")
                failed_tickers.append(ticker)
                continue

            financials = data.get("Financials", data)
            income_q = financials.get("Income_Statement", {}).get("quarterly", {})
            balance_q = financials.get("Balance_Sheet", {}).get("quarterly", {})

            ticker_rows = 0
            for date_key, income in income_q.items():
                try:
                    period_end_str = income.get("date", date_key)[:10]
                    if not period_end_str:
                        continue

                    period_end_date = pd.Timestamp(period_end_str)
                    filing_date = period_end_date + timedelta(days=FILING_DELAY_DAYS)

                    revenue = income.get("totalRevenue")
                    if revenue is not None:
                        revenue = float(revenue)

                    # Get matching balance sheet
                    bs = balance_q.get(date_key, {})
                    total_debt = bs.get("longTermDebt") or bs.get("shortLongTermDebt")
                    if total_debt is not None:
                        total_debt = float(total_debt)
                    cash = bs.get("cash") or bs.get("cashAndEquivalents")
                    if cash is not None:
                        cash = float(cash)
                    shares = bs.get("commonStockSharesOutstanding")
                    if shares is not None:
                        shares = float(shares)

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
            if e.response is not None and e.response.status_code in (403, 401):
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
    ingest_fundamentals_eodhd(tickers=["AAPL", "MSFT"])
