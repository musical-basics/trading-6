"""
fundamental_ingestion_edgar.py — Quarterly Fundamental Ingestion via SEC EDGAR.

Uses the SEC EDGAR XBRL companyfacts API — truly free, no API key needed.
Provides 10-20+ years of quarterly fundamentals (actual SEC filings).

Fixes applied:
  1. Uses actual SEC filing dates (e["filed"]) — no synthetic delays
  2. Filters revenue by duration (~90 days) to avoid YTD contamination
  3. Expanded revenue tags for banks/financials

Same output schema as fundamental_ingestion.py — writes to quarterly_fundamentals table.
"""

import sqlite3
import time
import requests
import pandas as pd
from datetime import datetime
from src.config import DB_PATH, DEFAULT_UNIVERSE

EDGAR_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
HEADERS = {"User-Agent": "TradingResearch research@example.com"}

# XBRL tags vary across companies — try multiple fallbacks
# Revenue for standard companies
REVENUE_TAGS = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    # Banks and financials
    "RevenuesNetOfInterestExpense",
    "InterestIncomeExpenseNet",
    "NoninterestIncome",
    # Insurance
    "PremiumsEarnedNet",
]

# Balance sheet items (instant concepts — no duration filtering needed)
DEBT_TAGS = [
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "LongTermDebtAndCapitalLeaseObligations",
    "DebtCurrent",
]
CASH_TAGS = [
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsAndShortTermInvestments",
    "Cash",
]
SHARES_TAGS = [
    "CommonStockSharesOutstanding",
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
    "EntityCommonStockSharesOutstanding",
]


def _get_cik_map():
    """Download ticker → CIK mapping from SEC."""
    resp = requests.get(EDGAR_TICKERS_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    cik_map = {}
    for _, entry in data.items():
        cik_map[entry["ticker"]] = str(entry["cik_str"]).zfill(10)
    return cik_map


def _extract_quarterly_instant(facts, tags, unit_key="USD"):
    """
    Extract quarterly 10-Q values for INSTANT concepts (balance sheet items).
    These don't have start dates — just a point-in-time snapshot.
    Deduplicates by end date, keeping the most recently filed version.
    Returns dict: end_date -> {val, filed, end, ...}
    """
    gaap = facts.get("facts", {}).get("us-gaap", {})
    for tag in tags:
        concept = gaap.get(tag, {})
        units = concept.get("units", {})
        entries = units.get(unit_key, [])
        quarterly = {}
        for e in entries:
            if e.get("form") == "10-Q" and e.get("end"):
                end_date = e["end"]
                if end_date not in quarterly or e.get("filed", "") > quarterly[end_date].get("filed", ""):
                    quarterly[end_date] = e
        if quarterly:
            return quarterly
    return {}


def _extract_quarterly_duration(facts, tags, unit_key="USD"):
    """
    Extract quarterly 10-Q values for DURATION concepts (income statement items).

    Critical: filters by duration to avoid YTD contamination.
    A valid single-quarter entry has (end - start) ≈ 85-100 days.
    YTD entries (180, 270 days) are excluded.

    Deduplicates by end date, keeping the most recently filed version.
    Returns dict: end_date -> {val, filed, start, end, ...}
    """
    gaap = facts.get("facts", {}).get("us-gaap", {})
    for tag in tags:
        concept = gaap.get(tag, {})
        units = concept.get("units", {})
        entries = units.get(unit_key, [])
        quarterly = {}
        for e in entries:
            if e.get("form") != "10-Q" or not e.get("end"):
                continue

            # Duration filter: only keep single-quarter entries (~85-100 days)
            start_str = e.get("start")
            end_str = e["end"]
            if start_str:
                try:
                    d_start = datetime.strptime(start_str, "%Y-%m-%d")
                    d_end = datetime.strptime(end_str, "%Y-%m-%d")
                    duration_days = (d_end - d_start).days
                    # Skip YTD (6-month=~180d, 9-month=~270d)
                    if duration_days > 120:
                        continue
                except ValueError:
                    continue

            end_date = end_str
            if end_date not in quarterly or e.get("filed", "") > quarterly[end_date].get("filed", ""):
                quarterly[end_date] = e

        if quarterly:
            return quarterly
    return {}


def ingest_fundamentals_edgar(tickers=None):
    """
    Fetch quarterly fundamentals from SEC EDGAR and upsert into quarterly_fundamentals.
    No API key needed — truly free.
    """
    if tickers is None:
        tickers = DEFAULT_UNIVERSE

    print("=" * 60)
    print("PHASE 1b: Quarterly Fundamental Ingestion (SEC EDGAR)")
    print("=" * 60)

    # Step 1: Get CIK mapping
    print("  Loading SEC ticker → CIK mapping...", end=" ")
    try:
        cik_map = _get_cik_map()
        print(f"✓ {len(cik_map)} tickers mapped.")
    except Exception as e:
        print(f"FAILED: {e}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_inserted = 0
    failed_tickers = []

    for i, ticker in enumerate(tickers):
        # SEC rate limit: max 10 requests/sec
        if i > 0 and i % 8 == 0:
            time.sleep(1.2)

        cik = cik_map.get(ticker)
        if not cik:
            print(f"  {ticker}: CIK not found. Skipping.")
            failed_tickers.append(ticker)
            continue

        try:
            print(f"  Fetching {ticker} (CIK {cik})...", end=" ")

            resp = requests.get(f"{EDGAR_BASE}/CIK{cik}.json", headers=HEADERS, timeout=15)
            resp.raise_for_status()
            facts = resp.json()

            # Revenue is a DURATION concept — must filter by ~90 day periods
            revenue_q = _extract_quarterly_duration(facts, REVENUE_TAGS, "USD")

            # Debt, cash, shares are INSTANT concepts — no duration filtering
            debt_q = _extract_quarterly_instant(facts, DEBT_TAGS, "USD")
            cash_q = _extract_quarterly_instant(facts, CASH_TAGS, "USD")
            shares_q = _extract_quarterly_instant(facts, SHARES_TAGS, "shares")

            # Only keep dates that have at least revenue
            if revenue_q:
                all_dates = set(revenue_q.keys())
            elif cash_q:
                all_dates = set(cash_q.keys())
            else:
                all_dates = set(debt_q.keys()) | set(shares_q.keys())

            ticker_rows = 0
            for end_date in sorted(all_dates):
                try:
                    period_end_date = pd.Timestamp(end_date)

                    # Use ACTUAL SEC filing date — no synthetic delay
                    rev_entry = revenue_q.get(end_date, {})
                    filed_str = rev_entry.get("filed")
                    if not filed_str:
                        # Fallback: check other fields for filing date
                        filed_str = (
                            debt_q.get(end_date, {}).get("filed")
                            or cash_q.get(end_date, {}).get("filed")
                            or shares_q.get(end_date, {}).get("filed")
                        )

                    # Enforce a strict minimum 45-day lag to prevent any ABBV-style leakage
                    # (where period_end_date accidentally masquerades as filing_date)
                    min_lag_date = period_end_date + pd.Timedelta(days=45)
                    
                    if filed_str:
                        actual_sec_date = pd.Timestamp(filed_str)
                        filing_date = max(actual_sec_date, min_lag_date)
                    else:
                        # Last resort: use period_end + 45 days
                        filing_date = min_lag_date

                    revenue = rev_entry.get("val")
                    debt = debt_q.get(end_date, {}).get("val")
                    cash = cash_q.get(end_date, {}).get("val")
                    shares = shares_q.get(end_date, {}).get("val")

                    cursor.execute("""
                        INSERT OR REPLACE INTO quarterly_fundamentals
                        (ticker, period_end_date, filing_date, revenue,
                         total_debt, cash_and_equivalents, shares_outstanding)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker,
                        period_end_date.strftime("%Y-%m-%d"),
                        filing_date.strftime("%Y-%m-%d"),
                        revenue, debt, cash, shares,
                    ))
                    ticker_rows += 1
                except Exception:
                    continue

            conn.commit()
            total_inserted += ticker_rows
            print(f"✓ {ticker_rows} quarters stored.")

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print("RATE LIMITED — waiting 5s...")
                time.sleep(5)
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
    ingest_fundamentals_edgar(tickers=["AAPL", "MSFT"])
