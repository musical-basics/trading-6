"""
ingestion_system.py — Level 4 ECS System 1: Data Ingestion

Polars-native data fetching that writes directly to Parquet component files.
Replaces the old Pandas-based data_ingestion.py and macro_ingestion.py.

Notes:
  - yfinance returns Pandas DataFrames, so we convert to Polars immediately.
  - All outputs are written as Parquet to data/components/.
  - daily_return is pre-computed per entity to avoid the teleportation bug.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import polars as pl

from src.config import DEFAULT_UNIVERSE, MACRO_TICKERS, PROJECT_ROOT
from src.core.entity_map import EntityMap
from src.core.duckdb_store import PARQUET_DIR, get_parquet_path
from src.ecs.fundamental_hygiene import canonicalize_quarterly_fundamentals


def _load_entity_map() -> EntityMap:
    """Load the persisted entity map, or create a new one from DEFAULT_UNIVERSE."""
    path = os.path.join(PARQUET_DIR, "entity_map.parquet")
    em = EntityMap()
    if os.path.exists(path):
        df = pl.read_parquet(path)
        tickers = df.sort("entity_id")["ticker"].to_list()
        em.register(tickers)
    else:
        em.register(DEFAULT_UNIVERSE)
    return em


def _save_entity_map(em: EntityMap) -> None:
    """Persist the entity map to Parquet."""
    entity_df = pl.DataFrame({
        "entity_id": pl.Series(em.all_ids(), dtype=pl.Int32),
        "ticker": pl.Series(em.all_tickers(), dtype=pl.Utf8),
    })
    entity_df.write_parquet(os.path.join(PARQUET_DIR, "entity_map.parquet"))


def _get_last_date(parquet_path: str, date_col: str = "date") -> str | None:
    """Get the most recent date in a Parquet file for incremental ingestion."""
    if not os.path.exists(parquet_path):
        return None
    try:
        df = pl.read_parquet(parquet_path)
        if df.is_empty():
            return None
        return str(df[date_col].max())
    except Exception:
        return None


def ingest_prices(
    tickers: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Fetch EOD prices via yfinance and write to market_data.parquet.

    Supports incremental ingestion: if market_data.parquet already exists,
    only fetches data after the last recorded date.
    """
    import yfinance as yf

    print("=" * 60)
    print("ECS SYSTEM 1: Price Ingestion (Polars)")
    print("=" * 60)

    em = _load_entity_map()
    fetch_tickers = tickers or DEFAULT_UNIVERSE
    em.register(fetch_tickers)
    _save_entity_map(em)

    parquet_path = get_parquet_path("market_data")

    # Determine start date (incremental)
    if start_date is None:
        last_date = _get_last_date(parquet_path)
        if last_date:
            start_date = (datetime.strptime(str(last_date), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"  Incremental: fetching from {start_date}")
        else:
            start_date = "2020-01-01"

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Fetch — yfinance returns Pandas, convert immediately
    print(f"  Fetching {len(fetch_tickers)} tickers: {start_date} → {end_date}")
    raw_pd = yf.download(fetch_tickers, start=start_date, end=end_date, auto_adjust=False, progress=False)

    if raw_pd.empty:
        print("  ⚠ No new data returned")
        return

    # Convert multi-level columns to flat Polars
    all_frames = []
    for ticker in fetch_tickers:
        try:
            if len(fetch_tickers) == 1:
                ticker_pd = raw_pd[["Adj Close", "Volume"]].copy()
            else:
                ticker_pd = raw_pd[[("Adj Close", ticker), ("Volume", ticker)]].copy()
                ticker_pd.columns = ["adj_close", "volume"]

            if isinstance(ticker_pd.columns[0], tuple):
                ticker_pd.columns = ["adj_close", "volume"]

            ticker_pd = ticker_pd.dropna()
            if ticker_pd.empty:
                continue

            ticker_df = pl.from_pandas(ticker_pd.reset_index())
            # Normalize column names
            col_map = {}
            for c in ticker_df.columns:
                cl = c.lower().replace(" ", "_")
                if cl == "adj_close" or cl == "adj close":
                    col_map[c] = "adj_close"
                elif cl in ("date", "index"):
                    col_map[c] = "date"
                elif cl == "volume":
                    col_map[c] = "volume"
            ticker_df = ticker_df.rename(col_map)

            entity_id = em.ticker_to_id(ticker)
            ticker_df = ticker_df.with_columns(
                pl.lit(entity_id).cast(pl.Int32).alias("entity_id"),
            )
            all_frames.append(ticker_df)
        except Exception as e:
            print(f"    ⚠ {ticker}: {e}")

    if not all_frames:
        print("  ⚠ No valid data frames")
        return

    new_df = pl.concat(all_frames)

    # Ensure date column is Date type
    if new_df["date"].dtype != pl.Date:
        new_df = new_df.with_columns(pl.col("date").cast(pl.Date))

    # Select and cast
    new_df = new_df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("date"),
        pl.col("adj_close").cast(pl.Float32),
        pl.col("volume").cast(pl.Int64),
    ])

    # Merge with existing data
    if os.path.exists(parquet_path):
        existing = pl.read_parquet(parquet_path).select([
            "entity_id", "date", "adj_close", "volume"
        ])
        combined = pl.concat([existing, new_df]).unique(subset=["entity_id", "date"], keep="last")
    else:
        combined = new_df

    # Sort and compute daily_return per entity
    combined = combined.sort(["entity_id", "date"])
    combined = combined.with_columns(
        (pl.col("adj_close") / pl.col("adj_close").shift(1).over("entity_id") - 1)
        .cast(pl.Float32)
        .alias("daily_return")
    )

    combined.write_parquet(parquet_path)
    print(f"  ✓ {len(new_df):,} new rows → {len(combined):,} total rows")


def ingest_fundamentals(
    tickers: list[str] | None = None,
) -> None:
    """Fetch quarterly fundamentals via yfinance and write to fundamental.parquet."""
    import yfinance as yf

    print("=" * 60)
    print("ECS SYSTEM 1: Fundamental Ingestion (Polars)")
    print("=" * 60)

    em = _load_entity_map()
    fetch_tickers = tickers or [t for t in DEFAULT_UNIVERSE if t not in ("SPY", "QQQ")]

    parquet_path = get_parquet_path("fundamental")
    all_frames = []

    for ticker in fetch_tickers:
        try:
            stock = yf.Ticker(ticker)
            bs = stock.quarterly_balance_sheet
            fins = stock.quarterly_financials

            if bs is None or bs.empty or fins is None or fins.empty:
                continue

            entity_id = em.ticker_to_id(ticker)
            records = []

            for col_date in bs.columns:
                # col_date is the quarter PERIOD-END date from yfinance.
                # We must add 45 days to simulate the SEC filing publication lag
                # (companies have 40-45 days after quarter-end to file 10-Q/10-K).
                # Without this offset, strategies can use fundamental data the same
                # day it is "filed", which is impossible in practice — this is the
                # exact earnings-leakage vector the forensic auditor detected.
                period_end = col_date.date()
                filing_date = period_end + timedelta(days=45)

                row: dict = {"entity_id": entity_id, "filing_date": filing_date}
                row["total_debt"] = float(bs.at["Total Debt", col_date]) if "Total Debt" in bs.index else None
                row["cash"] = float(bs.at["Cash And Cash Equivalents", col_date]) if "Cash And Cash Equivalents" in bs.index else None
                row["shares_out"] = float(bs.at["Ordinary Shares Number", col_date]) if "Ordinary Shares Number" in bs.index else None

                if col_date in fins.columns and "Total Revenue" in fins.index:
                    row["revenue"] = float(fins.at["Total Revenue", col_date])
                else:
                    row["revenue"] = None

                records.append(row)

            if records:
                df = pl.DataFrame(records)
                all_frames.append(df)
                print(f"    {ticker}: ✓ {len(records)} quarters (latest filing_date: {records[-1]['filing_date']})")
        except Exception as e:
            print(f"    {ticker}: ⚠ {e}")

    if not all_frames:
        print("  ⚠ No fundamental data fetched")
        return

    new_df = pl.concat(all_frames)
    new_df = new_df.with_columns(pl.col("filing_date").cast(pl.Date))
    new_df = new_df.select([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("filing_date"),
        pl.col("revenue").cast(pl.Float32),
        pl.col("total_debt").cast(pl.Float32),
        pl.col("cash").cast(pl.Float32),
        pl.col("shares_out").cast(pl.Float32),
    ])

    # Merge with existing
    if os.path.exists(parquet_path):
        existing = pl.read_parquet(parquet_path)
        combined = pl.concat([existing, new_df]).unique(
            subset=["entity_id", "filing_date"], keep="last"
        )
    else:
        combined = new_df

    combined = canonicalize_quarterly_fundamentals(combined)
    combined.write_parquet(parquet_path)
    print(f"  ✓ {len(combined):,} total fundamental rows")

    # ── Staleness report ────────────────────────────────────────────────────
    # Warn operators when a ticker's latest filing_date is older than 540 days.
    # This is the stale-data condition the forensic auditor checks at trade time.
    today = datetime.now().date()
    stale_threshold_days = 540
    latest_per_entity = (
        combined
        .group_by("entity_id")
        .agg(pl.col("filing_date").max().alias("latest_filing"))
        .with_columns(
            ((pl.lit(str(today)).str.to_date() - pl.col("latest_filing")).dt.total_days())
            .alias("days_stale")
        )
        .filter(pl.col("days_stale") > stale_threshold_days)
        .sort("days_stale", descending=True)
    )
    if not latest_per_entity.is_empty():
        print(f"\n  ⚠️  STALENESS WARNING — {len(latest_per_entity)} ticker(s) have fundamentals older than {stale_threshold_days} days:")
        # Load entity map for ticker labels
        em_path = os.path.join(os.path.dirname(parquet_path), "entity_map.parquet")
        if os.path.exists(em_path):
            emap = pl.read_parquet(em_path)
            stale_report = latest_per_entity.join(emap, on="entity_id", how="left")
            for row in stale_report.to_dicts():
                print(f"    • {row.get('ticker', row['entity_id'])}: last filing {row['latest_filing']} ({int(row['days_stale'])} days ago)")
        else:
            for row in latest_per_entity.to_dicts():
                print(f"    • entity_id={row['entity_id']}: last filing {row['latest_filing']} ({int(row['days_stale'])} days ago)")
        print(f"  → Re-run ingest_fundamentals() or switch to EDGAR source to refresh.")
    else:
        print(f"  ✓ All tickers have fresh fundamentals (none older than {stale_threshold_days} days).")


def ingest_macro(
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Fetch macro factors (VIX, VIX3M, 10Y Yield, SPY) and write to macro.parquet."""
    import yfinance as yf

    print("=" * 60)
    print("ECS SYSTEM 1: Macro Ingestion (Polars)")
    print("=" * 60)

    parquet_path = get_parquet_path("macro")

    if start_date is None:
        last_date = _get_last_date(parquet_path)
        if last_date:
            start_date = (datetime.strptime(str(last_date), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = "2020-01-01"

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"  Fetching macro factors: {start_date} → {end_date}")

    macro_symbols = list(MACRO_TICKERS.values())
    raw_pd = yf.download(macro_symbols, start=start_date, end=end_date, auto_adjust=False, progress=False)

    if raw_pd.empty:
        print("  ⚠ No new macro data")
        return

    frames = {}
    for label, symbol in MACRO_TICKERS.items():
        try:
            if len(macro_symbols) == 1:
                series = raw_pd["Adj Close"]
            else:
                series = raw_pd[("Adj Close", symbol)]
            frames[label] = series.dropna()
        except Exception:
            pass

    if not frames:
        print("  ⚠ No macro data parsed")
        return

    import pandas as pd  # Only used to merge yfinance output
    macro_pd = pd.DataFrame(frames).dropna()
    macro_pd.index.name = "date"

    new_df = pl.from_pandas(macro_pd.reset_index())
    new_df = new_df.rename({c: c.lower() for c in new_df.columns})

    if "date" in new_df.columns and new_df["date"].dtype != pl.Date:
        new_df = new_df.with_columns(pl.col("date").cast(pl.Date))

    new_df = new_df.select([
        pl.col("date"),
        pl.col("vix").cast(pl.Float32),
        pl.col("vix3m").cast(pl.Float32),
        pl.col("tnx").cast(pl.Float32),
        pl.col("spy").cast(pl.Float32),
    ])

    if os.path.exists(parquet_path):
        existing = pl.read_parquet(parquet_path)
        combined = pl.concat([existing, new_df]).unique(subset=["date"], keep="last")
    else:
        combined = new_df

    combined = combined.sort("date")
    combined.write_parquet(parquet_path)
    print(f"  ✓ {len(combined):,} total macro rows")


if __name__ == "__main__":
    ingest_prices()
    ingest_fundamentals()
    ingest_macro()
