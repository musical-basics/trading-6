"""
backfill_shares.py — Patch fundamental.parquet with shares_outstanding from yfinance.

For tickers with null shares_out, fetches current sharesOutstanding
from yfinance ticker.info and fills all null rows.

This is safe because shares outstanding changes slowly (stock splits
are already adjusted in adj_close), so using the latest figure
as a constant approximation is reasonable for EV calculations.

Usage:
    python3 -m src.scripts.backfill_shares
"""

from __future__ import annotations

import os
import polars as pl
import yfinance as yf

from src.core.duckdb_store import get_parquet_path, PARQUET_DIR


def backfill_shares_outstanding() -> None:
    """Patch null shares_out values in fundamental.parquet using yfinance."""
    print("=" * 60)
    print("BACKFILL: shares_outstanding from yfinance")
    print("=" * 60)

    # Load entity map and fundamentals
    em = pl.read_parquet(os.path.join(PARQUET_DIR, "entity_map.parquet"))
    emap = dict(zip(em["entity_id"].to_list(), em["ticker"].to_list()))

    fund_path = get_parquet_path("fundamental")
    fund = pl.read_parquet(fund_path)

    # Find tickers with null shares_out
    null_stats = (
        fund.group_by("entity_id")
        .agg(pl.col("shares_out").is_null().sum().alias("null_count"))
        .filter(pl.col("null_count") > 0)
    )

    need_fix = [
        emap[row["entity_id"]]
        for row in null_stats.iter_rows(named=True)
        if row["entity_id"] in emap
    ]

    if not need_fix:
        print("  ✓ No tickers need backfill!")
        return

    print(f"  {len(need_fix)} tickers need shares_outstanding backfill:")
    print(f"  {', '.join(sorted(need_fix))}\n")

    # Fetch shares outstanding from yfinance
    ticker_to_eid = {v: k for k, v in emap.items()}
    patched = 0
    failed = []

    shares_map: dict[int, float] = {}  # entity_id → shares

    for ticker in sorted(need_fix):
        try:
            info = yf.Ticker(ticker).info
            shares = info.get("sharesOutstanding")
            if shares and shares > 0:
                eid = ticker_to_eid[ticker]
                shares_map[eid] = float(shares)
                print(f"  ✓ {ticker}: {shares:,.0f} shares")
                patched += 1
            else:
                print(f"  ⚠ {ticker}: No sharesOutstanding in yfinance info")
                failed.append(ticker)
        except Exception as e:
            print(f"  ✗ {ticker}: {e}")
            failed.append(ticker)

    if not shares_map:
        print("\n  ⚠ No data fetched. Aborting.")
        return

    # Patch the DataFrame
    print(f"\n  Patching {patched} tickers in fundamental.parquet...")

    # Build a mapping: for each entity_id, fill null shares_out
    def fill_shares(entity_id: int, current_shares: float | None) -> float | None:
        if current_shares is not None:
            return current_shares
        return shares_map.get(entity_id)

    # Use Polars to patch: if shares_out is null AND entity_id is in our map, fill it
    patched_fund = fund.with_columns(
        pl.when(pl.col("shares_out").is_null())
        .then(
            pl.col("entity_id").replace_strict(
                shares_map, default=None
            ).cast(pl.Float32)
        )
        .otherwise(pl.col("shares_out"))
        .alias("shares_out")
    )

    # Verify
    before_nulls = fund["shares_out"].null_count()
    after_nulls = patched_fund["shares_out"].null_count()
    print(f"  Before: {before_nulls} null rows")
    print(f"  After:  {after_nulls} null rows")
    print(f"  Fixed:  {before_nulls - after_nulls} rows")

    # Write back
    patched_fund.write_parquet(fund_path)
    print(f"\n  ✓ fundamental.parquet updated!")

    if failed:
        print(f"  ⚠ Still missing: {', '.join(failed)}")

    print()


if __name__ == "__main__":
    backfill_shares_outstanding()
