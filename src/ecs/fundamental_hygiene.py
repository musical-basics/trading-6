"""
fundamental_hygiene.py — Helpers to sanitize quarterly fundamentals.

The core invariant is one canonical filing row per entity per calendar quarter,
using the latest filing_date when multiple near-duplicate rows exist.
"""

from __future__ import annotations

import polars as pl


def canonicalize_quarterly_fundamentals(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse duplicate fundamental rows to latest filing per entity/year/quarter.

    Multiple ingestion sources can emit rows for the same quarter with slightly
    different filing_date values. Keeping only the latest filing_date avoids
    early-date leakage in asof joins.
    """
    if df.is_empty() or "filing_date" not in df.columns:
        return df

    # Ensure date dtype so year/quarter extraction is stable.
    if df["filing_date"].dtype != pl.Date:
        df = df.with_columns(pl.col("filing_date").cast(pl.Date))

    # Infer fiscal period end from filing quarter and enforce minimum +45 day lag.
    # Q1 filings correspond to prior-year Q4 period end (Dec 31).
    q = pl.col("filing_date").dt.quarter()
    y = pl.col("filing_date").dt.year()
    period_end_year = pl.when(q == 1).then(y - 1).otherwise(y)
    period_end_month = (
        pl.when(q == 1).then(pl.lit(12))
        .when(q == 2).then(pl.lit(3))
        .when(q == 3).then(pl.lit(6))
        .otherwise(pl.lit(9))
    )
    period_end_day = (
        pl.when((period_end_month == 3) | (period_end_month == 12)).then(pl.lit(31))
        .otherwise(pl.lit(30))
    )
    inferred_period_end = pl.date(period_end_year, period_end_month, period_end_day)
    min_filing_date = inferred_period_end + pl.duration(days=45)

    with_keys = df.with_columns([
        pl.max_horizontal([pl.col("filing_date"), min_filing_date]).alias("filing_date"),
        pl.col("filing_date").dt.year().alias("_f_year"),
        pl.col("filing_date").dt.quarter().alias("_f_quarter"),
    ])

    payload_cols = [c for c in df.columns if c not in {"entity_id"}]

    canonical = (
        with_keys
        .sort(["entity_id", "filing_date"])
        .group_by(["entity_id", "_f_year", "_f_quarter"], maintain_order=True)
        .agg([pl.col(c).last().alias(c) for c in payload_cols])
        .drop(["_f_year", "_f_quarter"])
        .sort(["entity_id", "filing_date"])
    )

    return canonical
