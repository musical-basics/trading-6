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

    Two-pass disclosure-lag enforcement:

    Pass A — Raw period-end detection:
        A filing_date in the last 3 days of a standard quarter-end month
        (March, June, September, or December, day >= 28) is almost certainly
        the fiscal period-end date stored directly without the required SEC
        disclosure lag (e.g. ADBE's fiscal Q2 end June 29, fiscal Q3 end
        September 28).  These dates are shifted forward by 45 days.

    Pass B — 1-quarter-offset floor:
        For all other dates the covered period end is inferred via the
        standard 1-quarter offset rule (Q1 filing ↔ prior-year Q4 period end,
        Q2 filing ↔ Q1 period end, etc.) and filing_date is clamped to
        max(filing_date, inferred_period_end + 45).

    After correction, _f_year / _f_quarter are derived from the corrected
    date so that same-period de-duplication works regardless of whether the
    original date was a raw period-end or a true filing date.
    """
    if df.is_empty() or "filing_date" not in df.columns:
        return df

    # Ensure date dtype so year/quarter extraction is stable.
    if df["filing_date"].dtype != pl.Date:
        df = df.with_columns(pl.col("filing_date").cast(pl.Date))

    # ── Pass A: detect raw period-end dates (no disclosure lag applied) ────
    # A filing_date on the last 3 days of a standard quarter-end month is
    # a strong signal that yfinance's period-end index was stored verbatim.
    # Shift it forward by 45 days (the minimum SEC 10-Q/10-K publication lag).
    fd_month = pl.col("filing_date").dt.month()
    fd_day = pl.col("filing_date").dt.day()
    is_raw_period_end = fd_month.is_in([3, 6, 9, 12]) & (fd_day >= 28)

    # ── Pass B: 1-quarter-offset inference floor ────────────────────────────
    # For dates that don't look like raw period ends, infer the covered period
    # end and clamp filing_date to max(filing_date, period_end + 45).
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

    corrected_filing_date = (
        pl.when(is_raw_period_end)
        .then(pl.col("filing_date") + pl.duration(days=45))
        .otherwise(pl.max_horizontal([pl.col("filing_date"), min_filing_date]))
    )

    # Apply correction first, then derive group keys from the corrected date.
    df = df.with_columns(corrected_filing_date.alias("filing_date"))

    payload_cols = [c for c in df.columns if c not in {"entity_id"}]

    canonical = (
        df.with_columns([
            pl.col("filing_date").dt.year().alias("_f_year"),
            pl.col("filing_date").dt.quarter().alias("_f_quarter"),
        ])
        .sort(["entity_id", "filing_date"])
        .group_by(["entity_id", "_f_year", "_f_quarter"], maintain_order=True)
        .agg([pl.col(c).last().alias(c) for c in payload_cols])
        .drop(["_f_year", "_f_quarter"])
        .sort(["entity_id", "filing_date"])
    )

    return canonical
