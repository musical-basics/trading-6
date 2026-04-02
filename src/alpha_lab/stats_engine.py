"""
stats_engine.py — Aligned Data Profile Generator

Computes statistical distributions (min, max, mean, median, std, null%)
per column. Stats are computed PER SOURCE (raw parquet) to avoid the
sparse-join problem where fundamental data shows 99% null because it
only exists on quarterly filing dates but market_data is daily.

The features are then flattened into a single dict for the LLM and UI.
"""

import math
import json
import os
from typing import Optional

import polars as pl

from src.core.duckdb_store import get_parquet_path, PARQUET_DIR
from src.config import INDICATOR_METADATA


# Columns to skip in stats (identifiers, not features)
_SKIP_COLS = {"entity_id", "date", "ticker", "filing_date"}

# Which parquet sources to scan, and which columns to skip per source
_SOURCES = {
    "market_data": {"select_cols": None},  # None = all columns
    "feature": {"select_cols": None},
    "macro": {"select_cols": None},
    "fundamental": {"select_cols": None},
}


def _compute_column_stats(col: pl.Series, total_rows: int) -> dict:
    """Compute stats for a single Polars Series."""
    null_count = col.null_count()
    stats = {
        "null_pct": round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0,
    }

    if col.dtype.is_numeric():
        numeric = col.drop_nulls().cast(pl.Float64)
        if len(numeric) > 0:
            vals = {
                "min": float(numeric.min()),
                "max": float(numeric.max()),
                "mean": float(numeric.mean()),
                "median": float(numeric.median()),
                "std_dev": float(numeric.std()) if len(numeric) > 1 else 0.0,
            }
            # Clean NaN/Inf
            for k, v in vals.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    vals[k] = None
            stats.update(vals)

    return stats


def _compute_source_profile(source_name: str) -> Optional[dict]:
    """Compute stats from a single parquet source (raw, not joined).

    This avoids the sparse-join problem where fundamental data shows
    99% null because it only exists on quarterly filing dates.
    """
    path = get_parquet_path(source_name)
    if not os.path.exists(path):
        return None

    df = pl.read_parquet(path)
    if df.is_empty():
        return None

    total_rows = len(df)
    profile = {}

    for col_name in df.columns:
        if col_name in _SKIP_COLS:
            continue

        col = df[col_name]
        meta = INDICATOR_METADATA.get(col_name, {})
        if isinstance(meta, str):
            category = "other"
            description = meta
        elif isinstance(meta, dict):
            category = meta.get("category", "other")
            description = meta.get("description", "Engineered feature component.")
        else:
            category = "other"
            description = "Engineered feature component."

        profile[col_name] = {
            "dtype": str(col.dtype),
            "category": category,
            "description": description,
            "source": source_name,
            "source_rows": total_rows,
            "stats": _compute_column_stats(col, total_rows),
        }

    return profile


def generate_aligned_data_profile() -> dict:
    """Generate the full profile by scanning each parquet source independently.

    Returns:
        {
            "sources": {source_name: row_count, ...},
            "features": {col: {dtype, category, description, source, stats}, ...}
        }
    """
    all_features = {}
    sources_info = {}

    for source_name in _SOURCES:
        source_profile = _compute_source_profile(source_name)
        if source_profile:
            # Track source metadata
            first_col = next(iter(source_profile.values()), {})
            sources_info[source_name] = first_col.get("source_rows", 0)
            # Merge into flat dict
            all_features.update(source_profile)

    # Add universe metadata
    entity_map_path = os.path.join(PARQUET_DIR, "entity_map.parquet")
    universe_count = 0
    if os.path.exists(entity_map_path):
        emap = pl.read_parquet(entity_map_path)
        universe_count = len(emap)

    return {
        "sources": sources_info,
        "universe_size": universe_count,
        "features": all_features,
    }


def build_profile_for_llm() -> str:
    """Build a JSON string of the profile for LLM prompt injection.

    Returns the profile as formatted JSON that Claude can parse as a
    structured data reference for calibrating thresholds.
    """
    profile = generate_aligned_data_profile()
    if not profile.get("features"):
        return ""

    # Build compact version — description + key stats only
    llm_profile = {}
    for col_name, col_info in profile["features"].items():
        entry = {
            "description": col_info["description"],
            "dtype": col_info["dtype"],
        }
        stats = col_info.get("stats", {})
        for k in ["min", "max", "mean", "median", "std_dev", "null_pct"]:
            if k in stats and stats[k] is not None:
                entry[k] = stats[k]
        llm_profile[col_name] = entry

    return json.dumps(
        {
            "universe_size": profile.get("universe_size", 0),
            "features": llm_profile,
        },
        indent=2,
    )
