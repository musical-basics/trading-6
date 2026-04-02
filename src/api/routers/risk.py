"""
risk.py — FastAPI router for Risk War Room endpoints.
"""

from __future__ import annotations

import os

from fastapi import APIRouter
import polars as pl
import numpy as np

from src.core.duckdb_store import get_parquet_path, PARQUET_DIR

router = APIRouter(prefix="/api/risk", tags=["risk"])


@router.get("/summary")
async def risk_summary():
    """Return current risk dashboard data: VIX, macro regime, covariance, MCR."""
    result: dict = {}

    # Macro data
    try:
        macro = pl.read_parquet(get_parquet_path("macro")).sort("date")
        latest = macro.tail(1).to_dicts()[0]
        prev = macro.tail(2).head(1).to_dicts()[0] if len(macro) > 1 else latest

        result["vix"] = latest.get("vix")
        result["vix_change"] = round(latest.get("vix", 0) - prev.get("vix", 0), 2)
        result["ten_year_yield"] = latest.get("tnx")
        result["yield_change"] = round(latest.get("tnx", 0) - prev.get("tnx", 0), 3)

        vix = latest.get("vix", 20)
        if vix < 20:
            result["macro_regime"] = "Risk-On"
        elif vix > 30:
            result["macro_regime"] = "Risk-Off"
        else:
            result["macro_regime"] = "Caution"
    except Exception:
        result["vix"] = None
        result["macro_regime"] = "Unknown"

    # Covariance heatmap (top 8 stocks by weight in target portfolio)
    try:
        market = pl.read_parquet(get_parquet_path("market_data"))
        entity_map_path = os.path.join(PARQUET_DIR, "entity_map.parquet")
        emap = pl.read_parquet(entity_map_path)

        # Get top entities from target portfolio or just top by market cap
        target_path = get_parquet_path("target_portfolio")
        if os.path.exists(target_path):
            target = pl.read_parquet(target_path)
            latest_date = target["date"].max()
            top_entities = (
                target.filter(pl.col("date") == latest_date)
                .sort("target_weight", descending=True)
                .head(8)["entity_id"].to_list()
            )
        else:
            top_entities = emap.head(8)["entity_id"].to_list()

        # Get tickers for labels
        tickers = [
            emap.filter(pl.col("entity_id") == eid)["ticker"].to_list()[0]
            for eid in top_entities
            if not emap.filter(pl.col("entity_id") == eid).is_empty()
        ]

        # Compute correlation/covariance matrix
        returns = market.filter(pl.col("entity_id").is_in(top_entities))
        pivot = returns.pivot(on="entity_id", index="date", values="daily_return").sort("date").drop_nulls()

        ret_cols = [str(eid) for eid in top_entities if str(eid) in pivot.columns]
        if len(ret_cols) >= 2:
            ret_matrix = pivot.select(ret_cols).tail(90).to_numpy()
            corr = np.corrcoef(ret_matrix, rowvar=False)
            result["covariance_matrix"] = np.round(corr, 3).tolist()
            result["tickers"] = tickers[:len(ret_cols)]
    except Exception:
        result["covariance_matrix"] = []
        result["tickers"] = []

    # MCR data (from target portfolio)
    try:
        target = pl.read_parquet(get_parquet_path("target_portfolio"))
        latest_date = target["date"].max()
        emap = pl.read_parquet(os.path.join(PARQUET_DIR, "entity_map.parquet"))

        mcr_df = (
            target.filter(pl.col("date") == latest_date)
            .sort("mcr", descending=True)
            .head(5)
            .join(emap, on="entity_id", how="left")
        )

        result["mcr_data"] = [
            {
                "ticker": row["ticker"],
                "mcr": round(abs(row["mcr"]) * 100, 2),
                "threshold": 5.0,
            }
            for row in mcr_df.to_dicts()
        ]
    except Exception:
        result["mcr_data"] = []

    return result
