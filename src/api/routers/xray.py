"""
xray.py — FastAPI router for X-Ray diagnostic inspection.

The Glass Box: For a given (ticker, date), returns the full pipeline
vertical slice showing every stage of calculation.
"""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException
import polars as pl

from src.core.duckdb_store import get_parquet_path, PARQUET_DIR

router = APIRouter(prefix="/api/diagnostics", tags=["diagnostics"])


@router.get("/pipeline-coverage")
async def pipeline_coverage():
    """Return data-quality stats for every ticker across all pipeline stages.

    For each parquet component, reports: row count, date range,
    and null percentages for critical columns.
    """
    entity_map = _load_entity_map()
    if not entity_map:
        return {"tickers": []}

    # Reverse map
    id_to_ticker: dict[int, str] = {v: k for k, v in entity_map.items()}

    components = {
        "market_data":      {"key_cols": ["adj_close", "volume", "daily_return"]},
        "fundamental":      {"key_cols": ["revenue", "total_debt", "cash", "shares_out"],
                             "date_col": "filing_date"},
        "feature":          {"key_cols": ["ev_sales_zscore", "dynamic_discount_rate",
                                          "dcf_npv_gap", "beta_spy"]},
        "action_intent":    {"key_cols": ["strategy_id", "raw_weight"]},
        "target_portfolio": {"key_cols": ["target_weight", "mcr"]},
    }

    ticker_stats: dict[str, dict] = {}
    for ticker, eid in entity_map.items():
        ticker_stats[ticker] = {"entity_id": eid}

    for comp_name, meta in components.items():
        path = get_parquet_path(comp_name)
        if not os.path.exists(path):
            for ticker in ticker_stats:
                ticker_stats[ticker][comp_name] = None
            continue

        try:
            df = pl.read_parquet(path)
        except Exception:
            for ticker in ticker_stats:
                ticker_stats[ticker][comp_name] = None
            continue

        date_col = meta.get("date_col", "date")

        for eid, ticker in id_to_ticker.items():
            if ticker not in ticker_stats:
                continue

            rows = df.filter(pl.col("entity_id") == eid)
            if rows.is_empty():
                ticker_stats[ticker][comp_name] = {"rows": 0}
                continue

            n = len(rows)
            dates = rows[date_col].sort()
            info: dict = {
                "rows": n,
                "date_start": str(dates[0]),
                "date_end": str(dates[-1]),
            }

            # Null percentages for key columns
            nulls: dict[str, float] = {}
            for col in meta["key_cols"]:
                if col in rows.columns:
                    null_count = rows[col].null_count()
                    nulls[col] = round(null_count / n * 100, 1) if n > 0 else 0
                else:
                    nulls[col] = 100.0
            info["null_pct"] = nulls

            ticker_stats[ticker][comp_name] = info

    # Build response sorted alphabetically
    result = []
    for ticker in sorted(ticker_stats.keys()):
        entry = {"ticker": ticker, **ticker_stats[ticker]}
        result.append(entry)

    return {"tickers": result}


def _load_entity_map() -> dict[str, int]:
    """Load ticker → entity_id mapping."""
    path = os.path.join(PARQUET_DIR, "entity_map.parquet")
    if not os.path.exists(path):
        return {}
    df = pl.read_parquet(path)
    return dict(zip(df["ticker"].to_list(), df["entity_id"].to_list()))


@router.get("/tickers")
async def list_tickers():
    """Return all available tickers from the entity map."""
    entity_map = _load_entity_map()
    tickers = sorted(entity_map.keys())
    return {"tickers": tickers}


@router.get("/xray/{ticker}/{date}")
async def xray_inspection(ticker: str, date: str):
    """Get the full pipeline X-Ray for a specific ticker and date.

    Returns the calculation waterfall:
      1. Raw market data (price, volume)
      2. Fundamental data (revenue, debt, cash)
      3. Feature scores (Z-score, betas, DCF gap)
      4. Strategy intent (raw_weight from XGBoost/strategies)
      5. Risk adjustment (MCR, scaling)
      6. Final target weight
    """
    ticker = ticker.upper()
    entity_map = _load_entity_map()

    if ticker not in entity_map:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")

    entity_id = entity_map[ticker]
    target_date = date

    result = {"ticker": ticker, "date": date, "entity_id": entity_id}

    # Card 1: Raw Market Data
    try:
        market = pl.read_parquet(get_parquet_path("market_data"))
        row = market.filter(
            (pl.col("entity_id") == entity_id)
            & (pl.col("date") == pl.lit(target_date).str.to_date())
        )
        if not row.is_empty():
            r = row.to_dicts()[0]
            result["raw_data"] = {
                "price": r.get("adj_close"),
                "volume": r.get("volume"),
                "daily_return": r.get("daily_return"),
            }
        else:
            result["raw_data"] = None
    except Exception:
        result["raw_data"] = None

    # Card 2: Fundamental Data
    try:
        fundamental = pl.read_parquet(get_parquet_path("fundamental"))
        # Get the most recent fundamental before this date
        fund_row = fundamental.filter(
            (pl.col("entity_id") == entity_id)
            & (pl.col("filing_date") <= pl.lit(target_date).str.to_date())
        ).sort("filing_date", descending=True).head(1)

        if not fund_row.is_empty():
            f = fund_row.to_dicts()[0]
            result["fundamentals"] = {
                "filing_date": str(f.get("filing_date")),
                "revenue": f.get("revenue"),
                "total_debt": f.get("total_debt"),
                "cash": f.get("cash"),
                "shares_outstanding": f.get("shares_out"),
            }
        else:
            result["fundamentals"] = None
    except Exception:
        result["fundamentals"] = None

    # Card 3: Feature Scores (Heuristics + Betas)
    try:
        feature = pl.read_parquet(get_parquet_path("feature"))
        feat_row = feature.filter(
            (pl.col("entity_id") == entity_id)
            & (pl.col("date") == pl.lit(target_date).str.to_date())
        )
        if not feat_row.is_empty():
            fdict = feat_row.to_dicts()[0]
            result["features"] = {
                "ev_sales_zscore": fdict.get("ev_sales_zscore"),
                "dynamic_discount_rate": fdict.get("dynamic_discount_rate"),
                "dcf_npv_gap": fdict.get("dcf_npv_gap"),
                "beta_spy": fdict.get("beta_spy"),
                "beta_tnx": fdict.get("beta_tnx"),
                "beta_vix": fdict.get("beta_vix"),
            }
        else:
            result["features"] = None
    except Exception:
        result["features"] = None

    # Card 4: Strategy Intent (Raw Weight)
    try:
        intent_path = get_parquet_path("action_intent")
        if os.path.exists(intent_path):
            intent = pl.read_parquet(intent_path)
            intent_row = intent.filter(
                (pl.col("entity_id") == entity_id)
                & (pl.col("date") == pl.lit(target_date).str.to_date())
            )
            if not intent_row.is_empty():
                idict = intent_row.to_dicts()[0]
                result["strategy_intent"] = {
                    "strategy_id": idict.get("strategy_id"),
                    "raw_weight": idict.get("raw_weight"),
                }
            else:
                result["strategy_intent"] = None
        else:
            result["strategy_intent"] = None
    except Exception:
        result["strategy_intent"] = None

    # Card 5: Risk Adjustment
    try:
        target_path = get_parquet_path("target_portfolio")
        if os.path.exists(target_path):
            target = pl.read_parquet(target_path)
            target_row = target.filter(
                (pl.col("entity_id") == entity_id)
                & (pl.col("date") == pl.lit(target_date).str.to_date())
            )
            if not target_row.is_empty():
                tdict = target_row.to_dicts()[0]
                raw_w = result.get("strategy_intent", {})
                raw_weight = raw_w.get("raw_weight", 0) if raw_w else 0

                result["risk_adjustment"] = {
                    "target_weight": tdict.get("target_weight"),
                    "mcr": tdict.get("mcr"),
                    "mcr_threshold": 0.05,
                    "mcr_breach": abs(tdict.get("mcr", 0)) > 0.05,
                    "original_weight": raw_weight,
                    "scaled": raw_weight != tdict.get("target_weight", 0),
                }
            else:
                result["risk_adjustment"] = None
        else:
            result["risk_adjustment"] = None
    except Exception:
        result["risk_adjustment"] = None

    # Card 6: Final Target
    if result.get("risk_adjustment"):
        result["final_order"] = {
            "target_allocation": result["risk_adjustment"]["target_weight"],
        }
    elif result.get("strategy_intent"):
        result["final_order"] = {
            "target_allocation": result["strategy_intent"]["raw_weight"],
        }
    else:
        result["final_order"] = None

    return result
