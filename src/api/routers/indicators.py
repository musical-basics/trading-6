"""
indicators.py — Indicators Analysis API Router

Provides per-ticker computed indicators across three categories:
  - Technical: RSI, momentum, SMA trend, Bollinger Band position, mean reversion
  - Fundamental: EV/Sales, DCF gap, market cap, debt/equity, cash ratio
  - Statistical: CAPM beta, volatility, Sharpe, VaR, factor betas, max drawdown
"""

from __future__ import annotations

import os
import numpy as np
import polars as pl
from fastapi import APIRouter

from src.core.duckdb_store import get_parquet_path, PARQUET_DIR

router = APIRouter(prefix="/api/indicators", tags=["indicators"])


def _load_entity_map() -> dict[str, int]:
    path = os.path.join(PARQUET_DIR, "entity_map.parquet")
    if not os.path.exists(path):
        return {}
    df = pl.read_parquet(path)
    return dict(zip(df["ticker"].to_list(), df["entity_id"].to_list()))


@router.get("/tickers")
async def list_tickers():
    """Return list of available tickers."""
    em = _load_entity_map()
    return {"tickers": sorted(em.keys())}


@router.get("/{ticker}")
async def get_indicators(ticker: str, rfr_source: str = "irx"):
    """Compute all indicators for a given ticker using latest available data."""
    em = _load_entity_map()
    eid = em.get(ticker.upper())
    if eid is None:
        return {"error": f"Unknown ticker: {ticker}"}

    result: dict = {"ticker": ticker.upper(), "entity_id": eid}

    # ── Load data ────────────────────────────────────────────
    market = _load_ticker_market(eid)
    fund = _load_ticker_fundamentals(eid)
    feat = _load_ticker_features(eid)
    macro = _load_macro()

    # Determine risk free rate
    latest_macro = macro[-1] if not macro.is_empty() else None
    rf_value = 0.043 # Default config value
    if rfr_source == "tnx" and latest_macro is not None and latest_macro["tnx"][0] is not None:
        rf_value = float(latest_macro["tnx"][0]) / 100.0
    elif rfr_source == "irx" and latest_macro is not None and latest_macro["irx"][0] is not None:
        rf_value = float(latest_macro["irx"][0]) / 100.0

    # ── Technical Indicators ─────────────────────────────────
    result["technical"] = _compute_technical(market)

    # ── Fundamental Indicators ───────────────────────────────
    result["fundamental"] = _compute_fundamental(market, fund, feat, rf_value)

    # ── Statistical Indicators ───────────────────────────────
    result["statistical"] = _compute_statistical(market, feat, macro, rf_value)

    return result


# ═══════════════════════════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════════════════════════

def _load_ticker_market(eid: int) -> pl.DataFrame:
    path = get_parquet_path("market_data")
    if not os.path.exists(path):
        return pl.DataFrame()
    df = pl.read_parquet(path)
    return df.filter(pl.col("entity_id") == eid).sort("date")


def _load_ticker_fundamentals(eid: int) -> pl.DataFrame:
    path = get_parquet_path("fundamental")
    if not os.path.exists(path):
        return pl.DataFrame()
    df = pl.read_parquet(path)
    return df.filter(pl.col("entity_id") == eid).sort("filing_date")


def _load_ticker_features(eid: int) -> pl.DataFrame:
    path = get_parquet_path("feature")
    if not os.path.exists(path):
        return pl.DataFrame()
    df = pl.read_parquet(path)
    return df.filter(pl.col("entity_id") == eid).sort("date")


def _load_macro() -> pl.DataFrame:
    path = get_parquet_path("macro")
    if not os.path.exists(path):
        return pl.DataFrame()
    return pl.read_parquet(path).sort("date")


# ═══════════════════════════════════════════════════════════════
# Technical Indicators
# ═══════════════════════════════════════════════════════════════

def _compute_technical(market: pl.DataFrame) -> dict | None:
    if market.is_empty() or len(market) < 50:
        return None

    prices = market["adj_close"].to_numpy()
    returns = market["daily_return"].to_numpy()

    latest_price = float(prices[-1])
    n = len(prices)

    # SMA 20 / 50 / 200
    sma_20 = float(np.mean(prices[-20:])) if n >= 20 else None
    sma_50 = float(np.mean(prices[-50:])) if n >= 50 else None
    sma_200 = float(np.mean(prices[-200:])) if n >= 200 else None

    # SMA trend: price vs SMA200
    sma_trend = None
    if sma_200:
        sma_trend = "above" if latest_price > sma_200 else "below"

    # RSI (14-day)
    rsi_14 = _compute_rsi(prices, 14) if n >= 15 else None

    # Momentum (10-day, 20-day, 60-day)
    mom_10 = float((prices[-1] / prices[-11] - 1) * 100) if n >= 11 else None
    mom_20 = float((prices[-1] / prices[-21] - 1) * 100) if n >= 21 else None
    mom_60 = float((prices[-1] / prices[-61] - 1) * 100) if n >= 61 else None

    # Bollinger Band position (20-day, 2σ)
    bb_pos = None
    if n >= 20:
        bb_mean = np.mean(prices[-20:])
        bb_std = np.std(prices[-20:])
        if bb_std > 0:
            bb_pos = round(float((latest_price - bb_mean) / (2 * bb_std)), 3)

    # Mean reversion Z-score (price vs 60-day mean / std)
    mr_zscore = None
    if n >= 60:
        mr_mean = np.mean(prices[-60:])
        mr_std = np.std(prices[-60:])
        if mr_std > 0:
            mr_zscore = round(float((latest_price - mr_mean) / mr_std), 3)

    # Volume trend (20-day avg vs 60-day avg)
    volumes = market["volume"].to_numpy()
    vol_ratio = None
    if n >= 60:
        avg_20 = float(np.mean(volumes[-20:]))
        avg_60 = float(np.mean(volumes[-60:]))
        if avg_60 > 0:
            vol_ratio = round(avg_20 / avg_60, 3)

    return {
        "latest_price": round(latest_price, 2),
        "sma_20": round(sma_20, 2) if sma_20 else None,
        "sma_50": round(sma_50, 2) if sma_50 else None,
        "sma_200": round(sma_200, 2) if sma_200 else None,
        "sma_trend": sma_trend,
        "rsi_14": round(rsi_14, 1) if rsi_14 else None,
        "momentum_10d": round(mom_10, 2) if mom_10 is not None else None,
        "momentum_20d": round(mom_20, 2) if mom_20 is not None else None,
        "momentum_60d": round(mom_60, 2) if mom_60 is not None else None,
        "bollinger_position": bb_pos,
        "mean_reversion_zscore": mr_zscore,
        "volume_ratio_20_60": vol_ratio,
    }


def _compute_rsi(prices: np.ndarray, period: int = 14) -> float | None:
    """Compute RSI using exponential moving average of gains/losses."""
    deltas = np.diff(prices)
    if len(deltas) < period:
        return None

    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    # EMA-based RSI
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


# ═══════════════════════════════════════════════════════════════
# Fundamental Indicators
# ═══════════════════════════════════════════════════════════════

def _compute_fundamental(
    market: pl.DataFrame,
    fund: pl.DataFrame,
    feat: pl.DataFrame,
    rf_value: float,
) -> dict | None:
    if market.is_empty():
        return None

    latest_price = float(market["adj_close"][-1])
    result: dict = {}
    shares = None

    if not fund.is_empty():
        latest = fund[-1]
        revenue = float(latest["revenue"][0]) if latest["revenue"][0] else None
        total_debt = float(latest["total_debt"][0]) if latest["total_debt"][0] else None
        cash = float(latest["cash"][0]) if latest["cash"][0] else None
        shares = float(latest["shares_out"][0]) if latest["shares_out"][0] else None

        result["filing_date"] = str(latest["filing_date"][0])

        if shares and shares > 0:
            market_cap = latest_price * shares
            result["market_cap"] = round(market_cap, 0)
            result["market_cap_label"] = _format_large_num(market_cap)

            if revenue and revenue > 0:
                # Price/Sales
                result["price_to_sales"] = round(market_cap / revenue, 2)

                # EV/Sales
                ev = market_cap + (total_debt or 0) - (cash or 0)
                result["ev_to_sales"] = round(ev / revenue, 2)

        if total_debt and cash:
            result["net_debt"] = round(total_debt - cash, 0)
            result["net_debt_label"] = _format_large_num(total_debt - cash)

        if revenue:
            result["revenue"] = round(revenue, 0)
            result["revenue_label"] = _format_large_num(revenue)

            if cash:
                result["cash_to_revenue"] = round(cash / revenue, 3)

    # Calculate dynamic DCF directly to support custom RF and monthly breakdown
    if not feat.is_empty():
        last_feat = feat[-1]
        
        # Keep EV/Sales Z-Score from DB
        val = last_feat["ev_sales_zscore"][0]
        if val is not None:
            result["ev_sales_zscore"] = round(float(val), 4)
            
        result["feature_date"] = str(last_feat["date"][0])
        
        # Calculate custom DCF
        beta_spy = last_feat["beta_spy"][0]
        beta_vix = last_feat["beta_vix"][0]
        
        if beta_spy is not None and beta_vix is not None and "revenue" in result and result.get("revenue") is not None and shares and shares > 0:
            erp = 0.055
            vix_rp = 0.002
            g = 0.03
            
            # r_i = R_f + (β_spy × ERP) + (β_vix × λ_VIX)
            dynamic_discount_rate = rf_value + (float(beta_spy) * erp) + (float(beta_vix) * vix_rp)
            dynamic_discount_rate = max(dynamic_discount_rate, g + 0.01)
            result["dynamic_discount_rate"] = round(dynamic_discount_rate, 4)
            
            # Terminal Value approach inside pipeline: V_0 = Rev_ann * (1+g) / (r_i - g)
            # Since quarterly revenue is scaled by 4 in pipeline to get annualized:
            rev_ann = result["revenue"] * 4
            intrinsic_value = rev_ann * (1 + g) / (dynamic_discount_rate - g)
            npv_gap = (intrinsic_value / shares - latest_price) / latest_price
            result["dcf_npv_gap"] = round(npv_gap, 4)
            
            # Generate 60-month DCF Breakdown (Explicit Forecast connecting to Terminal Value)
            # This demonstrates how the Gordon proxy would map to an explicit monthly forecast.
            breakdown = []
            monthly_discount = (1 + dynamic_discount_rate) ** (1/12) - 1
            monthly_growth = (1 + g) ** (1/12) - 1
            
            # Start CF at current annualized rev / 12
            current_cf = rev_ann / 12
            cumulative_pv = 0
            
            for m in range(1, 61):
                projected_cf = current_cf * ((1 + monthly_growth) ** m)
                discount_factor = (1 + monthly_discount) ** m
                pv = projected_cf / discount_factor
                cumulative_pv += pv
                
                # In month 60, add Terminal Value transition
                tv = 0
                if m == 60:
                    tv = (projected_cf * (1 + monthly_growth)) / (monthly_discount - monthly_growth)
                    cumulative_pv += tv / discount_factor

                breakdown.append({
                    "month": m,
                    "cash_flow": float(projected_cf),
                    "discount_factor": float(discount_factor),
                    "present_value": float(pv),
                    "terminal_value": float(tv) if m == 60 else 0,
                    "cumulative_npv": float(cumulative_pv)
                })
                
            result["dcf_breakdown"] = breakdown
        else:
            # Fallback to feature values
            for col in ["dcf_npv_gap", "dynamic_discount_rate"]:
                val = last_feat[col][0]
                if val is not None:
                    result[col] = round(float(val), 4)

    return result if result else None


def _format_large_num(n: float) -> str:
    """Format large numbers for display (e.g. 1.5T, 234B, 56M)."""
    abs_n = abs(n)
    sign = "-" if n < 0 else ""
    if abs_n >= 1e12:
        return f"{sign}${abs_n/1e12:.1f}T"
    elif abs_n >= 1e9:
        return f"{sign}${abs_n/1e9:.1f}B"
    elif abs_n >= 1e6:
        return f"{sign}${abs_n/1e6:.0f}M"
    else:
        return f"{sign}${abs_n:,.0f}"


# ═══════════════════════════════════════════════════════════════
# Statistical Indicators
# ═══════════════════════════════════════════════════════════════

def _compute_statistical(
    market: pl.DataFrame,
    feat: pl.DataFrame,
    macro: pl.DataFrame,
    rf_value: float,
) -> dict | None:
    if market.is_empty() or len(market) < 30:
        return None

    returns = market["daily_return"].drop_nulls().to_numpy()
    n = len(returns)
    result: dict = {}

    result["risk_free_rate"] = round(rf_value * 100, 2)

    # Annualized volatility
    vol_30 = float(np.std(returns[-30:]) * np.sqrt(252)) if n >= 30 else None
    vol_90 = float(np.std(returns[-90:]) * np.sqrt(252)) if n >= 90 else None
    vol_252 = float(np.std(returns[-252:]) * np.sqrt(252)) if n >= 252 else None

    result["volatility_30d"] = round(vol_30 * 100, 1) if vol_30 else None
    result["volatility_90d"] = round(vol_90 * 100, 1) if vol_90 else None
    result["volatility_1y"] = round(vol_252 * 100, 1) if vol_252 else None

    # Annualized return
    if n >= 252:
        cum_return_1y = float(np.prod(1 + returns[-252:]) - 1)
        result["return_1y"] = round(cum_return_1y * 100, 1)
    if n >= 63:
        cum_return_3m = float(np.prod(1 + returns[-63:]) - 1)
        result["return_3m"] = round(cum_return_3m * 100, 1)

    # Sharpe ratio (annualized using dynamic rf_value)
    rf_daily = rf_value / 252
    if n >= 252 and vol_252 and vol_252 > 0:
        excess = returns[-252:] - rf_daily
        result["sharpe_1y"] = round(float(np.mean(excess) / np.std(excess) * np.sqrt(252)), 2)

    # Max drawdown (all time)
    prices = market["adj_close"].to_numpy()
    peak = np.maximum.accumulate(prices)
    dd = (prices - peak) / peak
    result["max_drawdown"] = round(float(np.min(dd)) * 100, 1)

    # Value at Risk (95% and 99%, 1-day)
    if n >= 252:
        sorted_returns = np.sort(returns[-252:])
        result["var_95"] = round(float(np.percentile(sorted_returns, 5)) * 100, 2)
        result["var_99"] = round(float(np.percentile(sorted_returns, 1)) * 100, 2)

    # Skewness and kurtosis
    if n >= 60:
        from scipy import stats as scipy_stats
        result["skewness"] = round(float(scipy_stats.skew(returns[-252:])), 3) if n >= 252 else None
        result["kurtosis"] = round(float(scipy_stats.kurtosis(returns[-252:])), 3) if n >= 252 else None

    # Factor betas from feature parquet
    if not feat.is_empty():
        last = feat[-1]
        for col in ["beta_spy", "beta_tnx", "beta_vix"]:
            val = last[col][0]
            if val is not None:
                result[col] = round(float(val), 4)

    # CAPM expected return (Rf + β * ERP)
    if "beta_spy" in result and result["beta_spy"] is not None:
        erp = 0.055
        result["capm_expected_return"] = round((rf_value + result["beta_spy"] * erp) * 100, 2)

    # Correlation with SPY
    if not macro.is_empty() and n >= 90:
        # Join on dates
        spy_joined = market.join(macro.select(["date", "spy"]), on="date", how="inner")
        if len(spy_joined) >= 90:
            spy_returns = spy_joined["spy"].pct_change().drop_nulls().to_numpy()[-90:]
            stock_returns = spy_joined["daily_return"].drop_nulls().to_numpy()[-90:]
            min_len = min(len(spy_returns), len(stock_returns))
            if min_len >= 30:
                corr = float(np.corrcoef(stock_returns[-min_len:], spy_returns[-min_len:])[0, 1])
                result["correlation_spy_90d"] = round(corr, 3)

    return result if result else None
