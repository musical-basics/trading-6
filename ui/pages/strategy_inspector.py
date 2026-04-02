"""
strategy_inspector.py — Inspect daily trades, holdings, and returns per strategy.

Lets you pick a strategy, see the exact daily positions, weights, and
return contributions for any date range. Useful for auditing that:
  1) Returns are calculated correctly
  2) Holdings follow the strategy rules
"""

import streamlit as st
import pandas as pd
import sqlite3
from ui.shared import get_db_connection, render_sidebar
from src.config import DB_PATH, MAX_SINGLE_WEIGHT

cfg = render_sidebar()

st.markdown("# 🔍 Trade Inspector")
st.caption("Audit daily positions, weights, and return calculations per strategy.")

conn = get_db_connection()

# ── Strategy Picker ──────────────────────────────────────────
strategy = st.selectbox("**Select Strategy**", [
    "XGBoost AI",
    "Momentum 6M",
    "Fortress B/S",
    "Low-Beta",
    "DCF Deep Value",
    "Buy & Hold (EW)",
])

# ── Date Range ───────────────────────────────────────────────
c1, c2 = st.columns(2)
all_dates = pd.read_sql_query("SELECT MIN(date) as mn, MAX(date) as mx FROM daily_bars", conn)
min_dt = pd.to_datetime(all_dates["mn"].iloc[0]).date()
max_dt = pd.to_datetime(all_dates["mx"].iloc[0]).date()

with c1:
    start_date = st.date_input("From", value=pd.to_datetime("2025-01-01").date(), min_value=min_dt, max_value=max_dt)
with c2:
    end_date = st.date_input("To", value=max_dt, min_value=min_dt, max_value=max_dt)


def inspect_xgboost(conn, start, end):
    """Show XGBoost daily holdings with weight, return, contribution."""
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date
    """, conn, parse_dates=["date"])
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()

    weights = pd.read_sql_query("""
        SELECT ticker, date, target_weight FROM target_portfolio
    """, conn, parse_dates=["date"])

    if weights.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.merge(prices, weights, on=["ticker", "date"], how="left")
    df["target_weight"] = df["target_weight"].fillna(0)

    df["capped_weight"] = df["target_weight"].clip(upper=MAX_SINGLE_WEIGHT)
    cap_sum = df.groupby("date")["capped_weight"].transform("sum")
    df["norm_weight"] = df["capped_weight"] / cap_sum.replace(0, 1)

    df = df.sort_values(["ticker", "date"])
    df["actual_weight"] = df.groupby("ticker")["norm_weight"].shift(1).fillna(0)
    df["contribution"] = df["actual_weight"] * df["daily_return"].fillna(0)

    mask = (df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))
    filtered = df[mask & (df["actual_weight"] > 0)].copy()

    # Holdings table
    holdings = filtered[["date", "ticker", "adj_close", "daily_return",
                         "target_weight", "norm_weight", "actual_weight", "contribution"]].copy()
    holdings = holdings.sort_values(["date", "ticker"])
    holdings.columns = ["Date", "Ticker", "Price", "Daily Ret", "Raw Weight",
                        "Norm Weight", "Actual Weight (lagged)", "Return Contribution"]

    # Portfolio summary per day
    daily = filtered.groupby("date").agg(
        n_holdings=("ticker", "count"),
        total_weight=("actual_weight", "sum"),
        portfolio_return=("contribution", "sum"),
        tickers=("ticker", lambda x: ", ".join(sorted(x))),
    ).reset_index()
    daily.columns = ["Date", "# Holdings", "Total Weight", "Portfolio Return", "Tickers"]
    daily = daily.sort_values("Date")

    return holdings, daily


def inspect_monthly_rank(conn, start, end, table, col, top=True, pct=0.20, exclude_fin=False):
    """Generic inspector for monthly-rebalance rank strategies."""
    FINANCIALS = {'GS', 'MS', 'BAC', 'JPM', 'WFC', 'C', 'BLK', 'SCHW'}

    if table == "daily_bars":
        raw = pd.read_sql_query(f"""
            SELECT ticker, date, adj_close FROM daily_bars
            WHERE ticker != 'SPY' ORDER BY ticker, date
        """, conn, parse_dates=["date"])
        raw = raw.sort_values(["ticker", "date"])
        if col == "ret_126":
            raw["ret_126"] = raw.groupby("ticker")["adj_close"].pct_change(periods=126)
        rank_col = col
    elif table == "factor_betas":
        raw = pd.read_sql_query(f"""
            SELECT ticker, date, {col} FROM factor_betas ORDER BY date, ticker
        """, conn, parse_dates=["date"])
        rank_col = col
    elif table == "cross_sectional_scores":
        # Fortress: needs merge with fundamentals
        scores = pd.read_sql_query("""
            SELECT ticker, date, market_value FROM cross_sectional_scores
            WHERE market_value > 0 ORDER BY ticker, date
        """, conn, parse_dates=["date"])
        if exclude_fin:
            scores = scores[~scores["ticker"].isin(FINANCIALS)]
        fundies = pd.read_sql_query("""
            SELECT ticker, filing_date, cash_and_equivalents, total_debt
            FROM quarterly_fundamentals ORDER BY ticker, filing_date
        """, conn, parse_dates=["filing_date"])

        parts = []
        for t in scores["ticker"].unique():
            s = scores[scores["ticker"] == t].sort_values("date").reset_index(drop=True)
            f = fundies[fundies["ticker"] == t].sort_values("filing_date").reset_index(drop=True)
            if f.empty:
                continue
            m = pd.merge_asof(s, f.drop(columns=["ticker"]),
                              left_on="date", right_on="filing_date", direction="backward")
            parts.append(m)
        if not parts:
            return pd.DataFrame(), pd.DataFrame()
        raw = pd.concat(parts, ignore_index=True)
        raw["net_cash_yield"] = (raw["cash_and_equivalents"].fillna(0) - raw["total_debt"].fillna(0)) / raw["market_value"]
        rank_col = "net_cash_yield"
    elif table == "ml_features":
        raw = pd.read_sql_query("""
            SELECT ticker, date, dcf_npv_gap FROM ml_features ORDER BY date, ticker
        """, conn, parse_dates=["date"])
        rank_col = col
    else:
        return pd.DataFrame(), pd.DataFrame()

    raw["month"] = raw["date"].dt.to_period("M")
    month_ends = raw.groupby(["ticker", "month"]).tail(1).copy()
    month_ends = month_ends.dropna(subset=[rank_col])
    month_ends["rank_pct"] = month_ends.groupby("month")[rank_col].rank(pct=True)

    if top:
        month_ends["in_portfolio"] = (month_ends["rank_pct"] >= (1 - pct)).astype(int)
    else:
        month_ends["in_portfolio"] = (month_ends["rank_pct"] <= pct).astype(int)

    portfolio_map = {}
    for _, row in month_ends[month_ends["in_portfolio"] == 1].iterrows():
        m = row["month"]
        if m not in portfolio_map:
            portfolio_map[m] = []
        portfolio_map[m].append(row["ticker"])

    # Get prices for return calc
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE ticker != 'SPY' ORDER BY ticker, date
    """, conn, parse_dates=["date"])
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()
    prices["month"] = prices["date"].dt.to_period("M")

    mask = (prices["date"] >= pd.Timestamp(start)) & (prices["date"] <= pd.Timestamp(end))
    prices = prices[mask].copy()

    rows = []
    for _, row in prices.iterrows():
        prev_month = row["month"] - 1
        tickers_in = portfolio_map.get(prev_month, [])
        if row["ticker"] in tickers_in:
            n = len(tickers_in)
            rows.append({
                "Date": row["date"],
                "Ticker": row["ticker"],
                "Price": row["adj_close"],
                "Daily Ret": row["daily_return"],
                "Weight": 1.0 / n,
                "Return Contribution": row["daily_return"] / n if pd.notna(row["daily_return"]) else 0,
                "Rebal Month": str(prev_month),
            })

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    holdings = pd.DataFrame(rows).sort_values(["Date", "Ticker"])

    daily = holdings.groupby("Date").agg(
        n_holdings=("Ticker", "count"),
        portfolio_return=("Return Contribution", "sum"),
        tickers=("Ticker", lambda x: ", ".join(sorted(x))),
    ).reset_index()
    daily.columns = ["Date", "# Holdings", "Portfolio Return", "Tickers"]

    return holdings, daily


def inspect_dcf(conn, start, end):
    """DCF Deep Value: signal-based daily."""
    import numpy as np
    features = pd.read_sql_query("""
        SELECT ticker, date, dcf_npv_gap FROM ml_features ORDER BY date, ticker
    """, conn, parse_dates=["date"])
    features["signal"] = (features["dcf_npv_gap"] > 0.15).astype(int)

    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE ticker != 'SPY' ORDER BY ticker, date
    """, conn, parse_dates=["date"])
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()

    merged = prices.merge(features[["ticker", "date", "signal", "dcf_npv_gap"]],
                          on=["ticker", "date"], how="left")
    merged["signal"] = merged["signal"].fillna(0)
    merged = merged.sort_values(["ticker", "date"])
    merged["position"] = merged.groupby("ticker")["signal"].shift(1).fillna(0)

    active_per_day = merged[merged["position"] == 1].groupby("date")["ticker"].count()
    merged["n_active"] = merged["date"].map(active_per_day).fillna(0)

    merged["contribution"] = np.where(
        (merged["position"] == 1) & (merged["n_active"] > 0),
        merged["daily_return"].fillna(0) / merged["n_active"],
        0.0
    )

    mask = (merged["date"] >= pd.Timestamp(start)) & (merged["date"] <= pd.Timestamp(end))
    filtered = merged[mask & (merged["position"] == 1)].copy()

    holdings = filtered[["date", "ticker", "adj_close", "daily_return",
                         "dcf_npv_gap", "n_active", "contribution"]].copy()
    holdings.columns = ["Date", "Ticker", "Price", "Daily Ret",
                        "DCF NPV Gap", "# Active", "Return Contribution"]
    holdings = holdings.sort_values(["Date", "Ticker"])

    daily = filtered.groupby("date").agg(
        n_holdings=("ticker", "count"),
        portfolio_return=("contribution", "sum"),
        tickers=("ticker", lambda x: ", ".join(sorted(x))),
    ).reset_index()
    daily.columns = ["Date", "# Holdings", "Portfolio Return", "Tickers"]

    return holdings, daily


def inspect_buyhold(conn, start, end):
    """Buy & Hold EW: equal weight all tickers."""
    prices = pd.read_sql_query("""
        SELECT ticker, date, adj_close FROM daily_bars
        WHERE ticker != 'SPY' ORDER BY ticker, date
    """, conn, parse_dates=["date"])
    prices = prices.sort_values(["ticker", "date"])
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()

    n_tickers = prices["ticker"].nunique()
    prices["weight"] = 1.0 / n_tickers
    prices["contribution"] = prices["daily_return"].fillna(0) / n_tickers

    mask = (prices["date"] >= pd.Timestamp(start)) & (prices["date"] <= pd.Timestamp(end))
    filtered = prices[mask].copy()

    holdings = filtered[["date", "ticker", "adj_close", "daily_return", "weight", "contribution"]].copy()
    holdings.columns = ["Date", "Ticker", "Price", "Daily Ret", "Weight", "Return Contribution"]

    daily = filtered.groupby("date").agg(
        n_holdings=("ticker", "count"),
        portfolio_return=("contribution", "sum"),
    ).reset_index()
    daily.columns = ["Date", "# Holdings", "Portfolio Return"]

    return holdings, daily


# ── Run Inspector ────────────────────────────────────────────
if st.button("🔍 Inspect Trades", type="primary", use_container_width=True):
    with st.spinner(f"Loading {strategy} trade log..."):
        if strategy == "XGBoost AI":
            holdings, daily = inspect_xgboost(conn, start_date, end_date)
        elif strategy == "Momentum 6M":
            holdings, daily = inspect_monthly_rank(conn, start_date, end_date,
                                                   "daily_bars", "ret_126", top=True, pct=0.20)
        elif strategy == "Fortress B/S":
            holdings, daily = inspect_monthly_rank(conn, start_date, end_date,
                                                   "cross_sectional_scores", "net_cash_yield",
                                                   top=True, pct=0.10, exclude_fin=True)
        elif strategy == "Low-Beta":
            holdings, daily = inspect_monthly_rank(conn, start_date, end_date,
                                                   "factor_betas", "beta_spy", top=False, pct=0.20)
        elif strategy == "DCF Deep Value":
            holdings, daily = inspect_dcf(conn, start_date, end_date)
        elif strategy == "Buy & Hold (EW)":
            holdings, daily = inspect_buyhold(conn, start_date, end_date)
        else:
            holdings, daily = pd.DataFrame(), pd.DataFrame()

    st.session_state["inspector_holdings"] = holdings
    st.session_state["inspector_daily"] = daily
    st.session_state["inspector_strategy"] = strategy

# ── Display Results ──────────────────────────────────────────
holdings = st.session_state.get("inspector_holdings")
daily = st.session_state.get("inspector_daily")
insp_strat = st.session_state.get("inspector_strategy", "")

if holdings is not None and not holdings.empty:
    st.markdown(f"### 📋 {insp_strat} — Daily Portfolio Summary")

    # Key metrics
    if "Portfolio Return" in daily.columns:
        total_ret = (1 + daily["Portfolio Return"]).prod() - 1
        avg_daily = daily["Portfolio Return"].mean()
        avg_hold = daily["# Holdings"].mean() if "# Holdings" in daily.columns else 0
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Return", f"{total_ret:+.2%}")
        m2.metric("Avg Daily Return", f"{avg_daily:+.4%}")
        m3.metric("Avg Holdings/Day", f"{avg_hold:.1f}")

    st.dataframe(daily, use_container_width=True, height=300)

    st.markdown(f"### 📊 {insp_strat} — Position-Level Detail")
    st.caption(f"{len(holdings):,} position-day records")
    st.dataframe(holdings, use_container_width=True, height=400)

    # Quick return verification
    st.markdown("### ✅ Return Verification")
    if "Return Contribution" in holdings.columns:
        daily_rets = holdings.groupby("Date")["Return Contribution"].sum()
        equity = 10000 * (1 + daily_rets).cumprod()
        st.line_chart(equity, use_container_width=True, height=250)
        st.caption(f"Equity: $10,000 → ${equity.iloc[-1]:,.0f} ({(equity.iloc[-1]/10000 - 1)*100:+.1f}%)")

elif holdings is not None:
    st.warning("No trades found for the selected date range.")

conn.close()
