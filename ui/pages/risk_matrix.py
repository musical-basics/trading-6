"""
ui/pages/risk_matrix.py — Level 3 Risk APT Visualization

Displays the covariance matrix heatmap, raw vs adjusted weights comparison,
and portfolio-level risk metrics.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import sqlite3
from src.config import DB_PATH, MAX_MCR_THRESHOLD, MAX_PORTFOLIO_VOL


st.header("🛡️ Risk Matrix")
st.caption("Covariance analysis and MCR-adjusted portfolio weights")

try:
    conn = sqlite3.connect(DB_PATH)

    # Load target portfolio (risk-adjusted)
    target_df = pd.read_sql_query(
        "SELECT ticker, date, target_weight, mcr FROM target_portfolio ORDER BY date",
        conn,
        parse_dates=["date"],
    )

    # Load raw predictions for comparison
    raw_df = pd.read_sql_query(
        "SELECT ticker, date, raw_weight FROM ml_predictions ORDER BY date",
        conn,
        parse_dates=["date"],
    )

    # Load prices for covariance
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )
    conn.close()
except Exception:
    st.warning("No risk data available. Run the pipeline first.")
    st.stop()

if target_df.empty:
    st.info("No target portfolio data. Run Phase 3b (Risk APT) first.")
    st.stop()

# ── Summary metrics ──────────────────────────────────────————
latest_date = target_df["date"].max()
latest_target = target_df[target_df["date"] == latest_date]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Active Positions", len(latest_target[latest_target["target_weight"] > 0]))
col2.metric("Total Weight", f"{latest_target['target_weight'].sum():.1%}")
col3.metric("Max MCR", f"{latest_target['mcr'].abs().max():.4f}")
col4.metric("MCR Threshold", f"{MAX_MCR_THRESHOLD:.2%}")

# ── Raw vs Adjusted weights comparison ───────────────────————
st.subheader("Raw vs Risk-Adjusted Weights (Latest Date)")

latest_raw = raw_df[raw_df["date"] == latest_date]
comparison = latest_target.merge(latest_raw, on=["ticker", "date"], how="outer")
comparison = comparison.fillna(0)
comparison = comparison[
    (comparison["raw_weight"] > 0) | (comparison["target_weight"] > 0)
].sort_values("raw_weight", ascending=False)

if not comparison.empty:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=comparison["ticker"],
        y=comparison["raw_weight"],
        name="Raw (XGBoost)",
        marker_color="#8b5cf6",
        opacity=0.7,
    ))
    fig.add_trace(go.Bar(
        x=comparison["ticker"],
        y=comparison["target_weight"],
        name="Adjusted (Risk APT)",
        marker_color="#10b981",
        opacity=0.7,
    ))
    fig.update_layout(
        barmode="group",
        xaxis_title="Ticker",
        yaxis_title="Weight",
        height=400,
        margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

# ── MCR bar chart ────────────────────────────────────────————
st.subheader("Marginal Contribution to Risk (MCR)")

if not latest_target.empty:
    mcr_data = latest_target[latest_target["target_weight"] > 0].sort_values("mcr", ascending=False)

    fig2 = go.Figure()
    colors = ["#ef4444" if abs(m) > MAX_MCR_THRESHOLD else "#3b82f6"
              for m in mcr_data["mcr"]]
    fig2.add_trace(go.Bar(
        x=mcr_data["ticker"],
        y=mcr_data["mcr"],
        marker_color=colors,
        name="MCR",
    ))
    fig2.add_hline(y=MAX_MCR_THRESHOLD, line_dash="dash", line_color="red",
                   annotation_text=f"Max MCR ({MAX_MCR_THRESHOLD:.0%})")
    fig2.update_layout(
        xaxis_title="Ticker",
        yaxis_title="MCR",
        height=350,
        margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Covariance heatmap (latest portfolio) ────────────────————
st.subheader("Covariance Matrix (Latest Portfolio)")

portfolio_tickers = latest_target[latest_target["target_weight"] > 0]["ticker"].tolist()

if portfolio_tickers and not prices_df.empty:
    port_prices = prices_df[prices_df["ticker"].isin(portfolio_tickers)]
    pivot = port_prices.pivot(index="date", columns="ticker", values="adj_close")
    returns = np.log(pivot / pivot.shift(1)).dropna().tail(90)

    if not returns.empty and len(returns.columns) > 1:
        cov = returns.cov() * 252  # Annualized
        fig3 = go.Figure(data=go.Heatmap(
            z=cov.values,
            x=cov.columns.tolist(),
            y=cov.index.tolist() if isinstance(cov.index, pd.Index) else cov.columns.tolist(),
            colorscale="RdBu_r",
            zmid=0,
            text=np.round(cov.values, 4),
            texttemplate="%{text}",
            textfont={"size": 10},
        ))
        # Fix: use column names for both axes
        fig3.update_layout(
            xaxis=dict(tickvals=list(range(len(cov.columns))), ticktext=cov.columns.tolist()),
            yaxis=dict(tickvals=list(range(len(cov.columns))), ticktext=cov.columns.tolist()),
            height=400,
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Not enough return data for covariance matrix.")

# ── Data table ───────────────────────────────────────────————
with st.expander("📋 Full Target Portfolio (latest date)"):
    st.dataframe(
        latest_target.sort_values("target_weight", ascending=False),
        use_container_width=True,
    )
