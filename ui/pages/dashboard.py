"""Dashboard — Master overview page."""

import streamlit as st
import pandas as pd
import numpy as np
import os
from ui.shared import get_db_connection, table_exists, get_table_count, render_sidebar, DB_PATH

cfg = render_sidebar()

st.markdown("# 🧪 Level 2: Quant Sandbox")
st.markdown("*Cross-Sectional Fundamental Ranking — Mission Control*")
st.divider()

# ── Pipeline health row ──────────────────────────────────────
st.markdown("### ⚡ Pipeline Status")
if os.path.exists(DB_PATH):
    s1, s2, s3, s4, s5, s6 = st.columns(6)
    s1.metric("Daily Bars", f"{get_table_count('daily_bars'):,}")
    s2.metric("Fundamentals", f"{get_table_count('quarterly_fundamentals'):,}")
    s3.metric("XS Scores", f"{get_table_count('cross_sectional_scores'):,}")
    s4.metric("SMA Signals", f"{get_table_count('strategy_signals'):,}")
    s5.metric("WFO Results", f"{get_table_count('wfo_results'):,}")
    s6.metric("Executions", f"{get_table_count('paper_executions'):,}")
else:
    st.info("ℹ️ Database not initialized. Go to **Data Pipeline** to run the pipeline.")

# ── Latest Z-score rankings ──────────────────────────────────
if table_exists("cross_sectional_scores"):
    st.divider()
    conn = get_db_connection()

    latest_date = pd.read_sql_query(
        "SELECT MAX(date) as d FROM cross_sectional_scores", conn
    )["d"].iloc[0]

    day_scores = pd.read_sql_query("""
        SELECT ticker, ev_sales_zscore, target_weight
        FROM cross_sectional_scores WHERE date = ?
        ORDER BY ev_sales_zscore ASC
    """, conn, params=(latest_date,))

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(f"### 📗 Cheapest (Long Candidates) — {latest_date}")
        top = day_scores.head(3).copy()
        top["ev_sales_zscore"] = top["ev_sales_zscore"].apply(lambda x: f"{x:.3f}")
        top["target_weight"] = top["target_weight"].apply(lambda x: f"{x:.1%}")
        st.dataframe(top, use_container_width=True, hide_index=True)

    with col_r:
        st.markdown(f"### 📕 Most Expensive (Short Candidates)")
        bottom = day_scores.tail(3).iloc[::-1].copy()
        bottom["ev_sales_zscore"] = bottom["ev_sales_zscore"].apply(lambda x: f"{x:.3f}")
        bottom["target_weight"] = bottom["target_weight"].apply(lambda x: f"{x:.1%}")
        st.dataframe(bottom, use_container_width=True, hide_index=True)

    conn.close()

# ── L/S strategy sparkline ───────────────────────────────────
if table_exists("cross_sectional_scores"):
    st.divider()
    st.markdown("### 📈 L/S Z-Score Strategy (Quick View)")
    try:
        from src.strategies.ls_zscore_strategy import simulate_ls_zscore
        ls_eq, _ = simulate_ls_zscore(n_long=2, n_short=2)
        if not ls_eq.empty:
            import plotly.graph_objects as go
            ls_eq["date"] = pd.to_datetime(ls_eq["date"])
            total_ret = ls_eq["equity"].iloc[-1] / 10000 - 1
            ls_returns = ls_eq["daily_return"]
            sharpe = ls_returns.mean() / ls_returns.std() * np.sqrt(252) if ls_returns.std() > 0 else 0

            m1, m2, m3 = st.columns(3)
            m1.metric("Total Return", f"{total_ret:+.1%}")
            m2.metric("Sharpe", f"{sharpe:.2f}")
            m3.metric("Trading Days", len(ls_eq))

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=ls_eq["date"], y=ls_eq["equity"],
                mode="lines", line=dict(color="#E040FB", width=2),
                fill="tozeroy", fillcolor="rgba(224,64,251,0.1)",
            ))
            fig.update_layout(template="plotly_dark", height=250,
                yaxis_title="Equity ($)", margin=dict(t=10, b=30))
            st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.caption("Run Phase 2 to see strategy results.")


