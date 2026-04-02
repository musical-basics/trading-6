"""Cross-Sectional Scores — EV/Sales Z-Score rankings."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.config import ZSCORE_BUY_THRESHOLD

cfg = render_sidebar()

st.markdown("# 🧬 Cross-Sectional EV/Sales Z-Score Rankings")
st.divider()

if not table_exists("cross_sectional_scores"):
    st.info("ℹ️ No cross-sectional scores yet. Run the pipeline first.")
else:
    conn = get_db_connection()

    dates = pd.read_sql_query(
        "SELECT DISTINCT date FROM cross_sectional_scores ORDER BY date DESC", conn
    )["date"].tolist()

    selected_date = st.selectbox("Select Date", dates, index=0, key="xs_date")

    if selected_date:
        day_scores = pd.read_sql_query("""
            SELECT ticker, market_value, enterprise_value, ev_to_sales, ev_sales_zscore, target_weight
            FROM cross_sectional_scores WHERE date = ?
            ORDER BY ev_sales_zscore ASC
        """, conn, params=(selected_date,))

        # Z-Score Bar Chart
        st.markdown(f"### Z-Score Rankings — {selected_date}")
        fig_zs = go.Figure()

        colors = ["#00E676" if z < ZSCORE_BUY_THRESHOLD else
                  "#FF9800" if z < 0 else
                  "#FF1744" for z in day_scores["ev_sales_zscore"]]

        fig_zs.add_trace(go.Bar(
            x=day_scores["ticker"],
            y=day_scores["ev_sales_zscore"],
            marker_color=colors,
            text=[f"{z:.2f}" for z in day_scores["ev_sales_zscore"]],
            textposition="outside",
        ))
        fig_zs.add_hline(y=ZSCORE_BUY_THRESHOLD, line_dash="dash",
                         line_color="#00E676", annotation_text=f"BUY Threshold ({ZSCORE_BUY_THRESHOLD})")
        fig_zs.update_layout(template="plotly_dark", height=500,
            yaxis_title="EV/Sales Z-Score", xaxis_title="Ticker")
        st.plotly_chart(fig_zs, use_container_width=True)

        # Summary metrics
        buy_count = (day_scores["ev_sales_zscore"] < ZSCORE_BUY_THRESHOLD).sum()
        total_weight = day_scores["target_weight"].sum()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tickers Scored", len(day_scores))
        c2.metric("BUY Candidates", buy_count)
        c3.metric("Total Weight", f"{total_weight:.1%}")
        c4.metric("Mean Z-Score", f"{day_scores['ev_sales_zscore'].mean():.3f}")

        # Data table
        st.markdown("### 📋 Full Score Table")
        display_df = day_scores.copy()
        display_df["market_value"] = display_df["market_value"].apply(lambda x: f"${x/1e9:.1f}B" if pd.notna(x) else "—")
        display_df["enterprise_value"] = display_df["enterprise_value"].apply(lambda x: f"${x/1e9:.1f}B")
        display_df["ev_to_sales"] = display_df["ev_to_sales"].apply(lambda x: f"{x:.2f}")
        display_df["ev_sales_zscore"] = display_df["ev_sales_zscore"].apply(lambda x: f"{x:.3f}")
        display_df["target_weight"] = display_df["target_weight"].apply(lambda x: f"{x:.1%}")
        st.dataframe(display_df, use_container_width=True, height=400)

        # Time-series Z-score
        st.divider()
        st.markdown("### 📈 Z-Score Over Time")
        ticker_for_ts = st.selectbox("Select Ticker", day_scores["ticker"].tolist(), key="zs_ts_ticker")
        if ticker_for_ts:
            ts_data = pd.read_sql_query("""
                SELECT date, ev_sales_zscore FROM cross_sectional_scores
                WHERE ticker = ? ORDER BY date
            """, conn, params=(ticker_for_ts,))
            ts_data["date"] = pd.to_datetime(ts_data["date"])

            fig_ts = go.Figure()
            fig_ts.add_trace(go.Scatter(
                x=ts_data["date"], y=ts_data["ev_sales_zscore"],
                mode="lines", name=ticker_for_ts,
                line=dict(color="#2196F3", width=2),
            ))
            fig_ts.add_hline(y=ZSCORE_BUY_THRESHOLD, line_dash="dash",
                             line_color="#00E676", annotation_text="BUY Threshold")
            fig_ts.add_hline(y=0, line_dash="dot", line_color="gray")
            fig_ts.update_layout(template="plotly_dark", height=400,
                yaxis_title="EV/Sales Z-Score")
            st.plotly_chart(fig_ts, use_container_width=True)

    conn.close()
