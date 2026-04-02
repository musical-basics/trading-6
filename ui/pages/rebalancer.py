"""Portfolio Rebalancer — Target weights vs current positions."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.config import MAX_SINGLE_WEIGHT
from src.pipeline.execution import portfolio_rebalancer
from src.pipeline.execution import order_router as execution
from src.pipeline.execution.portfolio_state import get_portfolio_state

cfg = render_sidebar()

st.markdown("# ⚖️ Portfolio Rebalancer")
st.divider()

if not table_exists("cross_sectional_scores"):
    st.info("ℹ️ No cross-sectional scores yet. Run the pipeline first.")
else:
    conn = get_db_connection()

    targets = pd.read_sql_query("""
        SELECT ticker, target_weight, ev_sales_zscore
        FROM cross_sectional_scores
        WHERE date = (SELECT MAX(date) FROM cross_sectional_scores)
        ORDER BY target_weight DESC, ev_sales_zscore ASC
    """, conn)

    latest_date = pd.read_sql_query(
        "SELECT MAX(date) as d FROM cross_sectional_scores", conn
    )["d"].iloc[0]

    total_equity, holdings = get_portfolio_state()

    st.markdown(f"### Portfolio State (as of {latest_date})")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Equity", f"${total_equity:,.0f}")
    c2.metric("Positions Held", len(holdings))
    active_targets = targets[targets["target_weight"] > 0]
    c3.metric("Target Positions", len(active_targets))
    c4.metric("Target Total Weight", f"{active_targets['target_weight'].sum():.1%}")

    # Target weights chart
    if not active_targets.empty:
        st.markdown("### 🎯 Target Weights")
        fig_tw = go.Figure()
        fig_tw.add_trace(go.Bar(
            x=active_targets["ticker"],
            y=active_targets["target_weight"],
            marker_color="#2196F3",
            text=[f"{w:.1%}" for w in active_targets["target_weight"]],
            textposition="outside",
        ))
        fig_tw.add_hline(y=MAX_SINGLE_WEIGHT, line_dash="dash",
                         line_color="#FF9800", annotation_text=f"Max Weight ({MAX_SINGLE_WEIGHT:.0%})")
        fig_tw.update_layout(template="plotly_dark", height=400,
            yaxis_title="Target Weight", yaxis_tickformat=".0%")
        st.plotly_chart(fig_tw, use_container_width=True)

    # Rebalance button
    st.divider()
    st.markdown("### 🚀 Execute Rebalance")

    if st.button("⚖️ CALCULATE & EXECUTE REBALANCE", type="primary", use_container_width=True):
        with st.spinner("Calculating rebalance orders..."):
            orders = portfolio_rebalancer.rebalance_portfolio()
            if orders:
                st.markdown("#### Order Preview")
                orders_df = pd.DataFrame(orders)
                st.dataframe(orders_df, use_container_width=True)

                if st.button("✅ Confirm & Route Orders"):
                    execution.route_orders(orders)
                    st.success(f"✅ {len(orders)} rebalance orders routed!")
                    st.rerun()
            else:
                st.success("✅ Portfolio already at target. No rebalance needed.")

    conn.close()
