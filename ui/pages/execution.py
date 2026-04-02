"""Execution Desk — Route paper trades, view execution ledger."""

import streamlit as st
import pandas as pd
from datetime import datetime
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.pipeline import (
    db_init, data_ingestion, fundamental_ingestion,
    cross_sectional_scoring, wfo_backtester,
    portfolio_rebalancer, simulation, execution,
)
from src.pipeline.data_sources.edgar import fundamentals as edgar_fundamentals

cfg = render_sidebar()

st.markdown("# 🚀 Execution Desk")
st.divider()

today_str = datetime.now().strftime("%Y-%m-%d")

# ── Today's Signals ──────────────────────────────────────────
st.markdown("### 📋 Today's Signals")
if table_exists("strategy_signals"):
    conn = get_db_connection()
    today_signals = pd.read_sql_query("""
        SELECT s.ticker, 'SMA' as strategy, b.adj_close as price,
               CASE WHEN s.signal=1 THEN 'BUY' WHEN s.signal=-1 THEN 'SELL' END as action
        FROM strategy_signals s JOIN daily_bars b ON s.ticker=b.ticker AND s.date=b.date
        WHERE s.signal != 0 AND s.date = ?
    """, conn, params=(today_str,))

    if table_exists("pullback_signals"):
        pb_today = pd.read_sql_query("""
            SELECT p.ticker, 'Pullback' as strategy, p.close as price,
                   CASE WHEN p.signal=1.0 THEN 'BUY' WHEN p.exit_signal IS NOT NULL THEN 'SELL' END as action
            FROM pullback_signals p
            WHERE (p.signal=1.0 OR p.exit_signal IS NOT NULL) AND p.date=?
        """, conn, params=(today_str,))
        today_signals = pd.concat([today_signals, pb_today], ignore_index=True)

    if today_signals.empty:
        st.info(f"No signals for today ({today_str}).")
    else:
        st.dataframe(today_signals, use_container_width=True)
    conn.close()
else:
    st.info("ℹ️ No signals computed yet.")

# ── Risk Check ───────────────────────────────────────────────
st.divider()
st.markdown("### 🛡️ Risk Check")

conn = get_db_connection()
try:
    positions_df = pd.read_sql_query("""
        SELECT ticker,
               SUM(CASE WHEN action='BUY' THEN quantity ELSE -quantity END) as net_shares
        FROM paper_executions GROUP BY ticker HAVING net_shares > 0
    """, conn)
except Exception:
    positions_df = pd.DataFrame()

current_open = len(positions_df) if not positions_df.empty else 0
c1, c2, c3 = st.columns(3)
c1.metric("Open Positions", f"{current_open} / {cfg['max_positions']}")
c2.metric("Capital Per Trade", f"${int(cfg['capital_per_trade']):,}")
slots = cfg["max_positions"] - current_open
c3.metric("Available Slots", slots if slots > 0 else "0 ⛔")
if not positions_df.empty:
    with st.expander("📊 Current Positions", expanded=True):
        st.dataframe(positions_df, use_container_width=True)
conn.close()

# ── Route Trades ─────────────────────────────────────────────
st.divider()
st.markdown("### 🚀 Route Paper Trades")

col_e1, col_e2 = st.columns([2, 1])
with col_e1:
    if st.button("🚀 ROUTE PAPER TRADES", type="primary", use_container_width=True):
        with st.spinner("Running simulation filter and routing..."):
            simulation.MAX_OPEN_POSITIONS = cfg["max_positions"]
            simulation.CAPITAL_PER_TRADE = cfg["capital_per_trade"]
            approved = simulation.simulate_and_filter()
            if approved:
                execution.route_orders(approved)
                st.success(f"✅ {len(approved)} orders routed!")
                st.rerun()
            else:
                st.info("ℹ️ No orders to route.")

with col_e2:
    if st.button("🔄 Run Full L2 Pipeline", use_container_width=True):
        with st.spinner("Running complete Level 2 pipeline..."):
            db_init.init_db()
            data_ingestion.UNIVERSE = cfg["universe"]
            data_ingestion.ingest()
            edgar_fundamentals.ingest_fundamentals_edgar(tickers=cfg["universe"])
            cross_sectional_scoring.compute_cross_sectional_scores()
            wfo_backtester.run_wfo_tournament()
            orders = portfolio_rebalancer.rebalance_portfolio()
            execution.route_orders(orders)
        st.success("✅ Full Level 2 pipeline executed!")
        st.rerun()

# ── Execution Ledger ─────────────────────────────────────────
st.divider()
st.markdown("### 📒 Execution Ledger")
if table_exists("paper_executions"):
    conn = get_db_connection()
    df_exec = pd.read_sql_query("SELECT * FROM paper_executions ORDER BY timestamp DESC", conn)
    conn.close()
    if not df_exec.empty:
        st.dataframe(df_exec, use_container_width=True, height=400)
    else:
        st.info("ℹ️ No trades executed yet.")
else:
    st.info("ℹ️ Initialize the database first.")
