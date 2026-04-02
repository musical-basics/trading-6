"""
ui/pages/macro_regimes.py — Level 3 Macro Factor Visualization

Displays macro factor time-series (VIX, TNX, SPY) with regime bands.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import sqlite3
from src.config import DB_PATH


st.header("🌍 Macro Regimes")
st.caption("Systemic factor data powering the APT model")

try:
    conn = sqlite3.connect(DB_PATH)
    macro_df = pd.read_sql_query(
        "SELECT date, vix_close, tnx_close, spy_close FROM macro_factors ORDER BY date",
        conn,
        parse_dates=["date"],
    )
    conn.close()
except Exception:
    st.warning("No macro data available. Run the pipeline first.")
    st.stop()

if macro_df.empty:
    st.info("No macro factor data. Run Phase 1c (Macro Ingestion) first.")
    st.stop()

# ── Summary metrics ──────────────────────────────────────————
latest = macro_df.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
col1.metric("VIX", f"{latest['vix_close']:.1f}")
col2.metric("10Y Yield", f"{latest['tnx_close']:.2f}%")
col3.metric("SPY", f"${latest['spy_close']:.2f}")
col4.metric("Data Points", f"{len(macro_df):,}")

# ── Time-series chart ────────────────────────────────────————
fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    subplot_titles=("VIX (Volatility Index)", "10Y Treasury Yield (TNX)", "SPY (Market Proxy)"),
    row_heights=[0.33, 0.33, 0.34],
)

# VIX
fig.add_trace(
    go.Scatter(x=macro_df["date"], y=macro_df["vix_close"],
               line=dict(color="#ef4444", width=1.5), name="VIX"),
    row=1, col=1,
)
# VIX regime band: > 25 = Risk-Off
fig.add_hrect(y0=25, y1=macro_df["vix_close"].max() * 1.1,
              fillcolor="rgba(239,68,68,0.1)", line_width=0,
              annotation_text="Risk-Off", row=1, col=1)

# TNX
fig.add_trace(
    go.Scatter(x=macro_df["date"], y=macro_df["tnx_close"],
               line=dict(color="#f59e0b", width=1.5), name="10Y Yield"),
    row=2, col=1,
)

# SPY
fig.add_trace(
    go.Scatter(x=macro_df["date"], y=macro_df["spy_close"],
               line=dict(color="#3b82f6", width=1.5), name="SPY"),
    row=3, col=1,
)

fig.update_layout(height=700, showlegend=False, margin=dict(t=40, b=20))
st.plotly_chart(fig, use_container_width=True)

# ── Latest data table ────────────────────────────────────————
with st.expander("📋 Latest Macro Data (last 20 rows)"):
    st.dataframe(
        macro_df.tail(20).sort_values("date", ascending=False),
        use_container_width=True,
    )
