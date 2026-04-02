"""Settings — Configure strategy parameters, universe, API keys, and risk limits."""

import streamlit as st
import os
from ui.shared import render_sidebar, _init_defaults

# Initialize defaults before sidebar
_init_defaults()
render_sidebar()

st.markdown("# ⚙️ Settings")
st.divider()

# ═══════════════════════════════════════════════════════
# TICKER UNIVERSE
# ═══════════════════════════════════════════════════════
st.markdown("### 🌐 Ticker Universe")

universe_input = st.text_area(
    "Comma-separated list of tickers",
    value=", ".join(st.session_state.get("universe", [])),
    height=100,
    key="universe_input",
)
universe_list = [t.strip().upper() for t in universe_input.split(",") if t.strip()]
st.caption(f"{len(universe_list)} tickers")

if st.button("💾 Save Universe", key="save_universe"):
    st.session_state["universe"] = universe_list
    st.success(f"✅ Universe updated: {len(universe_list)} tickers")

st.divider()

# ═══════════════════════════════════════════════════════
# STRATEGY PARAMETERS
# ═══════════════════════════════════════════════════════
st.markdown("### 📐 Strategy Parameters")

col_sma, col_pb = st.columns(2)

with col_sma:
    st.markdown("**SMA Crossover**")
    fast_sma = st.number_input("Fast SMA", min_value=5, max_value=100,
                                value=st.session_state.get("fast_sma", 50), step=5, key="set_fast_sma")
    slow_sma = st.number_input("Slow SMA", min_value=50, max_value=500,
                                value=st.session_state.get("slow_sma", 200), step=10, key="set_slow_sma")

with col_pb:
    st.markdown("**Pullback (RSI)**")
    rsi_period = st.number_input("RSI Period", min_value=2, max_value=14,
                                  value=st.session_state.get("rsi_period", 3), step=1, key="set_rsi_period")
    rsi_entry = st.number_input("RSI Entry (Oversold)", min_value=5, max_value=40,
                                 value=st.session_state.get("rsi_entry", 20), step=5, key="set_rsi_entry")
    rsi_exit = st.number_input("RSI Exit (Overbought)", min_value=50, max_value=90,
                                value=st.session_state.get("rsi_exit", 70), step=5, key="set_rsi_exit")

if st.button("💾 Save Strategy Parameters", key="save_strat"):
    st.session_state["fast_sma"] = int(fast_sma)
    st.session_state["slow_sma"] = int(slow_sma)
    st.session_state["rsi_period"] = int(rsi_period)
    st.session_state["rsi_entry"] = int(rsi_entry)
    st.session_state["rsi_exit"] = int(rsi_exit)
    st.success("✅ Strategy parameters saved")

st.divider()

# ═══════════════════════════════════════════════════════
# RISK LIMITS
# ═══════════════════════════════════════════════════════
st.markdown("### 🛡️ Risk Limits")

r1, r2 = st.columns(2)
with r1:
    capital_per_trade = st.number_input("Capital per Trade ($)", min_value=100, max_value=50000,
                                         value=int(st.session_state.get("capital_per_trade", 1000)), step=100, key="set_cpt")
with r2:
    max_positions = st.number_input("Max Positions", min_value=1, max_value=50,
                                     value=st.session_state.get("max_positions", 5), step=1, key="set_maxpos")

if st.button("💾 Save Risk Limits", key="save_risk"):
    st.session_state["capital_per_trade"] = float(capital_per_trade)
    st.session_state["max_positions"] = int(max_positions)
    st.success("✅ Risk limits saved")

st.divider()

# ═══════════════════════════════════════════════════════
# API STATUS
# ═══════════════════════════════════════════════════════
st.markdown("### 🔑 API Keys")
st.caption("Set in `.env.local` — restart Streamlit after changes")

keys = {
    "Alpaca": ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"),
    "FMP": ("FMP_API_KEY",),
    "Tiingo": ("TIINGO_API_KEY",),
    "Polygon": ("POLYGON_API_KEY",),
    "EODHD": ("EODHD_API_KEY",),
}

for name, env_vars in keys.items():
    all_set = all(os.getenv(v, "").strip() for v in env_vars)
    icon = "🟢" if all_set else "⚪"
    st.markdown(f"{icon} **{name}** — {'Configured' if all_set else 'Not set'}")
