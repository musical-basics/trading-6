"""
shared.py — Shared helpers and sidebar for all pages.

Configuration is handled by the Settings page (settings.py).
Values are stored in st.session_state for cross-page persistence.
"""

import sys
import os
import sqlite3
import pandas as pd
import streamlit as st

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.config import DB_PATH


def get_db_connection():
    return sqlite3.connect(DB_PATH)


def table_exists(table_name):
    try:
        conn = get_db_connection()
        count = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}", conn
        )["cnt"][0]
        conn.close()
        return count > 0
    except Exception:
        return False


def get_table_count(table_name):
    try:
        conn = get_db_connection()
        count = pd.read_sql_query(
            f"SELECT COUNT(*) as cnt FROM {table_name}", conn
        )["cnt"][0]
        conn.close()
        return count
    except Exception:
        return 0


# Default configuration values
DEFAULTS = {
    "universe": [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO",
        "ORCL", "CRM", "AMD", "ADBE", "INTC", "CSCO", "QCOM",
        "JPM", "V", "MA", "BAC", "WFC", "GS", "MS",
        "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO",
        "WMT", "PG", "KO", "PEP", "COST", "MCD", "NKE", "HD",
        "XOM", "CVX", "CAT", "BA", "UPS", "GE", "HON",
        "DIS", "NFLX", "CMCSA", "T", "VZ",
        "SPY", "QQQ",
    ],
    "fast_sma": 50,
    "slow_sma": 200,
    "rsi_period": 3,
    "rsi_entry": 20,
    "rsi_exit": 70,
    "capital_per_trade": 1000.0,
    "max_positions": 5,
}


def _init_defaults():
    """Initialize session_state with defaults if not already set."""
    for key, val in DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = val


def get_config():
    """Get the current config from session_state. Call _init_defaults() first."""
    _init_defaults()
    return {
        "universe": st.session_state["universe"],
        "fast_sma": st.session_state["fast_sma"],
        "slow_sma": st.session_state["slow_sma"],
        "rsi_period": st.session_state["rsi_period"],
        "rsi_entry": st.session_state["rsi_entry"],
        "rsi_exit": st.session_state["rsi_exit"],
        "capital_per_trade": st.session_state["capital_per_trade"],
        "max_positions": st.session_state["max_positions"],
    }


def render_sidebar():
    """Render a slim sidebar with status info only. Returns config dict."""
    _init_defaults()

    with st.sidebar:
        api_key = os.getenv("ALPACA_API_KEY", "").strip()
        api_secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
        if api_key and api_secret:
            st.success("🟢 Alpaca Connected")
        else:
            st.info("📋 Dry-Run Mode")

        # Quick status
        n_tickers = len(st.session_state.get("universe", []))
        st.caption(f"Universe: {n_tickers} tickers")

    return get_config()
