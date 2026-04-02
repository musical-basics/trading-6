"""
ui/pages/xgb_feature_importance.py — Level 3 XGBoost Feature Importance

Visualizes how XGBoost feature importances shift across WFO windows.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import sqlite3
from src.config import DB_PATH


st.header("🧠 XGBoost Feature Importance")
st.caption("How the model's attention shifts across macro regimes")

# ── Load predictions to show model output ────────────────————
try:
    conn = sqlite3.connect(DB_PATH)
    predictions_df = pd.read_sql_query(
        "SELECT ticker, date, xgb_prediction, raw_weight FROM ml_predictions ORDER BY date",
        conn,
        parse_dates=["date"],
    )

    # Load ML features for correlation analysis
    features_df = pd.read_sql_query(
        """SELECT ticker, date, ev_sales_zscore, dynamic_discount_rate,
                  dcf_npv_gap, beta_spy, beta_10y, beta_vix
           FROM ml_features ORDER BY date""",
        conn,
        parse_dates=["date"],
    )
    conn.close()
except Exception:
    st.warning("No ML data available. Run the pipeline first.")
    st.stop()

if predictions_df.empty:
    st.info("No XGBoost predictions. Run Phase 3a (XGBoost WFO) first.")
    st.stop()

# ── Summary metrics ──────────────────────────────────────————
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Predictions", f"{len(predictions_df):,}")
col2.metric("Weighted Entries", f"{(predictions_df['raw_weight'] > 0).sum():,}")
col3.metric("Date Start", str(predictions_df['date'].min().date()))
col4.metric("Date End", str(predictions_df['date'].max().date()))

# ── Feature correlation heatmap ──────────────────────────————
st.subheader("Feature Correlations")
if not features_df.empty:
    feature_cols = ["ev_sales_zscore", "dynamic_discount_rate", "dcf_npv_gap",
                    "beta_spy", "beta_10y", "beta_vix"]
    corr = features_df[feature_cols].corr()

    fig = go.Figure(data=go.Heatmap(
        z=corr.values,
        x=feature_cols,
        y=feature_cols,
        colorscale="RdBu_r",
        zmid=0,
        text=np.round(corr.values, 2),
        texttemplate="%{text}",
        textfont={"size": 11},
    ))
    fig.update_layout(height=400, margin=dict(t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)

# ── Prediction distribution ─────────────────────────────————
st.subheader("Prediction Distribution")
fig2 = go.Figure()
fig2.add_trace(go.Histogram(
    x=predictions_df["xgb_prediction"],
    nbinsx=50,
    marker_color="#8b5cf6",
    opacity=0.75,
    name="XGB Predictions",
))
fig2.update_layout(
    xaxis_title="Predicted Forward Return",
    yaxis_title="Count",
    height=350,
    margin=dict(t=20, b=20),
)
st.plotly_chart(fig2, use_container_width=True)

# ── Top picks per latest date ────────────────────────────————
st.subheader("Latest Portfolio Picks")
latest_date = predictions_df["date"].max()
latest_picks = predictions_df[
    (predictions_df["date"] == latest_date) & (predictions_df["raw_weight"] > 0)
].sort_values("xgb_prediction", ascending=False)

if not latest_picks.empty:
    st.dataframe(
        latest_picks[["ticker", "xgb_prediction", "raw_weight"]].reset_index(drop=True),
        use_container_width=True,
    )
else:
    st.info("No weighted picks for the latest date.")

# ── Predictions over time ────────────────────────────────————
with st.expander("📋 All Predictions (last 50 rows)"):
    st.dataframe(
        predictions_df.tail(50).sort_values("date", ascending=False),
        use_container_width=True,
    )
