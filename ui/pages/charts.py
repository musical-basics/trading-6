"""Charts & Simulation — Candlestick charts with strategy overlays."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.strategies import pullback_strategy

cfg = render_sidebar()

st.markdown("# 📊 Charts & Simulation")
st.divider()

if not table_exists("daily_bars"):
    st.info("ℹ️ No data yet. Run the pipeline first.")
else:
    conn = get_db_connection()

    available_tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM daily_bars ORDER BY ticker", conn
    )["ticker"].tolist()

    col_sel, col_strat = st.columns([1, 1])
    with col_sel:
        selected_ticker = st.selectbox("Select Ticker", available_tickers, index=0)
    with col_strat:
        strategy_view = st.selectbox("Strategy Overlay", ["SMA Crossover", "Pullback (RSI)", "Both"])

    if selected_ticker:
        df_chart = pd.read_sql_query("""
            SELECT date, open, high, low, close, adj_close, volume
            FROM daily_bars WHERE ticker = ? ORDER BY date
        """, conn, params=(selected_ticker,))
        df_chart["date"] = pd.to_datetime(df_chart["date"])

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.03, row_heights=[0.75, 0.25],
            subplot_titles=[f"{selected_ticker} — Price", "RSI (3-day)"]
        )

        fig.add_trace(go.Candlestick(
            x=df_chart["date"], open=df_chart["open"], high=df_chart["high"],
            low=df_chart["low"], close=df_chart["close"], name="OHLC",
            increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        ), row=1, col=1)

        # SMA overlay
        if strategy_view in ["SMA Crossover", "Both"] and table_exists("strategy_signals"):
            df_sma = pd.read_sql_query(
                "SELECT date, sma_50, sma_200, signal FROM strategy_signals WHERE ticker = ? ORDER BY date",
                conn, params=(selected_ticker,))
            df_sma["date"] = pd.to_datetime(df_sma["date"])

            fig.add_trace(go.Scatter(x=df_sma["date"], y=df_sma["sma_50"],
                name=f"SMA {cfg['fast_sma']}", line=dict(color="#2196F3", width=2)), row=1, col=1)
            fig.add_trace(go.Scatter(x=df_sma["date"], y=df_sma["sma_200"],
                name=f"SMA {cfg['slow_sma']}", line=dict(color="#FF9800", width=2)), row=1, col=1)

            buys = df_sma[df_sma["signal"] == 1].merge(df_chart[["date", "close"]], on="date")
            if not buys.empty:
                fig.add_trace(go.Scatter(x=buys["date"], y=buys["close"] * 0.97,
                    mode="markers", name="SMA BUY",
                    marker=dict(symbol="triangle-up", size=14, color="#00E676",
                                line=dict(width=1, color="#004D40"))), row=1, col=1)

            sells = df_sma[df_sma["signal"] == -1].merge(df_chart[["date", "close"]], on="date")
            if not sells.empty:
                fig.add_trace(go.Scatter(x=sells["date"], y=sells["close"] * 1.03,
                    mode="markers", name="SMA SELL",
                    marker=dict(symbol="triangle-down", size=14, color="#FF1744",
                                line=dict(width=1, color="#B71C1C"))), row=1, col=1)

        # Pullback overlay
        if strategy_view in ["Pullback (RSI)", "Both"] and table_exists("pullback_signals"):
            df_pb = pd.read_sql_query(
                "SELECT date, sma_200, rsi_3, signal, exit_signal FROM pullback_signals WHERE ticker = ? ORDER BY date",
                conn, params=(selected_ticker,))
            df_pb["date"] = pd.to_datetime(df_pb["date"])

            if strategy_view == "Pullback (RSI)":
                fig.add_trace(go.Scatter(x=df_pb["date"], y=df_pb["sma_200"],
                    name="SMA 200 (Trend)", line=dict(color="#FF9800", width=2, dash="dash")), row=1, col=1)

            pb_entries = df_pb[df_pb["signal"] == 1.0].merge(df_chart[["date", "close"]], on="date")
            if not pb_entries.empty:
                fig.add_trace(go.Scatter(x=pb_entries["date"], y=pb_entries["close"] * 0.96,
                    mode="markers", name="PB ENTRY",
                    marker=dict(symbol="diamond", size=12, color="#00BCD4",
                                line=dict(width=1, color="#006064"))), row=1, col=1)

            pb_tp = df_pb[df_pb["exit_signal"] == "TAKE_PROFIT"].merge(df_chart[["date", "close"]], on="date")
            if not pb_tp.empty:
                fig.add_trace(go.Scatter(x=pb_tp["date"], y=pb_tp["close"] * 1.04,
                    mode="markers", name="PB TAKE PROFIT",
                    marker=dict(symbol="star", size=12, color="#76FF03",
                                line=dict(width=1, color="#33691E"))), row=1, col=1)

            pb_sl = df_pb[df_pb["exit_signal"] == "STOP_LOSS"].merge(df_chart[["date", "close"]], on="date")
            if not pb_sl.empty:
                fig.add_trace(go.Scatter(x=pb_sl["date"], y=pb_sl["close"] * 1.04,
                    mode="markers", name="PB STOP LOSS",
                    marker=dict(symbol="x", size=12, color="#FF5252",
                                line=dict(width=1, color="#B71C1C"))), row=1, col=1)

            fig.add_trace(go.Scatter(x=df_pb["date"], y=df_pb["rsi_3"],
                name="RSI(3)", line=dict(color="#AB47BC", width=1.5)), row=2, col=1)
            fig.add_hline(y=cfg["rsi_entry"], line_dash="dash", line_color="green",
                          annotation_text=f"Oversold ({cfg['rsi_entry']})", row=2, col=1)
            fig.add_hline(y=cfg["rsi_exit"], line_dash="dash", line_color="red",
                          annotation_text=f"Overbought ({cfg['rsi_exit']})", row=2, col=1)

        fig.update_layout(template="plotly_dark", height=700,
            xaxis_rangeslider_visible=False,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(t=40))
        fig.update_yaxes(title_text="Price ($)", row=1, col=1)
        fig.update_yaxes(title_text="RSI", row=2, col=1)

        st.plotly_chart(fig, use_container_width=True)

        # ── Simulation Metrics ───────────────────────────
        st.divider()

        if strategy_view in ["SMA Crossover", "Both"] and table_exists("strategy_signals"):
            st.markdown("### 📈 SMA Crossover Simulation")
            df_sim = pd.read_sql_query("""
                SELECT s.date, b.adj_close, s.signal
                FROM strategy_signals s
                JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
                WHERE s.ticker = ? ORDER BY s.date
            """, conn, params=(selected_ticker,))

            if not df_sim.empty:
                df_sim["daily_return"] = df_sim["adj_close"].pct_change()
                pos = 0; positions = []
                for sig in df_sim["signal"]:
                    if sig == 1: pos = 1
                    elif sig == -1: pos = 0
                    positions.append(pos)
                df_sim["strategy_return"] = df_sim["daily_return"] * pd.Series(positions).shift(1).values
                strat_cum = (1 + df_sim["strategy_return"].fillna(0)).cumprod().iloc[-1] - 1
                bh_cum = (1 + df_sim["daily_return"].fillna(0)).cumprod().iloc[-1] - 1
                c1, c2, c3 = st.columns(3)
                c1.metric("SMA Strategy", f"{strat_cum:+.1%}", delta=f"{(strat_cum-bh_cum):+.1%} vs B&H")
                c2.metric("Buy & Hold", f"{bh_cum:+.1%}")
                c3.metric("Data Points", f"{len(df_sim):,}")

        if strategy_view in ["Pullback (RSI)", "Both"] and table_exists("pullback_signals"):
            st.markdown("### 🎯 Pullback Strategy Simulation")
            df_pb_sim = pullback_strategy.simulate_pullback(selected_ticker, conn)
            if not df_pb_sim.empty:
                df_pb_sim["daily_return"] = df_pb_sim["adj_close"].pct_change()
                pb_cum = (1 + df_pb_sim["strategy_return"].fillna(0)).cumprod().iloc[-1] - 1
                pb_bh = (1 + df_pb_sim["daily_return"].fillna(0)).cumprod().iloc[-1] - 1
                entries = (df_pb_sim["signal"] == 1.0).sum()
                exits = df_pb_sim["exit_signal"].notna().sum()
                p1, p2, p3, p4 = st.columns(4)
                p1.metric("Pullback", f"{pb_cum:+.1%}", delta=f"{(pb_cum-pb_bh):+.1%} vs B&H")
                p2.metric("Buy & Hold", f"{pb_bh:+.1%}")
                p3.metric("Entries", int(entries))
                p4.metric("Exits", int(exits))
            else:
                st.info("ℹ️ No pullback data for this ticker.")

    conn.close()
