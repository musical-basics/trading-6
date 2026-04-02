"""Strategy Lab — Charts, overlays, simulations, and strategy comparisons in one place."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.strategies import pullback_strategy
from src.strategies.ls_zscore_strategy import simulate_ls_zscore, REBALANCE_OPTIONS

cfg = render_sidebar()

st.markdown("# 🔬 Strategy Lab")
st.divider()

if not table_exists("daily_bars"):
    st.info("ℹ️ No data yet. Run the pipeline first.")
else:
    conn = get_db_connection()

    available_tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM daily_bars ORDER BY ticker", conn
    )["ticker"].tolist()

    has_sma = table_exists("strategy_signals")
    has_pb = table_exists("pullback_signals")
    has_xs = table_exists("cross_sectional_scores")

    # ═══════════════════════════════════════════════════════
    # TABS
    # ═══════════════════════════════════════════════════════
    tab_chart, tab_ls, tab_compare = st.tabs([
        "📊 Charts & Signals",
        "🧬 L/S Z-Score Portfolio",
        "⚔️ Per-Ticker Comparison",
    ])

    # ─────────────────────────────────────────────────────
    # TAB 1: Candlestick Charts with Strategy Overlays
    # ─────────────────────────────────────────────────────
    with tab_chart:
        col_sel, col_strat = st.columns([1, 1])
        with col_sel:
            selected_ticker = st.selectbox("Select Ticker", available_tickers, index=0, key="chart_ticker")
        with col_strat:
            strategy_view = st.selectbox("Strategy Overlay", ["SMA Crossover", "Pullback (RSI)", "Both"], key="chart_strategy")

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
            if strategy_view in ["SMA Crossover", "Both"] and has_sma:
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
            if strategy_view in ["Pullback (RSI)", "Both"] and has_pb:
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

            # Simulation metrics below the chart
            st.divider()

            if strategy_view in ["SMA Crossover", "Both"] and has_sma:
                st.markdown("#### 📈 SMA Crossover Simulation")
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

            if strategy_view in ["Pullback (RSI)", "Both"] and has_pb:
                st.markdown("#### 🎯 Pullback Strategy Simulation")
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

    # ─────────────────────────────────────────────────────
    # TAB 2: L/S Z-Score Portfolio Strategy
    # ─────────────────────────────────────────────────────
    with tab_ls:
        if not has_xs:
            st.info("ℹ️ Not enough cross-sectional data. Run the pipeline first.")
        else:
            st.caption("Long the cheapest, short the most expensive — rebalance at chosen frequency")

            ls_col1, ls_col2, ls_col3 = st.columns(3)
            with ls_col1:
                n_long = st.number_input("Stocks to LONG", min_value=1, max_value=10, value=2, step=1, key="ls_n_long")
            with ls_col2:
                n_short = st.number_input("Stocks to SHORT", min_value=1, max_value=10, value=2, step=1, key="ls_n_short")
            with ls_col3:
                rebalance_freq = st.selectbox("Rebalance Frequency", REBALANCE_OPTIONS, index=2, key="ls_rebal_freq")

            ls_eq, ls_trades = simulate_ls_zscore(
                n_long=int(n_long), n_short=int(n_short), rebalance_freq=rebalance_freq
            )

            if not ls_eq.empty:
                ls_eq["date"] = pd.to_datetime(ls_eq["date"])
                ls_total = ls_eq["equity"].iloc[-1] / 10000 - 1

                spy_bench = pd.read_sql_query("""
                    SELECT date, adj_close FROM daily_bars
                    WHERE ticker = 'SPY' ORDER BY date
                """, conn, parse_dates=["date"])

                spy_total = None
                if not spy_bench.empty:
                    spy_bench = spy_bench[spy_bench["date"] >= ls_eq["date"].iloc[0]]
                    spy_bench["daily_return"] = spy_bench["adj_close"].pct_change()
                    spy_bench["equity"] = 10000 * (1 + spy_bench["daily_return"].fillna(0)).cumprod()
                    spy_total = spy_bench["equity"].iloc[-1] / 10000 - 1

                ls_returns = ls_eq["daily_return"]
                ls_sharpe = ls_returns.mean() / ls_returns.std() * np.sqrt(252) if ls_returns.std() > 0 else 0
                ls_running_max = ls_eq["equity"].expanding().max()
                ls_max_dd = (1 - ls_eq["equity"] / ls_running_max).max()

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("L/S Z-Score", f"{ls_total:+.1%}",
                          delta=f"{(ls_total - (spy_total or 0)):+.1%} vs SPY" if spy_total is not None else None)
                m2.metric("SPY B&H", f"{spy_total:+.1%}" if spy_total is not None else "N/A")
                m3.metric("Sharpe", f"{ls_sharpe:.2f}")
                m4.metric("Max Drawdown", f"{ls_max_dd:.2%}")

                fig_ls = go.Figure()
                fig_ls.add_trace(go.Scatter(
                    x=ls_eq["date"], y=ls_eq["equity"],
                    name="L/S Z-Score", line=dict(color="#E040FB", width=2.5),
                ))
                if not spy_bench.empty:
                    fig_ls.add_trace(go.Scatter(
                        x=spy_bench["date"], y=spy_bench["equity"],
                        name="SPY Buy & Hold", line=dict(color="#FF9800", width=2, dash="dash"),
                    ))
                fig_ls.update_layout(template="plotly_dark", height=450,
                    yaxis_title="Portfolio Value ($)",
                    title=f"L/S {rebalance_freq} Rebalance vs SPY ($10,000)",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig_ls, use_container_width=True)

                if ls_trades:
                    with st.expander(f"📒 {rebalance_freq} Trade Log", expanded=False):
                        trade_rows = []
                        for t in ls_trades:
                            trade_rows.append({
                                "Period": t["month"],
                                "LONG": ", ".join(t["long"]),
                                "Long Z-Scores": ", ".join(f"{z:.2f}" for z in t["long_zscores"]),
                                "SHORT": ", ".join(t["short"]),
                                "Short Z-Scores": ", ".join(f"{z:.2f}" for z in t["short_zscores"]),
                            })
                        st.dataframe(pd.DataFrame(trade_rows), use_container_width=True, height=400)
            else:
                st.info("ℹ️ Not enough cross-sectional data. Run the pipeline first.")

    # ─────────────────────────────────────────────────────
    # TAB 3: Per-Ticker SMA vs Pullback Comparison
    # ─────────────────────────────────────────────────────
    with tab_compare:
        compare_ticker = st.selectbox("Select Ticker", available_tickers, index=0, key="compare_ticker")

        if compare_ticker:
            df_base = pd.read_sql_query(
                "SELECT date, adj_close FROM daily_bars WHERE ticker = ? ORDER BY date",
                conn, params=(compare_ticker,))
            df_base["date"] = pd.to_datetime(df_base["date"])
            df_base["daily_return"] = df_base["adj_close"].pct_change()
            df_base["buyhold_equity"] = (1 + df_base["daily_return"].fillna(0)).cumprod() * 10000

            sma_cum = None
            df_sma_eq = pd.DataFrame()
            if has_sma:
                df_s = pd.read_sql_query("""
                    SELECT s.date, s.signal, b.adj_close FROM strategy_signals s
                    JOIN daily_bars b ON s.ticker = b.ticker AND s.date = b.date
                    WHERE s.ticker = ? ORDER BY s.date
                """, conn, params=(compare_ticker,))
                if not df_s.empty:
                    df_s["date"] = pd.to_datetime(df_s["date"])
                    df_s["daily_return"] = df_s["adj_close"].pct_change()
                    pos = 0; pp = []
                    for sig in df_s["signal"]:
                        if sig == 1: pos = 1
                        elif sig == -1: pos = 0
                        pp.append(pos)
                    df_s["strategy_return"] = df_s["daily_return"] * pd.Series(pp).shift(1).values
                    df_s["equity"] = (1 + df_s["strategy_return"].fillna(0)).cumprod() * 10000
                    sma_cum = df_s["equity"].iloc[-1] / 10000 - 1
                    df_sma_eq = df_s

            pb_cum = None
            df_pb_eq = pd.DataFrame()
            if has_pb:
                df_p = pullback_strategy.simulate_pullback(compare_ticker, conn)
                if not df_p.empty:
                    df_p["date"] = pd.to_datetime(df_p["date"])
                    df_p["equity"] = (1 + df_p["strategy_return"].fillna(0)).cumprod() * 10000
                    pb_cum = df_p["equity"].iloc[-1] / 10000 - 1
                    df_pb_eq = df_p

            bh_cum = df_base["buyhold_equity"].iloc[-1] / 10000 - 1

            c1, c2, c3 = st.columns(3)
            c1.metric("Buy & Hold", f"{bh_cum:+.1%}")
            c2.metric("SMA Crossover", f"{sma_cum:+.1%}" if sma_cum is not None else "N/A",
                       delta=f"{(sma_cum-bh_cum):+.1%} vs B&H" if sma_cum is not None else None)
            c3.metric("Pullback (RSI)", f"{pb_cum:+.1%}" if pb_cum is not None else "N/A",
                       delta=f"{(pb_cum-bh_cum):+.1%} vs B&H" if pb_cum is not None else None)

            st.markdown("#### 📈 Equity Curves ($10,000)")
            fig_comp = go.Figure()
            fig_comp.add_trace(go.Scatter(x=df_base["date"], y=df_base["buyhold_equity"],
                name="Buy & Hold", line=dict(color="#FF9800", width=2, dash="dash")))
            if not df_sma_eq.empty:
                fig_comp.add_trace(go.Scatter(x=df_sma_eq["date"], y=df_sma_eq["equity"],
                    name="SMA Crossover", line=dict(color="#2196F3", width=2)))
            if not df_pb_eq.empty:
                fig_comp.add_trace(go.Scatter(x=df_pb_eq["date"], y=df_pb_eq["equity"],
                    name="Pullback (RSI)", line=dict(color="#00BCD4", width=2)))
            fig_comp.update_layout(template="plotly_dark", height=500,
                yaxis_title="Portfolio Value ($)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_comp, use_container_width=True)

    conn.close()
