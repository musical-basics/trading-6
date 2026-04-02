"""Strategy Comparison — L/S Z-Score + per-ticker SMA vs Pullback."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.strategies import pullback_strategy

cfg = render_sidebar()

st.markdown("# ⚔️ Strategy Comparison")
st.divider()

has_sma = table_exists("strategy_signals")
has_pb = table_exists("pullback_signals")
has_xs = table_exists("cross_sectional_scores")

if not has_sma and not has_pb and not has_xs:
    st.info("ℹ️ Compute strategies first from the Data Pipeline page.")
else:
    conn = get_db_connection()
    available_tickers = pd.read_sql_query(
        "SELECT DISTINCT ticker FROM daily_bars ORDER BY ticker", conn
    )["ticker"].tolist()

    # ── L/S Z-Score Strategy (portfolio-level) ───────────
    if has_xs:
        st.markdown("### 🧬 L/S Monthly Z-Score Strategy")
        st.caption("Long the cheapest, short the most expensive — rebalance monthly")

        ls_col1, ls_col2 = st.columns(2)
        with ls_col1:
            n_long = st.number_input("Stocks to LONG", min_value=1, max_value=10, value=2, step=1, key="ls_n_long")
        with ls_col2:
            n_short = st.number_input("Stocks to SHORT", min_value=1, max_value=10, value=2, step=1, key="ls_n_short")

        from src.strategies.ls_zscore_strategy import simulate_ls_zscore
        ls_eq, ls_trades = simulate_ls_zscore(n_long=int(n_long), n_short=int(n_short))

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
                title="L/S Monthly Rebalance vs SPY ($10,000)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_ls, use_container_width=True)

            if ls_trades:
                with st.expander("📒 Monthly Trade Log", expanded=False):
                    trade_rows = []
                    for t in ls_trades:
                        trade_rows.append({
                            "Month": t["month"],
                            "LONG": ", ".join(t["long"]),
                            "Long Z-Scores": ", ".join(f"{z:.2f}" for z in t["long_zscores"]),
                            "SHORT": ", ".join(t["short"]),
                            "Short Z-Scores": ", ".join(f"{z:.2f}" for z in t["short_zscores"]),
                        })
                    st.dataframe(pd.DataFrame(trade_rows), use_container_width=True, height=400)
        else:
            st.info("ℹ️ Not enough cross-sectional data. Run the pipeline first.")

    # ── Per-Ticker Comparison ────────────────────────────
    st.divider()
    st.markdown("### 📊 Per-Ticker: SMA Crossover vs Pullback")

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

        st.markdown("### 📈 Equity Curves ($10,000)")
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
