"""WFO Backtester — True Walk-Forward Optimization for all 4 strategies."""

import streamlit as st
import pandas as pd
import numpy as np
import importlib
import plotly.graph_objects as go
from ui.shared import get_db_connection, table_exists, render_sidebar
from src.pipeline.backtesting import wfo_multi
importlib.reload(wfo_multi)  # Force reload to pick up code changes

cfg = render_sidebar()

st.markdown("# 🔬 Walk-Forward Optimization")
st.caption("Train parameters on in-sample data → evaluate on out-of-sample data → stitch OOS equity curves")
st.divider()

if not table_exists("daily_bars"):
    st.info("ℹ️ No data yet. Run the pipeline first.")
else:
    # Explain the methodology
    with st.expander("📖 How WFO Works", expanded=False):
        st.markdown("""
        **Walk-Forward Optimization** prevents overfitting by never testing on data used for training.

        For each strategy:
        1. **Train window** — Sweep candidate parameters and find the best Sharpe ratio
        2. **Test window** — Apply the winning parameters to unseen data (out-of-sample)
        3. **Roll forward** — Shift windows and repeat
        4. **Stitch** — Combine all OOS test blocks into one valid equity curve

        | Strategy | Tunable Parameters |
        |----------|-------------------|
        | EV/Sales Long-Only | Z-score buy threshold |
        | L/S Z-Score | n_long, n_short |
        | SMA Crossover | fast_sma, slow_sma windows |
        | Pullback RSI | rsi_period, rsi_entry threshold |
        """)

    if st.button("🔬 Run Walk-Forward Optimization", type="primary", use_container_width=True):
        progress_bar = st.progress(0)
        status_text = st.empty()

        def _update_progress(name, step, total):
            progress_bar.progress(step / total, text=f"⏳ {name}...")
            status_text.caption(f"Step {step}/{total}: {name}")

        results = wfo_multi.run_all_wfo(progress_callback=_update_progress)
        progress_bar.progress(1.0, text="✅ Complete!")
        st.session_state["wfo_results"] = results
        st.success(f"✅ WFO complete — {len(results)} strategies optimized!")
        st.rerun()

    results = st.session_state.get("wfo_results")

    if results is None:
        progress_bar = st.progress(0)
        status_text = st.empty()

        def _update(name, step, total):
            progress_bar.progress(step / total, text=f"⏳ {name}...")
            status_text.caption(f"Step {step}/{total}: {name}")

        results = wfo_multi.run_all_wfo(progress_callback=_update)
        progress_bar.empty()
        status_text.empty()
        if results:
            st.session_state["wfo_results"] = results

    if results:
        # ── Summary table ────────────────────────────────────
        st.markdown("### 📊 OOS Performance Summary")
        st.caption("All metrics below are out-of-sample (never trained on)")

        rows = []
        for r in results:
            rows.append({
                "Strategy": r["name"],
                "OOS Sharpe": f"{r['overall']['sharpe']:.3f}",
                "OOS Max DD": f"{r['overall']['max_drawdown']:.2%}",
                "OOS CAGR": f"{r['overall']['cagr']:.2%}",
                "Windows": len(r["windows"]),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # ── Winner cards ─────────────────────────────────────
        best_sharpe = max(results, key=lambda r: r["overall"]["sharpe"])
        lowest_dd = min(results, key=lambda r: r["overall"]["max_drawdown"])
        best_cagr = max(results, key=lambda r: r["overall"]["cagr"])

        c1, c2, c3 = st.columns(3)
        c1.metric("🥇 Best OOS Sharpe", best_sharpe["name"],
                  delta=f"{best_sharpe['overall']['sharpe']:.3f}")
        c2.metric("🥇 Lowest OOS MaxDD", lowest_dd["name"],
                  delta=f"{lowest_dd['overall']['max_drawdown']:.2%}")
        c3.metric("🥇 Best OOS CAGR", best_cagr["name"],
                  delta=f"{best_cagr['overall']['cagr']:.2%}")

        # ── OOS Equity Curves ────────────────────────────────
        st.divider()
        st.markdown("### 📈 Stitched OOS Equity Curves")

        colors = {
            "EV/Sales Long-Only": "#2196F3",
            "L/S Z-Score": "#E040FB",
            "SMA Crossover (EW)": "#FF9800",
            "Pullback RSI (EW)": "#00BCD4",
        }

        fig = go.Figure()
        for r in results:
            eq = r["stitched"].copy()
            eq["date"] = pd.to_datetime(eq["date"])
            fig.add_trace(go.Scatter(
                x=eq["date"], y=eq["equity"],
                name=r["name"],
                line=dict(color=colors.get(r["name"], "#FFFFFF"), width=2.5),
            ))

        fig.update_layout(
            template="plotly_dark", height=500,
            yaxis_title="OOS Equity (normalized)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Per-strategy window details ──────────────────────
        st.divider()
        st.markdown("### 🔍 Per-Strategy Window Details")

        for r in results:
            with st.expander(f"**{r['name']}** — {len(r['windows'])} windows", expanded=False):
                win_rows = []
                for w in r["windows"]:
                    win_rows.append({
                        "Test Window": w["window"],
                        "Best Parameters": w["best_param"],
                        "Train Sharpe": f"{w['train_sharpe']:.3f}",
                        "OOS Sharpe": f"{w['sharpe']:.3f}",
                        "OOS MaxDD": f"{w['max_drawdown']:.2%}",
                        "OOS CAGR": f"{w['cagr']:.2%}",
                    })
                st.dataframe(pd.DataFrame(win_rows), use_container_width=True, hide_index=True)

                # Show train vs OOS Sharpe comparison
                if len(r["windows"]) > 0:
                    st.caption("**Key insight:** Compare Train Sharpe vs OOS Sharpe. "
                              "A large gap suggests overfitting.")
    else:
        st.info("ℹ️ Click **Run Walk-Forward Optimization** to analyze all strategies.")
