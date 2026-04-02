"""Data Pipeline — Run full pipeline, fill missing data, or individual steps."""

import streamlit as st
import pandas as pd
import os
import sqlite3
from ui.shared import get_db_connection, table_exists, get_table_count, render_sidebar, DB_PATH
from src.pipeline import db_init, data_ingestion, fundamental_ingestion, cross_sectional_scoring
from src.pipeline import macro_ingestion
from src.pipeline.data_sources.fmp import fundamentals as fmp_fundamentals
from src.pipeline.data_sources.edgar import fundamentals as edgar_fundamentals
from src.pipeline.data_sources.tiingo import fundamentals as tiingo_fundamentals
from src.pipeline.data_sources.polygon import fundamentals as polygon_fundamentals
from src.pipeline.data_sources.eodhd import fundamentals as eodhd_fundamentals
from src.strategies import strategy, pullback_strategy

cfg = render_sidebar()

st.markdown("# ⚙️ Data Pipeline")
st.divider()

# ── Fundamental Data Source Selector ─────────────────────────
DATA_SOURCES = {
    "SEC EDGAR  — free, 6+ years, no key needed": "edgar",
    "yfinance   — free, ~8 quarters (default)": "yfinance",
    "FMP        — free: 5 quarters, paid: 10+ years": "fmp",
    "Tiingo     — free: 3 years (Dow 30), paid: 20+ years": "tiingo",
    "Polygon.io — free: 5 calls/min, paid: 10+ years": "polygon",
    "EODHD      — free: 20 calls/day, paid: unlimited": "eodhd",
}

fund_source = st.selectbox(
    "📊 Fundamental Data Source",
    list(DATA_SOURCES.keys()),
    index=0,
    key="fund_source",
)
source_key = DATA_SOURCES[fund_source]


def _run_fundamentals(tickers):
    if source_key == "edgar":
        edgar_fundamentals.ingest_fundamentals_edgar(tickers=tickers)
    elif source_key == "fmp":
        fmp_fundamentals.ingest_fundamentals_fmp(tickers=tickers)
    elif source_key == "tiingo":
        tiingo_fundamentals.ingest_fundamentals_tiingo(tickers=tickers)
    elif source_key == "polygon":
        polygon_fundamentals.ingest_fundamentals_polygon(tickers=tickers)
    elif source_key == "eodhd":
        eodhd_fundamentals.ingest_fundamentals_eodhd(tickers=tickers)
    else:
        fundamental_ingestion.ingest_fundamentals(tickers=tickers)


# ── Primary Actions ──────────────────────────────────────────
btn_col1, btn_col2 = st.columns(2)

with btn_col1:
    run_full = st.button("🚀 Run Full Pipeline", type="primary", use_container_width=True)
with btn_col2:
    fill_missing = st.button("🔄 Fill Missing Data", use_container_width=True)

if run_full:
    progress = st.progress(0, text="Initializing database...")
    db_init.init_db()

    progress.progress(10, text="Ingesting EOD prices...")
    data_ingestion.UNIVERSE = cfg["universe"]
    data_ingestion.ingest()

    progress.progress(25, text=f"Ingesting quarterly fundamentals ({source_key})...")
    _run_fundamentals(cfg["universe"])

    progress.progress(38, text="Ingesting macro factors (VIX, TNX, SPY)...")
    macro_ingestion.ingest_macro_factors()

    progress.progress(50, text="Computing cross-sectional EV/Sales Z-scores...")
    cross_sectional_scoring.compute_cross_sectional_scores()

    progress.progress(65, text="Computing SMA crossover signals...")
    strategy.compute_signals()

    progress.progress(80, text="Computing pullback strategy signals...")
    pullback_strategy.RSI_PERIOD = cfg["rsi_period"]
    pullback_strategy.RSI_ENTRY_THRESHOLD = cfg["rsi_entry"]
    pullback_strategy.RSI_EXIT_THRESHOLD = cfg["rsi_exit"]
    pullback_strategy.compute_pullback_signals()

    progress.progress(100, text="✅ Pipeline complete!")
    st.success(f"✅ Full pipeline executed for {len(cfg['universe'])} tickers!")
    st.rerun()

if fill_missing:
    import yfinance as yf
    from datetime import datetime, timedelta

    progress = st.progress(0, text="Scanning database for gaps...")
    db_init.init_db()

    conn = sqlite3.connect(DB_PATH)
    today_str = datetime.now().strftime("%Y-%m-%d")
    status_log = []

    # ═══════════════════════════════════════════════════════════
    # 1. PRICES — only fetch from last date forward, per ticker
    # ═══════════════════════════════════════════════════════════
    progress.progress(5, text="Checking price data gaps...")

    # Get last date per ticker already in DB
    existing_prices = pd.read_sql_query(
        "SELECT ticker, MAX(date) as last_date FROM daily_bars GROUP BY ticker",
        conn,
    )
    existing_map = dict(zip(existing_prices["ticker"], existing_prices["last_date"])) if not existing_prices.empty else {}

    universe = cfg["universe"]
    tickers_needing_prices = []
    for t in universe:
        last = existing_map.get(t)
        if last is None:
            tickers_needing_prices.append((t, None))  # full fetch
        elif last < today_str:
            tickers_needing_prices.append((t, last))   # incremental
        # else: already up-to-date, skip

    if tickers_needing_prices:
        progress.progress(10, text=f"Fetching prices for {len(tickers_needing_prices)} tickers...")
        cursor = conn.cursor()
        for i, (ticker, last_date) in enumerate(tickers_needing_prices):
            try:
                # Fetch only from day after last_date (or 5 years back if new)
                if last_date:
                    start_dt = (pd.Timestamp(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_dt = (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d")

                df = yf.download(ticker, start=start_dt, end=today_str, progress=False, auto_adjust=False)
                if df.empty:
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                df = df.reset_index()
                df = df.rename(columns={
                    "Date": "date", "Open": "open", "High": "high",
                    "Low": "low", "Close": "close", "Adj Close": "adj_close",
                    "Volume": "volume",
                })
                df["ticker"] = ticker
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

                for _, row in df.iterrows():
                    cursor.execute(
                        """INSERT OR IGNORE INTO daily_bars
                           (ticker, date, open, high, low, close, adj_close, volume)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (row["ticker"], row["date"], row["open"], row["high"],
                         row["low"], row["close"], row["adj_close"], row["volume"])
                    )
                conn.commit()
            except Exception:
                continue

            if (i + 1) % 5 == 0:
                progress.progress(10 + int(20 * (i + 1) / len(tickers_needing_prices)),
                                  text=f"Prices: {i + 1}/{len(tickers_needing_prices)} tickers...")

        status_log.append(f"📊 Prices: updated {len(tickers_needing_prices)} tickers")
    else:
        status_log.append("📊 Prices: already up-to-date ✓")

    # ═══════════════════════════════════════════════════════════
    # 2. FUNDAMENTALS — skip tickers with recent filings (< 80 days)
    # ═══════════════════════════════════════════════════════════
    progress.progress(35, text="Checking fundamental data gaps...")

    if table_exists("quarterly_fundamentals"):
        fund_last = pd.read_sql_query(
            "SELECT ticker, MAX(filing_date) as last_filing FROM quarterly_fundamentals GROUP BY ticker",
            conn,
        )
        fund_map = dict(zip(fund_last["ticker"], fund_last["last_filing"])) if not fund_last.empty else {}
    else:
        fund_map = {}

    cutoff_date = (datetime.now() - timedelta(days=80)).strftime("%Y-%m-%d")
    tickers_needing_fundamentals = [
        t for t in universe
        if fund_map.get(t, "1900-01-01") < cutoff_date
    ]

    if tickers_needing_fundamentals:
        progress.progress(40, text=f"Fetching fundamentals for {len(tickers_needing_fundamentals)} tickers ({source_key})...")
        _run_fundamentals(tickers_needing_fundamentals)
        status_log.append(f"📈 Fundamentals: updated {len(tickers_needing_fundamentals)} tickers (skipped {len(universe) - len(tickers_needing_fundamentals)} up-to-date)")
    else:
        status_log.append(f"📈 Fundamentals: all {len(universe)} tickers have recent filings ✓")

    # ═══════════════════════════════════════════════════════════
    # 3. MACRO — only fetch from last date forward
    # ═══════════════════════════════════════════════════════════
    progress.progress(55, text="Checking macro factor gaps...")

    latest_macro = None
    if table_exists("macro_factors"):
        latest_macro = pd.read_sql_query(
            "SELECT MAX(date) as d FROM macro_factors", conn
        )["d"].iloc[0]

    if latest_macro and latest_macro >= (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"):
        status_log.append(f"🌍 Macro factors: already up-to-date (last: {latest_macro}) ✓")
    else:
        if latest_macro:
            progress.progress(60, text=f"Fetching macro data from {latest_macro}...")
        else:
            progress.progress(60, text="Fetching macro factor history...")
        macro_ingestion.ingest_macro_factors()
        status_log.append(f"🌍 Macro factors: updated (was: {latest_macro or 'empty'})")

    conn.close()

    # ═══════════════════════════════════════════════════════════
    # 4. RECOMPUTE derived scores (fast — pure SQL/pandas, no API)
    # ═══════════════════════════════════════════════════════════
    progress.progress(70, text="Recomputing cross-sectional scores...")
    cross_sectional_scoring.compute_cross_sectional_scores()

    progress.progress(80, text="Recomputing SMA signals...")
    strategy.compute_signals()

    progress.progress(90, text="Recomputing pullback signals...")
    pullback_strategy.RSI_PERIOD = cfg["rsi_period"]
    pullback_strategy.RSI_ENTRY_THRESHOLD = cfg["rsi_entry"]
    pullback_strategy.RSI_EXIT_THRESHOLD = cfg["rsi_exit"]
    pullback_strategy.compute_pullback_signals()

    progress.progress(100, text="✅ Done!")

    # Show summary
    for line in status_log:
        st.info(line)
    st.success("✅ Missing data filled! Only new data was fetched.")
    st.rerun()

# ── Pipeline Status ──────────────────────────────────────────
if os.path.exists(DB_PATH):
    st.markdown("### Pipeline Status")

    # Level 2 tables
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Daily Bars", f"{get_table_count('daily_bars'):,}")
    s2.metric("Fundamentals", f"{get_table_count('quarterly_fundamentals'):,}")
    s3.metric("XS Scores", f"{get_table_count('cross_sectional_scores'):,}")
    s4.metric("SMA Signals", f"{get_table_count('strategy_signals'):,}")
    s5.metric("Pullback Sig", f"{get_table_count('pullback_signals'):,}")

    # Level 3 tables
    l3_tables = ["macro_factors", "factor_betas", "ml_features", "ml_predictions", "target_portfolio"]
    l3_counts = {t: get_table_count(t) for t in l3_tables if table_exists(t)}

    if l3_counts:
        st.markdown("#### Level 3 — Neurosymbolic")
        l3_cols = st.columns(len(l3_counts))
        labels = {
            "macro_factors": "Macro",
            "factor_betas": "Betas",
            "ml_features": "ML Feats",
            "ml_predictions": "XGB Preds",
            "target_portfolio": "Target Wts",
        }
        for col, (table, count) in zip(l3_cols, l3_counts.items()):
            col.metric(labels.get(table, table), f"{count:,}")
else:
    st.info("ℹ️ Click **Run Full Pipeline** to get started.")

# ── Individual Steps ─────────────────────────────────────────
with st.expander("⚙️ Run Individual Steps", expanded=False):
    st.markdown("##### Level 2 — Core Pipeline")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("🗃️ Init DB Only", use_container_width=True):
            db_init.init_db()
            st.success("✅ Database initialized!")
            st.rerun()
        if st.button("📥 Ingest Fundamentals", use_container_width=True):
            _run_fundamentals(cfg["universe"])
            st.success("✅ Fundamentals ingested!")
            st.rerun()
        if st.button("🧮 SMA Signals Only", use_container_width=True):
            strategy.compute_signals()
            st.success("✅ SMA signals computed!")
            st.rerun()
        if st.button("🌍 Macro Factors Only", use_container_width=True):
            macro_ingestion.ingest_macro_factors()
            st.success("✅ Macro factors ingested!")
            st.rerun()
    with col_b:
        if st.button("📥 Ingest Prices Only", use_container_width=True):
            data_ingestion.UNIVERSE = cfg["universe"]
            data_ingestion.ingest()
            st.success("✅ Prices ingested!")
            st.rerun()
        if st.button("🧮 XS Scores Only", use_container_width=True):
            cross_sectional_scoring.compute_cross_sectional_scores()
            st.success("✅ Cross-sectional scores computed!")
            st.rerun()
        if st.button("🎯 Pullback Signals Only", use_container_width=True):
            pullback_strategy.RSI_PERIOD = cfg["rsi_period"]
            pullback_strategy.RSI_ENTRY_THRESHOLD = cfg["rsi_entry"]
            pullback_strategy.RSI_EXIT_THRESHOLD = cfg["rsi_exit"]
            pullback_strategy.compute_pullback_signals()
            st.success("✅ Pullback signals computed!")
            st.rerun()

    st.divider()
    st.markdown("##### Level 3 — Neurosymbolic")
    st.caption("⚠️ Requires `statsmodels`, `xgboost`, `scikit-learn`. Run with: `.venv/bin/python -m streamlit run ui/app.py`")
    col_c, col_d = st.columns(2)
    with col_c:
        if st.button("📊 Phase 2A: Factor Betas", use_container_width=True):
            try:
                before = get_table_count("factor_betas")
                from src.pipeline.scoring import factor_betas
                factor_betas.compute_factor_betas()
                after = get_table_count("factor_betas")
                st.toast(f"✅ Factor betas: {after:,} rows ({after - before:+,} new)", icon="📊")
                st.success(f"✅ Factor betas computed! **{after:,}** rows ({after - before:+,} new)")
            except Exception as e:
                st.error(f"❌ {e}")
        if st.button("💰 Phase 2C: Dynamic DCF", use_container_width=True):
            try:
                before = get_table_count("_dcf_staging")
                from src.pipeline.scoring import dynamic_dcf
                dynamic_dcf.compute_dynamic_dcf()
                after = get_table_count("_dcf_staging")
                st.toast(f"✅ Dynamic DCF: {after:,} rows ({after - before:+,} new)", icon="💰")
                st.success(f"✅ Dynamic DCF computed! **{after:,}** rows ({after - before:+,} new)")
            except Exception as e:
                st.error(f"❌ {e}")
        if st.button("🧬 Phase 2D: ML Feature Assembly", use_container_width=True):
            try:
                before = get_table_count("ml_features")
                from src.pipeline.scoring import ml_feature_assembly
                ml_feature_assembly.assemble_features()
                after = get_table_count("ml_features")
                st.toast(f"✅ ML features: {after:,} rows ({after - before:+,} new)", icon="🧬")
                st.success(f"✅ ML features assembled! **{after:,}** rows ({after - before:+,} new)")
            except Exception as e:
                st.error(f"❌ {e}")
    with col_d:
        if st.button("🧠 Phase 3a: XGBoost WFO", use_container_width=True):
            try:
                before = get_table_count("ml_predictions")
                from src.pipeline.backtesting import xgb_wfo_engine
                xgb_wfo_engine.run_xgb_wfo()
                after = get_table_count("ml_predictions")
                st.toast(f"✅ XGBoost WFO: {after:,} predictions ({after - before:+,} new)", icon="🧠")
                st.success(f"✅ XGBoost WFO complete! **{after:,}** predictions ({after - before:+,} new)")
            except Exception as e:
                st.error(f"❌ {e}")
        if st.button("🛡️ Phase 3b: Risk APT", use_container_width=True):
            try:
                before = get_table_count("target_portfolio")
                from src.pipeline.scoring import risk_apt
                risk_apt.apply_risk_constraints()
                after = get_table_count("target_portfolio")
                st.toast(f"✅ Risk APT: {after:,} weights ({after - before:+,} new)", icon="🛡️")
                st.success(f"✅ Risk APT complete! **{after:,}** weights ({after - before:+,} new)")
            except Exception as e:
                st.error(f"❌ {e}")
        if st.button("🚫 Phase 4: Squeeze Filter", use_container_width=True):
            before = get_table_count("target_portfolio")
            from src.pipeline.execution import squeeze_filter
            squeeze_filter.apply_squeeze_filter()
            after = get_table_count("target_portfolio")
            st.toast(f"✅ Squeeze filter applied! {after:,} entries checked", icon="🚫")
            st.success(f"✅ Squeeze filter applied! **{after:,}** entries checked")

# ── Data Previews ────────────────────────────────────────────
st.divider()
preview_tables = {
    "daily_bars": "SELECT * FROM daily_bars ORDER BY date DESC LIMIT 100",
    "quarterly_fundamentals": "SELECT * FROM quarterly_fundamentals ORDER BY filing_date DESC LIMIT 100",
    "cross_sectional_scores": "SELECT * FROM cross_sectional_scores ORDER BY date DESC, ev_sales_zscore ASC LIMIT 100",
    "strategy_signals": "SELECT * FROM strategy_signals ORDER BY date DESC LIMIT 100",
    "macro_factors": "SELECT * FROM macro_factors ORDER BY date DESC LIMIT 100",
}
for table_name, query in preview_tables.items():
    if table_exists(table_name):
        with st.expander(f"📋 Preview: {table_name}", expanded=False):
            conn = get_db_connection()
            st.dataframe(pd.read_sql_query(query, conn), use_container_width=True, height=400)
            conn.close()
