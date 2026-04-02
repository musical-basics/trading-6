"""
db_init.py — Level 3 Database Initialization

Creates the SQLite database with all required tables:
  Level 1 (preserved):
    - daily_bars: Raw EOD market data
    - strategy_signals: SMA crossover signals
    - pullback_signals: RSI pullback signals
    - paper_executions: Execution ledger for paper trades
  Level 2 (preserved):
    - quarterly_fundamentals: Raw quarterly financial reports
    - cross_sectional_scores: Daily EV/Sales Z-scores & target weights
    - wfo_results: Walk-Forward Optimization backtester metrics
  Level 3 (new):
    - macro_factors: Systemic macro data (VIX, 10Y Yield, SPY)
    - factor_betas: Rolling OLS regression betas per stock
    - ml_features: Fused feature matrix for XGBoost
    - ml_predictions: Raw XGBoost predictions & weights
    - target_portfolio: Risk-adjusted final portfolio weights

All tables use CREATE TABLE IF NOT EXISTS for idempotency.
"""

import sqlite3
import os
from src.config import DB_PATH


def init_db():
    """Initialize the SQLite database and create all required tables."""
    print("=" * 60)
    print("PHASE 0: Database Initialization")
    print("=" * 60)

    # Ensure data/ directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ── Level 1 Tables ───────────────────────────────────────────

    # Table 1: daily_bars (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            UNIQUE(ticker, date)
        )
    """)

    # Table 2: strategy_signals (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            sma_50 REAL,
            sma_200 REAL,
            signal INTEGER DEFAULT 0,
            UNIQUE(ticker, date)
        )
    """)

    # Table 3: paper_executions (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            ticker TEXT NOT NULL,
            action TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            simulated_price REAL NOT NULL,
            strategy_id TEXT DEFAULT 'sma_crossover'
        )
    """)

    # Table 4: pullback_signals (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pullback_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            close REAL,
            sma_200 REAL,
            rsi_3 REAL,
            adv_30 REAL,
            signal REAL DEFAULT 0.0,
            exit_signal TEXT DEFAULT NULL,
            UNIQUE(ticker, date)
        )
    """)

    # ── Level 2 Tables ───────────────────────────────────────────

    # Table 5: quarterly_fundamentals (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_fundamentals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            period_end_date DATE NOT NULL,
            filing_date DATE NOT NULL,
            revenue REAL,
            total_debt REAL,
            cash_and_equivalents REAL,
            shares_outstanding REAL,
            UNIQUE(ticker, period_end_date)
        )
    """)

    # Table 6: cross_sectional_scores (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cross_sectional_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            market_value REAL,
            enterprise_value REAL,
            ev_to_sales REAL,
            ev_sales_zscore REAL,
            target_weight REAL DEFAULT 0.0,
            UNIQUE(ticker, date)
        )
    """)

    # Table 7: wfo_results (unchanged)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wfo_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT NOT NULL,
            test_window_start DATE NOT NULL,
            test_window_end DATE NOT NULL,
            sharpe_ratio REAL,
            max_drawdown REAL,
            cagr REAL
        )
    """)

    # ── Level 3 Tables ───────────────────────────────────────────

    # Table 8: macro_factors — Systemic macro data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS macro_factors (
            date DATE PRIMARY KEY,
            vix_close REAL,
            vix3m_close REAL,
            tnx_close REAL,
            irx_close REAL,
            spy_close REAL
        )
    """)

    # Table 9: factor_betas — Rolling OLS regression betas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS factor_betas (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            beta_spy REAL,
            beta_vix REAL,
            beta_tnx REAL,
            PRIMARY KEY (ticker, date)
        )
    """)

    # Table 10: ml_features — Fused feature matrix for XGBoost
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_features (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            ev_sales_zscore REAL,
            dynamic_discount_rate REAL,
            dcf_npv_gap REAL,
            beta_spy REAL,
            beta_10y REAL,
            beta_vix REAL,
            fwd_return_20d REAL,
            PRIMARY KEY (ticker, date)
        )
    """)

    # Table 11: ml_predictions — Raw XGBoost output before risk limits
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_predictions (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            xgb_prediction REAL,
            raw_weight REAL,
            PRIMARY KEY (ticker, date)
        )
    """)

    # Table 12: target_portfolio — Final risk-adjusted weights
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS target_portfolio (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            target_weight REAL,
            mcr REAL,
            PRIMARY KEY (ticker, date)
        )
    """)

    # Table 13: macro_regime_signals — Daily macro regime classification
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS macro_regime_signals (
            date DATE PRIMARY KEY,
            vix_close REAL,
            vix_sma_50 REAL,
            vix_roc_20 REAL,
            tnx_roc_50 REAL,
            regime TEXT,
            exposure REAL
        )
    """)

    # Table 14: macro_regime2_signals — VIX term structure regime
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS macro_regime2_signals (
            date DATE PRIMARY KEY,
            vix_close REAL,
            vix3m_close REAL,
            term_ratio REAL,
            term_sma_10 REAL,
            regime TEXT,
            exposure REAL
        )
    """)

    # ── Level 4 Tables — Traders & Portfolios ────────────────────

    # Table 15: traders — Trader accounts
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            total_capital REAL NOT NULL DEFAULT 10000.0,
            unallocated_capital REAL NOT NULL DEFAULT 0.0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Table 16: trader_constraints — Risk limits per trader
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trader_constraints (
            trader_id INTEGER PRIMARY KEY,
            max_drawdown_pct REAL NOT NULL DEFAULT 0.20,
            max_open_positions INTEGER NOT NULL DEFAULT 50,
            max_capital_per_trade REAL NOT NULL DEFAULT 1000.0,
            halt_trading_flag INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (trader_id) REFERENCES traders(id)
        )
    """)

    # Table 17: portfolios — Sub-portfolios (10 per trader)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            allocated_capital REAL NOT NULL DEFAULT 1000.0,
            strategy_id TEXT DEFAULT NULL,
            rebalance_freq TEXT NOT NULL DEFAULT 'Daily',
            next_rebalance_date DATE DEFAULT NULL,
            FOREIGN KEY (trader_id) REFERENCES traders(id)
        )
    """)

    # ── Schema Migrations — Add columns to existing tables ───────

    # Add trader_id and portfolio_id to paper_executions (backward compat via defaults)
    for col_def in [
        ("trader_id", "INTEGER DEFAULT NULL"),
        ("portfolio_id", "INTEGER DEFAULT NULL"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE paper_executions ADD COLUMN {col_def[0]} {col_def[1]}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Add trader_id and portfolio_id to target_portfolio
    for col_def in [
        ("trader_id", "INTEGER DEFAULT NULL"),
        ("portfolio_id", "INTEGER DEFAULT NULL"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE target_portfolio ADD COLUMN {col_def[0]} {col_def[1]}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()
    conn.close()

    all_tables = [
        # Level 1
        "daily_bars", "strategy_signals", "pullback_signals", "paper_executions",
        # Level 2
        "quarterly_fundamentals", "cross_sectional_scores", "wfo_results",
        # Level 3
        "macro_factors", "factor_betas", "ml_features",
        "ml_predictions", "target_portfolio",
        "macro_regime_signals", "macro_regime2_signals",
        # Level 4
        "traders", "trader_constraints", "portfolios",
    ]
    print(f"  ✓ Database initialized at: {DB_PATH}")
    print(f"  ✓ Tables created: {', '.join(all_tables)}")
    print()


if __name__ == "__main__":
    init_db()

