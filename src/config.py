"""
src/config.py — Centralized Configuration (Level 3)

Single source of truth for paths, constants, and shared parameters.
All modules import from here instead of computing locally.
"""

import os
from dotenv import load_dotenv

# ── Project Root ─────────────────────────────────────────────
# config.py lives at <project>/src/config.py
# So project root is one directory up from this file
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── Database Path ────────────────────────────────────────────
DB_PATH = os.path.join(PROJECT_ROOT, "data", "level3_trading.db")

# ── Environment Variables ────────────────────────────────────
# Load .env.local first (takes priority), then .env as fallback
_env_local = os.path.join(PROJECT_ROOT, ".env.local")
_env_default = os.path.join(PROJECT_ROOT, ".env")
if os.path.exists(_env_local):
    load_dotenv(_env_local)
else:
    load_dotenv(_env_default)

# ═══════════════════════════════════════════════════════════════
# LEVEL 5 — Infrastructure Configuration
# ═══════════════════════════════════════════════════════════════

# Supabase REST API (transactional data — traders, experiments, executions)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# Legacy SQLite path (analytical data fallback)
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

# Task queue broker
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ── Default Universe (Expanded S&P 50 Subset) ───────────────
DEFAULT_UNIVERSE = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO",
    "ORCL", "CRM", "AMD", "ADBE", "INTC", "CSCO", "QCOM",
    # Financials
    "JPM", "V", "MA", "BAC", "WFC", "GS", "MS",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO",
    # Consumer
    "WMT", "PG", "KO", "PEP", "COST", "MCD", "NKE", "HD",
    # Industrials & Energy
    "XOM", "CVX", "CAT", "BA", "UPS", "GE", "HON",
    # Communications & Utilities
    "DIS", "NFLX", "CMCSA", "T", "VZ",
    # ETFs (reference benchmarks)
    "SPY", "QQQ",
]

# ── Level 2 WFO / Friction Constants ────────────────────────
SLIPPAGE_BPS = 0.0005           # 5 basis points per trade
COMMISSION_PER_SHARE = 0.005    # $0.005 per share

# ── Portfolio Constraints ────────────────────────────────────
MAX_SINGLE_WEIGHT = 0.10        # No single stock > 10% of portfolio
CASH_BUFFER = 0.05              # Always keep 5% cash

# ── Signal Thresholds ───────────────────────────────────────
ZSCORE_BUY_THRESHOLD = -1.0     # Z-score < -1.0 → undervalued → BUY

# ── Fundamental Data Alignment ──────────────────────────────
FILING_DELAY_DAYS = 45          # Proxy SEC filing delay (period_end + 45d)

# ── Liquidity Gating ────────────────────────────────────────
ADV_LOOKBACK = 30               # 30-day Average Daily Volume lookback
ADV_MAX_PCT = 0.01              # Max trade size = 1% of 30-day ADV

# ── WFO Window Parameters ───────────────────────────────────
WFO_TRAIN_YEARS = 2             # Lookback training window
WFO_TEST_YEARS = 1              # Forward test window
WFO_STEP_YEARS = 1              # Roll step
WFO_ZSCORE_CANDIDATES = [-0.5, -0.75, -1.0, -1.25, -1.5]

# ═══════════════════════════════════════════════════════════════
# LEVEL 3 — Neurosymbolic Pod Constants
# ═══════════════════════════════════════════════════════════════

# ── Macro Factor Symbols (Yahoo Finance) ────────────────────
MACRO_TICKERS = {
    "vix": "^VIX",
    "vix3m": "^VIX3M",
    "tnx": "^TNX",
    "irx": "^IRX",
    "spy": "SPY",
}

# ── Level 3 APT Constants ───────────────────────────────────
RISK_FREE_RATE = 0.043           # 4.3% (current ~Fed Funds proxy)
EQUITY_RISK_PREMIUM = 0.055     # 5.5% ERP
VIX_RISK_PREMIUM = 0.002        # λ_VIX loading
GROWTH_RATE = 0.03              # Gordon Growth Model perpetuity g

# ── Rolling Regression Parameters ───────────────────────────
OLS_ROLLING_WINDOW = 90         # 90-day rolling OLS for Beta calc
COVARIANCE_WINDOW = 90          # 90-day window for Σ (covariance)

# ── XGBoost Hyperparameters ─────────────────────────────────
XGB_N_ESTIMATORS = 200
XGB_MAX_DEPTH = 4
XGB_LEARNING_RATE = 0.05
XGB_SUBSAMPLE = 0.8
FWD_RETURN_DAYS = 20            # Forward label lookback
EMBARGO_DAYS = 20               # Purge buffer = FWD_RETURN_DAYS
TOP_N_HOLDINGS = 5              # Number of stocks in target portfolio

# ── Risk APT Limits ─────────────────────────────────────────
MAX_MCR_THRESHOLD = 0.05        # Max Marginal Contribution to Risk per asset
MAX_PORTFOLIO_VOL = 0.25        # Max annualized portfolio volatility (25%)

# ── Squeeze / Bouncer Filters ───────────────────────────────
VIX_EXTREME_THRESHOLD = 30      # VIX > 30 → kill short positions
MOMENTUM_SQUEEZE_PCT = 0.20     # 10-day momentum > 20% → truncate short
MOMENTUM_LOOKBACK = 10          # Lookback days for momentum check

# ── Macro Regime Strategy ───────────────────────────────────
VIX_RISK_ON_THRESHOLD = 20      # VIX < 20 → risk-on
VIX_RISK_OFF_THRESHOLD = 30     # VIX > 30 → risk-off
VIX_SPIKE_ROC_PCT = 0.40        # 20-day VIX ROC > 40% → risk-off
TNX_RATE_SHOCK_ROC_PCT = 0.10   # 50-day yield ROC > 10% → caution


# ═══════════════════════════════════════════════════════════════
# INDICATOR METADATA — Self-Documenting Data Dictionary
# ═══════════════════════════════════════════════════════════════
# Each entry: {"category": str, "description": str}
# Update this when you add new indicators to the pipeline.
# The Alpha Lab reads this at runtime to build LLM prompts.

INDICATOR_METADATA = {
    # ── Market Data ──────────────────────────────────────────
    "adj_close": {
        "category": "market",
        "description": "Adjusted close price (split/dividend corrected).",
    },
    "volume": {
        "category": "market",
        "description": "Daily trading volume (shares).",
    },
    "daily_return": {
        "category": "market",
        "description": "Daily percentage return.",
    },
    # ── Feature / Statistical ────────────────────────────────
    "ev_sales_zscore": {
        "category": "fundamental",
        "description": "EV/Sales cross-sectional z-score (lower = cheaper vs peers).",
    },
    "dcf_npv_gap": {
        "category": "fundamental",
        "description": "DCF intrinsic value gap (positive = undervalued).",
    },
    "dynamic_discount_rate": {
        "category": "fundamental",
        "description": "CAPM-derived per-stock discount rate (Rf + β·ERP + β_vix·λ).",
    },
    "beta_spy": {
        "category": "statistical",
        "description": "Beta vs S&P 500 (1.0 = market neutral). 90-day rolling OLS.",
    },
    "beta_tnx": {
        "category": "statistical",
        "description": "Beta vs 10Y Treasury yield. Measures interest rate sensitivity.",
    },
    "beta_vix": {
        "category": "statistical",
        "description": "Beta vs VIX. Measures volatility sensitivity.",
    },
    # ── Macro ────────────────────────────────────────────────
    "vix": {
        "category": "macro",
        "description": "VIX index level (fear gauge, typically 12-30).",
    },
    "vix3m": {
        "category": "macro",
        "description": "3-month VIX. Compare with VIX for term structure (backwardation = fear).",
    },
    "tnx": {
        "category": "macro",
        "description": "10Y Treasury yield (in %). Rising = tighter financial conditions.",
    },
    "irx": {
        "category": "macro",
        "description": "13-week T-bill rate (%). Proxy for risk-free rate.",
    },
    "spy": {
        "category": "macro",
        "description": "S&P 500 price level. Use for regime detection (e.g. vs 200-day SMA).",
    },
    # ── Fundamental ──────────────────────────────────────────
    "total_debt": {
        "category": "fundamental",
        "description": "Total debt on balance sheet (quarterly filing).",
    },
    "cash": {
        "category": "fundamental",
        "description": "Cash and equivalents (quarterly filing).",
    },
    "shares_out": {
        "category": "fundamental",
        "description": "Shares outstanding.",
    },
    "revenue": {
        "category": "fundamental",
        "description": "Quarterly revenue.",
    },
    # ── Identifiers (excluded from LLM feature list) ────────
    "entity_id": {
        "category": "_internal",
        "description": "Internal entity identifier.",
    },
    "date": {
        "category": "_internal",
        "description": "Trading date.",
    },
    "ticker": {
        "category": "_internal",
        "description": "Stock ticker symbol.",
    },
}


