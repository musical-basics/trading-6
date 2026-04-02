"""
src/pipeline — Backward-compatible re-exports.

Modules have been reorganized into subfolders:
  core/           → db_init
  data_sources/   → data_ingestion, macro_ingestion, fundamental providers
  scoring/        → cross_sectional_scoring, factor_betas, dynamic_dcf,
                    ml_feature_assembly, risk_apt
  backtesting/    → wfo_backtester, wfo_multi, strategy_tournament, xgb_wfo_engine
  execution/      → order_router, simulation, portfolio_rebalancer,
                    portfolio_state, squeeze_filter

Level 3 modules (statsmodels, xgboost) are lazily imported to avoid
crashing the Streamlit UI when those heavyweight libs aren't needed.
"""

# ── Always-available (lightweight) ───────────────────────────
from src.pipeline.core import db_init
from src.pipeline.data_sources import data_ingestion
from src.pipeline.data_sources import macro_ingestion
from src.pipeline.data_sources.yfinance import fundamentals as fundamental_ingestion
from src.pipeline.scoring import cross_sectional_scoring
from src.pipeline.backtesting import wfo_backtester, wfo_multi, strategy_tournament
from src.pipeline.execution import (
    order_router as execution,
    simulation,
    portfolio_rebalancer,
    portfolio_state,
    squeeze_filter,
)


# ── Lazy imports for Level 3 heavy modules ───────────────────
# These are only loaded when accessed (e.g. from main.py),
# NOT when the UI does `from src.pipeline import db_init`.

def __getattr__(name):
    """Lazy-load Level 3 modules that depend on statsmodels/xgboost."""
    lazy_map = {
        "factor_betas": "src.pipeline.scoring.factor_betas",
        "dynamic_dcf": "src.pipeline.scoring.dynamic_dcf",
        "ml_feature_assembly": "src.pipeline.scoring.ml_feature_assembly",
        "risk_apt": "src.pipeline.scoring.risk_apt",
        "xgb_wfo_engine": "src.pipeline.backtesting.xgb_wfo_engine",
    }
    if name in lazy_map:
        import importlib
        module = importlib.import_module(lazy_map[name])
        globals()[name] = module
        return module
    raise AttributeError(f"module 'src.pipeline' has no attribute {name!r}")
