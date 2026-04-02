"""
main.py — Level 5 CLI Dispatcher

Provides a command-line interface for running pipeline phases individually
or triggering the full pipeline. In production, these are called by
Celery tasks via the Master Clock — but this CLI remains available
for manual operation and debugging.

Usage:
    python3 main.py                 # Full pipeline (all phases)
    python3 main.py ingest          # Phase 1 only: Ingest market data
    python3 main.py pipeline        # Phases 2-4: ECS scoring + risk
    python3 main.py retrain         # XGBoost WFO retraining
    python3 main.py server          # Start FastAPI dev server
    python3 main.py migrate         # SQLite → Postgres migration
"""

import sys
import os
from datetime import datetime

# Ensure project root is on the path so src imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _run_full_pipeline():
    """Run all pipeline phases sequentially (legacy behavior)."""
    from src.pipeline import (
        db_init,
        data_ingestion,
        macro_ingestion,
        fundamental_ingestion,
        cross_sectional_scoring,
        factor_betas,
        dynamic_dcf,
        ml_feature_assembly,
        xgb_wfo_engine,
        risk_apt,
        squeeze_filter,
        wfo_backtester,
        portfolio_rebalancer,
        execution,
    )

    start_time = datetime.now()
    print()
    print("╔" + "═" * 58 + "╗")
    print("║  LEVEL 5 — GOD ENGINE PIPELINE                          ║")
    print("║  Started: " + start_time.strftime("%Y-%m-%d %H:%M:%S") + " " * 27 + "║")
    print("╚" + "═" * 58 + "╝")
    print()

    db_init.init_db()
    data_ingestion.ingest()
    fundamental_ingestion.ingest_fundamentals()
    macro_ingestion.ingest_macro_factors()
    factor_betas.compute_factor_betas()
    cross_sectional_scoring.compute_cross_sectional_scores()
    dynamic_dcf.compute_dynamic_dcf()
    ml_feature_assembly.assemble_features()
    xgb_wfo_engine.run_xgb_wfo()
    risk_apt.apply_risk_constraints()
    squeeze_filter.apply_squeeze_filter()
    wfo_backtester.run_wfo_tournament()
    orders = portfolio_rebalancer.rebalance_portfolio()
    execution.route_orders(orders)

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    print()
    print("╔" + "═" * 58 + "╗")
    print("║  PIPELINE COMPLETE                                      ║")
    print("║  Finished: " + end_time.strftime("%Y-%m-%d %H:%M:%S") + " " * 26 + "║")
    print(f"║  Elapsed: {elapsed:.1f}s" + " " * (47 - len(f"{elapsed:.1f}s")) + "║")
    print("╚" + "═" * 58 + "╝")
    print()


def _run_ingest():
    """Phase 1 only: ingest all market data, fundamentals, macro."""
    from src.pipeline.data_sources.data_ingestion import ingest
    from src.pipeline.data_sources.macro_ingestion import ingest_macro_factors
    print("🕓 Ingesting market data...")
    ingest()
    ingest_macro_factors()
    print("✅ Ingestion complete.")


def _run_pipeline():
    """Phases 2–4: scoring, ML, risk."""
    from src.pipeline.scoring.cross_sectional_scoring import compute_cross_sectional_scores
    from src.pipeline.scoring.factor_betas import compute_factor_betas
    from src.pipeline.scoring.dynamic_dcf import compute_dynamic_dcf
    from src.pipeline.scoring.ml_feature_assembly import assemble_features
    from src.pipeline.scoring.risk_apt import apply_risk_constraints
    from src.pipeline.backtesting.xgb_wfo_engine import run_xgb_wfo

    print("🧮 Running ECS pipeline...")
    factor_betas()
    compute_cross_sectional_scores()
    compute_dynamic_dcf()
    assemble_features()
    run_xgb_wfo()
    apply_risk_constraints()
    print("✅ Pipeline complete.")


def _run_retrain():
    """Weekend XGBoost retraining."""
    from src.pipeline.backtesting.xgb_wfo_engine import run_xgb_wfo
    print("🧠 Retraining XGBoost WFO model...")
    run_xgb_wfo()
    print("✅ Retrain complete.")


def _run_server():
    """Start FastAPI dev server."""
    import uvicorn
    print("🚀 Starting QuantPrime API on :8000...")
    uvicorn.run("src.api.server:app", host="0.0.0.0", port=8000, reload=True)


def _run_migrate():
    """Run SQLite → Postgres migration."""
    from src.scripts.migrate_sqlite_to_pg import migrate
    migrate()


COMMANDS = {
    "full": ("Run full pipeline (all phases)", _run_full_pipeline),
    "ingest": ("Phase 1: Ingest market data", _run_ingest),
    "pipeline": ("Phases 2-4: ECS scoring + risk", _run_pipeline),
    "retrain": ("Weekend XGBoost retraining", _run_retrain),
    "server": ("Start FastAPI dev server", _run_server),
    "migrate": ("SQLite → Postgres migration", _run_migrate),
}


def main():
    if len(sys.argv) < 2:
        _run_full_pipeline()
        return

    command = sys.argv[1].lower()

    if command in ("help", "--help", "-h"):
        print("Usage: python3 main.py [command]")
        print()
        print("Commands:")
        for cmd, (desc, _) in COMMANDS.items():
            print(f"  {cmd:<12} {desc}")
        print()
        print("If no command given, runs the full pipeline.")
        return

    if command in COMMANDS:
        _, fn = COMMANDS[command]
        fn()
    else:
        print(f"❌ Unknown command: {command}")
        print(f"   Valid: {', '.join(COMMANDS.keys())}")
        sys.exit(1)


if __name__ == "__main__":
    main()
