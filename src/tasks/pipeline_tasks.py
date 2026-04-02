"""
pipeline_tasks.py — Celery tasks for ECS pipeline (Systems 2–4) and XGBoost retrain.

- run_ecs_pipeline: Triggered daily at 4:15 PM EST
- run_xgb_retrain: Triggered Saturday at 2:00 AM EST
"""

from src.core.celery_app import app
import logging

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="src.tasks.pipeline_tasks.run_ecs_pipeline",
    max_retries=1,
    default_retry_delay=60,
)
def run_ecs_pipeline(self):
    """
    Systems 2–4: Alignment → Strategy Evaluation → Risk APT.
    Reads from Parquet, writes to Parquet. Does NOT touch Postgres.
    """
    try:
        logger.info("🧮 [Master Clock] Running ECS pipeline...")

        from src.ecs.alignment_system import run_alignment
        from src.ecs.strategy_registry import evaluate_strategies
        from src.ecs.risk_system import apply_risk_constraints

        run_alignment()
        logger.info("  ✓ System 2 (Alignment) complete")

        # System 3 is handled by the strategy registry
        # System 4 applies risk constraints
        apply_risk_constraints()
        logger.info("  ✓ System 4 (Risk APT) complete")

        # System 5 triggers Execution Routing (Portfolio Rebalancer -> Net-Delta -> Alpaca)
        from src.pipeline.execution.portfolio_rebalancer import extract_portfolio_intents
        from src.pipeline.execution.order_router import route_orders
        
        intents = extract_portfolio_intents()
        route_orders(intents)
        logger.info("  ✓ System 5 (Execution Routing) complete")

        logger.info("✅ [Master Clock] ECS pipeline complete.")
        return {"status": "success"}

    except Exception as exc:
        logger.error(f"❌ Pipeline failed: {exc}")
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    name="src.tasks.pipeline_tasks.run_xgb_retrain",
)
def run_xgb_retrain(self):
    """
    Continuous Learning: Roll the WFO training window forward and retrain.
    Triggered Saturday at 2:00 AM EST.
    """
    try:
        logger.info("🧠 [Weekend Retrain] Starting XGBoost WFO retraining...")

        from src.pipeline.backtesting.xgb_wfo_engine import run_xgb_wfo
        run_xgb_wfo()

        logger.info("✅ [Weekend Retrain] XGBoost model updated for Monday.")
        return {"status": "success"}

    except Exception as exc:
        logger.error(f"❌ XGBoost retrain failed: {exc}")
        return {"status": "failed", "error": str(exc)}
