"""
ingestion_tasks.py — Celery tasks for data ingestion (System 1).

Triggered daily at 4:00 PM EST by the Master Clock.
"""

from src.core.celery_app import app
import logging

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="src.tasks.ingestion_tasks.run_full_ingestion",
    max_retries=2,
    default_retry_delay=300,
)
def run_full_ingestion(self):
    """
    System 1: Fetch all market data, fundamentals, and macro factors.
    Writes to Parquet files in data/components/.
    """
    try:
        logger.info("🕓 [Master Clock] Starting daily ingestion...")

        from src.pipeline.data_sources.data_ingestion import ingest
        from src.pipeline.data_sources.macro_ingestion import ingest_macro_factors

        ingest()
        ingest_macro_factors()

        logger.info("✅ [Master Clock] Ingestion complete.")
        return {"status": "success"}

    except Exception as exc:
        logger.error(f"❌ Ingestion failed: {exc}")
        raise self.retry(exc=exc)
