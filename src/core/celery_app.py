"""
celery_app.py — Level 5 Celery Application & Master Clock

Initializes the Celery app bound to Redis and configures the
beat schedule (The Master Clock) for autonomous pipeline operation.
"""

from celery import Celery
from celery.schedules import crontab

from src.config import REDIS_URL


app = Celery("quantprime", broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="US/Eastern",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

# ═══════════════════════════════════════════════════════════════
# THE MASTER CLOCK — Autonomous Pipeline Scheduling
# ═══════════════════════════════════════════════════════════════

app.conf.beat_schedule = {
    # ── Daily: Ingest market data after market close ─────────
    "daily-ingestion": {
        "task": "src.tasks.ingestion_tasks.run_full_ingestion",
        "schedule": crontab(hour=16, minute=0),  # 4:00 PM EST
    },

    # ── Daily: Run ECS pipeline 15 min after ingestion ───────
    "daily-pipeline": {
        "task": "src.tasks.pipeline_tasks.run_ecs_pipeline",
        "schedule": crontab(hour=16, minute=15),  # 4:15 PM EST
    },

    # ── Weekend: Retrain XGBoost WFO model ───────────────────
    "weekend-wfo-retrain": {
        "task": "src.tasks.pipeline_tasks.run_xgb_retrain",
        "schedule": crontab(hour=2, minute=0, day_of_week="saturday"),
    },

    # ── Nightly: Alpha Lab genetic evolution ─────────────────
    "nightly-genetic-evolution": {
        "task": "src.tasks.alpha_tasks.run_genetic_evolution",
        "schedule": crontab(hour=2, minute=0),
    },

    # ── Intraday: VIX squeeze monitor (every 5 min, market hours) ──
    "intraday-squeeze-monitor": {
        "task": "src.tasks.squeeze_monitor.check_vix_squeeze",
        "schedule": crontab(minute="*/5", hour="9-16", day_of_week="1-5"),
    },

    # ── Post-execution: Broker state reconciliation ──────────
    "post-execution-reconciliation": {
        "task": "src.tasks.reconciliation_tasks.sync_broker_state",
        "schedule": crontab(hour=16, minute=30),  # 4:30 PM EST
    },
}

# Auto-discover tasks from the tasks package
app.autodiscover_tasks(["src.tasks"])
