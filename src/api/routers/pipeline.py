"""
pipeline.py — Pipeline Trigger API Router

Provides endpoints to trigger data ingestion phases from the UI.
Runs pipeline phases in background threads to avoid blocking the API.
Captures both logging output AND print() output for the live log panel.
"""

import io
import sys
import threading
import logging
import time
from collections import deque
from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["Pipeline"])

# Track running state + log buffer
_pipeline_status = {"running": False, "phase": None, "error": None}
_pipeline_logs: deque = deque(maxlen=500)


class PipelineLogHandler(logging.Handler):
    """Custom log handler that captures pipeline logs to a deque."""
    def emit(self, record):
        try:
            msg = self.format(record)
            _pipeline_logs.append({
                "ts": time.strftime("%H:%M:%S"),
                "level": record.levelname,
                "msg": msg,
            })
        except Exception:
            pass


class PrintCapture:
    """Captures print() output and forwards to the log deque."""
    def __init__(self, original_stdout):
        self.original = original_stdout
        self.buffer = ""

    def write(self, text):
        self.original.write(text)  # Still print to console
        self.buffer += text
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            line = line.strip()
            if line:
                _pipeline_logs.append({
                    "ts": time.strftime("%H:%M:%S"),
                    "level": "INFO",
                    "msg": line,
                })

    def flush(self):
        self.original.flush()
        if self.buffer.strip():
            _pipeline_logs.append({
                "ts": time.strftime("%H:%M:%S"),
                "level": "INFO",
                "msg": self.buffer.strip(),
            })
            self.buffer = ""


# Install the handler on the root logger so all pipeline modules' logs are captured
_log_handler = PipelineLogHandler()
_log_handler.setLevel(logging.INFO)
_log_handler.setFormatter(logging.Formatter("%(name)s — %(message)s"))


def _run_in_background(phase: str, func):
    """Run a pipeline phase in a background thread with log capture."""
    global _pipeline_status
    _pipeline_logs.clear()
    _pipeline_status = {"running": True, "phase": phase, "error": None}

    # Attach handler to root logger to capture logging output
    root_logger = logging.getLogger()
    root_logger.addHandler(_log_handler)

    # Capture print() output
    original_stdout = sys.stdout
    sys.stdout = PrintCapture(original_stdout)

    _pipeline_logs.append({
        "ts": time.strftime("%H:%M:%S"),
        "level": "INFO",
        "msg": f"🚀 Starting pipeline phase: {phase}",
    })

    try:
        func()
        _pipeline_status = {"running": False, "phase": phase, "error": None}
        _pipeline_logs.append({
            "ts": time.strftime("%H:%M:%S"),
            "level": "INFO",
            "msg": f"✅ Pipeline phase '{phase}' completed successfully",
        })
        logger.info(f"✅ Pipeline phase '{phase}' completed")
    except Exception as e:
        _pipeline_status = {"running": False, "phase": phase, "error": str(e)}
        _pipeline_logs.append({
            "ts": time.strftime("%H:%M:%S"),
            "level": "ERROR",
            "msg": f"❌ Pipeline phase '{phase}' failed: {e}",
        })
        logger.error(f"❌ Pipeline phase '{phase}' failed: {e}")
    finally:
        sys.stdout = original_stdout
        root_logger.removeHandler(_log_handler)


@router.get("/status")
def pipeline_status():
    """Get current pipeline run status."""
    return _pipeline_status


@router.get("/logs")
def pipeline_logs(since: int = 0):
    """Get pipeline logs. Pass 'since' as the last index you received."""
    logs = list(_pipeline_logs)
    return {
        "logs": logs[since:],
        "total": len(logs),
        "running": _pipeline_status["running"],
    }


@router.post("/run/ingest")
def run_ingest():
    """Phase 1: Ingest market data, fundamentals, macro → SQLite → Parquet."""
    if _pipeline_status["running"]:
        return {"ok": False, "error": f"Pipeline already running: {_pipeline_status['phase']}"}

    def _ingest():
        from src.pipeline.core.db_init import init_db
        from src.pipeline.data_sources.data_ingestion import ingest
        from src.pipeline.data_sources.macro_ingestion import ingest_macro_factors
        from src.pipeline.data_sources.yfinance.fundamentals import ingest_fundamentals
        from src.core.migrate_sqlite_to_parquet import run_migration

        init_db()
        ingest()
        ingest_fundamentals()
        ingest_macro_factors()

        # Convert SQLite → Parquet so DuckDB/coverage can read it
        run_migration()

    thread = threading.Thread(target=_run_in_background, args=("ingest", _ingest))
    thread.daemon = True
    thread.start()

    return {"ok": True, "message": "Ingestion started (market data + fundamentals + macro → parquet)"}


@router.post("/run/ingest_edgar")
def run_ingest_edgar():
    """Phase 1 Target: Ingest only EDGAR Fundamentals → SQLite → Parquet."""
    if _pipeline_status["running"]:
        return {"ok": False, "error": f"Pipeline already running: {_pipeline_status['phase']}"}

    def _ingest_edgar():
        from src.pipeline.core.db_init import init_db
        from src.pipeline.data_sources.edgar.fundamentals import ingest_fundamentals_edgar
        from src.core.migrate_sqlite_to_parquet import run_migration

        init_db()
        ingest_fundamentals_edgar()

        # Convert SQLite → Parquet so DuckDB/coverage can read it
        run_migration()

    thread = threading.Thread(target=_run_in_background, args=("ingest_edgar", _ingest_edgar))
    thread.daemon = True
    thread.start()

    return {"ok": True, "message": "EDGAR Ingestion started (EDGAR fundamentals → parquet)"}


@router.post("/run/pipeline")
def run_pipeline():
    """Phases 2-4: Scoring, features, ML, risk."""
    if _pipeline_status["running"]:
        return {"ok": False, "error": f"Pipeline already running: {_pipeline_status['phase']}"}

    def _pipeline():
        from src.pipeline.scoring.factor_betas import compute_factor_betas
        from src.pipeline.scoring.cross_sectional_scoring import compute_cross_sectional_scores
        from src.pipeline.scoring.dynamic_dcf import compute_dynamic_dcf
        from src.pipeline.scoring.ml_feature_assembly import assemble_features
        from src.pipeline.scoring.risk_apt import apply_risk_constraints
        from src.core.migrate_sqlite_to_parquet import run_migration
        from src.pipeline.execution.portfolio_rebalancer import extract_portfolio_intents
        from src.pipeline.execution.order_router import route_orders

        compute_factor_betas()
        compute_cross_sectional_scores()
        compute_dynamic_dcf()
        assemble_features()
        apply_risk_constraints()

        # Phase 4 (Level 5): Execution Engine
        intents = extract_portfolio_intents()
        route_orders(intents)

        # Convert SQLite → Parquet so coverage matrix can read it
        run_migration()

    thread = threading.Thread(target=_run_in_background, args=("pipeline", _pipeline))
    thread.daemon = True
    thread.start()

    return {"ok": True, "message": "ECS pipeline started (scoring + features + risk)"}


@router.post("/run/full")
def run_full():
    """Run the complete pipeline: ingest + migrate + scoring + ML + risk."""
    if _pipeline_status["running"]:
        return {"ok": False, "error": f"Pipeline already running: {_pipeline_status['phase']}"}

    def _full():
        from src.pipeline.core.db_init import init_db
        from src.pipeline.data_sources.data_ingestion import ingest
        from src.pipeline.data_sources.macro_ingestion import ingest_macro_factors
        from src.pipeline.data_sources.yfinance.fundamentals import ingest_fundamentals
        from src.core.migrate_sqlite_to_parquet import run_migration
        from src.pipeline.scoring.factor_betas import compute_factor_betas
        from src.pipeline.scoring.cross_sectional_scoring import compute_cross_sectional_scores
        from src.pipeline.scoring.dynamic_dcf import compute_dynamic_dcf
        from src.pipeline.scoring.ml_feature_assembly import assemble_features
        from src.pipeline.scoring.risk_apt import apply_risk_constraints
        from src.pipeline.execution.portfolio_rebalancer import extract_portfolio_intents
        from src.pipeline.execution.order_router import route_orders

        init_db()
        ingest()
        ingest_fundamentals()
        ingest_macro_factors()

        # Convert SQLite → Parquet
        run_migration()

        # Scoring pipeline
        compute_factor_betas()
        compute_cross_sectional_scores()
        compute_dynamic_dcf()
        assemble_features()
        apply_risk_constraints()

        # Phase 4 (Level 5): Execution Engine
        intents = extract_portfolio_intents()
        route_orders(intents)

    thread = threading.Thread(target=_run_in_background, args=("full", _full))
    thread.daemon = True
    thread.start()

    return {"ok": True, "message": "Full pipeline started (ingest + migrate + scoring)"}


@router.post("/run/rebalance")
def run_rebalance():
    """Phase 5: Run only the portfolio rebalancer and order router."""
    if _pipeline_status["running"]:
        return {"ok": False, "error": f"Pipeline already running: {_pipeline_status['phase']}"}

    def _rebalance():
        from src.pipeline.execution.portfolio_rebalancer import extract_portfolio_intents
        from src.pipeline.execution.order_router import route_orders
        from src.core.migrate_sqlite_to_parquet import run_migration

        # Phase 5: Execution Engine
        intents = extract_portfolio_intents()
        route_orders(intents)

        # Convert SQLite → Parquet so coverage matrix can read it
        run_migration()

    thread = threading.Thread(target=_run_in_background, args=("rebalance", _rebalance))
    thread.daemon = True
    thread.start()

    return {"ok": True, "message": "Execution routing started (rebalancer + order router)"}
