"""
database.py — Level 5 SQLAlchemy Session Factory

Provides the engine, session factory, and FastAPI dependency
for injecting database sessions into route handlers.

Connection priority:
  1. DATABASE_URL (Supabase/Postgres) — tries to connect
  2. If connection fails (e.g., local WiFi blocks outbound Postgres),
     falls back to local SQLite at data/quantprime.db

This ensures local dev always works, while production uses Supabase.
"""

import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.config import DATABASE_URL, PROJECT_ROOT

logger = logging.getLogger(__name__)

# SQLite fallback path — must match DB_PATH in config.py so Base.metadata.create_all
# migrates the same file that DATABASE_URL points to by default.
from src.config import DB_PATH as _DB_PATH
_SQLITE_PATH = _DB_PATH
_SQLITE_URL = f"sqlite:///{_SQLITE_PATH}"


def _build_engine():
    """Build the SQLAlchemy engine, falling back to SQLite if Postgres is unreachable."""
    url = DATABASE_URL

    # If Postgres URL, try to connect — fall back to SQLite on failure
    if url.startswith("postgresql"):
        try:
            connect_args = {"connect_timeout": 5}
            # Supabase pooler requires SSL
            if "supabase" in url:
                connect_args["sslmode"] = "require"

            test_engine = create_engine(url, pool_pre_ping=True, connect_args=connect_args)
            with test_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Connected to Supabase Postgres")
            return test_engine

        except Exception as e:
            logger.warning(f"⚠ Postgres connection failed: {e}")
            logger.warning(f"⚠ Falling back to local SQLite at {_SQLITE_PATH}")
            url = _SQLITE_URL

    # SQLite path
    os.makedirs(os.path.dirname(_SQLITE_PATH), exist_ok=True)
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False} if url.startswith("sqlite") else {},
    )


engine = _build_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Auto-create tables in SQLite (Supabase tables created via SQL Editor)
if str(engine.url).startswith("sqlite"):
    from src.core.models import Base, AlphaLabExperiment
    Base.metadata.create_all(bind=engine)
    logger.info("✅ SQLite tables created/verified")

    # Auto-seed from old Parquet experiments if SQLite is empty
    _parquet_path = os.path.join(PROJECT_ROOT, "data", "alpha_lab", "experiments.parquet")
    if os.path.exists(_parquet_path):
        _seed_session = SessionLocal()
        _count = _seed_session.query(AlphaLabExperiment).count()
        if _count == 0:
            try:
                import polars as pl
                from datetime import datetime as dt
                _df = pl.read_parquet(_parquet_path)
                for row in _df.to_dicts():
                    _seed_session.add(AlphaLabExperiment(
                        experiment_id=row["experiment_id"],
                        hypothesis=row.get("hypothesis", ""),
                        strategy_code=row.get("strategy_code", ""),
                        strategy_name=row.get("strategy_name", ""),
                        model_tier=row.get("model_tier", "sonnet"),
                        status=row.get("status", "generated"),
                        metrics_json=row.get("metrics_json"),
                        rationale=row.get("rationale", ""),
                        cost_input_tokens=row.get("cost_input_tokens", 0),
                        cost_output_tokens=row.get("cost_output_tokens", 0),
                        cost_usd=row.get("cost_usd", 0.0),
                        created_at=dt.fromisoformat(row["created_at"]) if row.get("created_at") else dt.utcnow(),
                    ))
                _seed_session.commit()
                logger.info(f"✅ Seeded {len(_df)} experiments from Parquet → SQLite")
            except Exception as e:
                logger.warning(f"⚠ Failed to seed from Parquet: {e}")
                _seed_session.rollback()
            finally:
                _seed_session.close()


def get_db():
    """FastAPI dependency for DB sessions.

    Usage:
        @router.get("/items")
        def list_items(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
