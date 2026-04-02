"""
alpha_lab_store.py — Alpha Lab experiment storage via Supabase REST API.

Level 5: Uses supabase-py SDK (service_role key) for all CRUD operations.
Falls back to local Parquet if Supabase is not configured (local dev).

Equity curves remain in local Parquet files (too large for REST API rows).
"""

import json
import uuid
import os
from datetime import datetime
from typing import Optional

import polars as pl

from src.config import PROJECT_ROOT

# Equity curves stay local (large columnar data)
ALPHA_LAB_DIR = os.path.join(PROJECT_ROOT, "data", "alpha_lab")
EQUITY_CURVES_DIR = os.path.join(ALPHA_LAB_DIR, "equity_curves")
TRADES_DIR = os.path.join(ALPHA_LAB_DIR, "trades")
EXPERIMENTS_PATH = os.path.join(ALPHA_LAB_DIR, "experiments.parquet")

TABLE = "alpha_lab_experiments"


def _ensure_dirs():
    os.makedirs(ALPHA_LAB_DIR, exist_ok=True)
    os.makedirs(EQUITY_CURVES_DIR, exist_ok=True)
    os.makedirs(TRADES_DIR, exist_ok=True)


def _use_supabase():
    """Check if Supabase is available, otherwise fall back to Parquet."""
    from src.core.supabase_client import get_supabase
    sb = get_supabase()
    return sb


# ═══════════════════════════════════════════════════════════════
# SUPABASE CRUD OPERATIONS
# ═══════════════════════════════════════════════════════════════


def save_experiment(
    hypothesis: str,
    strategy_code: str,
    strategy_name: str,
    model_tier: str,
    rationale: str = "",
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost_usd: float = 0.0,
) -> str:
    """Create a new experiment record. Returns experiment_id."""
    eid = str(uuid.uuid4())[:8]
    record = {
        "experiment_id": eid,
        "hypothesis": hypothesis,
        "strategy_code": strategy_code,
        "strategy_name": strategy_name,
        "model_tier": model_tier,
        "status": "generated",
        "rationale": rationale,
        "cost_input_tokens": input_tokens,
        "cost_output_tokens": output_tokens,
        "cost_usd": cost_usd,
    }

    sb = _use_supabase()
    if sb:
        sb.table(TABLE).insert(record).execute()
    else:
        _parquet_save(record)

    return eid


def update_experiment_status(
    experiment_id: str,
    status: str,
    metrics: Optional[dict] = None,
):
    """Update status and optionally metrics for an experiment."""
    updates = {"status": status, "updated_at": datetime.utcnow().isoformat()}
    if metrics is not None:
        updates["metrics_json"] = json.dumps(metrics)

    sb = _use_supabase()
    if sb:
        sb.table(TABLE).update(updates).eq("experiment_id", experiment_id).execute()
    else:
        _parquet_update(experiment_id, updates)


def list_experiments() -> list[dict]:
    """Return all experiments as a list of dicts, newest first."""
    sb = _use_supabase()
    if sb:
        result = sb.table(TABLE).select("*").order("created_at", desc=True).execute()
        return result.data or []
    else:
        return _parquet_list()


def get_experiment(experiment_id: str) -> Optional[dict]:
    """Get a single experiment by ID."""
    sb = _use_supabase()
    if sb:
        result = sb.table(TABLE).select("*").eq("experiment_id", experiment_id).execute()
        return result.data[0] if result.data else None
    else:
        return _parquet_get(experiment_id)


def delete_experiment(experiment_id: str) -> bool:
    """Delete an experiment and its equity curve."""
    sb = _use_supabase()
    if sb:
        result = sb.table(TABLE).delete().eq("experiment_id", experiment_id).execute()
        deleted = len(result.data) > 0 if result.data else False
    else:
        deleted = _parquet_delete(experiment_id)

    # Clean up local equity curve
    ec_path = os.path.join(EQUITY_CURVES_DIR, f"{experiment_id}.parquet")
    if os.path.exists(ec_path):
        os.remove(ec_path)

    return deleted


def update_experiment_code(experiment_id: str, strategy_code: str):
    """Update the strategy code for an experiment (for human edits or self-healing)."""
    updates = {
        "strategy_code": strategy_code,
        "status": "generated",
        "updated_at": datetime.utcnow().isoformat(),
    }

    sb = _use_supabase()
    if sb:
        sb.table(TABLE).update(updates).eq("experiment_id", experiment_id).execute()
    else:
        _parquet_update(experiment_id, updates)

def update_experiment_name(experiment_id: str, strategy_name: str):
    """Rename an experiment."""
    updates = {
        "strategy_name": strategy_name,
        "updated_at": datetime.utcnow().isoformat(),
    }

    sb = _use_supabase()
    if sb:
        sb.table(TABLE).update(updates).eq("experiment_id", experiment_id).execute()
    else:
        _parquet_update(experiment_id, updates)


# ═══════════════════════════════════════════════════════════════
# EQUITY CURVES (always local Parquet — too large for REST rows)
# ═══════════════════════════════════════════════════════════════


def save_equity_curve(experiment_id: str, equity_df: pl.DataFrame):
    """Save equity curve data for an experiment."""
    _ensure_dirs()
    path = os.path.join(EQUITY_CURVES_DIR, f"{experiment_id}.parquet")
    equity_df.write_parquet(path)


def get_equity_curve(experiment_id: str) -> Optional[pl.DataFrame]:
    """Load equity curve for an experiment."""
    path = os.path.join(EQUITY_CURVES_DIR, f"{experiment_id}.parquet")
    if not os.path.exists(path):
        return None
    return pl.read_parquet(path)


# ═══════════════════════════════════════════════════════════════
# TRADE LEDGER (local Parquet — generated by lab_backtester)
# ═══════════════════════════════════════════════════════════════


def save_trade_ledger(experiment_id: str, ledger_df: pl.DataFrame):
    """Persist the discrete trade ledger for a completed backtest."""
    _ensure_dirs()
    path = os.path.join(TRADES_DIR, f"trades_{experiment_id}.parquet")
    ledger_df.write_parquet(path)


def get_trade_ledger(experiment_id: str) -> Optional[pl.DataFrame]:
    """Load the trade ledger for an experiment. Returns None if not found."""
    path = os.path.join(TRADES_DIR, f"trades_{experiment_id}.parquet")
    if not os.path.exists(path):
        return None
    return pl.read_parquet(path)


# ═══════════════════════════════════════════════════════════════
# AUDIT RESULTS
# ═══════════════════════════════════════════════════════════════


def update_audit_result(experiment_id: str, audit_status: str, audit_report_json: str):
    """Persist forensic audit verdict back to the experiment record."""
    updates = {
        "audit_status": audit_status,
        "audit_report_json": audit_report_json,
        "updated_at": datetime.utcnow().isoformat(),
    }

    sb = _use_supabase()
    if sb:
        sb.table(TABLE).update(updates).eq("experiment_id", experiment_id).execute()
    else:
        _parquet_update(experiment_id, updates)


# ═══════════════════════════════════════════════════════════════
# LOCAL PARQUET FALLBACK (when Supabase is not configured)
# ═══════════════════════════════════════════════════════════════


def _empty_experiments() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "experiment_id": pl.Utf8,
            "hypothesis": pl.Utf8,
            "strategy_code": pl.Utf8,
            "strategy_name": pl.Utf8,
            "model_tier": pl.Utf8,
            "status": pl.Utf8,
            "created_at": pl.Utf8,
            "metrics_json": pl.Utf8,
            "cost_input_tokens": pl.Int64,
            "cost_output_tokens": pl.Int64,
            "cost_usd": pl.Float64,
            "rationale": pl.Utf8,
        }
    )


def _parquet_save(record: dict):
    _ensure_dirs()
    record["created_at"] = datetime.now().isoformat()
    new_row = pl.DataFrame({k: [v] for k, v in record.items()})

    if os.path.exists(EXPERIMENTS_PATH):
        existing = pl.read_parquet(EXPERIMENTS_PATH)
        combined = pl.concat([existing, new_row], how="diagonal_relaxed")
    else:
        combined = new_row

    combined.write_parquet(EXPERIMENTS_PATH)


def _parquet_update(experiment_id: str, updates: dict):
    if not os.path.exists(EXPERIMENTS_PATH):
        return
    df = pl.read_parquet(EXPERIMENTS_PATH)
    mask = df["experiment_id"] == experiment_id

    update_exprs = {}
    for key, value in updates.items():
        if key in df.columns:
            update_exprs[key] = pl.when(mask).then(pl.lit(value)).otherwise(pl.col(key))

    if update_exprs:
        df = df.with_columns(**update_exprs)
        df.write_parquet(EXPERIMENTS_PATH)


def _parquet_list() -> list[dict]:
    if not os.path.exists(EXPERIMENTS_PATH):
        return []
    df = pl.read_parquet(EXPERIMENTS_PATH).sort("created_at", descending=True)
    return df.to_dicts()


def _parquet_get(experiment_id: str) -> Optional[dict]:
    if not os.path.exists(EXPERIMENTS_PATH):
        return None
    df = pl.read_parquet(EXPERIMENTS_PATH).filter(
        pl.col("experiment_id") == experiment_id
    )
    if df.is_empty():
        return None
    return df.to_dicts()[0]


def _parquet_delete(experiment_id: str) -> bool:
    if not os.path.exists(EXPERIMENTS_PATH):
        return False
    df = pl.read_parquet(EXPERIMENTS_PATH)
    filtered = df.filter(pl.col("experiment_id") != experiment_id)
    if len(filtered) == len(df):
        return False
    filtered.write_parquet(EXPERIMENTS_PATH)
    return True


# ═══════════════════════════════════════════════════════════════
# EDITOR SETTINGS OPERATIONS
# ═══════════════════════════════════════════════════════════════

def save_editor_setting(key: str, value: dict) -> None:
    """Save an arbitrary JSON dict to the editor_settings table."""
    sb = _use_supabase()
    if sb:
        record = {
            "key": key,
            "value": value,
            "updated_at": datetime.utcnow().isoformat()
        }
        sb.table("editor_settings").upsert(record, on_conflict="key").execute()


def get_editor_setting(key: str) -> Optional[dict]:
    """Retrieve an arbitrary JSON dict from the editor_settings table."""
    sb = _use_supabase()
    if sb:
        resp = sb.table("editor_settings").select("value").eq("key", key).execute()
        if resp.data and len(resp.data) > 0:
            return resp.data[0]["value"]
    return None
