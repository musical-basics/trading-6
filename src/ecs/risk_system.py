"""
risk_system.py — Level 4 ECS System 4: Iterative Risk APT

The Bouncer. Takes raw strategy weights and applies variance-based constraints
using the Iterative MCR Fix:

  1. Compute covariance matrix (Σ) from trailing returns.
  2. Compute Marginal Contribution to Risk (MCR) per asset.
  3. If any MCR breaches MAX_MCR_THRESHOLD:
     a) Scale down ONLY the breaching assets.
     b) Re-allocate excess cash to non-breaching assets
        ONLY up to the point where they hit the MCR limit.
     c) Repeat until all assets MCR < 5% and Total Weight <= 95%.
  4. Emit RiskAuditComponent for X-Ray transparency.

This fixes the Level 3 bug where naive re-normalization inflated safe stocks.
"""

from __future__ import annotations

import os
from datetime import datetime

import numpy as np
import polars as pl

from src.config import (
    COVARIANCE_WINDOW, MAX_MCR_THRESHOLD,
    MAX_PORTFOLIO_VOL, CASH_BUFFER,
)
from src.core.duckdb_store import get_parquet_path, PARQUET_DIR


def compute_mcr(
    weights: np.ndarray,
    cov_matrix: np.ndarray,
) -> tuple[np.ndarray, float]:
    """Compute Marginal Contribution to Risk for each asset.

    MCR_i = (Σw)_i / √(w^T Σ w)

    Returns:
        (mcr_array, portfolio_vol)
    """
    port_var = weights @ cov_matrix @ weights
    port_vol = np.sqrt(max(port_var, 1e-10))
    sigma_w = cov_matrix @ weights
    mcr = sigma_w / port_vol
    return mcr, port_vol


def iterative_mcr_scale(
    weights: np.ndarray,
    cov_matrix: np.ndarray,
    max_mcr: float = MAX_MCR_THRESHOLD,
    max_exposure: float = 1.0 - CASH_BUFFER,
    max_iterations: int = 50,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Iterative Risk scaling that bounds Relative Risk Contribution (RRC).

    This is the Level 4 fix for normalization bugs:
    - Analyzes Relative Risk Contribution (RRC = w * Marginal Vol / Total Vol)
    - Dynamically relaxes constraints if there are too few assets to physically enforce it
    - Scales down breaching assets
    - Re-allocates freed cash to non-breaching assets ONLY up to MCR limit
    - Repeats until convergence

    Returns:
        (adjusted_weights, mcr_values, iterations_used)
    """
    w = weights.copy()
    n = len(w)

    for iteration in range(max_iterations):
        mcr, port_vol = compute_mcr(w, cov_matrix)
        
        # Calculate Relative Risk Contribution per asset
        rrc = (w * mcr) / max(port_vol, 1e-10)

        # Dynamic limit based on active assets to prevent impossible convergence scenarios (e.g. N < 20)
        active_assets = np.sum(w > 1e-6)
        dynamic_limit = max(max_mcr, 1.25 / max(active_assets, 1))

        # Check for breaches
        breaching = rrc > dynamic_limit
        if not np.any(breaching):
            # Ensure total exposure <= max_exposure
            total = np.sum(w)
            if total > max_exposure:
                w *= max_exposure / max(total, 1e-10)
            break

        # Scale down ONLY breaching assets
        for j in range(n):
            if breaching[j] and w[j] > 0 and rrc[j] > 0:
                scale = dynamic_limit / rrc[j]
                w[j] *= min(scale, 1.0)

        # Calculate freed cash
        total_weight = np.sum(w)
        available_cash = max_exposure - total_weight

        if available_cash > 0:
            # Distribute freed cash to non-breaching positive-weight assets
            # But ONLY up to the point where they'd breach RRC limits
            non_breaching = (~breaching) & (w > 0)
            if np.any(non_breaching):
                n_safe = np.sum(non_breaching)
                increment = available_cash / n_safe

                # Apply incrementally and re-check
                for j in range(n):
                    if non_breaching[j]:
                        test_w = w.copy()
                        test_w[j] += increment
                        test_mcr, test_vol = compute_mcr(test_w, cov_matrix)
                        test_rrc = (test_w[j] * test_mcr[j]) / max(test_vol, 1e-10)
                        
                        if test_rrc <= dynamic_limit:
                            w[j] += increment

        # Ensure no accidental negative weights for long only parts (if it was somehow subtracted)
        w = np.maximum(w, 0)
        
        # Recalculate if it accidentally exploded
        total_weight = np.sum(w)
        if total_weight == 0:
            break

    # Final MCR for audit (just return raw MCR values, not RRC)
    mcr, _ = compute_mcr(w, cov_matrix)

    return w, mcr, iteration + 1


def apply_risk_constraints(
    weights_df: pl.DataFrame,
    market_df: pl.DataFrame | None = None,
    strategy_col: str = "raw_weight",
) -> pl.DataFrame:
    """Apply iterative risk constraints to a DataFrame of weights.

    Args:
        weights_df: Must have columns [entity_id, date, {strategy_col}]
        market_df: Market data with daily returns (loaded if None)
        strategy_col: Name of the raw weight column

    Returns:
        DataFrame with [entity_id, date, target_weight, mcr]
    """
    if market_df is None:
        market_df = pl.read_parquet(get_parquet_path("market_data"))

    # Process each date
    dates = weights_df.filter(pl.col(strategy_col) != 0)["date"].unique().sort().to_list()

    results = []
    audit_records = []

    for date in dates:
        day_weights = weights_df.filter(
            (pl.col("date") == date) & (pl.col(strategy_col) != 0)
        )

        if day_weights.is_empty():
            continue

        entity_ids = day_weights["entity_id"].to_list()
        raw_w = day_weights[strategy_col].to_numpy().astype(np.float64)

        # Normalize raw weights
        total_abs = np.sum(np.abs(raw_w))
        if total_abs > 0:
            raw_w = raw_w / total_abs * (1 - CASH_BUFFER)

        if len(entity_ids) < 2:
            # Can't compute covariance with < 2 assets
            for i, eid in enumerate(entity_ids):
                results.append({
                    "entity_id": eid, "date": date,
                    "target_weight": float(raw_w[i]), "mcr": 0.0,
                })
            continue

        # Get trailing returns for covariance
        import datetime as dt
        if isinstance(date, str):
            date_obj = dt.datetime.strptime(date, "%Y-%m-%d").date()
        else:
            date_obj = date

        lookback_days = int(COVARIANCE_WINDOW * 1.5)

        returns_data = market_df.filter(
            pl.col("entity_id").is_in(entity_ids)
            & (pl.col("date") <= date)
        ).sort(["entity_id", "date"])

        # Pivot to returns matrix
        pivot = returns_data.pivot(
            on="entity_id", index="date", values="daily_return"
        ).sort("date").drop_nulls()

        # Get only available entity columns
        available_ids = [eid for eid in entity_ids if str(eid) in pivot.columns]
        if len(available_ids) < 2:
            for i, eid in enumerate(entity_ids):
                results.append({
                    "entity_id": eid, "date": date,
                    "target_weight": float(raw_w[i]), "mcr": 0.0,
                })
            continue

        # Build returns matrix (last COVARIANCE_WINDOW days)
        ret_cols = [str(eid) for eid in available_ids]
        ret_matrix = pivot.select(ret_cols).tail(COVARIANCE_WINDOW).to_numpy()

        if ret_matrix.shape[0] < 30:
            for i, eid in enumerate(entity_ids):
                results.append({
                    "entity_id": eid, "date": date,
                    "target_weight": float(raw_w[i]), "mcr": 0.0,
                })
            continue

        # Annualized covariance
        cov = np.cov(ret_matrix, rowvar=False) * 252

        # Align weights to available entities
        w_aligned = np.array([
            raw_w[entity_ids.index(eid)] if eid in entity_ids else 0.0
            for eid in available_ids
        ])

        # Run iterative MCR scaling
        adj_w, mcr_values, iters = iterative_mcr_scale(w_aligned, cov)

        # Portfolio vol check
        port_var = adj_w @ cov @ adj_w
        port_vol = np.sqrt(max(port_var, 1e-10))

        if port_vol > MAX_PORTFOLIO_VOL and port_vol > 0:
            adj_w *= MAX_PORTFOLIO_VOL / port_vol
            mcr_values, _ = compute_mcr(adj_w, cov)

        # Record results
        for j, eid in enumerate(available_ids):
            results.append({
                "entity_id": eid, "date": date,
                "target_weight": float(adj_w[j]),
                "mcr": float(mcr_values[j]),
            })

        # Audit record
        audit_records.append({
            "date": date,
            "portfolio_vol": float(port_vol),
            "iterations": iters,
            "max_mcr": float(np.max(np.abs(mcr_values))),
            "total_weight": float(np.sum(adj_w)),
            "n_assets": len(available_ids),
        })

    if not results:
        return pl.DataFrame(schema={
            "entity_id": pl.Int32, "date": pl.Date,
            "target_weight": pl.Float32, "mcr": pl.Float32,
        })

    result_df = pl.DataFrame(results)
    result_df = result_df.with_columns([
        pl.col("entity_id").cast(pl.Int32),
        pl.col("target_weight").cast(pl.Float32),
        pl.col("mcr").cast(pl.Float32),
    ])

    # Write audit
    if audit_records:
        audit_df = pl.DataFrame(audit_records)
        audit_path = get_parquet_path("risk_audit")
        audit_df.write_parquet(audit_path)

    return result_df
