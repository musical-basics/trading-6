"""
internal_ledger.py — Level 5 Internal Fractional Ledger

After Net-Delta aggregation produces a single bulk order and it fills,
this module distributes the fill back to each sub-portfolio proportionally.

Each sub-portfolio's internal_shares column is updated in the database,
keeping the fund-level inventory consistent without multiple broker orders.
"""

from __future__ import annotations

from typing import Optional
import logging

logger = logging.getLogger(__name__)


def apply_internal_fills(
    intents: list[dict],
    net_fills: dict[str, dict],
) -> list[dict]:
    """
    Distribute Net-Delta fills back to sub-portfolios.

    Args:
        intents: Original per-portfolio intents
            [{ticker, side, quantity, portfolio_id, trader_id}, ...]
        net_fills: Actual fills from the broker
            {ticker: {filled_qty, avg_price, order_id}}

    Returns:
        List of internal execution records for each sub-portfolio.
    """
    if not intents or not net_fills:
        return []

    executions = []

    # Group intents by ticker
    ticker_intents: dict[str, list[dict]] = {}
    for intent in intents:
        ticker = intent["ticker"]
        if ticker not in ticker_intents:
            ticker_intents[ticker] = []
        ticker_intents[ticker].append(intent)

    for ticker, fill in net_fills.items():
        if ticker not in ticker_intents:
            continue

        sub_intents = ticker_intents[ticker]
        total_intent_qty = sum(i["quantity"] for i in sub_intents)

        if total_intent_qty == 0:
            continue

        for intent in sub_intents:
            fraction = intent["quantity"] / total_intent_qty
            allocated_qty = round(fill["filled_qty"] * fraction, 4)

            exec_record = {
                "portfolio_id": intent["portfolio_id"],
                "trader_id": intent.get("trader_id"),
                "ticker": ticker,
                "side": intent["side"],
                "allocated_qty": allocated_qty,
                "avg_price": fill.get("avg_price", 0),
                "broker_order_id": fill.get("order_id"),
                "fill_fraction": round(fraction, 6),
            }
            executions.append(exec_record)

            logger.info(
                f"  📒 Internal fill: Portfolio #{intent['portfolio_id']} "
                f"→ {intent['side']} {allocated_qty:.2f} {ticker} "
                f"@ ${fill.get('avg_price', 0):.2f} ({fraction:.1%} of bulk)"
            )

    # Persist to database
    _save_ledger_entries(executions)

    return executions


def _save_ledger_entries(entries: list[dict]) -> None:
    """Persist internal ledger entries to Postgres/SQLite."""
    if not entries:
        return

    try:
        from src.core.database import SessionLocal
        from src.core.models import PaperExecution

        session = SessionLocal()
        for entry in entries:
            session.add(PaperExecution(
                ticker=entry["ticker"],
                action=entry["side"],
                quantity=int(entry["allocated_qty"]),
                simulated_price=entry["avg_price"],
                strategy_id="net_delta",
                trader_id=entry.get("trader_id"),
                portfolio_id=entry.get("portfolio_id"),
            ))
        session.commit()
        session.close()
        logger.info(f"  ✅ {len(entries)} internal ledger entries saved.")
    except Exception as e:
        logger.error(f"  ⚠ Failed to save ledger entries: {e}")
