"""
net_delta.py — Level 5 Net-Delta Aggregation Engine

Aggregates all sub-portfolio BUY/SELL intents into a single net order
per ticker to minimize broker commissions and bid/ask spread.

Example:
  Portfolio A: BUY 100 AAPL  
  Portfolio B: SELL 50 AAPL  
  Net Delta:   BUY 50 AAPL (single bulk order)
"""

from __future__ import annotations

import polars as pl


def calculate_net_delta(intents: list[dict]) -> pl.DataFrame:
    """
    Aggregate all sub-portfolio intents into net ticker-level orders.

    Args:
        intents: List of dicts with keys:
            - ticker, side (BUY/SELL), quantity, portfolio_id, trader_id

    Returns:
        Polars DataFrame with columns: ticker, net_quantity, net_side
        (filtered to exclude HOLD / zero-net tickers)
    """
    if not intents:
        return pl.DataFrame(schema={
            "ticker": pl.Utf8,
            "net_quantity": pl.Float64,
            "net_side": pl.Utf8,
        })

    df = pl.DataFrame(intents)

    # Convert SELL quantities to negative
    df = df.with_columns(
        pl.when(pl.col("side") == "SELL")
        .then(-pl.col("quantity").cast(pl.Float64))
        .otherwise(pl.col("quantity").cast(pl.Float64))
        .alias("signed_quantity")
    )

    # Group by ticker → sum quantities
    net = (
        df.group_by("ticker")
        .agg(pl.col("signed_quantity").sum().alias("net_quantity"))
        .with_columns(
            pl.when(pl.col("net_quantity") > 0)
            .then(pl.lit("BUY"))
            .when(pl.col("net_quantity") < 0)
            .then(pl.lit("SELL"))
            .otherwise(pl.lit("HOLD"))
            .alias("net_side")
        )
        .filter(pl.col("net_side") != "HOLD")
        .with_columns(pl.col("net_quantity").abs())
    )

    return net


def distribute_fills(
    intents: list[dict],
    net_fills: dict[str, dict],
) -> list[dict]:
    """
    After the master bulk order fills, distribute the shares
    proportionally back to each sub-portfolio.

    Args:
        intents: Original sub-portfolio intents
        net_fills: Dict of {ticker: {filled_qty, avg_price, order_id}}

    Returns:
        List of execution records for each sub-portfolio
    """
    if not intents or not net_fills:
        return []

    df = pl.DataFrame(intents)
    executions = []

    for ticker, fill in net_fills.items():
        ticker_intents = df.filter(pl.col("ticker") == ticker)
        total_intent = ticker_intents["quantity"].sum()

        if total_intent == 0:
            continue

        for row in ticker_intents.iter_rows(named=True):
            fraction = row["quantity"] / total_intent
            allocated_qty = fill["filled_qty"] * fraction

            executions.append({
                "portfolio_id": row["portfolio_id"],
                "trader_id": row.get("trader_id"),
                "strategy_id": row.get("strategy_id"),
                "ticker": ticker,
                "side": row["side"],
                "quantity": round(allocated_qty, 4),
                "price": fill["avg_price"],
                "order_id": fill.get("order_id"),
            })

    return executions
