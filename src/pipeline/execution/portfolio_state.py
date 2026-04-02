"""
portfolio_state.py — Level 2 Portfolio State Utility

Reconstructs the current portfolio from either:
  - Alpaca API (live mode): queries real account equity and positions
  - Paper executions (dry-run mode): reconstructs from paper_executions table

Provides a unified interface for the portfolio rebalancer.
"""

import sqlite3
import os
from src.config import DB_PATH


def get_portfolio_state():
    """
    Get current portfolio state (total equity and holdings).
    Auto-selects Alpaca API or paper-based reconstruction.

    Returns:
        total_equity (float): Total portfolio value
        holdings (dict): {ticker: {'shares': int, 'avg_price': float}}
    """
    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()

    if api_key and secret_key:
        return _get_portfolio_from_alpaca()
    else:
        return _get_portfolio_from_paper()


def _get_portfolio_from_alpaca():
    """Query Alpaca API for live portfolio state."""
    try:
        import alpaca_trade_api as tradeapi

        api_key = os.getenv("ALPACA_API_KEY", "").strip()
        secret_key = os.getenv("ALPACA_SECRET_KEY", "").strip()
        base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

        api = tradeapi.REST(api_key, secret_key, base_url, api_version="v2")
        account = api.get_account()
        total_equity = float(account.equity)

        positions = api.list_positions()
        holdings = {}
        for pos in positions:
            holdings[pos.symbol] = {
                "shares": int(pos.qty),
                "avg_price": float(pos.avg_entry_price),
            }

        return total_equity, holdings

    except Exception as e:
        print(f"  ⚠ Alpaca API failed: {e}. Falling back to paper state.")
        return _get_portfolio_from_paper()


def _get_portfolio_from_paper():
    """
    Reconstruct portfolio from paper_executions table.
    Uses a default starting equity of $100,000.
    """
    STARTING_EQUITY = 100_000.0

    conn = sqlite3.connect(DB_PATH)
    try:
        import pandas as pd

        executions = pd.read_sql_query("""
            SELECT ticker, action, quantity, simulated_price
            FROM paper_executions
            ORDER BY timestamp
        """, conn)

        holdings = {}
        cash_spent = 0.0

        if not executions.empty:
            for _, row in executions.iterrows():
                ticker = row["ticker"]
                qty = int(row["quantity"])
                price = float(row["simulated_price"])

                if ticker not in holdings:
                    holdings[ticker] = {"shares": 0, "avg_price": 0.0}

                if row["action"] == "BUY":
                    # Update average price
                    current = holdings[ticker]
                    total_cost = (current["shares"] * current["avg_price"]) + (qty * price)
                    total_shares = current["shares"] + qty
                    holdings[ticker]["shares"] = total_shares
                    holdings[ticker]["avg_price"] = total_cost / total_shares if total_shares > 0 else 0
                    cash_spent += qty * price

                elif row["action"] == "SELL":
                    holdings[ticker]["shares"] -= qty
                    cash_spent -= qty * price

            # Remove liquidated positions
            holdings = {t: h for t, h in holdings.items() if h["shares"] > 0}

        # Estimate current equity (starting - cash spent + current value of holdings)
        # For simplicity, use the most recent price from daily_bars
        holdings_value = 0.0
        for ticker, info in holdings.items():
            try:
                price_row = pd.read_sql_query(
                    "SELECT adj_close FROM daily_bars WHERE ticker = ? ORDER BY date DESC LIMIT 1",
                    conn, params=(ticker,)
                )
                if not price_row.empty:
                    holdings_value += info["shares"] * price_row["adj_close"].iloc[0]
                else:
                    holdings_value += info["shares"] * info["avg_price"]
            except Exception:
                holdings_value += info["shares"] * info["avg_price"]

        remaining_cash = STARTING_EQUITY - cash_spent
        total_equity = remaining_cash + holdings_value

        return total_equity, holdings

    finally:
        conn.close()


if __name__ == "__main__":
    equity, holdings = get_portfolio_state()
    print(f"Total equity: ${equity:,.2f}")
    print(f"Holdings: {holdings}")


# ── Portfolio-Scoped State ──────────────────────────────────────

def get_portfolio_state_by_id(portfolio_id: int):
    """Get equity + holdings for a specific sub-portfolio.

    Reconstructs holdings from paper_executions WHERE portfolio_id matches,
    using the portfolio's allocated_capital as the starting cash base.

    Returns:
        total_equity (float), holdings (dict)
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        import pandas as pd

        # Get allocated capital for this portfolio
        capital_row = conn.execute(
            "SELECT allocated_capital FROM portfolios WHERE id = ?",
            (portfolio_id,),
        ).fetchone()
        starting_equity = capital_row[0] if capital_row else 1000.0

        executions = pd.read_sql_query("""
            SELECT ticker, action, quantity, simulated_price, strategy_id
            FROM paper_executions
            WHERE portfolio_id = ?
            ORDER BY timestamp
        """, conn, params=(portfolio_id,))

        holdings = {}
        cash_spent = 0.0

        if not executions.empty:
            for _, row in executions.iterrows():
                ticker = row["ticker"]
                qty = int(row["quantity"])
                price = float(row["simulated_price"])
                strat_id = row.get("strategy_id")

                if ticker not in holdings:
                    holdings[ticker] = {"shares": 0, "avg_price": 0.0, "strategies": set()}

                if strat_id and isinstance(strat_id, str):
                    holdings[ticker]["strategies"].add(strat_id)

                if row["action"] == "BUY":
                    current = holdings[ticker]
                    total_cost = (current["shares"] * current["avg_price"]) + (qty * price)
                    total_shares = current["shares"] + qty
                    holdings[ticker]["shares"] = total_shares
                    holdings[ticker]["avg_price"] = (
                        total_cost / total_shares if total_shares > 0 else 0
                    )
                    cash_spent += qty * price
                elif row["action"] == "SELL":
                    holdings[ticker]["shares"] -= qty
                    cash_spent -= qty * price

            holdings = {t: h for t, h in holdings.items() if h["shares"] > 0}
            for t in holdings:
                holdings[t]["strategies"] = list(holdings[t]["strategies"])

        # Estimate current equity
        holdings_value = 0.0
        for ticker, info in holdings.items():
            try:
                price_row = pd.read_sql_query(
                    "SELECT adj_close FROM daily_bars WHERE ticker = ? "
                    "ORDER BY date DESC LIMIT 1",
                    conn, params=(ticker,),
                )
                if not price_row.empty:
                    holdings_value += info["shares"] * price_row["adj_close"].iloc[0]
                else:
                    holdings_value += info["shares"] * info["avg_price"]
            except Exception:
                holdings_value += info["shares"] * info["avg_price"]

        remaining_cash = starting_equity - cash_spent
        total_equity = remaining_cash + holdings_value

        return total_equity, holdings
    finally:
        conn.close()


def get_trader_state(trader_id: int):
    """Sum real-time equity of all sub-portfolios for a trader.

    Returns:
        total_equity (float), all_holdings (dict)
    """
    conn = sqlite3.connect(DB_PATH)
    portfolio_ids = [
        r[0] for r in conn.execute(
            "SELECT id FROM portfolios WHERE trader_id = ?", (trader_id,)
        ).fetchall()
    ]
    conn.close()

    total_equity = 0.0
    all_holdings = {}
    for pid in portfolio_ids:
        equity, holdings = get_portfolio_state_by_id(pid)
        total_equity += equity
        for ticker, info in holdings.items():
            if ticker in all_holdings:
                all_holdings[ticker]["shares"] += info["shares"]
                # Average price recalculation is omitted for trader state (not used downstream), 
                # but we need to merge strategies
                all_holdings[ticker]["strategies"] = list(set(
                    all_holdings[ticker].get("strategies", []) + info.get("strategies", [])
                ))
            else:
                all_holdings[ticker] = info.copy()

    return total_equity, all_holdings

