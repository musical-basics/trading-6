"""
squeeze_filter.py — Level 3 Phase 4: Squeeze / Widowmaker Defense

Hardcoded heuristic override that sits between target_portfolio and
the final execution delta. Catches dangerous short positions that
XGBoost might recommend.

Rules:
  - If target_weight < 0 (short) AND VIX > 30: override to 0.0
  - If target_weight < 0 AND 10-day momentum > 20%: truncate to 0.0

Note: Level 3 is long-only (XGBoost assigns positive weights to top N).
This module is scaffolding for Level 4's long-short strategies.
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import (
    DB_PATH, VIX_EXTREME_THRESHOLD,
    MOMENTUM_SQUEEZE_PCT, MOMENTUM_LOOKBACK
)


def apply_squeeze_filter():
    """
    Check target_portfolio for short positions and apply squeeze defense.
    In Level 3, this is mostly a pass-through since we're long-only.
    """
    print("=" * 60)
    print("PHASE 4: Squeeze Filter (Bouncer Defense)")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # Load target portfolio
    portfolio_df = pd.read_sql_query(
        "SELECT ticker, date, target_weight, mcr FROM target_portfolio",
        conn,
        parse_dates=["date"],
    )

    if portfolio_df.empty:
        print("  ⚠ No target portfolio entries. Run Phase 3b first.")
        conn.close()
        return

    # Check for short positions
    short_positions = portfolio_df[portfolio_df["target_weight"] < 0]

    if short_positions.empty:
        print(f"  ✓ No short positions detected. Portfolio is long-only.")
        print(f"  ✓ {len(portfolio_df):,} entries passed through unmodified.")
        print()
        conn.close()
        return

    # ── Apply squeeze rules ──────────────────────────────────
    print(f"  ⚠ {len(short_positions)} short positions detected. Applying filters...")

    # Load latest VIX
    vix_df = pd.read_sql_query(
        "SELECT date, vix_close FROM macro_factors ORDER BY date DESC LIMIT 1",
        conn,
    )
    latest_vix = vix_df["vix_close"].iloc[0] if not vix_df.empty else 0

    # Load prices for momentum check
    prices_df = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn,
        parse_dates=["date"],
    )

    kills = 0

    for idx, row in short_positions.iterrows():
        ticker = row["ticker"]
        kill_reason = None

        # Rule 1: VIX extreme
        if latest_vix > VIX_EXTREME_THRESHOLD:
            kill_reason = f"VIX={latest_vix:.1f} > {VIX_EXTREME_THRESHOLD}"

        # Rule 2: Momentum squeeze
        if kill_reason is None:
            ticker_prices = prices_df[prices_df["ticker"] == ticker].tail(MOMENTUM_LOOKBACK + 1)
            if len(ticker_prices) > MOMENTUM_LOOKBACK:
                momentum = (
                    ticker_prices["adj_close"].iloc[-1] / ticker_prices["adj_close"].iloc[0]
                ) - 1
                if momentum > MOMENTUM_SQUEEZE_PCT:
                    kill_reason = f"10d momentum={momentum:.1%} > {MOMENTUM_SQUEEZE_PCT:.0%}"

        if kill_reason:
            portfolio_df.loc[idx, "target_weight"] = 0.0
            kills += 1
            print(f"    ✗ {ticker}: SHORT killed ({kill_reason})")

    # Update DB if any were killed
    if kills > 0:
        cursor = conn.cursor()
        for _, row in portfolio_df.iterrows():
            cursor.execute(
                """UPDATE target_portfolio SET target_weight = ?
                   WHERE ticker = ? AND date = ?""",
                (row["target_weight"], row["ticker"],
                 row["date"].strftime("%Y-%m-%d"))
            )
        conn.commit()
        print(f"\n  ✓ {kills} short positions killed by Bouncer")
    else:
        print("  ✓ All short positions passed Bouncer checks")

    print(f"  ✓ {len(portfolio_df):,} entries processed")
    print()

    conn.close()


if __name__ == "__main__":
    apply_squeeze_filter()
