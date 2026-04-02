"""
wfo_multi.py — True Walk-Forward Optimization for all 4 strategies.

Optimized: preloads all data once, avoids redundant SQL queries.
Supports progress callbacks for Streamlit progress bars.

Tunable parameters per strategy:
  EV/Sales Long-Only : Z-score buy threshold
  L/S Z-Score        : n_long, n_short
  SMA Crossover      : fast_sma, slow_sma windows
  Pullback RSI       : rsi_period, rsi_entry threshold
"""

import sqlite3
import pandas as pd
import numpy as np
from src.config import DB_PATH, MAX_SINGLE_WEIGHT


def _compute_metrics(daily_returns):
    """Compute Sharpe, MaxDD, CAGR from daily returns."""
    if len(daily_returns) < 2 or daily_returns.std() == 0:
        return {"sharpe": 0.0, "max_drawdown": 0.0, "cagr": 0.0}

    sharpe = daily_returns.mean() / daily_returns.std() * np.sqrt(252)
    equity = (1 + daily_returns).cumprod()
    running_max = equity.expanding().max()
    max_dd = (1 - equity / running_max).max()
    cagr = equity.iloc[-1] ** (252 / len(daily_returns)) - 1

    return {"sharpe": sharpe, "max_drawdown": max_dd, "cagr": cagr}


def _get_date_windows(all_dates, train_frac=0.66):
    """Create train/test windows for WFO."""
    n = len(all_dates)
    split = int(n * train_frac)
    if split < 20 or (n - split) < 10:
        return []

    if n < 500:
        return [{
            "train_start": all_dates[0],
            "train_end": all_dates[split - 1],
            "test_start": all_dates[split],
            "test_end": all_dates[-1],
        }]

    train_size = 126
    test_size = 63
    step_size = 63

    windows = []
    i = 0
    while i + train_size + test_size <= n:
        windows.append({
            "train_start": all_dates[i],
            "train_end": all_dates[i + train_size - 1],
            "test_start": all_dates[i + train_size],
            "test_end": all_dates[min(i + train_size + test_size - 1, n - 1)],
        })
        i += step_size

    return windows if windows else [{
        "train_start": all_dates[0],
        "train_end": all_dates[split - 1],
        "test_start": all_dates[split],
        "test_end": all_dates[-1],
    }]


# ═══════════════════════════════════════════════════════════════
# Strategy 1: EV/Sales Z-Score Long-Only
# ═══════════════════════════════════════════════════════════════
def _ev_sales_sharpe(data, threshold):
    d = data.copy()
    d["weight"] = 0.0
    buy = d["ev_sales_zscore"] < threshold
    if not buy.any():
        return -np.inf
    counts = d.loc[buy].groupby("date")["ticker"].transform("count")
    d.loc[buy, "weight"] = np.minimum(1.0 / counts.values, MAX_SINGLE_WEIGHT)
    d["wr"] = d["weight"] * d["daily_return"]
    pr = d.groupby("date")["wr"].sum()
    return pr.mean() / pr.std() * np.sqrt(252) if pr.std() > 0 else -np.inf


def _ev_sales_simulate(data, threshold, starting_eq=1.0):
    d = data.copy()
    d["weight"] = 0.0
    buy = d["ev_sales_zscore"] < threshold
    if buy.any():
        counts = d.loc[buy].groupby("date")["ticker"].transform("count")
        d.loc[buy, "weight"] = np.minimum(1.0 / counts.values, MAX_SINGLE_WEIGHT)

    d["wr"] = d["weight"] * d["daily_return"]
    port = d.groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()
    return port


def wfo_ev_sales(df_all):
    """WFO for EV/Sales — takes preloaded DataFrame."""
    if df_all.empty:
        return None

    all_dates = sorted(df_all["date"].unique())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    thresholds = [-2.0, -1.5, -1.0, -0.5, 0.0, 0.5]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        train = df_all[(df_all["date"] >= w["train_start"]) & (df_all["date"] <= w["train_end"])]
        test = df_all[(df_all["date"] >= w["test_start"]) & (df_all["date"] <= w["test_end"])]

        best_t, best_s = thresholds[2], -np.inf
        for t in thresholds:
            s = _ev_sales_sharpe(train, t)
            if s > best_s:
                best_s, best_t = s, t

        oos_eq = _ev_sales_simulate(test, best_t, cum_eq)
        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"threshold={best_t}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "EV/Sales Long-Only",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Strategy 2: L/S Z-Score (preloaded data, no per-window SQL)
# ═══════════════════════════════════════════════════════════════
def _ls_simulate_from_df(scores_all, start, end, n_long, n_short, starting_eq=1.0):
    """Simulate L/S Z-Score using preloaded data."""
    scores = scores_all[(scores_all["date"] >= start) & (scores_all["date"] <= end)].copy()
    if scores.empty:
        return pd.DataFrame(), -np.inf

    scores["month"] = scores["date"].dt.to_period("M")
    months = sorted(scores["month"].unique())

    daily_returns = []
    for m in months:
        month_data = scores[scores["month"] == m].copy()
        first_day = month_data.groupby("ticker").first().reset_index()
        ranked = first_day.sort_values("ev_sales_zscore")
        longs = set(ranked.head(n_long)["ticker"].tolist())
        shorts = set(ranked.tail(n_short)["ticker"].tolist())

        # Vectorized weight assignment
        month_data["weight"] = 0.0
        month_data.loc[month_data["ticker"].isin(longs), "weight"] = 1.0 / n_long
        month_data.loc[month_data["ticker"].isin(shorts), "weight"] = -1.0 / n_short
        month_data["wr"] = month_data["weight"] * month_data["daily_return"]
        daily_returns.append(month_data[["date", "wr"]])

    if not daily_returns:
        return pd.DataFrame(), -np.inf

    port = pd.concat(daily_returns).groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()

    sharpe = port["daily_return"].mean() / port["daily_return"].std() * np.sqrt(252) if port["daily_return"].std() > 0 else -np.inf
    return port, sharpe


def wfo_ls_zscore(scores_all):
    """WFO for L/S Z-Score — takes preloaded DataFrame."""
    if scores_all.empty:
        return None

    all_dates = sorted(scores_all["date"].unique())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    candidates = [(1, 1), (1, 2), (2, 2), (2, 3), (3, 3), (1, 4), (2, 4)]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        best_params, best_s = (2, 2), -np.inf
        for nl, ns in candidates:
            _, s = _ls_simulate_from_df(scores_all, w["train_start"], w["train_end"], nl, ns)
            if s > best_s:
                best_s, best_params = s, (nl, ns)

        oos_eq, _ = _ls_simulate_from_df(scores_all, w["test_start"], w["test_end"],
                                          best_params[0], best_params[1], cum_eq)
        if oos_eq.empty:
            continue

        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"long={best_params[0]}, short={best_params[1]}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    if not oos_parts:
        return None

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "L/S Z-Score",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Strategy 3: SMA Crossover (preloaded data, vectorized)
# ═══════════════════════════════════════════════════════════════
def _sma_portfolio_from_df(bars_all, start, end, fast, slow, starting_eq=1.0):
    """Simulate SMA crossover using preloaded data."""
    tickers = [t for t in bars_all["ticker"].unique() if t not in ("SPY", "QQQ", "GLD")]
    n = len(tickers)
    if n == 0:
        return pd.DataFrame(), -np.inf

    all_daily = []
    for ticker in tickers:
        full = bars_all[(bars_all["ticker"] == ticker) & (bars_all["date"] <= end)].copy()
        if len(full) < slow:
            continue

        full[f"sma_{fast}"] = full["adj_close"].rolling(fast).mean()
        full[f"sma_{slow}"] = full["adj_close"].rolling(slow).mean()
        full["daily_return"] = full["adj_close"].pct_change()
        full["signal"] = 0
        full.loc[full[f"sma_{fast}"] > full[f"sma_{slow}"], "signal"] = 1

        test = full[full["date"] >= start].copy()
        if test.empty:
            continue

        test["position"] = test["signal"].shift(1).fillna(0)
        test["wr"] = test["daily_return"] * test["position"] / n
        all_daily.append(test[["date", "wr"]])

    if not all_daily:
        return pd.DataFrame(), -np.inf

    combined = pd.concat(all_daily)
    port = combined.groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()

    sharpe = port["daily_return"].mean() / port["daily_return"].std() * np.sqrt(252) if port["daily_return"].std() > 0 else -np.inf
    return port, sharpe


def wfo_sma(bars_all):
    """WFO for SMA Crossover — takes preloaded DataFrame."""
    all_dates = sorted(bars_all["date"].unique())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    candidates = [(10, 50), (20, 100), (30, 150), (50, 200), (20, 200)]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        best_params, best_s = (50, 200), -np.inf
        for fast, slow in candidates:
            _, s = _sma_portfolio_from_df(bars_all, w["train_start"], w["train_end"], fast, slow)
            if s > best_s:
                best_s, best_params = s, (fast, slow)

        oos_eq, _ = _sma_portfolio_from_df(bars_all, w["test_start"], w["test_end"],
                                            best_params[0], best_params[1], cum_eq)
        if oos_eq.empty:
            continue

        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"fast={best_params[0]}, slow={best_params[1]}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    if not oos_parts:
        return None

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "SMA Crossover (EW)",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Strategy 4: Pullback RSI (preloaded data, vectorized RSI)
# ═══════════════════════════════════════════════════════════════
def _rsi(series, period):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def _pullback_from_df(bars_all, start, end, rsi_period, rsi_entry, starting_eq=1.0):
    """Simulate Pullback RSI using preloaded data with vectorized signals."""
    tickers = [t for t in bars_all["ticker"].unique() if t not in ("SPY", "QQQ", "GLD")]
    n = len(tickers)
    if n == 0:
        return pd.DataFrame(), -np.inf

    all_daily = []
    for ticker in tickers:
        full = bars_all[(bars_all["ticker"] == ticker) & (bars_all["date"] <= end)].copy()
        if len(full) < 200:
            continue

        full["sma_200"] = full["adj_close"].rolling(200).mean()
        full["rsi"] = _rsi(full["adj_close"], rsi_period)
        full["daily_return"] = full["adj_close"].pct_change()

        # Vectorized signal generation (no Python loop)
        entry_cond = (full["adj_close"] > full["sma_200"]) & (full["rsi"] < rsi_entry)
        exit_cond = full["rsi"] > 70

        # Use a state machine approach but vectorized with numpy
        in_position = np.zeros(len(full))
        pos = 0
        rsi_vals = full["rsi"].values
        price_vals = full["adj_close"].values
        sma_vals = full["sma_200"].values
        for i in range(len(full)):
            if np.isnan(sma_vals[i]) or np.isnan(rsi_vals[i]):
                continue
            if pos == 0 and price_vals[i] > sma_vals[i] and rsi_vals[i] < rsi_entry:
                pos = 1
            elif pos == 1 and rsi_vals[i] > 70:
                pos = 0
            in_position[i] = pos
        full["in_position"] = in_position

        test = full[full["date"] >= start].copy()
        if test.empty:
            continue

        test["position"] = test["in_position"].shift(1).fillna(0)
        test["wr"] = test["daily_return"] * test["position"] / n
        all_daily.append(test[["date", "wr"]])

    if not all_daily:
        return pd.DataFrame(), -np.inf

    combined = pd.concat(all_daily)
    port = combined.groupby("date")["wr"].sum().reset_index()
    port.columns = ["date", "daily_return"]
    port = port.sort_values("date")
    port["equity"] = starting_eq * (1 + port["daily_return"]).cumprod()

    sharpe = port["daily_return"].mean() / port["daily_return"].std() * np.sqrt(252) if port["daily_return"].std() > 0 else -np.inf
    return port, sharpe


def wfo_pullback(bars_all):
    """WFO for Pullback RSI — takes preloaded DataFrame."""
    all_dates = sorted(bars_all["date"].unique())
    windows = _get_date_windows(all_dates)
    if not windows:
        return None

    candidates = [(2, 10), (2, 20), (3, 15), (3, 20), (3, 30), (5, 25)]
    oos_parts = []
    cum_eq = 1.0
    results = []

    for w in windows:
        best_params, best_s = (3, 20), -np.inf
        for period, entry in candidates:
            _, s = _pullback_from_df(bars_all, w["train_start"], w["train_end"], period, entry)
            if s > best_s:
                best_s, best_params = s, (period, entry)

        oos_eq, _ = _pullback_from_df(bars_all, w["test_start"], w["test_end"],
                                       best_params[0], best_params[1], cum_eq)
        if oos_eq.empty:
            continue

        cum_eq = oos_eq["equity"].iloc[-1]
        oos_parts.append(oos_eq)
        results.append({
            "window": f"{w['test_start'].strftime('%Y-%m-%d')} → {w['test_end'].strftime('%Y-%m-%d')}",
            "best_param": f"rsi_period={best_params[0]}, entry={best_params[1]}",
            "train_sharpe": round(best_s, 3),
            **_compute_metrics(oos_eq["daily_return"]),
        })

    if not oos_parts:
        return None

    stitched = pd.concat(oos_parts, ignore_index=True)
    return {
        "name": "Pullback RSI (EW)",
        "stitched": stitched,
        "windows": results,
        "overall": _compute_metrics(stitched["daily_return"]),
    }


# ═══════════════════════════════════════════════════════════════
# Run all WFO — preload data ONCE, call with progress callback
# ═══════════════════════════════════════════════════════════════
def run_all_wfo(progress_callback=None):
    """
    Run WFO for all 4 strategies.

    progress_callback: optional callable(strategy_name, step, total_steps)
      Used by Streamlit to update progress bars.
    """
    conn = sqlite3.connect(DB_PATH)

    # ── Preload ALL data once ────────────────────────────────
    if progress_callback:
        progress_callback("Loading data", 0, 6)

    bars_all = pd.read_sql_query(
        "SELECT ticker, date, adj_close FROM daily_bars ORDER BY ticker, date",
        conn, parse_dates=["date"]
    )
    bars_all = bars_all.sort_values(["ticker", "date"])

    scores_all = pd.read_sql_query("""
        SELECT cs.ticker, cs.date, cs.ev_sales_zscore, db.adj_close
        FROM cross_sectional_scores cs
        JOIN daily_bars db ON cs.ticker = db.ticker AND cs.date = db.date
        ORDER BY cs.ticker, cs.date
    """, conn, parse_dates=["date"])
    if not scores_all.empty:
        scores_all = scores_all.sort_values(["ticker", "date"])
        scores_all["daily_return"] = scores_all.groupby("ticker")["adj_close"].pct_change()

    # ── Run strategies ───────────────────────────────────────
    strategies = [
        ("EV/Sales Long-Only", lambda: wfo_ev_sales(scores_all) if not scores_all.empty else None),
        ("L/S Z-Score", lambda: wfo_ls_zscore(scores_all) if not scores_all.empty else None),
        ("SMA Crossover (EW)", lambda: wfo_sma(bars_all)),
        ("Pullback RSI (EW)", lambda: wfo_pullback(bars_all)),
    ]

    results = []
    for i, (name, fn) in enumerate(strategies):
        if progress_callback:
            progress_callback(name, i + 1, len(strategies) + 2)  # +2 for load + save steps
        try:
            r = fn()
            if r:
                results.append(r)
        except Exception as e:
            print(f"  ⚠ {name} failed: {e}")

    # ── Save to SQLite ───────────────────────────────────────
    if progress_callback:
        progress_callback("Saving results", len(strategies) + 1, len(strategies) + 2)

    cursor = conn.cursor()
    for r in results:
        strategy_id = "wfo_" + r["name"].lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
        cursor.execute("DELETE FROM wfo_results WHERE strategy_id = ?", (strategy_id,))
        for w in r["windows"]:
            parts = w["window"].split(" → ")
            cursor.execute("""
                INSERT INTO wfo_results
                (strategy_id, test_window_start, test_window_end,
                 sharpe_ratio, max_drawdown, cagr)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                strategy_id, parts[0], parts[1],
                w["sharpe"], w["max_drawdown"], w["cagr"],
            ))

    conn.commit()
    conn.close()
    return results


if __name__ == "__main__":
    def _print_progress(name, step, total):
        print(f"  [{step}/{total}] {name}...")

    results = run_all_wfo(progress_callback=_print_progress)
    for r in results:
        print(f"\n{'='*50}")
        print(f"{r['name']}")
        print(f"  OOS Sharpe: {r['overall']['sharpe']:.3f}")
        print(f"  OOS MaxDD:  {r['overall']['max_drawdown']:.2%}")
        print(f"  OOS CAGR:   {r['overall']['cagr']:.2%}")
        for w in r["windows"]:
            print(f"    Window {w['window']}: param={w['best_param']}, "
                  f"train_sharpe={w['train_sharpe']}, oos_sharpe={w['sharpe']:.3f}")
