"""
squeeze_monitor.py — Intraday VIX squeeze defense monitor.

Polls VIX every 5 minutes during market hours (9 AM–4 PM, Mon–Fri).
If VIX > 30, bypasses the EOD schedule and triggers emergency
liquidation of short positions.
"""

from src.core.celery_app import app
from src.config import VIX_EXTREME_THRESHOLD
import logging

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="src.tasks.squeeze_monitor.check_vix_squeeze",
)
def check_vix_squeeze(self):
    """
    High-frequency intraday monitor for VIX spikes.
    Triggers emergency short liquidation if VIX > threshold.
    """
    try:
        import yfinance as yf

        vix = yf.Ticker("^VIX")
        current_vix = vix.fast_info.get("lastPrice", 0)

        logger.info(f"📊 VIX Check: {current_vix:.2f}")

        if current_vix > VIX_EXTREME_THRESHOLD:
            logger.critical(
                f"🚨 VIX SQUEEZE ALERT: {current_vix:.2f} > {VIX_EXTREME_THRESHOLD}! "
                f"Triggering emergency short liquidation..."
            )

            # Import and execute squeeze filter
            try:
                from src.pipeline.execution.squeeze_filter import apply_squeeze_filter
                apply_squeeze_filter()
                logger.info("✅ Squeeze filter applied — dangerous shorts liquidated.")
            except Exception as e:
                logger.error(f"❌ Squeeze filter failed: {e}")

            return {
                "status": "squeeze_triggered",
                "vix": current_vix,
            }

        return {"status": "normal", "vix": current_vix}

    except Exception as exc:
        logger.error(f"❌ VIX check failed: {exc}")
        return {"status": "error", "error": str(exc)}
