"""
logging.py — Structured logging and exception handling for the app.

All modules should use get_logger() instead of the standard logging module.
This centralizes exception context, JSON serialization, and observability.
"""

import logging
import sys
from typing import Any, Dict, Optional

# Structured logging with JSON output for production
try:
    from pythonjsonlogger import jsonlogger
    HAS_JSON_LOGGER = True
except ImportError:
    HAS_JSON_LOGGER = False


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger with structured output."""
    logger = logging.getLogger(name)
    
    if logger.hasHandlers():
        return logger  # Already configured
    
    logger.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    
    # Format: either JSON or human-readable
    if HAS_JSON_LOGGER:
        formatter = jsonlogger.JsonFormatter(
            fmt='%(timestamp)s %(level)s %(name)s %(message)s'
        )
    else:
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


class StructuredException(Exception):
    """Base exception with structured context."""
    
    def __init__(
        self,
        message: str,
        error_code: str = "UNKNOWN",
        context: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.context = context or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dict for logging."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context,
        }


class DataIngestionError(StructuredException):
    """Raised when data ingestion fails."""
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(message, "DATA_INGESTION_ERROR", context)


class BacktestError(StructuredException):
    """Raised when backtest execution fails."""
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(message, "BACKTEST_ERROR", context)


class RiskError(StructuredException):
    """Raised when risk calculations fail."""
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(message, "RISK_ERROR", context)


class StrategyError(StructuredException):
    """Raised when strategy execution fails."""
    def __init__(self, message: str, context: Optional[Dict] = None):
        super().__init__(message, "STRATEGY_ERROR", context)


def safe_execute(
    func,
    *args,
    error_class: type = StructuredException,
    context: Optional[Dict] = None,
    **kwargs
) -> Any:
    """Execute a function with structured exception handling.
    
    Usage:
        result = safe_execute(
            some_func,
            arg1, arg2,
            error_class=BacktestError,
            context={"experiment_id": "abc123"}
        )
    """
    logger = get_logger(__name__)
    
    try:
        return func(*args, **kwargs)
    except StructuredException:
        raise  # Re-raise if already structured
    except Exception as e:
        ctx = context or {}
        logger.error(
            f"{error_class.__name__}: {str(e)}",
            extra=ctx
        )
        raise error_class(
            message=str(e),
            context={**ctx, "original_type": type(e).__name__}
        )
