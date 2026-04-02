"""
sandbox_executor.py — Restricted execution environment for LLM-generated strategy code.

Runs generated Polars expressions in a sandboxed scope with only `pl` and `np`
available. Validates output shape and column presence.

Level 5: Full traceback capture for self-healing reflection loop.
"""

import re
import traceback
from typing import Optional

import polars as pl
import numpy as np


# Banned patterns in generated code (safety guardrails)
BANNED_PATTERNS = [
    r"\bimport\s+(?!polars|numpy)",   # No imports beyond pl/np
    r"\b__import__\b",
    r"\bexec\b",
    r"\beval\b",
    r"\bopen\s*\(",
    r"\bos\.",
    r"\bsubprocess\b",
    r"\bsys\.",
    r"\bshutil\b",
    r"\bpathlib\b",
    r"\brequests\b",
    r"\burllib\b",
    r"\bsocket\b",
    r"\bwrite_parquet\b",
    r"\bto_csv\b",
    r"\bto_pandas\b",
    r"\bglobals\b",
    r"\blocals\b",
    r"\bgetattr\b",
    r"\bsetattr\b",
    r"\bdelattr\b",
]


def validate_code(code: str) -> tuple[bool, Optional[str]]:
    """Check generated code for dangerous patterns.

    Returns:
        (is_valid, error_message)
    """
    if not code or not code.strip():
        return False, "Empty code"

    for pattern in BANNED_PATTERNS:
        match = re.search(pattern, code)
        if match:
            return False, f"Banned pattern detected: '{match.group()}'"

    # Must contain a function definition
    if "def " not in code:
        return False, "No function definition found"

    # Must produce a raw_weight column
    if "raw_weight_" not in code:
        return False, "Must produce a 'raw_weight_*' column"

    return True, None


def execute_strategy(
    code: str,
    data: pl.DataFrame,
) -> tuple[Optional[pl.DataFrame], Optional[str]]:
    """Execute a strategy in a restricted sandbox.

    Args:
        code: Python code defining a strategy function
        data: Input DataFrame with market/feature data

    Returns:
        (result_df, error_message) — one will be None
    """
    # Validate first
    is_valid, error = validate_code(code)
    if not is_valid:
        return None, f"Validation failed: {error}"

    def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "polars":
            return pl
        if name == "numpy":
            return np
        raise ImportError(f"Import of {name} is prohibited in sandbox")

    # Build restricted execution scope
    sandbox_globals = {
        "pl": pl,
        "np": np,
        "__builtins__": {
            "__import__": safe_import,
            "range": range,
            "len": len,
            "max": max,
            "min": min,
            "abs": abs,
            "round": round,
            "float": float,
            "int": int,
            "str": str,
            "bool": bool,
            "list": list,
            "dict": dict,
            "tuple": tuple,
            "None": None,
            "True": True,
            "False": False,
            "print": lambda *a, **k: None,  # Silenced
        },
    }
    sandbox_locals = {}

    try:
        # Execute the function definition
        exec(code, sandbox_globals, sandbox_locals)
    except Exception as e:
        return None, f"Code compilation error: {type(e).__name__}: {e}"

    # Find the strategy function
    fn = None
    for name, obj in sandbox_locals.items():
        if callable(obj) and name.startswith(("strategy_", "")) and not name.startswith("_"):
            fn = obj
            break

    if fn is None:
        return None, "No callable strategy function found in generated code"

    try:
        result = fn(data)
    except Exception as e:
        tb = traceback.format_exc()
        return None, f"Runtime error: {type(e).__name__}: {e}\n\nTraceback:\n{tb}"

    # Validate output
    if not isinstance(result, pl.DataFrame):
        return None, f"Strategy must return a pl.DataFrame, got {type(result).__name__}"

    weight_cols = [c for c in result.columns if c.startswith("raw_weight_")]
    if not weight_cols:
        return None, "Output DataFrame missing 'raw_weight_*' column"

    # Safety net: auto-cast any unsigned int columns to Float64
    # (rank() returns u32 which can't be negated/subtracted — causes crashes)
    cast_exprs = []
    for col_name in result.columns:
        dtype = result[col_name].dtype
        if dtype in (pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            cast_exprs.append(pl.col(col_name).cast(pl.Float64))
    if cast_exprs:
        result = result.with_columns(cast_exprs)

    # Safety net: replace NaN/Inf in weight columns (division by ~0 creates Inf)
    weight_col = weight_cols[0]
    result = result.with_columns(
        pl.when(pl.col(weight_col).is_nan() | pl.col(weight_col).is_infinite())
        .then(0.0)
        .otherwise(pl.col(weight_col))
        .alias(weight_col)
    )

    return result, None
