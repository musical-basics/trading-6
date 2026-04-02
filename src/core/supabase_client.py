"""
supabase_client.py — Supabase REST API Client

Provides a singleton Supabase client using the service_role key
for server-side CRUD operations on transactional data.

All transactional data (traders, portfolios, executions, experiments)
goes through this client — never via direct DB connections.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_supabase_client = None


def get_supabase():
    """
    Get or create a singleton Supabase client.

    Returns None if Supabase is not configured (local dev without keys).
    """
    global _supabase_client

    if _supabase_client is not None:
        return _supabase_client

    from src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.warning("⚠ Supabase not configured — SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing")
        return None

    try:
        from supabase import create_client
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        logger.info(f"✅ Connected to Supabase REST API: {SUPABASE_URL}")
        return _supabase_client
    except ImportError:
        logger.warning("⚠ supabase-py not installed. Run: pip install supabase")
        return None
    except Exception as e:
        logger.warning(f"⚠ Supabase client creation failed: {e}")
        return None


def is_supabase_available() -> bool:
    """Check if Supabase is configured and reachable."""
    return get_supabase() is not None
