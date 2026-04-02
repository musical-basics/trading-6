"""
cache_manifest.py — Parquet cache invalidation via file hash tracking.

Tracks the hash of each component Parquet file. If unchanged,
reuse cached DataFrame instead of re-reading from disk.

Usage:
    manifest = CacheManifest()
    if manifest.is_valid("market_data"):
        df = manifest.load_cached("market_data")
    else:
        df = pl.read_parquet(get_parquet_path("market_data"))
        manifest.save_cached("market_data", df)
"""

import hashlib
import json
import os
from pathlib import Path
from typing import Optional, Dict, Any

import polars as pl

from src.core.duckdb_store import get_parquet_path, PARQUET_DIR


MANIFEST_PATH = os.path.join(PARQUET_DIR, "_cache_manifest.json")


def _compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of a file for change detection."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        return ""


class CacheManifest:
    """Track Parquet file hashes and manage cached DataFrames in memory."""
    
    def __init__(self):
        self.manifest: Dict[str, Dict[str, Any]] = {}
        self.cached_dfs: Dict[str, pl.DataFrame] = {}
        self._load_manifest()
    
    def _load_manifest(self) -> None:
        """Load manifest from disk if it exists."""
        if os.path.exists(MANIFEST_PATH):
            try:
                with open(MANIFEST_PATH, "r") as f:
                    self.manifest = json.load(f)
            except Exception:
                self.manifest = {}
    
    def _save_manifest(self) -> None:
        """Persist manifest to disk."""
        os.makedirs(PARQUET_DIR, exist_ok=True)
        try:
            with open(MANIFEST_PATH, "w") as f:
                json.dump(self.manifest, f, indent=2)
        except Exception:
            pass  # Non-critical; don't block on manifest write
    
    def is_valid(self, component_name: str) -> bool:
        """Check if cached DataFrame is valid (file hash unchanged)."""
        if component_name not in self.cached_dfs:
            return False
        
        if component_name not in self.manifest:
            return False
        
        file_path = get_parquet_path(component_name)
        if not os.path.exists(file_path):
            return False
        
        current_hash = _compute_file_hash(file_path)
        stored_hash = self.manifest[component_name].get("file_hash", "")
        
        return current_hash == stored_hash and current_hash != ""
    
    def load_cached(self, component_name: str) -> Optional[pl.DataFrame]:
        """Load cached DataFrame if valid."""
        if self.is_valid(component_name):
            return self.cached_dfs.get(component_name)
        return None
    
    def save_cached(self, component_name: str, df: pl.DataFrame) -> None:
        """Cache a DataFrame and update manifest."""
        file_path = get_parquet_path(component_name)
        file_hash = _compute_file_hash(file_path)
        
        self.cached_dfs[component_name] = df
        self.manifest[component_name] = {
            "file_hash": file_hash,
            "shape": (len(df), len(df.columns)),
            "timestamp": str(Path(file_path).stat().st_mtime),
        }
        self._save_manifest()
    
    def invalidate(self, component_name: str) -> None:
        """Manually invalidate a cached component."""
        if component_name in self.cached_dfs:
            del self.cached_dfs[component_name]
        if component_name in self.manifest:
            del self.manifest[component_name]
        self._save_manifest()
    
    def invalidate_all(self) -> None:
        """Clear all caches."""
        self.cached_dfs.clear()
        self.manifest.clear()
        if os.path.exists(MANIFEST_PATH):
            os.remove(MANIFEST_PATH)


# Global instance
_global_manifest: Optional[CacheManifest] = None


def get_manifest() -> CacheManifest:
    """Get or create the global manifest."""
    global _global_manifest
    if _global_manifest is None:
        _global_manifest = CacheManifest()
    return _global_manifest
