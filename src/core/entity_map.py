"""
entity_map.py — Level 4 Entity-Component-System: Entity Map

Bidirectional mapping between ticker symbols (e.g. "AAPL") and
integer entity IDs for fast columnar operations. Entity IDs start at 1.

Usage:
    em = EntityMap()
    em.register(["AAPL", "MSFT", "GOOGL"])
    em.ticker_to_id("AAPL")   # → 1
    em.id_to_ticker(2)          # → "MSFT"
"""

from __future__ import annotations


class EntityMap:
    """Bidirectional ticker ↔ entity_id mapping.

    Thread-safe for reads after initial registration.
    Entity IDs are 1-indexed contiguous integers.
    """

    def __init__(self) -> None:
        self._ticker_to_id: dict[str, int] = {}
        self._id_to_ticker: dict[int, str] = {}
        self._next_id: int = 1

    # ── Registration ─────────────────────────────────────────

    def register(self, tickers: list[str]) -> None:
        """Register a list of tickers, assigning IDs to any new ones."""
        for ticker in tickers:
            if ticker not in self._ticker_to_id:
                self._ticker_to_id[ticker] = self._next_id
                self._id_to_ticker[self._next_id] = ticker
                self._next_id += 1

    def register_one(self, ticker: str) -> int:
        """Register a single ticker and return its entity_id."""
        if ticker not in self._ticker_to_id:
            self._ticker_to_id[ticker] = self._next_id
            self._id_to_ticker[self._next_id] = ticker
            self._next_id += 1
        return self._ticker_to_id[ticker]

    # ── Lookups ──────────────────────────────────────────────

    def ticker_to_id(self, ticker: str) -> int:
        """Get entity_id for a ticker. Raises KeyError if not registered."""
        return self._ticker_to_id[ticker]

    def id_to_ticker(self, entity_id: int) -> str:
        """Get ticker for an entity_id. Raises KeyError if not registered."""
        return self._id_to_ticker[entity_id]

    def get_id(self, ticker: str, default: int | None = None) -> int | None:
        """Get entity_id for a ticker, or default if not found."""
        return self._ticker_to_id.get(ticker, default)

    # ── Bulk operations ──────────────────────────────────────

    def all_tickers(self) -> list[str]:
        """Return all registered tickers in registration order."""
        return [self._id_to_ticker[i] for i in range(1, self._next_id)]

    def all_ids(self) -> list[int]:
        """Return all entity IDs in order."""
        return list(range(1, self._next_id))

    def as_dict(self) -> dict[int, str]:
        """Return a copy of the id → ticker mapping."""
        return {k: v for k, v in self._id_to_ticker.items()}

    def __len__(self) -> int:
        return len(self._ticker_to_id)

    def __contains__(self, ticker: str) -> bool:
        return ticker in self._ticker_to_id

    def __repr__(self) -> str:
        return f"EntityMap({len(self)} tickers, next_id={self._next_id})"
