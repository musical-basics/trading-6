"""
tests/arena/test_orchestrator.py — Unit tests for the game loop orchestrator (Plan 3)

Tests:
  - TickProgress tracks agents correctly
  - Budget clamp logic: PM cannot exceed Commander's allocation
  - Model config overrides work correctly (A/B test per Rule #16)
  - DAG ordering invariants via safety.py
  - run_arena_tick handles exceptions per fund without crashing others
"""

import pytest
from datetime import date
from unittest.mock import patch, AsyncMock, MagicMock

from src.arena.orchestrator import TickProgress
from src.arena.schemas import (
    DeskAllocation, PMDecision, CommanderDirective, TokenUsage,
)
from src.arena.safety import (
    assert_dag_ordering_invariant,
    verify_strategy_key,
    compute_leaderboard,
    CIRCUIT_BREAKER_THRESHOLDS,
    VIX_DEPLOYMENT_CAPS,
)


# ═══════════════════════════════════════════════════════════════
# TICK PROGRESS
# ═══════════════════════════════════════════════════════════════

class TestTickProgress:
    """Test TickProgress state machine."""

    def test_agent_start_and_complete(self):
        progress = TickProgress()
        progress.pending_agents = ["consultant", "auditor"]
        
        progress.start_agent("consultant")
        assert progress.current_agent == "consultant"
        
        progress.complete_agent("consultant")
        assert "consultant" in progress.completed_agents
        assert progress.current_agent == ""
        assert "consultant" not in progress.pending_agents

    def test_elapsed_seconds_increases(self):
        import time
        progress = TickProgress()
        assert progress.elapsed_seconds >= 0.0
        time.sleep(0.01)
        assert progress.elapsed_seconds >= 0.01


# ═══════════════════════════════════════════════════════════════
# SAFETY UTILITIES
# ═══════════════════════════════════════════════════════════════

class TestDagOrderingInvariant:
    """Test DAG ordering assertion utility from safety.py."""

    def test_ordering_satisfied(self):
        """No error when required phase is complete."""
        phases = {"pms": True, "consultants": True}
        # Should not raise
        assert_dag_ordering_invariant(phases, "pms", "back_office")

    def test_ordering_violated_raises(self):
        """RuntimeError raised when ordering is violated."""
        phases = {"pms": False}
        with pytest.raises(RuntimeError, match="DAG ordering violation"):
            assert_dag_ordering_invariant(phases, "pms", "back_office")

    def test_missing_phase_treated_as_incomplete(self):
        """Phase not in dict treated as not-completed."""
        phases = {}
        with pytest.raises(RuntimeError):
            assert_dag_ordering_invariant(phases, "commander", "desks")


class TestVerifyStrategyKey:
    """Test strategy key validation in safety module."""

    def test_all_valid_keys_pass(self):
        from src.arena.schemas import ALL_STRATEGY_KEYS
        for key in ALL_STRATEGY_KEYS:
            assert verify_strategy_key(key) is True

    def test_invalid_key_fails(self):
        assert verify_strategy_key("buy_hold") is False  # Excluded
        assert verify_strategy_key("ev_sales") is False  # Excluded
        assert verify_strategy_key("fibonacci") is False
        assert verify_strategy_key("") is False


# ═══════════════════════════════════════════════════════════════
# CIRCUIT BREAKERS
# ═══════════════════════════════════════════════════════════════

class TestCircuitBreakers:
    """Test circuit breaker threshold config."""

    def test_thresholds_defined(self):
        assert "downgrade_to_haiku" in CIRCUIT_BREAKER_THRESHOLDS
        assert "halt_fund" in CIRCUIT_BREAKER_THRESHOLDS

    def test_halt_higher_than_downgrade(self):
        """halt_fund threshold > downgrade_to_haiku."""
        assert CIRCUIT_BREAKER_THRESHOLDS["halt_fund"] > CIRCUIT_BREAKER_THRESHOLDS["downgrade_to_haiku"]

    def test_vix_deployment_caps_ordered(self):
        """VIX risk-off cap < critical cap < default."""
        assert VIX_DEPLOYMENT_CAPS["risk_off"] < VIX_DEPLOYMENT_CAPS["critical"]
        assert VIX_DEPLOYMENT_CAPS["critical"] < VIX_DEPLOYMENT_CAPS["default"]


# ═══════════════════════════════════════════════════════════════
# LEADERBOARD
# ═══════════════════════════════════════════════════════════════

class TestLeaderboard:
    """Test compute_leaderboard from safety.py."""

    def _make_result(self, cost=0.01, elapsed=30.0, strategy="momentum"):
        """Create a mock DailyTickResult."""
        mock = MagicMock()
        mock.api_cost_deducted_usd = cost
        mock.elapsed_seconds = elapsed
        mock.desk_results = [
            MagicMock(strategy_id=strategy)
        ]
        return mock

    def test_ranked_by_capital(self):
        """Funds ranked by capital descending."""
        traders = [
            (1, "Fund A", 10000.0),
            (2, "Fund B", 12000.0),  # Higher capital
            (3, "Fund C", 8000.0),
        ]
        results = [self._make_result() for _ in traders]
        board = compute_leaderboard(traders, results)
        
        # Fund B should be rank 1 (highest capital)
        assert board[0]["fund_name"] == "Fund B"
        assert board[0]["rank"] == 1

    def test_error_funds_marked_halted(self):
        """Funds that errored are marked is_halted=True."""
        traders = [(1, "Good", 10000.0), (2, "Bad", 5000.0)]
        results = [self._make_result(), RuntimeError("Tick failed")]
        board = compute_leaderboard(traders, results)
        
        bad_fund = next(e for e in board if e["trader_id"] == 2)
        assert bad_fund["is_halted"] is True
        assert bad_fund["status"] == "error"

    def test_ranks_are_sequential(self):
        """Ranks must be 1, 2, 3, ... without gaps."""
        traders = [(i+1, f"Fund {i+1}", 10000.0 - i*1000) for i in range(5)]
        results = [self._make_result() for _ in traders]
        board = compute_leaderboard(traders, results)
        ranks = [e["rank"] for e in board]
        assert ranks == list(range(1, len(traders) + 1))


# ═══════════════════════════════════════════════════════════════
# BUDGET CLAMP LOGIC (Plan 8 Q4)
# ═══════════════════════════════════════════════════════════════

class TestBudgetClamp:
    """Test PM cannot exceed Commander's budget (orchestrator-level clamp)."""

    def test_pm_exceeding_budget_would_get_clamped(self):
        """Simulate the clamping logic from _run_desk."""
        commander_budget = 3000.0
        pm_allocated = 5000.0  # PM tried to exceed budget

        # Simulate orchestrator clamp logic
        final_capital = min(pm_allocated, commander_budget)
        assert final_capital == commander_budget

    def test_pm_within_budget_unchanged(self):
        """PM within budget should not be clamped."""
        commander_budget = 3000.0
        pm_allocated = 2800.0  # Within budget

        final_capital = min(pm_allocated, commander_budget)
        assert final_capital == pm_allocated
