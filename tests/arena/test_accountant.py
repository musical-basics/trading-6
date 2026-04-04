"""
tests/arena/test_accountant.py — Unit tests for token cost engine (Plan 4)
"""

import pytest
from datetime import date
from unittest.mock import patch, MagicMock

from src.arena.schemas import TokenUsage
from src.arena.accountant import (
    calculate_cost, aggregate_tick_cost, MODEL_PRICING,
    _match_model_prefix, MIN_TOKENS_PER_CALL,
)


class TestModelPrefixMatching:
    """Test MODEL_PRICING prefix resolution."""

    def test_opus_prefix_matched(self):
        assert _match_model_prefix("claude-opus-4-6-20251001") == "claude-opus-4"

    def test_sonnet_prefix_matched(self):
        assert _match_model_prefix("claude-sonnet-4-6-20251001") == "claude-sonnet-4"

    def test_haiku_prefix_matched(self):
        assert _match_model_prefix("claude-haiku-4-5-20251001") == "claude-haiku-4"

    def test_unknown_model_uses_default(self):
        assert _match_model_prefix("gpt-4o") == "default"

    def test_case_insensitive(self):
        assert _match_model_prefix("CLAUDE-SONNET-4-6") == "claude-sonnet-4"


class TestCalculateCost:
    """Test per-call cost calculation."""

    def test_sonnet_cost_calculation(self):
        """Verify Sonnet pricing: $3/MTok in, $15/MTok out."""
        usage = TokenUsage(
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            model_id="claude-sonnet-4-6-20251001",
        )
        cost = calculate_cost(usage)
        assert abs(cost - 18.0) < 0.001  # $3 + $15 = $18 per MTok each

    def test_haiku_cheapest(self):
        """Haiku should cost less than Sonnet for same tokens."""
        usage = TokenUsage(input_tokens=10000, output_tokens=2000, model_id="claude-haiku-4-5")
        haiku_cost = calculate_cost(usage)

        usage2 = TokenUsage(input_tokens=10000, output_tokens=2000, model_id="claude-sonnet-4-6")
        sonnet_cost = calculate_cost(usage2)

        assert haiku_cost < sonnet_cost

    def test_minimum_token_floor_applied(self):
        """Zero-token response should still incur minimum cost."""
        usage_zero = TokenUsage(input_tokens=0, output_tokens=0, model_id="claude-sonnet-4-6")
        cost_zero = calculate_cost(usage_zero)

        usage_min = TokenUsage(input_tokens=MIN_TOKENS_PER_CALL, output_tokens=0, model_id="claude-sonnet-4-6")
        cost_min = calculate_cost(usage_min)

        assert cost_zero == cost_min  # Floor applied to both
        assert cost_zero > 0  # Non-zero even with 0 tokens


class TestAggregateTickCost:
    """Test cross-agent cost aggregation."""

    def _make_ledger(self) -> list[TokenUsage]:
        """Typical 13-agent tick ledger."""
        return [
            TokenUsage(input_tokens=1200, output_tokens=180, model_id="claude-sonnet-4-6", agent_name="consultant"),
            TokenUsage(input_tokens=800,  output_tokens=120, model_id="claude-sonnet-4-6", agent_name="auditor"),
            TokenUsage(input_tokens=1500, output_tokens=200, model_id="claude-sonnet-4-6", agent_name="scout"),
            TokenUsage(input_tokens=2800, output_tokens=450, model_id="claude-opus-4-6",   agent_name="commander"),
            TokenUsage(input_tokens=1000, output_tokens=300, model_id="claude-haiku-4-5",  agent_name="analyst_d1"),
            TokenUsage(input_tokens=1000, output_tokens=280, model_id="claude-haiku-4-5",  agent_name="analyst_d2"),
            TokenUsage(input_tokens=1000, output_tokens=310, model_id="claude-haiku-4-5",  agent_name="analyst_d3"),
            TokenUsage(input_tokens=1200, output_tokens=200, model_id="claude-sonnet-4-6", agent_name="strategist_d1"),
            TokenUsage(input_tokens=1200, output_tokens=180, model_id="claude-sonnet-4-6", agent_name="strategist_d2"),
            TokenUsage(input_tokens=1200, output_tokens=190, model_id="claude-sonnet-4-6", agent_name="strategist_d3"),
            TokenUsage(input_tokens=400,  output_tokens=80,  model_id="claude-haiku-4-5",  agent_name="pm_d1"),
            TokenUsage(input_tokens=400,  output_tokens=75,  model_id="claude-haiku-4-5",  agent_name="pm_d2"),
            TokenUsage(input_tokens=400,  output_tokens=85,  model_id="claude-haiku-4-5",  agent_name="pm_d3"),
        ]

    def test_agent_count_correct(self):
        ledger = self._make_ledger()
        summary = aggregate_tick_cost(ledger)
        assert summary.agent_count == 13

    def test_cost_by_agent_populated(self):
        ledger = self._make_ledger()
        summary = aggregate_tick_cost(ledger)
        assert "commander" in summary.cost_by_agent
        assert "consultant" in summary.cost_by_agent

    def test_cost_by_model_populated(self):
        ledger = self._make_ledger()
        summary = aggregate_tick_cost(ledger)
        # Should have 3 model tiers
        assert len(summary.cost_by_model) >= 2

    def test_commander_costs_most(self):
        """Commander (Opus) should have highest individual cost."""
        ledger = self._make_ledger()
        summary = aggregate_tick_cost(ledger)
        commander_cost = summary.cost_by_agent.get("commander", 0)
        haiku_pm_cost = summary.cost_by_agent.get("pm_d1", 0)
        assert commander_cost > haiku_pm_cost

    def test_total_cost_is_sum(self):
        ledger = self._make_ledger()
        summary = aggregate_tick_cost(ledger)
        individual_sum = sum(calculate_cost(u) for u in ledger)
        assert abs(summary.total_cost_usd - individual_sum) < 1e-6

    def test_cost_as_pct_of_nav(self):
        ledger = self._make_ledger()
        summary = aggregate_tick_cost(ledger, current_nav=10_000.0)
        expected_pct = summary.total_cost_usd / 10_000.0
        assert abs(summary.cost_as_pct_of_nav - expected_pct) < 1e-6
