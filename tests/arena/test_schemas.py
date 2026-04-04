"""
tests/arena/test_schemas.py — Unit tests for inter-agent schemas (Plan 1)

Tests:
  - Invalid strategy keys are rejected by StrategistRecommendation
  - Cash buffer constraint (>=5%) enforced on CommanderDirective  
  - PMDecision.confirmation must be "LOCKED"
  - Token costs aggregate correctly
  - CommanderDirective budget conservation validator
"""

import pytest
from pydantic import ValidationError
from datetime import date

from src.arena.schemas import (
    StrategyKey, TokenUsage, MacroBrief, AuditBrief, ScoutBrief,
    DeskAllocation, CommanderDirective, AnalystInsight,
    StrategistRecommendation, PMDecision, DailyTickResult,
    ALL_STRATEGY_KEYS,
)


# ═══════════════════════════════════════════════════════════════
# STRATEGY KEY / HALLUCINATION GUARD
# ═══════════════════════════════════════════════════════════════

class TestStrategyKeyValidation:
    """Test the primary hallucination guard."""

    def test_valid_strategy_accepted(self):
        """All valid strategy keys should be accepted."""
        for key in ALL_STRATEGY_KEYS:
            rec = StrategistRecommendation(
                desk_id=1,
                recommended_strategy=key,
                confidence=0.8,
                reasoning="Test reasoning",
            )
            assert rec.recommended_strategy == key

    def test_invalid_strategy_rejected(self):
        """Hallucinated strategy names must raise ValidationError."""
        invalid_strategies = [
            "fibonacci_expansion",
            "rsi_divergence",
            "neural_network",
            "buy_hold",      # excluded (benchmark)
            "ev_sales",      # excluded (superseded)
            "MOMENTUM",      # wrong case
            "",              # empty
            "ls_z_score",    # typo
        ]
        for bad_key in invalid_strategies:
            with pytest.raises(ValidationError):
                StrategistRecommendation(
                    desk_id=1,
                    recommended_strategy=bad_key,
                    confidence=0.8,
                    reasoning="Test",
                )

    def test_all_strategy_keys_present(self):
        """Verify all 10 core strategy keys are defined."""
        expected = {
            "ls_zscore", "fortress", "low_beta", "xgboost", "dcf_value",
            "momentum", "pullback_rsi", "sma_crossover", "macro_regime", "macro_v2",
        }
        assert set(ALL_STRATEGY_KEYS) == expected


# ═══════════════════════════════════════════════════════════════
# COMMANDER DIRECTIVE
# ═══════════════════════════════════════════════════════════════

class TestCommanderDirective:
    """Test CommanderDirective validators."""

    def _valid_allocation(self, desk_id=1, budget=3000.0):
        return DeskAllocation(
            desk_id=desk_id,
            tickers=["AAPL", "MSFT"],
            capital_budget_usd=budget,
            strategic_directive="Test directive",
            risk_tolerance="moderate",
        )

    def test_valid_directive(self):
        """A well-formed directive should pass all validators."""
        directive = CommanderDirective(
            commander_reasoning="Test reasoning",
            desk_allocations=[self._valid_allocation(1), self._valid_allocation(2), self._valid_allocation(3)],
            total_deployed_pct=0.85,
            cash_reserve_pct=0.15,
        )
        assert directive.total_deployed_pct == 0.85
        assert directive.cash_reserve_pct == 0.15

    def test_cash_reserve_minimum_enforced(self):
        """cash_reserve_pct < 5% must be rejected."""
        with pytest.raises(ValidationError):
            CommanderDirective(
                commander_reasoning="Test",
                desk_allocations=[self._valid_allocation()],
                total_deployed_pct=0.97,
                cash_reserve_pct=0.03,  # Below 5% minimum!
            )

    def test_budget_conservation_validator(self):
        """total_deployed_pct + cash_reserve_pct > 1.0 must be rejected."""
        with pytest.raises(ValidationError):
            CommanderDirective(
                commander_reasoning="Test",
                desk_allocations=[self._valid_allocation()],
                total_deployed_pct=0.95,
                cash_reserve_pct=0.10,  # 1.05 total → over 100%!
            )

    def test_desk_allocations_min_length(self):
        """At least 1 desk allocation required."""
        with pytest.raises(ValidationError):
            CommanderDirective(
                commander_reasoning="Test",
                desk_allocations=[],
                total_deployed_pct=0.85,
                cash_reserve_pct=0.15,
            )

    def test_desk_id_bounds(self):
        """Desk IDs must be between 1 and 3."""
        with pytest.raises(ValidationError):
            DeskAllocation(
                desk_id=4,  # Invalid!
                tickers=["AAPL"],
                capital_budget_usd=1000.0,
                strategic_directive="Test",
                risk_tolerance="moderate",
            )


# ═══════════════════════════════════════════════════════════════
# PM DECISION
# ═══════════════════════════════════════════════════════════════

class TestPMDecision:
    """Test PMDecision constraints."""

    def test_valid_pm_decision(self):
        """A valid PM decision with LOCKED confirmation."""
        pm = PMDecision(
            desk_id=1,
            portfolio_id=42,
            strategy_id="momentum",
            allocated_capital=3400.0,
            confirmation="LOCKED",
        )
        assert pm.confirmation == "LOCKED"
        assert pm.strategy_id == "momentum"

    def test_confirmation_must_be_locked(self):
        """confirmation field only accepts 'LOCKED'."""
        with pytest.raises(ValidationError):
            PMDecision(
                desk_id=1,
                portfolio_id=42,
                strategy_id="momentum",
                allocated_capital=3400.0,
                confirmation="CONFIRMED",  # Must be exactly "LOCKED"!
            )

    def test_confirmation_empty_rejected(self):
        """Empty confirmation rejected."""
        with pytest.raises(ValidationError):
            PMDecision(
                desk_id=1,
                portfolio_id=42,
                strategy_id="momentum",
                allocated_capital=3400.0,
                confirmation="",
            )

    def test_pm_strategy_must_be_valid(self):
        """PMDecision.strategy_id is also guarded by StrategyKey."""
        with pytest.raises(ValidationError):
            PMDecision(
                desk_id=1,
                portfolio_id=42,
                strategy_id="invalid_strategy",
                allocated_capital=1000.0,
                confirmation="LOCKED",
            )

    def test_allocated_capital_must_be_positive(self):
        """allocated_capital > 0 required."""
        with pytest.raises(ValidationError):
            PMDecision(
                desk_id=1,
                portfolio_id=42,
                strategy_id="momentum",
                allocated_capital=0.0,  # Must be > 0!
                confirmation="LOCKED",
            )


# ═══════════════════════════════════════════════════════════════
# TOKEN USAGE
# ═══════════════════════════════════════════════════════════════

class TestTokenUsage:
    """Test TokenUsage aggregation."""

    def test_token_usage_defaults(self):
        """Default TokenUsage has zero costs."""
        usage = TokenUsage()
        assert usage.input_tokens == 0
        assert usage.output_tokens == 0
        assert usage.estimated_cost_usd == 0.0

    def test_token_usage_accumulation(self):
        """TokenUsage.add() correctly sums two usages."""
        u1 = TokenUsage(input_tokens=1000, output_tokens=200, estimated_cost_usd=0.006)
        u2 = TokenUsage(input_tokens=500, output_tokens=100, estimated_cost_usd=0.003)
        combined = u1.add(u2)
        assert combined.input_tokens == 1500
        assert combined.output_tokens == 300
        assert abs(combined.estimated_cost_usd - 0.009) < 1e-10


# ═══════════════════════════════════════════════════════════════
# CONSULTANT BRIEFS
# ═══════════════════════════════════════════════════════════════

class TestConsultantBriefs:
    """Test C-suite brief validation."""

    def test_macro_brief_valid_regimes(self):
        """MacroBrief only accepts 3 macro regimes."""
        for regime in ["Risk-On", "Risk-Off", "Caution"]:
            brief = MacroBrief(
                macro_regime=regime,
                vix_level=20.0,
                ten_year_yield=4.5,
                risk_assessment="Test",
            )
            assert brief.macro_regime == regime

    def test_macro_brief_invalid_regime(self):
        """Invalid macro regime rejected."""
        with pytest.raises(ValidationError):
            MacroBrief(
                macro_regime="Bullish",  # Invalid!
                vix_level=20.0,
                ten_year_yield=4.5,
                risk_assessment="Test",
            )

    def test_audit_brief_valid_health_statuses(self):
        """AuditBrief accepts 3 health statuses."""
        for status in ["HEALTHY", "WARNING", "CRITICAL"]:
            brief = AuditBrief(
                data_freshness_ok=True,
                overall_health=status,
            )
            assert brief.overall_health == status

    def test_vix_level_must_be_non_negative(self):
        """VIX level must be >= 0."""
        with pytest.raises(ValidationError):
            MacroBrief(
                macro_regime="Risk-On",
                vix_level=-5.0,  # Invalid!
                ten_year_yield=4.5,
                risk_assessment="Test",
            )
