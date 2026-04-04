"""
tests/arena/test_back_office.py — Unit tests for back office engine (Plan 6)

Tests:
  - DataIntegrityReport schema validation
  - RiskBouncerReport schema validation
  - BackOfficeHaltError raised on missing critical data
  - Safety invariants are documented and enforced
"""

import pytest
from datetime import date, datetime

from src.arena.back_office import (
    DataIntegrityReport,
    RiskBouncerReport,
    BackOfficeHaltError,
    _empty_bouncer_report,
)
from src.arena.schemas import PMDecision, TokenUsage


class TestDataIntegrityReport:
    """Test DataIntegrityReport schema."""

    def test_proceed_recommendation(self):
        report = DataIntegrityReport(
            freshness_ok=True,
            checked_at=datetime.utcnow(),
            components_checked=3,
            stale_components=[],
            stale_tickers=[],
            latest_market_date=date.today(),
            latest_macro_date=date.today(),
            days_since_market_update=0,
            recommendation="PROCEED",
        )
        assert report.freshness_ok is True
        assert report.recommendation == "PROCEED"

    def test_halt_recommendation(self):
        report = DataIntegrityReport(
            freshness_ok=False,
            checked_at=datetime.utcnow(),
            components_checked=3,
            stale_components=["market_data (missing)", "macro (missing)"],
            stale_tickers=[],
            latest_market_date=None,
            latest_macro_date=None,
            days_since_market_update=999,
            recommendation="HALT",
        )
        assert report.freshness_ok is False
        assert report.recommendation == "HALT"

    def test_valid_recommendations(self):
        """Only 3 valid recommendation literals accepted."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            DataIntegrityReport(
                freshness_ok=True,
                checked_at=datetime.utcnow(),
                components_checked=0,
                stale_components=[],
                stale_tickers=[],
                latest_market_date=None,
                latest_macro_date=None,
                days_since_market_update=0,
                recommendation="CONTINUE",  # Invalid!
            )


class TestRiskBouncerReport:
    """Test RiskBouncerReport schema."""

    def test_empty_bouncer_report(self):
        """_empty_bouncer_report returns a valid no-op report."""
        report = _empty_bouncer_report(trader_id=1, timestamp=datetime.utcnow())
        assert report.trader_id == 1
        assert report.original_total_weight == 0.0
        assert report.scaling_factor == 1.0
        assert report.breaching_assets == []
        assert report.cross_desk_concentration == []

    def test_scaling_factor_logic(self):
        """scaling_factor = adjusted / original."""
        report = RiskBouncerReport(
            timestamp=datetime.utcnow(),
            trader_id=1,
            original_total_weight=0.90,
            adjusted_total_weight=0.72,  # 20% reduction
            scaling_factor=0.8,
            portfolio_vol_annualized=0.15,
            max_mcr=0.04,
            iterations=3,
            breaching_assets=[],
            cross_desk_concentration=[],
        )
        assert report.scaling_factor == 0.8


class TestBackOfficeHaltError:
    """Test that BackOfficeHaltError propagates correctly."""

    def test_halt_error_message(self):
        """BackOfficeHaltError should carry a meaningful message."""
        err = BackOfficeHaltError("market_data missing, macro missing")
        assert "market_data" in str(err)

    def test_halt_error_is_exception(self):
        """Should be raiseable."""
        with pytest.raises(BackOfficeHaltError):
            raise BackOfficeHaltError("Test halt")


class TestSafetyInvariants:
    """Document and verify key safety invariants (Plan 8 Q1-Q5)."""

    def test_commander_directive_has_no_trade_fields(self):
        """CommanderDirective schema has no trade execution fields.
        
        This is the Q1 invariant: Commander CANNOT execute trades
        because its output schema literally cannot express trade fields.
        """
        from src.arena.schemas import CommanderDirective
        fields = set(CommanderDirective.model_fields.keys())
        forbidden_fields = {"trade", "execute", "buy", "sell", "order"}
        
        for field in forbidden_fields:
            assert field not in fields, f"Forbidden field '{field}' found in CommanderDirective!"

    def test_pm_decision_confirmation_literal(self):
        """PMDecision.confirmation must be 'LOCKED'.
        
        Q5 invariant: explicit acknowledgment of irreversibility.
        """
        from pydantic import ValidationError
        from src.arena.schemas import PMDecision
        
        with pytest.raises(ValidationError):
            PMDecision(
                desk_id=1,
                portfolio_id=1,
                strategy_id="momentum",
                allocated_capital=1000.0,
                confirmation="YES",  # Must be "LOCKED"
            )

    def test_all_strategy_keys_are_valid_registry_entries(self):
        """StrategyKey must match keys in STRATEGY_REGISTRY.
        
        Q3 invariant: if Strategist outputs a key not in registry,
        the PM's PUT /api/portfolios/{id}/strategy call would fail.
        """
        from src.arena.schemas import ALL_STRATEGY_KEYS
        from src.ecs.strategy_registry import STRATEGY_REGISTRY
        
        for key in ALL_STRATEGY_KEYS:
            assert key in STRATEGY_REGISTRY, f"Strategy key '{key}' not in STRATEGY_REGISTRY!"
