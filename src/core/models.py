"""
models.py — Level 5 SQLAlchemy ORM Models

Translates the existing SQLite transactional schemas (traders, trader_constraints,
portfolios, paper_executions) into SQLAlchemy ORM models, plus adds
AlphaLabExperiment for migrating Alpha Lab metadata out of Parquet.

Analytical data (market_data, fundamental, macro, feature, action_intent,
target_portfolio) remains in Parquet/DuckDB — this module only covers
transactional state.
"""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey, Text, JSON, Boolean,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ── Traders ─────────────────────────────────────────────────────

class Trader(Base):
    __tablename__ = "traders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    total_capital = Column(Float, nullable=False, default=10000.0)
    unallocated_capital = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

    constraints = relationship(
        "TraderConstraint", back_populates="trader",
        uselist=False, cascade="all, delete-orphan",
    )
    portfolios = relationship(
        "Portfolio", back_populates="trader",
        cascade="all, delete-orphan",
    )


class TraderConstraint(Base):
    __tablename__ = "trader_constraints"

    trader_id = Column(Integer, ForeignKey("traders.id"), primary_key=True)
    max_drawdown_pct = Column(Float, nullable=False, default=0.20)
    max_open_positions = Column(Integer, nullable=False, default=50)
    max_capital_per_trade = Column(Float, nullable=False, default=1000.0)
    halt_trading_flag = Column(Boolean, nullable=False, default=False)

    trader = relationship("Trader", back_populates="constraints")


# ── Portfolios ──────────────────────────────────────────────────

class Portfolio(Base):
    __tablename__ = "portfolios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trader_id = Column(Integer, ForeignKey("traders.id"), nullable=False)
    name = Column(String(100), nullable=False)
    allocated_capital = Column(Float, nullable=False, default=1000.0)
    strategy_id = Column(String(50), nullable=True)
    rebalance_freq = Column(String(20), nullable=False, default="Daily")
    next_rebalance_date = Column(String(10), nullable=True)

    trader = relationship("Trader", back_populates="portfolios")
    executions = relationship(
        "PaperExecution", back_populates="portfolio",
        cascade="all, delete-orphan",
    )


# ── Paper Executions ────────────────────────────────────────────

class PaperExecution(Base):
    __tablename__ = "paper_executions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    ticker = Column(String(10), nullable=False)
    action = Column(String(4), nullable=False)          # BUY / SELL
    quantity = Column(Integer, nullable=False)
    simulated_price = Column(Float, nullable=False)
    strategy_id = Column(String(50), default="sma_crossover")
    trader_id = Column(Integer, ForeignKey("traders.id"), nullable=True)
    portfolio_id = Column(Integer, ForeignKey("portfolios.id"), nullable=True)

    portfolio = relationship("Portfolio", back_populates="executions")


# ── Alpha Lab ───────────────────────────────────────────────────

class AlphaLabExperiment(Base):
    __tablename__ = "alpha_lab_experiments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(String(20), nullable=False, unique=True, index=True)
    hypothesis = Column(Text, nullable=True)
    strategy_code = Column(Text, nullable=False)
    strategy_name = Column(String(200), nullable=False)
    model_tier = Column(String(20), nullable=True)          # haiku / sonnet / opus
    status = Column(String(20), default="generated")        # generated / backtesting / passed / failed / error
    metrics_json = Column(Text, nullable=True)               # JSON string of backtest metrics
    rationale = Column(Text, nullable=True)
    cost_input_tokens = Column(Integer, default=0)
    cost_output_tokens = Column(Integer, default=0)
    cost_usd = Column(Float, default=0.0)
    promoted = Column(Boolean, default=False)
    audit_status = Column(String(20), nullable=True)          # PASS / FAIL / WARNING / None
    audit_report_json = Column(Text, nullable=True)            # JSON string from forensic auditor
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
