-- ═══════════════════════════════════════════════════════════════
-- QuantPrime Level 5 — Supabase Schema
-- Run this in Supabase SQL Editor (Dashboard → SQL Editor → New Query)
-- ═══════════════════════════════════════════════════════════════

-- ── Traders ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS traders (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    total_capital FLOAT NOT NULL DEFAULT 10000.0,
    unallocated_capital FLOAT NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ── Trader Constraints ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS trader_constraints (
    trader_id INTEGER PRIMARY KEY REFERENCES traders(id) ON DELETE CASCADE,
    max_drawdown_pct FLOAT NOT NULL DEFAULT 0.20,
    max_open_positions INTEGER NOT NULL DEFAULT 50,
    max_capital_per_trade FLOAT NOT NULL DEFAULT 1000.0,
    halt_trading_flag BOOLEAN NOT NULL DEFAULT FALSE
);

-- ── Portfolios ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolios (
    id SERIAL PRIMARY KEY,
    trader_id INTEGER NOT NULL REFERENCES traders(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    allocated_capital FLOAT NOT NULL DEFAULT 1000.0,
    strategy_id VARCHAR(50),
    rebalance_freq VARCHAR(20) NOT NULL DEFAULT 'Daily',
    next_rebalance_date VARCHAR(10)
);

-- ── Paper Executions ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS paper_executions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    ticker VARCHAR(10) NOT NULL,
    action VARCHAR(4) NOT NULL,
    quantity INTEGER NOT NULL,
    simulated_price FLOAT NOT NULL,
    strategy_id VARCHAR(50) DEFAULT 'sma_crossover',
    trader_id INTEGER REFERENCES traders(id),
    portfolio_id INTEGER REFERENCES portfolios(id)
);

-- ── Alpha Lab Experiments ───────────────────────────────────
CREATE TABLE IF NOT EXISTS alpha_lab_experiments (
    id SERIAL PRIMARY KEY,
    experiment_id VARCHAR(20) NOT NULL UNIQUE,
    hypothesis TEXT,
    strategy_code TEXT NOT NULL,
    strategy_name VARCHAR(200) NOT NULL,
    model_tier VARCHAR(20),
    status VARCHAR(20) DEFAULT 'generated',
    metrics_json TEXT,
    rationale TEXT,
    cost_input_tokens INTEGER DEFAULT 0,
    cost_output_tokens INTEGER DEFAULT 0,
    cost_usd FLOAT DEFAULT 0.0,
    promoted BOOLEAN DEFAULT FALSE,
    audit_status VARCHAR(20),
    audit_report_json TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ── Migration: Add audit columns if table already exists ─────
ALTER TABLE alpha_lab_experiments ADD COLUMN IF NOT EXISTS audit_status VARCHAR(20);
ALTER TABLE alpha_lab_experiments ADD COLUMN IF NOT EXISTS audit_report_json TEXT;

-- ── Indexes ─────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_experiments_status ON alpha_lab_experiments(status);
CREATE INDEX IF NOT EXISTS idx_experiments_created ON alpha_lab_experiments(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_executions_date ON paper_executions(timestamp);
CREATE INDEX IF NOT EXISTS idx_executions_ticker ON paper_executions(ticker);
CREATE INDEX IF NOT EXISTS idx_portfolios_trader ON portfolios(trader_id);

-- ── Editor Settings ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS editor_settings (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) NOT NULL UNIQUE,
    value JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_editor_settings_key ON editor_settings(key);
