"""
src/arena/__init__.py — Hedge Fund Swarm Package

Production-grade multi-agent orchestration layer for the QuantPrime
Level 5 "God Engine" Arena. Implements a hierarchical LLM swarm with:
  - Commander (CEO) allocating capital to 3 Trading Desks
  - Analyst → Strategist → PM pipeline per desk
  - Deterministic Back Office safety layer
  - Token-cost Accountant deducting from fund P/L
"""
