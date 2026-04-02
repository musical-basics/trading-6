# Trading-6 Improvements Summary

Date: 2026-04-02

This file summarizes the code changes made in trading-6 during this session, with emphasis on the structural forensic issue and the data pipeline implications.

## Scope Confirmation

All code changes described below were made in trading-6.

Modified code files:

- frontend/components/strategy-studio.tsx
- frontend/lib/api.ts
- src/alpha_lab/forensic_auditor.py
- src/alpha_lab/strategy_generator.py
- src/alpha_lab/swarm_generator.py
- src/ecs/alignment_system.py
- src/ecs/ingestion_system.py
- src/ecs/tournament_system.py
- src/ecs/fundamental_hygiene.py

Notes:

- There are also local workspace changes under data/, .venv/, and frontend/node_modules/ that are environment artifacts, not application code improvements.

## 1. Tournament Metrics Expansion

Files:

- src/ecs/tournament_system.py
- frontend/lib/api.ts
- frontend/components/strategy-studio.tsx

What changed:

- Expanded tournament performance metrics from a small basic set into a much broader analytics set.
- Added metrics including volatility, downside volatility, Sortino, Calmar, drawdown duration, recovery factor, win-rate stats, streaks, profit factor, expectancy, monthly stats, skewness, kurtosis, VaR, and CVaR.
- Updated the frontend API types to match the richer response payload.
- Expanded the Strategy Studio metrics table to display more of the new metrics.

Why it matters:

- Strategy comparison is now closer to institutional-quality evaluation instead of only return, Sharpe, and drawdown.

## 2. Structural Forensic Investigation

Files:

- src/alpha_lab/forensic_auditor.py
- src/ecs/alignment_system.py
- src/ecs/ingestion_system.py
- src/ecs/fundamental_hygiene.py

Problem investigated:

- The Forensic Auditor was flagging experiment 5ab8259a as:
  - status: FAIL
  - category: STRUCTURAL
  - subtype: A2_DATA_LEAKAGE

What was found:

- The fundamentals dataset contains multiple rows for the same ticker in closely related quarter windows, with different filing_date values.
- That can cause joins and forensic evidence selection to pick an earlier filing row than intended.
- The existing strategy did reference filing_date, but the protection was only applied to an intermediate fundamental signal, not as a hard final raw_weight circuit breaker.

## 3. Data Pipeline Hardening

Files:

- src/ecs/fundamental_hygiene.py
- src/ecs/ingestion_system.py
- src/ecs/alignment_system.py
- src/alpha_lab/forensic_auditor.py

### 3.1 New canonicalization helper

File:

- src/ecs/fundamental_hygiene.py

Added:

- `canonicalize_quarterly_fundamentals(df)`

What it does:

- Normalizes quarterly fundamentals so duplicate rows are collapsed to one canonical row per entity and quarter.
- Keeps the latest filing row when multiple rows compete for the same quarter.
- Applies inferred minimum period-end + 45 day lag logic before deduping to reduce early-date contamination from legacy rows.

Why it matters:

- This reduces inconsistent filing timelines that can trigger structural leakage findings.

### 3.2 Ingestion write-path improvement

File:

- src/ecs/ingestion_system.py

What changed:

- Before writing `fundamental.parquet`, the combined fundamentals data is now passed through the canonicalization helper.

Why it matters:

- Newly ingested or refreshed fundamentals are cleaned before being persisted.

### 3.3 Alignment read-path improvement

File:

- src/ecs/alignment_system.py

What changed:

- Fundamental data is canonicalized before `join_asof` alignment to market data.

Why it matters:

- Even if the parquet still contains legacy duplicates, alignment uses the cleaned view.

### 3.4 Forensic evidence read-path improvement

File:

- src/alpha_lab/forensic_auditor.py

What changed:

- Fundamental data is canonicalized before the auditor selects the latest filing prior to trade date.

Why it matters:

- The forensic evidence builder now uses the same cleaned filing timeline as the alignment path.

## 4. Strategy Guardrail Hardening

Files:

- src/alpha_lab/strategy_generator.py
- src/alpha_lab/swarm_generator.py

What changed:

- Added a new AST guardrail: G5.

What G5 enforces:

- If a generated strategy uses fundamental data, the filing-date stale mask must be applied directly on the final `raw_weight_*` expression.
- It is no longer sufficient to guard only an intermediate sub-signal.

Accepted pattern:

```python
return df.with_columns(
    pl.when(
        pl.col("filing_date").is_null() |
        ((pl.col("date") - pl.col("filing_date").cast(pl.Date)).dt.total_days() > 540) |
        ((pl.col("date") - pl.col("filing_date").cast(pl.Date)).dt.total_days() < 45)
    ).then(0.0).otherwise(pl.col("some_pre_weight")).alias("raw_weight_strategy")
)
```

Rejected pattern:

- Guarding only `cash_debt_ratio_clean` or another intermediate expression, then aliasing that into `raw_weight_*` later without the final circuit-breaker.

Why it matters:

- This closes the exact loophole that allowed structurally suspect strategies to pass generation-time checks.

## 5. Prompt-Level Generator Improvements

File:

- src/alpha_lab/swarm_generator.py

What changed:

- Updated the developer/system prompt guidance so LLM-generated strategies are explicitly instructed to apply the stale mask to the final raw weight, not only to intermediate features.

Why it matters:

- This reduces future invalid strategies at generation time instead of relying only on post-generation rejection.

## 6. What Still Remains True

- The direct forensic rerun for experiment 5ab8259a still returned STRUCTURAL / A2 after these fixes.
- The reason is that this specific stored experiment code would now fail the stronger G5 guardrail, but the experiment already exists and its historical code/backtest record has not been regenerated.

In short:

- The system is now better protected going forward.
- Existing historical experiments are not automatically rewritten.

## 7. Do You Need to Rerun the Data Pipeline Ingest?

Short answer:

- Not strictly for the guardrail changes.
- Recommended for the data cleanup changes.

### If you do nothing right now

- The updated alignment path and forensic auditor both canonicalize fundamentals on read.
- That means the code already uses a cleaner view of the existing data without requiring an immediate full re-ingest.

### If you want the underlying stored data to be physically cleaned

You should rerun at least the fundamentals ingest step.

Recommended minimum rerun:

1. Rerun fundamentals ingestion.
2. Rerun the alignment/math pipeline that produces derived features.

Reason:

- `fundamental.parquet` itself is only rewritten through ingestion.
- Derived feature outputs may still reflect older pre-cleanup alignments if they were produced before this fix.

### Practical recommendation

Best path:

1. Rerun fundamental ingestion.
2. Rerun ECS System 2 alignment/math pipeline.
3. Regenerate any new Alpha Lab strategies that use fundamentals.
4. Re-run forensic audit on newly generated strategies.

### Do you need to rerun prices or macro ingest?

- No, not for this specific fix.
- This issue is centered on fundamentals and the way they are aligned and audited.

## 8. Recommended Next Actions

1. Re-ingest fundamentals so `fundamental.parquet` is rewritten in canonical form.
2. Rebuild aligned/derived feature outputs.
3. Regenerate the currently flagged strategy so it satisfies G5.
4. Re-run the forensic audit after regeneration.
