"""
alpha_tasks.py — Celery tasks for Alpha Lab 2.0 autonomous operations.

- run_genetic_evolution: Nightly at 2:00 AM, mutates top strategies.
"""

from src.core.celery_app import app
import logging

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="src.tasks.alpha_tasks.run_genetic_evolution",
)
def run_genetic_evolution(self):
    """
    Nightly Discovery: Query top-performing experiments from Postgres,
    feed their code back to the LLM with a mutation prompt,
    and generate new variants.
    """
    try:
        logger.info("🧬 [Nightly] Starting genetic evolution...")

        from src.alpha_lab.alpha_lab_store import list_experiments
        import json

        # Get all experiments, find top passed ones by Sharpe
        experiments = list_experiments()
        passed = [
            e for e in experiments
            if e.get("status") == "passed" and e.get("metrics_json")
        ]

        if not passed:
            logger.info("🧬 No passed experiments to evolve. Skipping.")
            return {"status": "skipped", "reason": "no passed experiments"}

        # Parse metrics and sort by Sharpe
        for exp in passed:
            try:
                metrics = json.loads(exp["metrics_json"]) if isinstance(exp["metrics_json"], str) else exp["metrics_json"]
                exp["_sharpe"] = metrics.get("sharpe", 0)
            except (json.JSONDecodeError, TypeError):
                exp["_sharpe"] = 0

        top_5 = sorted(passed, key=lambda x: x["_sharpe"], reverse=True)[:5]

        # Build mutation prompt
        code_summaries = "\n\n".join([
            f"### Strategy: {exp.get('strategy_name', 'Unknown')} "
            f"(Sharpe: {exp['_sharpe']:.3f})\n"
            f"```python\n{exp['strategy_code']}\n```"
            for exp in top_5
        ])

        mutation_prompt = (
            f"Here are our best performing strategies:\n\n"
            f"{code_summaries}\n\n"
            f"Mutate them by introducing a new feature, altering the window sizing, "
            f"or combining their logic to reduce Max Drawdown. "
            f"Generate 3 new variants, each as a separate generate_signals() function."
        )

        logger.info(f"🧬 Built mutation prompt from {len(top_5)} top strategies.")
        # The actual LLM call + sandbox execution would go here
        # For now, log the prompt for manual review
        logger.info(f"🧬 Mutation prompt length: {len(mutation_prompt)} chars")

        return {"status": "success", "top_strategies": len(top_5)}

    except Exception as exc:
        logger.error(f"❌ Genetic evolution failed: {exc}")
        return {"status": "failed", "error": str(exc)}
