"""
src/arena/llm_client.py — LLM Call Wrapper & Structured Output Engine (Plan 7)

Single point of contact between the swarm and the Anthropic API.
Handles:
  1. Structured output parsing (JSON → Pydantic V2)
  2. Retry logic with error injection (LLM sees its own parse error)
  3. Token usage tracking (cumulative across retries)
  4. Rate limiting via asyncio semaphore (max 5 concurrent calls)
  5. Model selection and A/B testing support

Key behaviors:
  - Token costs are ALWAYS cumulative (retries are expensive!)
  - On parse failure: injects the ValidationError back so the LLM self-corrects
  - Semaphore prevents overwhelming Anthropic rate limits during Phase 1+3
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import TypeVar

from pydantic import BaseModel, ValidationError

from src.arena.schemas import TokenUsage

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# ═══════════════════════════════════════════════════════════════
# CUSTOM EXCEPTIONS
# ═══════════════════════════════════════════════════════════════

class LLMError(Exception):
    """Base exception for LLM operations."""


class LLMParseError(LLMError):
    """LLM output could not be parsed into expected schema after all retries."""
    def __init__(self, agent_name: str, model_id: str, raw_text: str, error: str):
        self.agent_name = agent_name
        self.model_id = model_id
        self.raw_text = raw_text
        self.error = error
        super().__init__(f"[{agent_name}] Parse failed after retries: {error}")


class LLMAPIError(LLMError):
    """Anthropic API returned a non-retryable error."""


class LLMConfigError(LLMError):
    """Missing configuration (API key, etc.)."""


class LLMRateLimitError(LLMError):
    """API rate limit exceeded."""


# ═══════════════════════════════════════════════════════════════
# SINGLETON CLIENT
# ═══════════════════════════════════════════════════════════════

_client = None
_api_semaphore: asyncio.Semaphore | None = None


def _get_client():
    """Return singleton async Anthropic client (reuses connection pool)."""
    global _client
    if _client is None:
        try:
            import anthropic
        except ImportError:
            raise LLMConfigError("anthropic package not installed. Run: pip install anthropic")

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise LLMConfigError(
                "ANTHROPIC_API_KEY not set. Add it to .env.local:\n"
                "  ANTHROPIC_API_KEY=sk-ant-..."
            )
        _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client


def _get_semaphore() -> asyncio.Semaphore:
    """Return global semaphore (5 concurrent calls max)."""
    global _api_semaphore
    if _api_semaphore is None:
        _api_semaphore = asyncio.Semaphore(5)
    return _api_semaphore


# ═══════════════════════════════════════════════════════════════
# RESPONSE PARSING
# ═══════════════════════════════════════════════════════════════

def _parse_llm_response(raw_text: str, model: type[T]) -> T:
    """Parse LLM text output into a Pydantic model.
    
    Handles common LLM output quirks:
    1. Strips markdown code fences (```json ... ```)
    2. Strips leading/trailing whitespace and preamble text
    3. Extracts JSON object if surrounded by non-JSON text
    4. Passes through Pydantic V2 model_validate_json()
    """
    cleaned = raw_text.strip()

    # Strip markdown fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        if len(lines) > 2:
            # Remove first line (fence open) and last line (fence close)
            inner = "\n".join(lines[1:-1])
            # Remove "json" language specifier if present
            if inner.startswith("json"):
                inner = inner[4:].strip()
            cleaned = inner.strip()
        # else: malformed fence, try as-is

    # Try to extract JSON object if there's surrounding text
    if not cleaned.startswith("{") and not cleaned.startswith("["):
        # Find first { or [
        start = min(
            cleaned.find("{") if "{" in cleaned else len(cleaned),
            cleaned.find("[") if "[" in cleaned else len(cleaned),
        )
        if start < len(cleaned):
            cleaned = cleaned[start:]

    # Final trim
    cleaned = cleaned.strip()

    return model.model_validate_json(cleaned)


# ═══════════════════════════════════════════════════════════════
# CORE CALL FUNCTION
# ═══════════════════════════════════════════════════════════════

async def call_llm(
    system_prompt: str,
    user_message: str,
    response_model: type[T],
    agent_name: str,
    model_id: str = "claude-sonnet-4-6-20251001",
    max_retries: int = 2,
    temperature: float = 0.3,
    max_tokens: int = 1024,
) -> tuple[T, TokenUsage]:
    """Call an LLM and parse the response into a Pydantic model.
    
    Args:
        system_prompt: System prompt for the agent role
        user_message: Formatted data payload (JSON or structured text)
        response_model: Pydantic V2 model class to parse output into
        agent_name: Identifier for cost tracking (e.g., "strategist_d1")
        model_id: Full Anthropic model ID
        max_retries: Number of retry attempts on parse failure
        temperature: LLM temperature (lower = more deterministic)
        max_tokens: Max output tokens
    
    Returns:
        (parsed_model, cumulative_token_usage)
    
    Raises:
        LLMParseError: If all retries exhausted
        LLMAPIError: If Anthropic API returns a non-retryable error
        LLMConfigError: If API key not configured
    """
    client = _get_client()
    semaphore = _get_semaphore()

    cumulative = TokenUsage(model_id=model_id, agent_name=agent_name)
    current_user_message = user_message
    last_error = None
    start_time = time.time()

    async with semaphore:
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.warning(
                    f"[{agent_name}] Retry {attempt}/{max_retries} after parse failure"
                )
                # Inject error feedback for self-correction
                current_user_message = (
                    f"Your previous response was invalid JSON.\n"
                    f"Error: {last_error}\n\n"
                    f"Original request:\n{user_message}\n\n"
                    f"Output ONLY valid JSON matching the schema. No markdown. No explanation."
                )

            logger.info(
                f"[{agent_name}] Calling {model_id} "
                f"(system:{len(system_prompt)}chars, user:{len(current_user_message)}chars)"
            )

            try:
                response = await client.messages.create(
                    model=model_id,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system_prompt,
                    messages=[{"role": "user", "content": current_user_message}],
                )
            except Exception as e:
                error_str = str(e).lower()
                if "rate limit" in error_str or "429" in error_str:
                    raise LLMRateLimitError(f"[{agent_name}] Rate limited: {e}")
                elif "api key" in error_str or "authentication" in error_str:
                    raise LLMConfigError(f"[{agent_name}] Auth error: {e}")
                else:
                    raise LLMAPIError(f"[{agent_name}] API error: {e}")

            # Accumulate tokens (retries are additive — anti-gaming)
            cumulative.input_tokens += response.usage.input_tokens
            cumulative.output_tokens += response.usage.output_tokens

            raw_text = response.content[0].text

            try:
                parsed = _parse_llm_response(raw_text, response_model)
                elapsed_ms = int((time.time() - start_time) * 1000)

                # Calculate cost
                from src.arena.accountant import calculate_cost
                cumulative.estimated_cost_usd = calculate_cost(cumulative)

                logger.info(
                    f"[{agent_name}] ✓ {cumulative.input_tokens}in+"
                    f"{cumulative.output_tokens}out tokens, "
                    f"${cumulative.estimated_cost_usd:.6f}, {elapsed_ms}ms"
                )
                return parsed, cumulative

            except (ValidationError, json.JSONDecodeError, ValueError) as e:
                last_error = str(e)
                logger.warning(f"[{agent_name}] Parse attempt {attempt+1} failed: {last_error[:200]}")

                if attempt == max_retries:
                    raise LLMParseError(
                        agent_name=agent_name,
                        model_id=model_id,
                        raw_text=raw_text,
                        error=last_error,
                    )

    # Should never reach here
    raise LLMParseError(agent_name, model_id, "", "Unexpected exit from retry loop")


# ═══════════════════════════════════════════════════════════════
# HEALTH CHECK
# ═══════════════════════════════════════════════════════════════

async def check_llm_health() -> dict:
    """Quick health check — sends a minimal prompt and checks response.
    
    Used by /api/arena/health to verify API connectivity.
    Uses Haiku (cheapest) for minimal cost.
    """
    try:
        client = _get_client()
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{"role": "user", "content": "Say OK"}],
        )
        return {
            "status": "healthy",
            "model": response.model,
            "test_response": response.content[0].text,
        }
    except LLMConfigError as e:
        return {"status": "unconfigured", "error": str(e)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


# ═══════════════════════════════════════════════════════════════
# MODEL DISCOVERY — User Rule #13: never hardcode model names
# ═══════════════════════════════════════════════════════════════

async def list_available_models() -> list[dict]:
    """Fetch available Anthropic models from the API.
    
    Implements User Rule #13: ping the API server, don't hardcode model names.
    Returns models with display names suitable for frontend dropdown.
    """
    try:
        client = _get_client()
        response = await client.models.list()
        models = []
        for m in response.data:
            # Categorize by tier for UI display
            model_id = m.id
            if "opus" in model_id:
                tier = "Opus (Most Capable)"
                default_role = "commander"
            elif "sonnet" in model_id:
                tier = "Sonnet (Balanced)"
                default_role = "strategist, consultant"
            elif "haiku" in model_id:
                tier = "Haiku (Fastest)"
                default_role = "analyst, pm"
            else:
                tier = "Other"
                default_role = "any"

            models.append({
                "id": model_id,
                "display_name": getattr(m, "display_name", model_id),
                "tier": tier,
                "default_role": default_role,
            })
        return models
    except Exception as e:
        logger.error(f"[LLMClient] Failed to list models: {e}")
        # Return known models as fallback
        return [
            {"id": "claude-opus-4-6-20251001",   "display_name": "Claude Opus 4.6",   "tier": "Opus (Most Capable)",   "default_role": "commander"},
            {"id": "claude-sonnet-4-6-20251001",  "display_name": "Claude Sonnet 4.6", "tier": "Sonnet (Balanced)",      "default_role": "strategist, consultant"},
            {"id": "claude-haiku-4-5-20251001",   "display_name": "Claude Haiku 4.5",  "tier": "Haiku (Fastest)",        "default_role": "analyst, pm"},
        ]


# ═══════════════════════════════════════════════════════════════
# MODEL CONFIGURATION — Per-role selection with A/B support
# ═══════════════════════════════════════════════════════════════

# Default model config (Config A)
DEFAULT_MODEL_CONFIG: dict[str, str] = {
    "commander":   "claude-opus-4-6-20251001",
    "strategist":  "claude-sonnet-4-6-20251001",
    "consultant":  "claude-sonnet-4-6-20251001",
    "analyst":     "claude-haiku-4-5-20251001",
    "pm":          "claude-haiku-4-5-20251001",
}

# Cheaper config for A/B testing or circuit breaker downgrade (Config B)
HAIKU_MODEL_CONFIG: dict[str, str] = {
    "commander":   "claude-sonnet-4-6-20251001",  # Downgraded from Opus
    "strategist":  "claude-haiku-4-5-20251001",
    "consultant":  "claude-haiku-4-5-20251001",
    "analyst":     "claude-haiku-4-5-20251001",
    "pm":          "claude-haiku-4-5-20251001",
}
