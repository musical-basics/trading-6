# Plan 7: LLM Call Wrapper & Structured Output Engine (`src/arena/llm_client.py`)

## Objective
Build a robust, reusable LLM client that handles all agent-to-Anthropic-API communication. This is the single point of contact between the swarm and the LLM provider. It handles:
1. Structured output parsing (JSON → Pydantic)
2. Retry logic with error injection
3. Token usage tracking per call
4. Rate limiting and concurrency control
5. Model selection and A/B testing support

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Agent Function                           │
│  _run_strategist(desk_id, insight, directive) → Rec          │
└──────────────────────┬───────────────────────────────────────┘
                       │ calls
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                   call_llm()                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │ Build       │→ │ Anthropic   │→ │ Parse + Validate     │ │
│  │ Messages    │  │ API Call    │  │ (Pydantic V2)        │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
│                        ↓ failure                             │
│              ┌─────────────────────┐                         │
│              │ Retry with error    │                         │
│              │ feedback injected   │                         │
│              │ (max 2 retries)     │                         │
│              └─────────────────────┘                         │
│                        ↓                                     │
│              ┌─────────────────────┐                         │
│              │ TokenUsage returned │                         │
│              │ (cumulative across  │                         │
│              │  all attempts)      │                         │
│              └─────────────────────┘                         │
└──────────────────────────────────────────────────────────────┘
```

## Core Function

```python
async def call_llm(
    system_prompt: str,
    user_message: str,
    response_model: type[T],
    agent_name: str,
    model_id: str = "claude-sonnet-4-6",
    max_retries: int = 2,
    temperature: float = 0.3,
    max_tokens: int = 1024,
) -> tuple[T, TokenUsage]:
    """Call an LLM and parse the response into a Pydantic model.
    
    Args:
        system_prompt: System prompt for the agent
        user_message: Formatted data payload
        response_model: Pydantic V2 model class to parse output into
        agent_name: Identifier for cost tracking (e.g., "strategist_d1")
        model_id: Anthropic model ID
        max_retries: Number of retry attempts on parse failure
        temperature: LLM temperature (lower = more deterministic)
        max_tokens: Max output tokens
    
    Returns:
        (parsed_model, cumulative_token_usage)
    
    Raises:
        LLMParseError: If all retries exhausted
        LLMAPIError: If Anthropic API returns a non-retryable error
    """
```

## Implementation Details

### 1. Anthropic Client Setup

```python
import anthropic

# Singleton async client (reuses connection pool)
_client: anthropic.AsyncAnthropic | None = None

def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise LLMConfigError("ANTHROPIC_API_KEY not set")
        _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client
```

### 2. Response Parsing Pipeline

```python
def _parse_llm_response(raw_text: str, model: type[T]) -> T:
    """Parse LLM text output into a Pydantic model.
    
    Handles common LLM output quirks:
    1. Strips markdown code fences (```json ... ```)
    2. Strips leading/trailing whitespace
    3. Handles single vs double quotes
    4. Passes through Pydantic V2 model_validate_json()
    """
    cleaned = raw_text.strip()
    
    # Strip markdown fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Remove first and last lines (fence markers)
        cleaned = "\n".join(lines[1:-1]) if len(lines) > 2 else cleaned
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()
    
    return model.model_validate_json(cleaned)
```

### 3. Retry Logic with Error Injection

When parsing fails, the retry injects the error message back to the LLM:

```python
# On first failure:
retry_user_message = f"""Your previous response was invalid JSON.
Error: {str(validation_error)}

Original request:
{user_message}

Output ONLY valid JSON matching the schema. No markdown. No explanation."""
```

This is critical because LLMs often wrap JSON in markdown fences or add preamble text. The error feedback helps them self-correct.

### 4. Token Usage Accumulation

```python
# Across retries, tokens are SUMMED (not replaced)
cumulative = TokenUsage(model_id=model_id)

for attempt in range(max_retries + 1):
    response = await client.messages.create(...)
    
    cumulative.input_tokens += response.usage.input_tokens
    cumulative.output_tokens += response.usage.output_tokens
    
    try:
        parsed = _parse_llm_response(response.content[0].text, response_model)
        break
    except ValidationError:
        if attempt == max_retries:
            raise LLMParseError(...)

# Cost is calculated from cumulative tokens (retries are expensive!)
cumulative.estimated_cost_usd = calculate_cost(cumulative)
```

### 5. Concurrency Control

```python
# Global semaphore to prevent overwhelming the API
_api_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent LLM calls

async def call_llm(...):
    async with _api_semaphore:
        # ... actual API call
```

This is important because Phase 1 (3 consultants) and Phase 3 (3 desks × 3 agents) can generate up to 9 concurrent calls. Anthropic has rate limits.

### 6. Logging

Every call logs:
```python
logger.info(f"[{agent_name}] Calling {model_id} "
            f"(system: {len(system_prompt)} chars, user: {len(user_message)} chars)")
logger.info(f"[{agent_name}] Response: {in_tok}in + {out_tok}out tokens, "
            f"${cost:.4f}, {elapsed_ms}ms")
```

## Custom Exceptions

```python
class LLMError(Exception):
    """Base exception for LLM operations."""

class LLMParseError(LLMError):
    """LLM output could not be parsed into expected schema."""
    def __init__(self, agent_name: str, model_id: str, raw_text: str, error: str):
        self.agent_name = agent_name
        self.raw_text = raw_text

class LLMAPIError(LLMError):
    """Anthropic API returned an error."""

class LLMConfigError(LLMError):
    """Missing configuration (API key, etc.)."""

class LLMRateLimitError(LLMError):
    """API rate limit exceeded."""
```

## Model A/B Testing Support (User Rule #16)

The LLM client supports model overrides per agent role:

```python
# In orchestrator, model selection:
model_config = {
    "commander": "claude-opus-4-6",
    "strategist": "claude-sonnet-4-6",
    "analyst": "claude-haiku-4-5",
    "pm": "claude-haiku-4-5",
    "consultant": "claude-sonnet-4-6",
}

# When user wants to A/B test:
model_config_b = {
    "commander": "claude-sonnet-4-6",  # Cheaper commander
    # ... same for others
}
```

Both configurations coexist as selectable options in the frontend dropdown (not replacing each other).

## Health Check

```python
async def check_llm_health() -> dict:
    """Quick health check — sends a minimal prompt and checks response.
    
    Used by /api/arena/health to verify API connectivity.
    """
    try:
        client = _get_client()
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{"role": "user", "content": "Say OK"}],
        )
        return {"status": "healthy", "model": response.model}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## Dependencies
- `anthropic>=0.39` (already in requirements)
- `asyncio` (stdlib)
- Pydantic V2 (already via FastAPI)
- `TokenUsage` from schemas.py
- `calculate_cost` from accountant.py

## Testing Strategy
- Mock `anthropic.AsyncAnthropic.messages.create` for unit tests
- Test retry logic with intentionally malformed JSON responses
- Test token accumulation across retries
- Test semaphore prevents >5 concurrent calls
- Integration test with live API using Haiku (cheapest model)
