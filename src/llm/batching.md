# LLM Batching Strategy

## Overview

AIMO batches multiple URL signatures into a single LLM request to:
1. Reduce API call overhead
2. Improve cost efficiency
3. Maintain context coherence

## Batch Size Determination

Each batch is limited by **whichever constraint is reached first**:

| Constraint | Value | Rationale |
|------------|-------|-----------|
| Max signatures | 20 | Keep response manageable for JSON parsing |
| Max sample chars | 8,000 | Reserve tokens for system prompt + schema + output |

## Algorithm

```python
def create_batches(signatures: list[dict]) -> list[list[dict]]:
    batches = []
    current_batch = []
    current_chars = 0
    
    for sig in signatures:
        sig_chars = len(format_signature(sig))
        
        if (len(current_batch) >= MAX_SIGS_PER_REQUEST or
            current_chars + sig_chars > MAX_SAMPLE_CHARS):
            if current_batch:
                batches.append(current_batch)
            current_batch = [sig]
            current_chars = sig_chars
        else:
            current_batch.append(sig)
            current_chars += sig_chars
    
    if current_batch:
        batches.append(current_batch)
    
    return batches
```

## Token Estimation

Before sending a batch, estimate token cost:

```
input_tokens ≈ (system_prompt_tokens + schema_tokens + sample_chars / 4)
output_tokens ≈ signatures_count × 150  # ~150 tokens per classification
```

## Retry Policy

| Attempt | Delay | Notes |
|---------|-------|-------|
| 1 | 0s | Initial request |
| 2 | 1s + jitter(0-300ms) | First retry |
| 3 | 2s + jitter(0-300ms) | Final retry |

After 3 failed attempts, mark signatures as `needs_review`.

## JSON Validation Retry

If the LLM returns invalid JSON:
1. Parse error message
2. Send `JSON_RETRY_PROMPT` with error details
3. Only retry once for JSON issues

## Priority During Budget Exhaustion

When daily budget is nearly exhausted:

1. **A candidates**: Always analyze (high-volume = high risk)
2. **B candidates**: Always analyze (burst/cumulative = suspicious)
3. **C candidates**: Skip (coverage sample can wait)

## Metrics Recorded

For each batch:
- `input_tokens`, `output_tokens`
- `cost_usd_estimated`
- `latency_ms`
- `success_rate` (valid JSON responses / total)
