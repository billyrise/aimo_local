# LLM Error Handling Policy

## Error Classification

### Permanent Errors (Do Not Retry)

These errors indicate a fundamental problem that retrying won't fix:

| Error Type | Description | Action |
|------------|-------------|--------|
| `invalid_request_error` | Malformed request, invalid model | Mark `status='skipped'` |
| `context_length_exceeded` | Input too long for model | Mark `status='skipped'`, log for investigation |
| `invalid_api_key` | API key missing/invalid | Mark `status='skipped'`, trigger ops alert |
| `authentication_error` | Auth failure | Mark `status='skipped'`, trigger ops alert |
| `json_schema_error` | LLM output doesn't match schema after retry | Mark `status='needs_review'` |

### Transient Errors (Retry with Backoff)

These errors may resolve on retry:

| Error Type | Description | Retry Policy |
|------------|-------------|--------------|
| `rate_limit_error` / 429 | Rate limit exceeded | Exponential backoff, respect `Retry-After` header |
| `timeout` | Request timed out | Retry up to max_retries |
| `network_error` | Connection failed | Retry up to max_retries |
| `server_error` / 5xx | Provider server error | Retry up to max_retries |

## Retry Policy

```python
RETRY_CONFIG = {
    "max_retries": 2,
    "initial_delay_seconds": 1.0,
    "exponential_base": 2.0,
    "jitter_max_ms": 300,
}

def calculate_delay(attempt: int) -> float:
    base_delay = RETRY_CONFIG["initial_delay_seconds"] * (
        RETRY_CONFIG["exponential_base"] ** (attempt - 1)
    )
    jitter = random.uniform(0, RETRY_CONFIG["jitter_max_ms"] / 1000)
    return base_delay + jitter
```

## JSON Validation Failure

When LLM returns syntactically valid JSON that fails schema validation:

1. **First attempt**: Parse error, extract specific validation failure
2. **Retry once**: Send `JSON_RETRY_PROMPT` with:
   - Original samples
   - Schema
   - Specific error message
3. **If still fails**: Mark as `status='needs_review'`

## Status Transitions

```
                    ┌─────────────┐
                    │   active    │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │ skipped  │    │ analyzed │    │needs_review│
    └──────────┘    └──────────┘    └──────────┘
         │               │               │
         │               │               │
         ▼               ▼               ▼
    (never resend)  (cached)      (human queue)
```

## Database Recording

For each error:

```sql
UPDATE analysis_cache SET
    status = 'skipped' | 'needs_review',
    error_type = :error_type,
    error_reason = :error_message,
    failure_count = failure_count + 1,
    last_error_at = CURRENT_TIMESTAMP,
    retry_after = :retry_after  -- NULL for permanent errors
WHERE url_signature = :sig;
```

## Alerting

Trigger operational alerts for:
- `authentication_error`: Immediate (API key issue)
- `rate_limit_error` > 10 in 5 minutes: Warning (may need to reduce batch size)
- `skipped` signatures > 5% of batch: Warning (review prompt/schema)

## Recovery

For `needs_review` signatures:
1. Human analyst reviews in dashboard
2. Can manually set classification and `is_human_verified=true`
3. Once verified, signature is never re-sent to LLM
