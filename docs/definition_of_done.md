# Definition of Done (DoD)

A change or release is considered DONE only when all items below are satisfied.

## Functional correctness
1. **Deterministic signatures**: same `signature_version` and same normalized URL input always yields same `url_signature`.
2. **Idempotent reruns**: re-running a run with identical `run_key` produces identical aggregates and does not double-count in DuckDB.
3. **Candidate selection integrity**: A/B/C logic includes:
   - A: high-volume threshold
   - B: high-risk small including burst/cumulative triggers
   - C: deterministic coverage sampling from low-volume band
4. **Report completeness**:
   - includes audit narrative (A/B/C counts, sample rate, seed, exclusions)
   - includes top Shadow AI apps, high-risk users, department rollups, time series

## Privacy and security
1. **PII outbound = 0**: outbound LLM payloads contain no user_id, src_ip, device_id, raw log lines, or unredacted URL PII.
2. **PII redaction**: PII-like tokens in URL path/query are redacted, and entries are logged in `pii_audit`.

## Reliability
1. **Crash-safe output**: any file output is atomic (tmp + rename).
2. **Checkpoint resume**: if a stage fails, rerun resumes from the last completed stage.
3. **Single-writer DuckDB**: all DB writes are serialized via writer queue.

## Performance
1. Parquet is Hive-partitioned: `vendor=<v>/date=<YYYY-MM-DD>/...`.
2. Excel generation uses constant-memory mode and streams data in chunks.
3. LLM is called only for unknown signatures and respects budget controls.

## Evidence
- Unit tests and smoke tests exist for changed behavior.
- Metrics are recorded for each stage: duration, rows/sec, unknown rate, cache hit rate, estimated LLM cost.
