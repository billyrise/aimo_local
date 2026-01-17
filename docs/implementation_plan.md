# Implementation Plan (AIMO Analysis Engine)

This plan is the canonical build order for Cursor and human reviewers.

## Phase 0: Project bootstrap
**Deliverables**
- `src/` skeleton (orchestrator, db client, config loader)
- `.env.example`
- logging setup (JSONL)

**Acceptance**
- `python -m src.main --help` works
- config loads and validates

## Phase 1: Run orchestration + DuckDB schema
**Deliverables**
- `db/schema.sql` (tables: runs, input_files, analysis_cache, signature_stats, api_costs, performance_metrics, pii_audit)
- migrations scaffolding
- run locking (file lock) + stage checkpoints

**Acceptance**
- create run, mark stages, resume from checkpoint
- no concurrent runs allowed

## Phase 2: Vendor ingestion (one vendor end-to-end)
**Deliverables**
- one vendor ingestor implemented using mapping file in `schemas/vendors/<vendor>/...`
- canonical event output in Parquet (Hive partitioned)

**Acceptance**
- ingests sample logs and produces canonical events with required columns
- stable time parsing and bytes mapping

## Phase 3: URL normalization + PII detection + signature generation
**Deliverables**
- deterministic normalization per `config/url_normalization.yml`
- PII detection and redaction with audit log
- signature builder that writes `url_signature` and signature metadata

**Acceptance**
- normalization tests cover common and adversarial cases
- same input always yields same signature

## Phase 4: Candidate selection (A/B/C + burst + cumulative)
**Deliverables**
- deterministic sampling seeded by `run_id`
- A/B/C labels with counts and audit metadata

**Acceptance**
- small-volume events are not zero-excluded
- sample rate and seed recorded

## Phase 5: Cache lookup + unknown signature set
**Deliverables**
- join against `analysis_cache` by `url_signature`
- unknown set is exactly those without active cached classification

**Acceptance**
- cache hit ratio computed and recorded

## Phase 6: LLM analyzer
**Deliverables**
- structured output using `llm/schemas/analysis_output.schema.json` (or equivalent)
- JSON schema validation, retry policy, and permanent skip logic
- budget control (token bucket)
- writer queue single-writer UPSERT into DuckDB

**Acceptance**
- invalid JSON is retried up to policy
- permanent failures are marked skipped and not resent
- PII never appears in outgoing payloads (unit test)

## Phase 7: Reporting
**Deliverables**
- Excel report (constant-memory, aggregated sheets)
- Dashboard JSON
- Sanitized export CSV
- Audit narrative section (A/B/C, exclusions, sample rate, seed, LLM scope)

**Acceptance**
- report builds on sample data
- narrative includes required audit items

## Phase 8: Operationalization
**Deliverables**
- launchd plist
- runbook + audit checklist

**Acceptance**
- scheduled run executes end-to-end on a test dataset
