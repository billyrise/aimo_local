-- AIMO Analysis Engine Database Schema (DuckDB)
-- Version: 1.6
-- 
-- This schema defines all core tables for the AIMO engine.
-- DuckDB supports most standard SQL with some extensions.
-- Reference: https://duckdb.org/docs/sql/statements/create_table
--
-- AIMO Standard Taxonomy (v0.1.1+):
-- - 8 dimensions: FS, UC, DT, CH, IM, RS, OB, LG
-- - Cardinality: FS=1, IM=1, UC/DT/CH/RS/LG=1+, OB=0+
-- - Array columns stored as canonical JSON: sorted, deduplicated

--------------------------------------------------------------------------------
-- RUNS: Execution tracking and idempotency
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    run_key VARCHAR NOT NULL,                    -- Deterministic key for idempotency
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL DEFAULT 'running',   -- running/succeeded/failed/partial
    last_completed_stage INTEGER DEFAULT 0,      -- Checkpoint for resume
    
    -- Scope
    target_range_start DATE,
    target_range_end DATE,
    vendor_scope VARCHAR,                        -- Comma-separated vendor list or 'all'
    
    -- Versioning (for reproducibility)
    code_version VARCHAR,                        -- Git commit hash
    signature_version VARCHAR NOT NULL,
    rule_version VARCHAR NOT NULL,
    prompt_version VARCHAR NOT NULL,
    taxonomy_version VARCHAR,                    -- Taxonomy version (for Taxonomyセット)
    evidence_pack_version VARCHAR,               -- Evidence Pack version
    engine_spec_version VARCHAR,                 -- Engine spec version (v1.5)
    psl_hash VARCHAR,                            -- Public Suffix List hash
    
    -- AIMO Standard versioning (required for audit reproducibility)
    aimo_standard_version VARCHAR,               -- e.g., "0.1.1"
    aimo_standard_commit VARCHAR,                -- Full git commit hash of Standard
    aimo_standard_artifacts_dir_sha256 VARCHAR,  -- SHA256 of artifacts directory
    aimo_standard_artifacts_zip_sha256 VARCHAR,  -- SHA256 of artifacts zip (if exists)
    
    -- Input tracking
    input_manifest_hash VARCHAR NOT NULL,        -- Hash of all input files
    
    -- Metrics summary
    total_events BIGINT DEFAULT 0,
    unique_signatures BIGINT DEFAULT 0,
    cache_hit_count BIGINT DEFAULT 0,
    llm_sent_count BIGINT DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);

--------------------------------------------------------------------------------
-- INPUT_FILES: Track ingested files for idempotency
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS input_files (
    file_id VARCHAR PRIMARY KEY,                 -- sha256 of (path + size + mtime), used as PK per spec 9.3
    run_id VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    file_size BIGINT NOT NULL,
    file_hash VARCHAR NOT NULL,                  -- sha256 of file content (for deduplication)
    
    -- Metadata
    vendor VARCHAR NOT NULL,
    log_type VARCHAR,
    
    -- Time range in file
    min_time TIMESTAMP,
    max_time TIMESTAMP,
    
    -- Stats
    row_count BIGINT DEFAULT 0,
    parse_error_count BIGINT DEFAULT 0,
    
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
    -- FOREIGN KEY (run_id) REFERENCES runs(run_id)  -- Removed for test stability
);

CREATE INDEX IF NOT EXISTS idx_input_files_run ON input_files(run_id);
CREATE INDEX IF NOT EXISTS idx_input_files_hash ON input_files(file_hash);

--------------------------------------------------------------------------------
-- ANALYSIS_CACHE: Service classification cache (core for cost reduction)
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analysis_cache (
    url_signature VARCHAR PRIMARY KEY,
    
    -- Classification
    service_name VARCHAR,
    usage_type VARCHAR,                          -- business/genai/devtools/storage/social/unknown
    risk_level VARCHAR,                          -- low/medium/high
    category VARCHAR,
    
    -- Confidence and rationale
    confidence DOUBLE,
    rationale_short VARCHAR,                     -- Max 400 chars, rationale from LLM or rule notes
    classification_source VARCHAR,               -- RULE/LLM/HUMAN
    
    -- Versioning
    signature_version VARCHAR,
    rule_version VARCHAR,
    prompt_version VARCHAR,
    taxonomy_version VARCHAR,                    -- Taxonomy version (legacy, use taxonomy_schema_version)
    model VARCHAR,                               -- LLM model used (if applicable)
    
    -- AIMO Standard Taxonomy (8 dimensions, v0.1.7+)
    -- Single-value dimensions (exactly 1)
    fs_code VARCHAR,                             -- FS: Functional Scope (exactly 1)
    im_code VARCHAR,                             -- IM: Integration Mode (exactly 1)
    -- Array dimensions (stored as canonical JSON: sorted, deduplicated)
    uc_codes_json VARCHAR NOT NULL DEFAULT '[]', -- UC: Use Case Class (1+)
    dt_codes_json VARCHAR NOT NULL DEFAULT '[]', -- DT: Data Type (1+)
    ch_codes_json VARCHAR NOT NULL DEFAULT '[]', -- CH: Channel (1+)
    rs_codes_json VARCHAR NOT NULL DEFAULT '[]', -- RS: Risk Surface (1+)
    ev_codes_json VARCHAR NOT NULL DEFAULT '[]', -- LG: Log/Event Type (1+), column name kept for compatibility
    ob_codes_json VARCHAR NOT NULL DEFAULT '[]', -- OB: Outcome/Benefit (0+, optional)
    taxonomy_schema_version VARCHAR,             -- AIMO Standard version used (e.g., "0.1.1")
    
    -- Legacy taxonomy columns (deprecated, kept for backward compatibility)
    fs_uc_code VARCHAR,                          -- DEPRECATED: Use fs_code
    dt_code VARCHAR,                             -- DEPRECATED: Use dt_codes_json
    ch_code VARCHAR,                             -- DEPRECATED: Use ch_codes_json
    rs_code VARCHAR,                             -- DEPRECATED: Use rs_codes_json
    ob_code VARCHAR,                             -- DEPRECATED: Use ob_codes_json
    ev_code VARCHAR,                             -- DEPRECATED: Use ev_codes_json (stores first LG code)
    
    -- Status tracking
    status VARCHAR DEFAULT 'active',             -- active/needs_review/skipped/failed_permanent
    is_human_verified BOOLEAN DEFAULT FALSE,     -- If true, never overwrite
    
    -- Error tracking
    error_type VARCHAR,
    error_reason VARCHAR,
    retry_after TIMESTAMP,
    failure_count INTEGER DEFAULT 0,
    last_error_at TIMESTAMP,
    
    -- Timestamps
    analysis_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cache_status ON analysis_cache(status);
CREATE INDEX IF NOT EXISTS idx_cache_usage_type ON analysis_cache(usage_type);
CREATE INDEX IF NOT EXISTS idx_cache_updated ON analysis_cache(updated_at);
CREATE INDEX IF NOT EXISTS idx_cache_human_verified ON analysis_cache(is_human_verified);

--------------------------------------------------------------------------------
-- SIGNATURE_STATS: Per-run statistics for each signature
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS signature_stats (
    run_id VARCHAR NOT NULL,
    url_signature VARCHAR NOT NULL,
    
    -- Domain info
    norm_host VARCHAR,
    norm_path_template VARCHAR,
    dest_domain VARCHAR,                         -- eTLD+1
    
    -- Bucket
    bytes_sent_bucket VARCHAR,                   -- T/L/M/H/X (T=tiny, not candidate C)
    
    -- Counts
    access_count BIGINT DEFAULT 0,
    unique_users BIGINT DEFAULT 0,
    
    -- Bytes aggregates
    bytes_sent_sum BIGINT DEFAULT 0,
    bytes_sent_max BIGINT DEFAULT 0,
    bytes_sent_p95 BIGINT,
    bytes_received_sum BIGINT DEFAULT 0,
    
    -- Risk indicators
    burst_max_5min INTEGER DEFAULT 0,            -- Max events in any 5-min window
    cumulative_user_domain_day_max BIGINT,       -- Max daily bytes per user×domain
    
    -- Candidate selection
    candidate_flags VARCHAR,                     -- A/B/C flags
    sampled BOOLEAN DEFAULT FALSE,               -- Part of C sample
    
    -- AIMO Standard Taxonomy (8 dimensions, v0.1.7+)
    -- Single-value dimensions (exactly 1)
    fs_code VARCHAR,                             -- FS: Functional Scope (exactly 1)
    im_code VARCHAR,                             -- IM: Integration Mode (exactly 1)
    -- Array dimensions (stored as canonical JSON: sorted, deduplicated)
    uc_codes_json VARCHAR NOT NULL DEFAULT '[]', -- UC: Use Case Class (1+)
    dt_codes_json VARCHAR NOT NULL DEFAULT '[]', -- DT: Data Type (1+)
    ch_codes_json VARCHAR NOT NULL DEFAULT '[]', -- CH: Channel (1+)
    rs_codes_json VARCHAR NOT NULL DEFAULT '[]', -- RS: Risk Surface (1+)
    ev_codes_json VARCHAR NOT NULL DEFAULT '[]', -- LG: Log/Event Type (1+), column name kept for compatibility
    ob_codes_json VARCHAR NOT NULL DEFAULT '[]', -- OB: Outcome/Benefit (0+, optional)
    taxonomy_schema_version VARCHAR,             -- AIMO Standard version used (e.g., "0.1.1")
    
    -- Legacy taxonomy columns (deprecated, kept for backward compatibility)
    fs_uc_code VARCHAR,                          -- DEPRECATED: Use fs_code
    dt_code VARCHAR,                             -- DEPRECATED: Use dt_codes_json
    ch_code VARCHAR,                             -- DEPRECATED: Use ch_codes_json
    rs_code VARCHAR,                             -- DEPRECATED: Use rs_codes_json
    ob_code VARCHAR,                             -- DEPRECATED: Use ob_codes_json
    ev_code VARCHAR,                             -- DEPRECATED: Use ev_codes_json (stores first LG code)
    taxonomy_version VARCHAR,                    -- DEPRECATED: Use taxonomy_schema_version
    
    -- Time range
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    
    PRIMARY KEY (run_id, url_signature)
    -- FOREIGN KEY (run_id) REFERENCES runs(run_id)  -- Removed for test stability
);

CREATE INDEX IF NOT EXISTS idx_sigstats_sig ON signature_stats(url_signature);
CREATE INDEX IF NOT EXISTS idx_sigstats_run ON signature_stats(run_id);
CREATE INDEX IF NOT EXISTS idx_sigstats_domain ON signature_stats(dest_domain);

--------------------------------------------------------------------------------
-- API_COSTS: LLM API cost tracking
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api_costs (
    id INTEGER PRIMARY KEY,                      -- Auto-increment via sequence
    run_id VARCHAR NOT NULL,
    
    -- Provider info
    provider VARCHAR NOT NULL,                   -- openai/azure_openai/anthropic
    model VARCHAR NOT NULL,
    
    -- Token counts
    request_count INTEGER DEFAULT 1,
    input_tokens BIGINT DEFAULT 0,
    output_tokens BIGINT DEFAULT 0,
    
    -- Cost
    cost_usd_estimated DOUBLE DEFAULT 0.0,
    
    -- Performance
    latency_ms BIGINT,
    
    -- Timestamp
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
    -- FOREIGN KEY (run_id) REFERENCES runs(run_id)  -- Removed for test stability
);

CREATE INDEX IF NOT EXISTS idx_apicosts_run ON api_costs(run_id);
CREATE INDEX IF NOT EXISTS idx_apicosts_recorded ON api_costs(recorded_at);

-- Create sequence for api_costs id
CREATE SEQUENCE IF NOT EXISTS seq_api_costs_id START 1;

--------------------------------------------------------------------------------
-- PERFORMANCE_METRICS: Stage-level performance tracking
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS performance_metrics (
    id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    
    -- Stage info
    stage VARCHAR NOT NULL,                      -- ingest/normalize/select/sign/llm/report
    metric_name VARCHAR NOT NULL,                -- duration_ms/rows_per_sec/memory_mb/etc
    
    -- Value
    value DOUBLE NOT NULL,
    unit VARCHAR,                                -- ms/rows/bytes/etc
    
    -- Timestamps
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
    -- FOREIGN KEY (run_id) REFERENCES runs(run_id)  -- Removed for test stability
);

CREATE INDEX IF NOT EXISTS idx_perfmetrics_run ON performance_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_perfmetrics_stage ON performance_metrics(stage);

-- Create sequence for performance_metrics id
CREATE SEQUENCE IF NOT EXISTS seq_perf_metrics_id START 1;

--------------------------------------------------------------------------------
-- PII_AUDIT: Track PII detection and redaction
--------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pii_audit (
    id INTEGER PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    
    -- Detection info
    url_signature VARCHAR,
    pii_type VARCHAR NOT NULL,                   -- email/ipv4/uuid/token/etc
    field_source VARCHAR,                        -- url_path/url_query/user_id/etc
    
    -- Redaction
    redaction_applied VARCHAR,                   -- The replacement token (:email, :ip, etc)
    original_hash VARCHAR,                       -- sha256 of original value (for audit)
    
    -- Count
    occurrence_count BIGINT DEFAULT 1,
    
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
    -- FOREIGN KEY (run_id) REFERENCES runs(run_id)  -- Removed for test stability
);

CREATE INDEX IF NOT EXISTS idx_piiaudit_run ON pii_audit(run_id);
CREATE INDEX IF NOT EXISTS idx_piiaudit_type ON pii_audit(pii_type);

-- Create sequence for pii_audit id
CREATE SEQUENCE IF NOT EXISTS seq_pii_audit_id START 1;

--------------------------------------------------------------------------------
-- VIEWS: Convenience views for common queries
--------------------------------------------------------------------------------

-- Summary of runs with key metrics
CREATE OR REPLACE VIEW v_run_summary AS
SELECT 
    r.run_id,
    r.started_at,
    r.finished_at,
    r.status,
    r.vendor_scope,
    r.total_events,
    r.unique_signatures,
    r.cache_hit_count,
    r.llm_sent_count,
    COALESCE(SUM(ac.cost_usd_estimated), 0) as total_cost_usd,
    COUNT(DISTINCT if.file_id) as file_count
FROM runs r
LEFT JOIN api_costs ac ON r.run_id = ac.run_id
LEFT JOIN input_files if ON r.run_id = if.run_id
GROUP BY r.run_id, r.started_at, r.finished_at, r.status, 
         r.vendor_scope, r.total_events, r.unique_signatures,
         r.cache_hit_count, r.llm_sent_count;

-- Cache health check
CREATE OR REPLACE VIEW v_cache_status AS
SELECT 
    status,
    usage_type,
    COUNT(*) as count,
    AVG(confidence) as avg_confidence,
    SUM(CASE WHEN is_human_verified THEN 1 ELSE 0 END) as human_verified_count
FROM analysis_cache
GROUP BY status, usage_type;

-- GenAI detection summary
CREATE OR REPLACE VIEW v_genai_summary AS
SELECT 
    service_name,
    risk_level,
    COUNT(*) as signature_count,
    AVG(confidence) as avg_confidence
FROM analysis_cache
WHERE usage_type = 'genai' AND status = 'active'
GROUP BY service_name, risk_level
ORDER BY signature_count DESC;
