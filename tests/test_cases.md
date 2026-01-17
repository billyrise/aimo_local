# AIMO Test Cases

## Overview

This document defines the minimum test cases required for AIMO Analysis Engine.
All tests should be automated and run as part of CI/CD.

---

## 1. URL Normalization Tests

### 1.1 Scheme and Port Normalization

| Input | Expected Output | Notes |
|-------|-----------------|-------|
| `https://example.com:443/path` | `example.com/path` | Remove https + default port |
| `http://example.com:80/path` | `example.com/path` | Remove http + default port |
| `https://example.com:8443/path` | `example.com:8443/path` | Keep non-default port |
| `HTTP://EXAMPLE.COM/Path` | `example.com/Path` | Host lowercase, path case preserved |

### 1.2 Path Normalization

| Input | Expected Output | Notes |
|-------|-----------------|-------|
| `example.com//double//slash` | `example.com/double/slash` | Collapse slashes |
| `example.com/path/` | `example.com/path` | Remove trailing slash |
| `example.com/` | `example.com/` | Keep root slash |
| `example.com/a/../b` | `example.com/b` | Resolve relative paths |

### 1.3 Query Normalization

| Input | Expected Output | Notes |
|-------|-----------------|-------|
| `example.com?b=2&a=1` | `example.com?a=1&b=2` | Sort keys |
| `example.com?utm_source=x&id=1` | `example.com?id=1` | Remove tracking params |
| `example.com?gclid=abc&key=val` | `example.com?key=val` | Remove gclid |
| `example.com?a=&b=2` | `example.com?b=2` | Drop empty values |

### 1.4 ID/Token Abstraction

| Input | Expected Output | Notes |
|-------|-----------------|-------|
| `/user/550e8400-e29b-41d4-a716-446655440000` | `/user/:uuid` | UUID |
| `/doc/abc123def456789012345678` | `/doc/:hex` | Long hex |
| `/token/eyJhbGciOiJIUzI1NiIsInR5cCI6` | `/token/:tok` | Base64-like |
| `/user/user@example.com/files` | `/user/:email/files` | Email |
| `/api/192.168.1.100/status` | `/api/:ip/status` | IPv4 |
| `/order/12345678` | `/order/:id` | Numeric ID |

### 1.5 Punycode/IDN

| Input | Expected Output | Notes |
|-------|-----------------|-------|
| `xn--n3h.com` | `xn--n3h.com` | Keep punycode |
| `münchen.de` (if normalized) | `xn--mnchen-3ya.de` | IDN to punycode |

---

## 2. Signature Stability Tests

### 2.1 Determinism

```python
def test_signature_determinism():
    """Same input must always produce same signature."""
    url = "https://api.openai.com/v1/chat/completions"
    sig1 = generate_signature(url, method="POST", bytes_bucket="M")
    sig2 = generate_signature(url, method="POST", bytes_bucket="M")
    assert sig1 == sig2
```

### 2.2 Version Consistency

```python
def test_signature_version():
    """Signatures include version for reproducibility."""
    sig = generate_signature(url, version="1.0")
    assert "1.0" in sig_metadata.signature_version
```

### 2.3 Method Group Impact

```python
def test_method_affects_signature():
    """Different methods produce different signatures."""
    sig_get = generate_signature(url, method="GET")
    sig_post = generate_signature(url, method="POST")
    assert sig_get != sig_post
```

---

## 3. A/B/C Candidate Selection Tests

### 3.1 A Threshold Boundary

| bytes_sent | Expected | Notes |
|------------|----------|-------|
| 1,048,575 | NOT A | 1 byte below 1MB |
| 1,048,576 | A | Exactly 1MB |
| 1,048,577 | A | 1 byte above 1MB |

### 3.2 B Burst Detection

```python
def test_burst_detection():
    """Detect burst: 20+ events in 5-min window."""
    events = [
        {"user": "u1", "domain": "d1", "ts": base_ts + timedelta(seconds=i * 10)}
        for i in range(25)
    ]
    candidates = select_candidates(events)
    assert all(e["candidate_flag"] == "B" for e in events)
```

### 3.3 B Cumulative Detection

```python
def test_cumulative_detection():
    """Detect cumulative: 20MB+ per user×domain×day."""
    events = [
        {"user": "u1", "domain": "d1", "bytes_sent": 1_000_000, "ts": day1}
        for _ in range(25)  # 25MB total
    ]
    candidates = select_candidates(events)
    assert events[0]["candidate_flag"] == "B"
```

### 3.4 C Sampling Reproducibility

```python
def test_c_sampling_reproducibility():
    """C sampling must be reproducible with same seed."""
    events = generate_test_events(1000)  # Non-A, non-B events
    
    c1 = select_c_candidates(events, seed="run_123")
    c2 = select_c_candidates(events, seed="run_123")
    
    assert c1 == c2  # Same seed = same sample
```

### 3.5 Small Volume Not Zero-Excluded

```python
def test_small_volume_coverage():
    """Events with bytes_sent < 1MB must not be zero-excluded."""
    events = [
        {"bytes_sent": 100, "method": "GET", "category": "Business"}
    ]
    candidates = select_candidates(events, sample_rate=0.02)
    
    # Either selected as C or explicitly logged as "not selected"
    assert events[0]["candidate_flag"] in ["C", None]
    # Verify audit log includes count of non-selected
```

---

## 4. Writer Queue Integrity Tests

### 4.1 No Duplicate UPSERT

```python
def test_no_duplicate_upsert():
    """Same signature should not create duplicate rows."""
    db.upsert_analysis(sig="abc", service="Test", confidence=0.9)
    db.upsert_analysis(sig="abc", service="Test2", confidence=0.8)
    
    rows = db.query("SELECT * FROM analysis_cache WHERE url_signature = 'abc'")
    assert len(rows) == 1
    assert rows[0]["service_name"] == "Test2"  # Latest wins
```

### 4.2 Partial Failure Recovery

```python
def test_partial_failure_recovery():
    """After partial failure, re-run should resume correctly."""
    # Simulate failure after stage 3
    run1 = orchestrator.run(fail_after_stage=3)
    assert run1.status == "partial"
    assert run1.last_completed_stage == 3
    
    # Re-run should resume from stage 4
    run2 = orchestrator.run(run_id=run1.run_id)
    assert run2.status == "succeeded"
```

### 4.3 Human Verified Not Overwritten

```python
def test_human_verified_preserved():
    """is_human_verified=true rows should not be overwritten."""
    db.upsert_analysis(sig="abc", service="Human", is_human_verified=True)
    db.upsert_analysis(sig="abc", service="LLM", is_human_verified=False)
    
    row = db.query("SELECT * FROM analysis_cache WHERE url_signature = 'abc'")[0]
    assert row["service_name"] == "Human"  # Human-verified preserved
```

---

## 5. Error Handling Tests

### 5.1 Permanent Error Not Retried

```python
def test_permanent_error_not_retried():
    """Signatures with permanent errors should not be resent."""
    db.update_status(sig="abc", status="skipped", error_type="context_length_exceeded")
    
    pending = db.get_pending_for_llm()
    assert "abc" not in [s["url_signature"] for s in pending]
```

### 5.2 Transient Error Retried

```python
def test_transient_error_retried():
    """Signatures with transient errors should be retried after delay."""
    db.update_status(
        sig="abc", 
        status="active", 
        error_type="rate_limit_error",
        retry_after=datetime.now() + timedelta(seconds=60)
    )
    
    # Before retry_after
    pending = db.get_pending_for_llm(now=datetime.now())
    assert "abc" not in [s["url_signature"] for s in pending]
    
    # After retry_after
    pending = db.get_pending_for_llm(now=datetime.now() + timedelta(seconds=120))
    assert "abc" in [s["url_signature"] for s in pending]
```

---

## 6. PII Detection Tests

### 6.1 No PII in LLM Payload

```python
def test_no_pii_in_llm_payload():
    """LLM payloads must not contain PII."""
    payload = llm.build_payload([
        {"url_signature": "abc", "norm_host": "api.example.com", 
         "user_id": "should_not_appear", "src_ip": "192.168.1.1"}
    ])
    
    assert "should_not_appear" not in json.dumps(payload)
    assert "192.168.1.1" not in json.dumps(payload)
```

### 6.2 PII Audit Recorded

```python
def test_pii_audit_recorded():
    """PII detections must be logged to pii_audit table."""
    normalize_url("/user/test@example.com/files", run_id="run_123")
    
    audits = db.query("SELECT * FROM pii_audit WHERE run_id = 'run_123'")
    assert len(audits) > 0
    assert audits[0]["pii_type"] == "email"
```

---

## 7. Report Generation Tests

### 7.1 Audit Narrative Required Fields

```python
def test_audit_narrative_complete():
    """Audit narrative must include all required fields."""
    report = generate_report(run_id="run_123")
    narrative = report.get_audit_narrative()
    
    required = ["count_a", "count_b", "count_c", "sample_rate", 
                "sample_seed", "signature_version"]
    for field in required:
        assert f"{{{field}}}" not in narrative  # All placeholders replaced
```

### 7.2 Excel Constant Memory

```python
def test_excel_constant_memory():
    """Excel generation should use constant memory mode."""
    # Generate with 1M rows
    with memory_monitor() as mm:
        generate_excel(rows=1_000_000)
    
    # Memory should not scale linearly with row count
    assert mm.peak_mb < 500  # Configurable threshold
```

---

## Running Tests

```bash
# All tests
pytest tests/ -v

# Specific category
pytest tests/test_normalization.py -v
pytest tests/test_candidates.py -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```
