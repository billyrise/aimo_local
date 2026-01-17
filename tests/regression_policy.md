# Regression Testing Policy

## Purpose

Ensure that changes to AIMO (especially signature version, rules, or normalization) do not unexpectedly degrade classification quality or break reproducibility.

---

## When to Run Regression Tests

| Change Type | Required Tests |
|-------------|----------------|
| `signature_version` bump | Full signature regression |
| `rule_version` bump | Rule classification regression |
| `prompt_version` bump | LLM output sampling check |
| Normalization logic change | URL normalization suite |
| Candidate selection change | A/B/C distribution check |

---

## Signature Regression Test

### Baseline

Maintain a golden set of 100 representative URL signatures:
- 20 GenAI services
- 20 Business SaaS
- 20 Storage services
- 20 DevTools
- 20 Unknown/Other

### Test Procedure

```python
def test_signature_regression():
    """Verify signature stability across version changes."""
    golden = load_golden_signatures("tests/fixtures/golden_signatures.json")
    
    matches = 0
    for item in golden:
        new_sig = generate_signature(item["url"], item["method"], item["bytes_bucket"])
        if new_sig == item["expected_signature"]:
            matches += 1
    
    match_rate = matches / len(golden)
    assert match_rate >= 0.98, f"Signature match rate {match_rate:.2%} below threshold"
```

### Acceptance Criteria

- **Pass**: ≥ 98% of golden signatures unchanged
- **Warning**: 95-98% match rate → review changes
- **Fail**: < 95% match rate → requires explicit approval

---

## Rule Classification Regression

### Test Procedure

```python
def test_rule_classification():
    """Verify rule-based classification accuracy."""
    test_cases = load_test_cases("tests/fixtures/rule_test_cases.json")
    
    correct = 0
    for case in test_cases:
        result = apply_rules(case["domain"], case["path"])
        if result["service_name"] == case["expected_service"]:
            correct += 1
    
    accuracy = correct / len(test_cases)
    assert accuracy >= 0.95, f"Rule accuracy {accuracy:.2%} below threshold"
```

### Acceptance Criteria

- **Pass**: ≥ 95% accuracy on test cases
- **Fail**: < 95% requires rule review

---

## A/B/C Distribution Check

When thresholds change, verify distribution impact:

```python
def test_abc_distribution():
    """Verify A/B/C candidate distribution is reasonable."""
    events = load_sample_events("tests/fixtures/sample_events.parquet")
    result = select_candidates(events)
    
    total = len(events)
    a_pct = len(result["A"]) / total
    b_pct = len(result["B"]) / total
    c_pct = len(result["C"]) / total
    
    # Sanity checks (adjust based on expected data profile)
    assert 0.001 <= a_pct <= 0.10, f"A candidates {a_pct:.2%} out of expected range"
    assert 0.01 <= b_pct <= 0.20, f"B candidates {b_pct:.2%} out of expected range"
    
    # C should be ~2% of non-A, non-B
    remaining = total - len(result["A"]) - len(result["B"])
    expected_c = remaining * 0.02
    assert abs(len(result["C"]) - expected_c) / expected_c < 0.1, "C sample rate deviation"
```

---

## Change Impact Documentation

When a regression test fails or shows significant change, document:

1. **What changed**: Specific code/config modification
2. **Impact**: Number of signatures/classifications affected
3. **Justification**: Why the change is acceptable
4. **Approval**: Sign-off from project lead

### Template

```markdown
## Regression Impact Report

**Date**: YYYY-MM-DD
**Change**: [description]
**Version**: signature_version X.X → Y.Y

### Impact Summary
- Affected signatures: N
- Match rate: XX.X%
- Classification changes: N

### Changed Classifications
| Domain | Old Classification | New Classification |
|--------|-------------------|-------------------|
| example.com | business | devtools |

### Justification
[Explanation of why this change is correct/acceptable]

### Approval
- [ ] Reviewed by: [name]
- [ ] Approved by: [name]
```

---

## Golden Data Maintenance

### Update Frequency

- Review golden signatures quarterly
- Add new services as they become significant
- Remove deprecated/defunct services

### Update Process

1. Propose changes in PR
2. Run regression with new golden set
3. Document any expected changes
4. Get approval before merge

---

## CI/CD Integration

```yaml
# .github/workflows/regression.yml
name: Regression Tests

on:
  push:
    paths:
      - 'config/url_normalization.yml'
      - 'config/bytes_buckets.yml'
      - 'rules/**'
      - 'src/normalize/**'
      - 'src/signatures/**'

jobs:
  regression:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run regression tests
        run: pytest tests/regression/ -v --tb=short
      - name: Upload regression report
        uses: actions/upload-artifact@v4
        with:
          name: regression-report
          path: tests/output/regression_report.html
```
