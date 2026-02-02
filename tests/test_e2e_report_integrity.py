"""
E2E Report Integrity Tests

Phase 6 受入ゲート: E2E成果物の整合性チェック

このテストは以下を検証する:
a) report_summary.schema.json に合致
b) rule_hit + unknown_count == total_signatures
c) llm_analyzed_count / needs_review_count / skipped_count の整合

使用方法:
    pytest tests/test_e2e_report_integrity.py -v
    
    # 特定のレポートファイルを検証
    pytest tests/test_e2e_report_integrity.py --report-path=data/output/report_summary.json -v
"""

import json
import pytest
from pathlib import Path
from typing import Dict, Any, Optional
import jsonschema


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def report_schema() -> Dict[str, Any]:
    """Load the report summary schema."""
    schema_path = Path(__file__).parent.parent / "schemas" / "report_summary.schema.json"
    if not schema_path.exists():
        pytest.skip(f"Schema file not found: {schema_path}")
    
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def latest_report_path() -> Optional[Path]:
    """Find the latest report summary JSON in data/output."""
    output_dir = Path(__file__).parent.parent / "data" / "output"
    if not output_dir.exists():
        return None
    
    # Find all *_summary.json files
    report_files = list(output_dir.glob("*_summary.json"))
    if not report_files:
        return None
    
    # Return the most recently modified one
    return max(report_files, key=lambda p: p.stat().st_mtime)


@pytest.fixture
def sample_report(tmp_path: Path, report_schema: Dict[str, Any]) -> Path:
    """Create a sample report for testing (when no real report exists)."""
    report = {
        "run_id": "test_run_001",
        "run_key": "test_run_key_001",
        "started_at": "2024-01-17T10:00:00Z",
        "finished_at": "2024-01-17T10:05:00Z",
        "input_file": "sample_logs/paloalto/normal.csv",
        "vendor": "paloalto",
        "thresholds_used": {
            "A_min_bytes": 10485760,
            "B_burst_count": 100,
            "B_burst_window_seconds": 300,
            "B_cumulative_bytes": 104857600,
            "C_sample_rate": 0.05
        },
        "counts": {
            "total_events": 1000,
            "total_signatures": 100,
            "unique_users": 50,
            "unique_domains": 30,
            "abc_count_a": 5,
            "abc_count_b": 3,
            "abc_count_c": 10,
            "burst_hit": 2,
            "cumulative_hit": 1
        },
        "sample": {
            "sample_rate": 0.05,
            "sample_method": "deterministic_hash",
            "seed": "test_run_001"
        },
        "rule_coverage": {
            "rule_hit": 70,
            "unknown_count": 30
        },
        "llm_coverage": {
            "llm_analyzed_count": 25,
            "needs_review_count": 3,
            "failed_permanent_count": 2,
            "cache_hit_rate": 0.8,
            "skipped_count": 0,
            "llm_provider": "gemini",
            "llm_model": "gemini-2.0-flash",
            "structured_output": True,
            "schema_sanitized": True,
            "retry_summary": {
                "attempts": 3,
                "backoff_ms_total": 1500,
                "last_error_code": None,
                "rate_limit_events": 0
            },
            "rate_limit_events": 0
        },
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "code_version": "test"
    }
    
    report_path = tmp_path / "test_report_summary.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    return report_path


# =============================================================================
# Helper Functions
# =============================================================================

def load_report(report_path: Path) -> Dict[str, Any]:
    """Load a report JSON file."""
    with open(report_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_schema(report: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """Validate report against JSON schema."""
    jsonschema.validate(instance=report, schema=schema)


def check_rule_coverage_consistency(report: Dict[str, Any]) -> None:
    """
    Check: rule_hit + unknown_count == total_signatures
    
    This ensures that every signature is accounted for in rule classification.
    """
    counts = report.get("counts", {})
    rule_coverage = report.get("rule_coverage", {})
    
    total_signatures = counts.get("total_signatures", 0)
    rule_hit = rule_coverage.get("rule_hit", 0)
    unknown_count = rule_coverage.get("unknown_count", 0)
    
    assert rule_hit + unknown_count == total_signatures, (
        f"Rule coverage inconsistency: "
        f"rule_hit ({rule_hit}) + unknown_count ({unknown_count}) "
        f"!= total_signatures ({total_signatures})"
    )


def check_llm_coverage_consistency(report: Dict[str, Any]) -> None:
    """
    Check LLM coverage counts are non-negative and logically consistent.
    
    - llm_analyzed_count >= 0
    - needs_review_count >= 0
    - skipped_count >= 0 (if present)
    - failed_permanent_count >= 0 (if present)
    """
    llm_coverage = report.get("llm_coverage", {})
    
    llm_analyzed = llm_coverage.get("llm_analyzed_count", 0)
    needs_review = llm_coverage.get("needs_review_count", 0)
    skipped = llm_coverage.get("skipped_count", 0)
    failed_permanent = llm_coverage.get("failed_permanent_count", 0)
    
    # All counts must be non-negative
    assert llm_analyzed >= 0, f"llm_analyzed_count must be >= 0, got {llm_analyzed}"
    assert needs_review >= 0, f"needs_review_count must be >= 0, got {needs_review}"
    assert skipped >= 0, f"skipped_count must be >= 0, got {skipped}"
    assert failed_permanent >= 0, f"failed_permanent_count must be >= 0, got {failed_permanent}"
    
    # Cache hit rate must be in [0, 1]
    cache_hit_rate = llm_coverage.get("cache_hit_rate", 0)
    assert 0 <= cache_hit_rate <= 1, f"cache_hit_rate must be in [0,1], got {cache_hit_rate}"


def check_audit_fields_present(report: Dict[str, Any]) -> None:
    """
    Check that mandatory audit fields are present.
    
    Phase 6 requires:
    - thresholds_used with all A/B/C thresholds
    - sample with seed
    - rule_coverage
    - llm_coverage with provider/model info
    """
    # thresholds_used
    thresholds = report.get("thresholds_used", {})
    required_thresholds = ["A_min_bytes", "B_burst_count", "B_burst_window_seconds", 
                          "B_cumulative_bytes", "C_sample_rate"]
    for key in required_thresholds:
        assert key in thresholds, f"Missing threshold: {key}"
    
    # sample
    sample = report.get("sample", {})
    assert "seed" in sample, "Missing sample.seed"
    assert "sample_rate" in sample, "Missing sample.sample_rate"
    assert "sample_method" in sample, "Missing sample.sample_method"
    
    # rule_coverage
    rule_coverage = report.get("rule_coverage", {})
    assert "rule_hit" in rule_coverage, "Missing rule_coverage.rule_hit"
    assert "unknown_count" in rule_coverage, "Missing rule_coverage.unknown_count"
    
    # llm_coverage
    llm_coverage = report.get("llm_coverage", {})
    assert "llm_analyzed_count" in llm_coverage, "Missing llm_coverage.llm_analyzed_count"
    assert "needs_review_count" in llm_coverage, "Missing llm_coverage.needs_review_count"
    assert "cache_hit_rate" in llm_coverage, "Missing llm_coverage.cache_hit_rate"


# =============================================================================
# Test Classes
# =============================================================================

class TestReportSchemaValidation:
    """Test report schema validation."""
    
    def test_sample_report_validates_against_schema(
        self, sample_report: Path, report_schema: Dict[str, Any]
    ):
        """Sample report should validate against schema."""
        report = load_report(sample_report)
        validate_schema(report, report_schema)
    
    def test_schema_rejects_missing_required_fields(
        self, tmp_path: Path, report_schema: Dict[str, Any]
    ):
        """Schema should reject reports missing required fields."""
        incomplete_report = {
            "run_id": "test",
            # Missing: run_key, started_at, thresholds_used, counts, sample, rule_coverage, llm_coverage
        }
        
        report_path = tmp_path / "incomplete_report.json"
        with open(report_path, "w") as f:
            json.dump(incomplete_report, f)
        
        report = load_report(report_path)
        
        with pytest.raises(jsonschema.ValidationError):
            validate_schema(report, report_schema)


class TestRuleCoverageConsistency:
    """Test rule coverage consistency."""
    
    def test_rule_hit_plus_unknown_equals_total(self, sample_report: Path):
        """rule_hit + unknown_count should equal total_signatures."""
        report = load_report(sample_report)
        check_rule_coverage_consistency(report)
    
    def test_detects_inconsistent_rule_coverage(self, tmp_path: Path):
        """Should detect when rule coverage is inconsistent."""
        # Create an inconsistent report
        report = {
            "counts": {"total_signatures": 100},
            "rule_coverage": {"rule_hit": 50, "unknown_count": 30}  # 50+30=80 != 100
        }
        
        report_path = tmp_path / "inconsistent_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f)
        
        loaded = load_report(report_path)
        
        with pytest.raises(AssertionError, match="Rule coverage inconsistency"):
            check_rule_coverage_consistency(loaded)


class TestLLMCoverageConsistency:
    """Test LLM coverage consistency."""
    
    def test_llm_counts_are_non_negative(self, sample_report: Path):
        """All LLM counts should be non-negative."""
        report = load_report(sample_report)
        check_llm_coverage_consistency(report)
    
    def test_detects_negative_llm_count(self, tmp_path: Path):
        """Should detect negative LLM counts."""
        report = {
            "llm_coverage": {
                "llm_analyzed_count": -1,
                "needs_review_count": 0,
                "cache_hit_rate": 0.5
            }
        }
        
        report_path = tmp_path / "negative_count_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f)
        
        loaded = load_report(report_path)
        
        with pytest.raises(AssertionError, match="llm_analyzed_count must be >= 0"):
            check_llm_coverage_consistency(loaded)
    
    def test_detects_invalid_cache_hit_rate(self, tmp_path: Path):
        """Should detect cache_hit_rate outside [0,1]."""
        report = {
            "llm_coverage": {
                "llm_analyzed_count": 10,
                "needs_review_count": 0,
                "cache_hit_rate": 1.5  # Invalid: > 1
            }
        }
        
        report_path = tmp_path / "invalid_cache_rate_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f)
        
        loaded = load_report(report_path)
        
        with pytest.raises(AssertionError, match="cache_hit_rate must be in"):
            check_llm_coverage_consistency(loaded)


class TestAuditFieldsPresence:
    """Test presence of mandatory audit fields."""
    
    def test_all_audit_fields_present(self, sample_report: Path):
        """All mandatory audit fields should be present."""
        report = load_report(sample_report)
        check_audit_fields_present(report)
    
    def test_detects_missing_threshold(self, tmp_path: Path):
        """Should detect missing threshold fields."""
        report = {
            "thresholds_used": {
                "A_min_bytes": 10000
                # Missing other thresholds
            },
            "sample": {"seed": "x", "sample_rate": 0.1, "sample_method": "hash"},
            "rule_coverage": {"rule_hit": 0, "unknown_count": 0},
            "llm_coverage": {"llm_analyzed_count": 0, "needs_review_count": 0, "cache_hit_rate": 0}
        }
        
        report_path = tmp_path / "missing_threshold_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f)
        
        loaded = load_report(report_path)
        
        with pytest.raises(AssertionError, match="Missing threshold"):
            check_audit_fields_present(loaded)


class TestLatestReportIntegrity:
    """Test the latest actual report file (if exists)."""
    
    def test_latest_report_schema_valid(
        self, latest_report_path: Optional[Path], report_schema: Dict[str, Any]
    ):
        """Latest report should validate against schema."""
        if latest_report_path is None:
            pytest.skip("No report file found in data/output/")
        
        report = load_report(latest_report_path)
        validate_schema(report, report_schema)
    
    def test_latest_report_rule_coverage_consistent(
        self, latest_report_path: Optional[Path]
    ):
        """Latest report should have consistent rule coverage."""
        if latest_report_path is None:
            pytest.skip("No report file found in data/output/")
        
        report = load_report(latest_report_path)
        check_rule_coverage_consistency(report)
    
    def test_latest_report_llm_coverage_consistent(
        self, latest_report_path: Optional[Path]
    ):
        """Latest report should have consistent LLM coverage."""
        if latest_report_path is None:
            pytest.skip("No report file found in data/output/")
        
        report = load_report(latest_report_path)
        check_llm_coverage_consistency(report)
    
    def test_latest_report_audit_fields_present(
        self, latest_report_path: Optional[Path]
    ):
        """Latest report should have all audit fields."""
        if latest_report_path is None:
            pytest.skip("No report file found in data/output/")
        
        report = load_report(latest_report_path)
        check_audit_fields_present(report)


# =============================================================================
# Utility: Standalone validation function for scripts
# =============================================================================

def validate_report_integrity(report_path: Path) -> bool:
    """
    Validate a report file for integrity (for use in scripts).
    
    Args:
        report_path: Path to report JSON file
    
    Returns:
        True if valid, raises AssertionError otherwise
    """
    schema_path = Path(__file__).parent.parent / "schemas" / "report_summary.schema.json"
    
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    
    report = load_report(report_path)
    
    # 1. Schema validation
    validate_schema(report, schema)
    
    # 2. Rule coverage consistency
    check_rule_coverage_consistency(report)
    
    # 3. LLM coverage consistency
    check_llm_coverage_consistency(report)
    
    # 4. Audit fields presence
    check_audit_fields_present(report)
    
    return True


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python test_e2e_report_integrity.py <report_path>")
        sys.exit(1)
    
    report_path = Path(sys.argv[1])
    if not report_path.exists():
        print(f"Error: File not found: {report_path}")
        sys.exit(1)
    
    try:
        validate_report_integrity(report_path)
        print(f"✓ Report integrity validated: {report_path}")
        sys.exit(0)
    except Exception as e:
        print(f"✗ Report integrity failed: {e}")
        sys.exit(1)
