"""
E2E Validation Tests for AIMO Analysis Engine

Tests that E2E execution produces valid reports with all required audit fields.
"""

import pytest
import json
import sys
from pathlib import Path
from datetime import datetime
import jsonschema

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.report_builder import ReportBuilder


class TestE2EReportValidation:
    """Test that E2E execution produces valid reports."""
    
    def test_report_schema_validation(self):
        """Report must validate against report_summary.schema.json."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_e2e_123",
            run_key="test_e2e_key_456",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="sample_logs/paloalto_sample.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 5,
                "total_signatures": 4,
                "unique_users": 3,
                "unique_domains": 4,
                "abc_count_a": 0,
                "abc_count_b": 0,
                "abc_count_c": 0,
                "burst_hit": 0,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_e2e_123"
            },
            rule_coverage={
                "rule_hit": 2,
                "unknown_count": 2
            },
            llm_coverage={
                "llm_analyzed_count": 0,
                "needs_review_count": 0,
                "cache_hit_rate": 0.0,
                "skipped_count": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        # Validation is done in build_report, so if we get here, it's valid
        assert report is not None
        assert report["run_id"] == "test_e2e_123"
    
    def test_audit_required_fields_present(self):
        """All audit-required fields must be present in report."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_audit_123",
            run_key="test_audit_key_456",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="sample_logs/paloalto_sample.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 5,
                "total_signatures": 4,
                "unique_users": 3,
                "unique_domains": 4,
                "abc_count_a": 0,
                "abc_count_b": 0,
                "abc_count_c": 0,
                "burst_hit": 0,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_audit_123"
            },
            rule_coverage={
                "rule_hit": 2,
                "unknown_count": 2
            },
            llm_coverage={
                "llm_analyzed_count": 0,
                "needs_review_count": 0,
                "cache_hit_rate": 0.0,
                "skipped_count": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        # Check all required audit fields
        assert "thresholds_used" in report
        assert "counts" in report
        assert "sample" in report
        assert "rule_coverage" in report
        assert "llm_coverage" in report
        
        # Check thresholds_used structure
        thresholds = report["thresholds_used"]
        assert "A_min_bytes" in thresholds
        assert "B_burst_count" in thresholds
        assert "B_burst_window_seconds" in thresholds
        assert "B_cumulative_bytes" in thresholds
        assert "C_sample_rate" in thresholds
        
        # Check counts structure
        counts = report["counts"]
        assert "total_events" in counts
        assert "total_signatures" in counts
        assert "abc_count_a" in counts
        assert "abc_count_b" in counts
        assert "abc_count_c" in counts
        assert "burst_hit" in counts
        assert "cumulative_hit" in counts
        
        # Check sample structure
        sample = report["sample"]
        assert "sample_rate" in sample
        assert "sample_method" in sample
        assert "seed" in sample
        assert sample["seed"] == "test_audit_123"  # seed should be run_id
        
        # Check rule_coverage structure
        rule_coverage = report["rule_coverage"]
        assert "rule_hit" in rule_coverage
        assert "unknown_count" in rule_coverage
        
        # Check llm_coverage structure
        llm_coverage = report["llm_coverage"]
        assert "llm_analyzed_count" in llm_coverage
        assert "needs_review_count" in llm_coverage
        assert "cache_hit_rate" in llm_coverage
