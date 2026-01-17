"""
Test Report Contains Audit Fields

Tests that reports contain all required audit fields and fail validation if missing.
"""

import pytest
from pathlib import Path
import sys
import json
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.report_builder import ReportBuilder


class TestReportAuditFields:
    """Test that reports contain all required audit fields."""
    
    def test_report_contains_all_required_fields(self):
        """Report must contain all required fields."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_run_123",
            run_key="test_key_456",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="/path/to/input.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 1000,
                "total_signatures": 100,
                "unique_users": 50,
                "unique_domains": 20,
                "abc_count_a": 2,
                "abc_count_b": 1,
                "abc_count_c": 0,
                "burst_hit": 1,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_run_123"
            },
            rule_coverage={
                "rule_hit": 80,
                "unknown_count": 20
            },
            llm_coverage={
                "llm_analyzed_count": 15,
                "needs_review_count": 2,
                "cache_hit_rate": 0.85,
                "skipped_count": 3
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        # Check all required fields are present
        assert "run_id" in report
        assert "run_key" in report
        assert "started_at" in report
        assert "thresholds_used" in report
        assert "counts" in report
        assert "sample" in report
        assert "rule_coverage" in report
        assert "llm_coverage" in report
        
        # Check nested required fields
        assert "A_min_bytes" in report["thresholds_used"]
        assert "B_burst_count" in report["thresholds_used"]
        assert "total_events" in report["counts"]
        assert "abc_count_a" in report["counts"]
        assert "sample_rate" in report["sample"]
        assert "seed" in report["sample"]
        assert "rule_hit" in report["rule_coverage"]
        assert "llm_analyzed_count" in report["llm_coverage"]
    
    def test_report_fails_without_required_fields(self):
        """Report should fail validation if required fields are missing."""
        builder = ReportBuilder()
        
        # Missing thresholds_used
        with pytest.raises(ValueError, match="validation failed"):
            builder.build_report(
                run_id="test_run_123",
                run_key="test_key_456",
                started_at=datetime.utcnow(),
                finished_at=None,
                input_file="/path/to/input.csv",
                vendor="paloalto",
                thresholds_used={},  # Missing required fields
                counts={
                    "total_events": 1000,
                    "total_signatures": 100,
                    "abc_count_a": 2,
                    "abc_count_b": 1,
                    "abc_count_c": 0,
                    "burst_hit": 1,
                    "cumulative_hit": 0
                },
                sample={
                    "sample_rate": 0.02,
                    "sample_method": "deterministic_hash",
                    "seed": "test_run_123"
                },
                rule_coverage={
                    "rule_hit": 80,
                    "unknown_count": 20
                },
                llm_coverage={
                    "llm_analyzed_count": 15,
                    "needs_review_count": 2,
                    "cache_hit_rate": 0.85
                },
                signature_version="1.0",
                rule_version="1",
                prompt_version="1"
            )
    
    def test_report_saves_to_file(self, tmp_path):
        """Report should save to file correctly."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_run_123",
            run_key="test_key_456",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="/path/to/input.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 1000,
                "total_signatures": 100,
                "abc_count_a": 2,
                "abc_count_b": 1,
                "abc_count_c": 0,
                "burst_hit": 1,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_run_123"
            },
            rule_coverage={
                "rule_hit": 80,
                "unknown_count": 20
            },
            llm_coverage={
                "llm_analyzed_count": 15,
                "needs_review_count": 2,
                "cache_hit_rate": 0.85
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        output_path = tmp_path / "test_report.json"
        builder.save_report(report, output_path)
        
        # Verify file exists
        assert output_path.exists()
        
        # Verify content
        with open(output_path, 'r', encoding='utf-8') as f:
            loaded_report = json.load(f)
        
        assert loaded_report["run_id"] == "test_run_123"
        assert loaded_report["thresholds_used"]["A_min_bytes"] == 1048576
        assert loaded_report["counts"]["total_events"] == 1000
    
    def test_report_atomic_write(self, tmp_path):
        """Report should be written atomically (via temp file)."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_run_123",
            run_key="test_key_456",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="/path/to/input.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 1000,
                "total_signatures": 100,
                "abc_count_a": 2,
                "abc_count_b": 1,
                "abc_count_c": 0,
                "burst_hit": 1,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_run_123"
            },
            rule_coverage={
                "rule_hit": 80,
                "unknown_count": 20
            },
            llm_coverage={
                "llm_analyzed_count": 15,
                "needs_review_count": 2,
                "cache_hit_rate": 0.85
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        output_path = tmp_path / "test_report.json"
        builder.save_report(report, output_path)
        
        # Verify temp file doesn't exist
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        assert not temp_path.exists()
        
        # Verify final file exists
        assert output_path.exists()
    
    def test_rule_coverage_integrity(self):
        """Test that rule_coverage maintains integrity: rule_hit + unknown_count == total_signatures."""
        builder = ReportBuilder()
        
        # Test case 1: rule_hit > 0, should satisfy rule_hit + unknown_count == total_signatures
        report1 = builder.build_report(
            run_id="test_integrity_1",
            run_key="test_key_1",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="/path/to/input.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 1000,
                "total_signatures": 100,
                "abc_count_a": 0,
                "abc_count_b": 0,
                "abc_count_c": 0,
                "burst_hit": 0,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_integrity_1"
            },
            rule_coverage={
                "rule_hit": 80,
                "unknown_count": 20
            },
            llm_coverage={
                "llm_analyzed_count": 15,
                "needs_review_count": 2,
                "cache_hit_rate": 0.85
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        total_sigs = report1["counts"]["total_signatures"]
        rule_hit = report1["rule_coverage"]["rule_hit"]
        unknown_count = report1["rule_coverage"]["unknown_count"]
        
        assert rule_hit + unknown_count == total_sigs, \
            f"Integrity check failed: rule_hit ({rule_hit}) + unknown_count ({unknown_count}) != total_signatures ({total_sigs})"
        
        # Test case 2: rule_hit == 0, unknown_count should equal total_signatures
        report2 = builder.build_report(
            run_id="test_integrity_2",
            run_key="test_key_2",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="/path/to/input.csv",
            vendor="paloalto",
            thresholds_used={
                "A_min_bytes": 1048576,
                "B_burst_count": 20,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 20971520,
                "C_sample_rate": 0.02
            },
            counts={
                "total_events": 50,
                "total_signatures": 10,
                "abc_count_a": 0,
                "abc_count_b": 0,
                "abc_count_c": 0,
                "burst_hit": 0,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.02,
                "sample_method": "deterministic_hash",
                "seed": "test_integrity_2"
            },
            rule_coverage={
                "rule_hit": 0,
                "unknown_count": 10  # Must equal total_signatures when rule_hit == 0
            },
            llm_coverage={
                "llm_analyzed_count": 0,
                "needs_review_count": 0,
                "cache_hit_rate": 0.0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        total_sigs2 = report2["counts"]["total_signatures"]
        rule_hit2 = report2["rule_coverage"]["rule_hit"]
        unknown_count2 = report2["rule_coverage"]["unknown_count"]
        
        assert rule_hit2 == 0, "rule_hit should be 0 in this test case"
        assert unknown_count2 == total_sigs2, \
            f"When rule_hit == 0, unknown_count ({unknown_count2}) must equal total_signatures ({total_sigs2})"
        assert rule_hit2 + unknown_count2 == total_sigs2, \
            f"Integrity check failed: rule_hit ({rule_hit2}) + unknown_count ({unknown_count2}) != total_signatures ({total_sigs2})"