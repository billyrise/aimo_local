"""
Test LLM Coverage Audit (Phase 7-4)

Tests that LLM coverage metrics computed from DB match report values.
This ensures audit-ready definitions are correctly implemented.
"""

import pytest
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.report_builder import ReportBuilder
from db.duckdb_client import DuckDBClient


class TestLLMCoverageAudit:
    """Test LLM coverage audit definitions."""
    
    def test_compute_llm_coverage_from_db_basic(self, tmp_path):
        """Test basic computation of LLM coverage from DB."""
        # Create temporary DB
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        reader = db_client.get_reader()
        
        # Insert test data using direct SQL (status is indexed, cannot be updated via ON CONFLICT)
        # active + LLM: should be counted in llm_analyzed_count
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES 
                ('sig_active_llm_0', 'Test Service', 'LLM', 'active'),
                ('sig_active_llm_1', 'Test Service', 'LLM', 'active'),
                ('sig_active_llm_2', 'Test Service', 'LLM', 'active'),
                ('sig_active_llm_3', 'Test Service', 'LLM', 'active'),
                ('sig_active_llm_4', 'Test Service', 'LLM', 'active')
        """)
        
        # active + RULE: should NOT be counted in llm_analyzed_count
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES 
                ('sig_active_rule_0', 'Test Service', 'RULE', 'active'),
                ('sig_active_rule_1', 'Test Service', 'RULE', 'active'),
                ('sig_active_rule_2', 'Test Service', 'RULE', 'active')
        """)
        
        # needs_review: should be counted in needs_review_count
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES 
                ('sig_needs_review_0', 'Unknown', 'LLM', 'needs_review'),
                ('sig_needs_review_1', 'Unknown', 'LLM', 'needs_review')
        """)
        
        # failed_permanent: should be counted in failed_permanent_count
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES 
                ('sig_failed_0', 'Unknown', 'LLM', 'failed_permanent')
        """)
        
        db_client.flush()
        
        # Compute LLM coverage from DB
        unknown_count = 10  # Total unknown signatures
        llm_coverage = ReportBuilder.compute_llm_coverage_from_db(
            db_reader=reader,
            run_id="test_run",
            unknown_count=unknown_count
        )
        
        # Verify counts
        assert llm_coverage["llm_analyzed_count"] == 5, "Should count only active + LLM"
        assert llm_coverage["needs_review_count"] == 2, "Should count needs_review"
        assert llm_coverage["failed_permanent_count"] == 1, "Should count failed_permanent"
        
        # cache_hit_rate: activeな署名数 / unknown_count
        # activeな署名数 = 5 (LLM) + 3 (RULE) = 8
        # cache_hit_rate = 8 / 10 = 0.8
        assert llm_coverage["cache_hit_rate"] == pytest.approx(0.8, abs=0.01)
    
    def test_compute_llm_coverage_from_db_empty(self, tmp_path):
        """Test computation with empty DB."""
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        reader = db_client.get_reader()
        
        unknown_count = 10
        llm_coverage = ReportBuilder.compute_llm_coverage_from_db(
            db_reader=reader,
            run_id="test_run",
            unknown_count=unknown_count
        )
        
        # All counts should be 0
        assert llm_coverage["llm_analyzed_count"] == 0
        assert llm_coverage["needs_review_count"] == 0
        assert llm_coverage["failed_permanent_count"] == 0
        assert llm_coverage["cache_hit_rate"] == 0.0
    
    def test_compute_llm_coverage_from_db_zero_unknown(self, tmp_path):
        """Test computation with zero unknown_count."""
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        reader = db_client.get_reader()
        
        # Insert some active signatures using direct SQL
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES ('sig1', 'Test', 'LLM', 'active')
        """)
        
        unknown_count = 0
        llm_coverage = ReportBuilder.compute_llm_coverage_from_db(
            db_reader=reader,
            run_id="test_run",
            unknown_count=unknown_count
        )
        
        # cache_hit_rate should be 0.0 when unknown_count is 0
        assert llm_coverage["cache_hit_rate"] == 0.0
    
    def test_report_matches_db_computation(self, tmp_path):
        """Test that report llm_coverage matches DB computation."""
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        reader = db_client.get_reader()
        
        # Insert test data using direct SQL
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES 
                ('sig1', 'Service A', 'LLM', 'active'),
                ('sig2', 'Service B', 'LLM', 'needs_review'),
                ('sig3', 'Service C', 'LLM', 'failed_permanent')
        """)
        
        # Compute from DB
        unknown_count = 5
        llm_coverage_from_db = ReportBuilder.compute_llm_coverage_from_db(
            db_reader=reader,
            run_id="test_run",
            unknown_count=unknown_count
        )
        
        # Build report with DB-computed values
        builder = ReportBuilder()
        report = builder.build_report(
            run_id="test_run",
            run_key="test_key",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="test",
            thresholds_used={
                "A_min_bytes": 1000000,
                "B_burst_count": 10,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 5000000,
                "C_sample_rate": 0.1
            },
            counts={
                "total_events": 100,
                "total_signatures": 50,
                "abc_count_a": 5,
                "abc_count_b": 3,
                "abc_count_c": 2,
                "burst_hit": 0,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_key"
            },
            rule_coverage={"rule_hit": 0, "unknown_count": unknown_count},
            llm_coverage={
                **llm_coverage_from_db,
                "skipped_count": 0,
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {
                    "attempts": 0,
                    "backoff_ms_total": 0,
                    "last_error_code": None,
                    "rate_limit_events": 0
                },
                "rate_limit_events": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        # Verify report values match DB computation
        report_llm = report["llm_coverage"]
        assert report_llm["llm_analyzed_count"] == llm_coverage_from_db["llm_analyzed_count"]
        assert report_llm["needs_review_count"] == llm_coverage_from_db["needs_review_count"]
        assert report_llm["failed_permanent_count"] == llm_coverage_from_db["failed_permanent_count"]
        assert report_llm["cache_hit_rate"] == pytest.approx(llm_coverage_from_db["cache_hit_rate"], abs=0.01)
    
    def test_failed_permanent_count_in_report(self, tmp_path):
        """Test that failed_permanent_count is included in report."""
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        reader = db_client.get_reader()
        
        # Insert failed_permanent signatures using direct SQL
        reader.execute("""
            INSERT INTO analysis_cache (url_signature, service_name, classification_source, status)
            VALUES 
                ('sig_failed_0', 'Unknown', 'LLM', 'failed_permanent'),
                ('sig_failed_1', 'Unknown', 'LLM', 'failed_permanent'),
                ('sig_failed_2', 'Unknown', 'LLM', 'failed_permanent')
        """)
        
        unknown_count = 10
        llm_coverage = ReportBuilder.compute_llm_coverage_from_db(
            db_reader=reader,
            run_id="test_run",
            unknown_count=unknown_count
        )
        
        # Verify failed_permanent_count is present
        assert "failed_permanent_count" in llm_coverage
        assert llm_coverage["failed_permanent_count"] == 3
        
        # Build report and verify
        builder = ReportBuilder()
        report = builder.build_report(
            run_id="test_run",
            run_key="test_key",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="test",
            thresholds_used={
                "A_min_bytes": 1000000,
                "B_burst_count": 10,
                "B_burst_window_seconds": 300,
                "B_cumulative_bytes": 5000000,
                "C_sample_rate": 0.1
            },
            counts={
                "total_events": 100,
                "total_signatures": 50,
                "abc_count_a": 5,
                "abc_count_b": 3,
                "abc_count_c": 2,
                "burst_hit": 0,
                "cumulative_hit": 0
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_key"
            },
            rule_coverage={"rule_hit": 0, "unknown_count": unknown_count},
            llm_coverage={
                **llm_coverage,
                "skipped_count": 0,
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {
                    "attempts": 0,
                    "backoff_ms_total": 0,
                    "last_error_code": None,
                    "rate_limit_events": 0
                },
                "rate_limit_events": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1"
        )
        
        # Verify failed_permanent_count is in report
        assert "failed_permanent_count" in report["llm_coverage"]
        assert report["llm_coverage"]["failed_permanent_count"] == 3
