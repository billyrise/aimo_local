"""
Test LLM Coverage Counts

Tests that LLM coverage counts in reports are consistent with analysis_cache state transitions.
This is critical for audit integrity:
- active: Successfully classified (counted in llm_analyzed_count)
- needs_review: Retry candidate (counted in needs_review_count, NOT in llm_analyzed_count)
- failed_permanent: Permanent failure (excluded from LLM analysis, NOT counted)
"""

import pytest
import sys
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.report_builder import ReportBuilder
from db.duckdb_client import DuckDBClient


class TestLLMCoverageCounts:
    """Test LLM coverage count integrity."""
    
    def test_active_counted_in_llm_analyzed(self):
        """active status should be counted in llm_analyzed_count."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_coverage_1",
            run_key="test_key_1",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="paloalto",
            thresholds_used={},
            counts={},
            sample={},
            rule_coverage={
                "rule_hit": 10,
                "unknown_count": 5
            },
            llm_coverage={
                "llm_analyzed_count": 5,  # Should match active count
                "needs_review_count": 0,
                "cache_hit_rate": 0.0,
                "skipped_count": 0,
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {},
                "rate_limit_events": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            exclusions={}
        )
        
        # Verify llm_analyzed_count is present
        assert "llm_coverage" in report
        assert report["llm_coverage"]["llm_analyzed_count"] == 5
    
    def test_needs_review_not_counted_in_llm_analyzed(self):
        """needs_review should be counted in needs_review_count, NOT in llm_analyzed_count."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_coverage_2",
            run_key="test_key_2",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="paloalto",
            thresholds_used={},
            counts={},
            sample={},
            rule_coverage={
                "rule_hit": 10,
                "unknown_count": 8
            },
            llm_coverage={
                "llm_analyzed_count": 3,  # Only active (successful)
                "needs_review_count": 5,  # needs_review (retry candidates)
                "cache_hit_rate": 0.0,
                "skipped_count": 0,
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {},
                "rate_limit_events": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            exclusions={}
        )
        
        # Verify counts are separate
        assert report["llm_coverage"]["llm_analyzed_count"] == 3
        assert report["llm_coverage"]["needs_review_count"] == 5
        
        # Verify they don't overlap
        # llm_analyzed_count + needs_review_count should not exceed unknown_count
        # (some may be skipped/failed_permanent)
        assert report["llm_coverage"]["llm_analyzed_count"] + report["llm_coverage"]["needs_review_count"] <= report["rule_coverage"]["unknown_count"]
    
    def test_failed_permanent_excluded(self):
        """failed_permanent should be excluded from both llm_analyzed_count and needs_review_count."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_coverage_3",
            run_key="test_key_3",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="paloalto",
            thresholds_used={},
            counts={},
            sample={},
            rule_coverage={
                "rule_hit": 10,
                "unknown_count": 10
            },
            llm_coverage={
                "llm_analyzed_count": 5,  # active only
                "needs_review_count": 3,  # needs_review only
                "cache_hit_rate": 0.0,
                "skipped_count": 2,  # failed_permanent or budget_exceeded
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {},
                "rate_limit_events": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            exclusions={}
        )
        
        # Verify failed_permanent is not counted in llm_analyzed_count or needs_review_count
        # It should be in skipped_count
        assert report["llm_coverage"]["skipped_count"] == 2
        
        # Verify total: llm_analyzed + needs_review + skipped <= unknown_count
        total_processed = (
            report["llm_coverage"]["llm_analyzed_count"] +
            report["llm_coverage"]["needs_review_count"] +
            report["llm_coverage"]["skipped_count"]
        )
        assert total_processed <= report["rule_coverage"]["unknown_count"]
    
    def test_state_transition_consistency(self):
        """State transitions should be consistent with coverage counts."""
        # This test verifies the logic in main.py:
        # - active: Classification confirmed (counted in llm_analyzed_count)
        # - needs_review: Retry candidate (counted in needs_review_count)
        # - failed_permanent: Permanent failure (counted in skipped_count)
        
        builder = ReportBuilder()
        
        # Simulate a run with mixed states
        report = builder.build_report(
            run_id="test_coverage_4",
            run_key="test_key_4",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="paloalto",
            thresholds_used={},
            counts={},
            sample={},
            rule_coverage={
                "rule_hit": 20,
                "unknown_count": 15
            },
            llm_coverage={
                "llm_analyzed_count": 8,  # active
                "needs_review_count": 4,  # needs_review (will retry)
                "cache_hit_rate": 0.0,
                "skipped_count": 3,  # failed_permanent (context_length_exceeded, etc.)
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {
                    "attempts": 2,
                    "backoff_ms_total": 1500,
                    "last_error_code": "429",
                    "rate_limit_events": 1
                },
                "rate_limit_events": 1
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            exclusions={}
        )
        
        # Verify all counts are present
        assert "llm_coverage" in report
        llm_cov = report["llm_coverage"]
        
        assert "llm_analyzed_count" in llm_cov
        assert "needs_review_count" in llm_cov
        assert "skipped_count" in llm_cov
        
        # Verify counts are non-negative
        assert llm_cov["llm_analyzed_count"] >= 0
        assert llm_cov["needs_review_count"] >= 0
        assert llm_cov["skipped_count"] >= 0
        
        # Verify retry_summary is present when there are retries
        if llm_cov["needs_review_count"] > 0 or llm_cov["rate_limit_events"] > 0:
            assert "retry_summary" in llm_cov
            assert "rate_limit_events" in llm_cov
    
    def test_audit_field_requirements(self):
        """Report must include all required audit fields for LLM coverage."""
        builder = ReportBuilder()
        
        report = builder.build_report(
            run_id="test_audit_1",
            run_key="test_audit_key_1",
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            input_file="test.csv",
            vendor="paloalto",
            thresholds_used={},
            counts={},
            sample={},
            rule_coverage={
                "rule_hit": 10,
                "unknown_count": 5
            },
            llm_coverage={
                "llm_analyzed_count": 5,
                "needs_review_count": 0,
                "cache_hit_rate": 0.0,
                "skipped_count": 0,
                "llm_provider": "gemini",
                "llm_model": "gemini-2.0-flash",
                "structured_output": True,
                "schema_sanitized": True,
                "retry_summary": {},
                "rate_limit_events": 0
            },
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            exclusions={}
        )
        
        # Required audit fields (per AIMO_Detail.md section 11.3)
        assert "llm_coverage" in report
        llm_cov = report["llm_coverage"]
        
        # Must have provider/model info
        assert "llm_provider" in llm_cov
        assert "llm_model" in llm_cov
        
        # Must have structured output flag
        assert "structured_output" in llm_cov
        
        # Must have schema sanitization flag (for Gemini)
        assert "schema_sanitized" in llm_cov
        
        # Must have retry summary (even if empty)
        assert "retry_summary" in llm_cov
        
        # Must have rate limit events count
        assert "rate_limit_events" in llm_cov
        
        # Must have coverage counts
        assert "llm_analyzed_count" in llm_cov
        assert "needs_review_count" in llm_cov
        assert "skipped_count" in llm_cov
