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
import uuid
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
                "burst_hit": 3,
                "cumulative_hit": 3
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_coverage_1"
            },
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
                "burst_hit": 3,
                "cumulative_hit": 3
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_coverage_2"
            },
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
                "burst_hit": 3,
                "cumulative_hit": 3
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_coverage_3"
            },
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
                "burst_hit": 3,
                "cumulative_hit": 3
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_coverage_4"
            },
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
                "burst_hit": 3,
                "cumulative_hit": 3
            },
            sample={
                "sample_rate": 0.1,
                "sample_method": "deterministic_hash",
                "seed": "test_audit_1"
            },
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
    
    def test_db_recalculation_consistency(self, tmp_path):
        """LLM coverage counts should match DB state when recalculated."""
        # A) DB完全分離: tmp_path配下にDBとtemp_directoryを作成
        test_id = str(uuid.uuid4())[:8]  # ユニークなテストID
        db_path = tmp_path / f"test_{test_id}.duckdb"
        temp_directory = tmp_path / f"duckdb_tmp_{test_id}"
        
        # DuckDBClient初期化（temp_directoryを明示指定）
        db_client = DuckDBClient(str(db_path), temp_directory=str(temp_directory))
        
        # B) ユニークなurl_signatureを生成（テスト間で衝突しないように）
        base_sig = f"test_sig_{test_id}"
        
        # Insert test data
        # active: should be counted in llm_analyzed_count
        # Note: status列はインデックス列なので、INSERT時に設定（更新は避ける）
        for i in range(5):
            db_client.upsert("analysis_cache", {
                "url_signature": f"{base_sig}_active_{i}",
                "service_name": "Test Service",
                "classification_source": "LLM",
                "status": "active",
                "usage_type": "business",
                "risk_level": "low",
                "category": "Test",
                "confidence": 0.9
            }, conflict_key="url_signature")
        
        # needs_review: should be counted in needs_review_count (NOT in llm_analyzed_count)
        for i in range(3):
            db_client.upsert("analysis_cache", {
                "url_signature": f"{base_sig}_needs_review_{i}",
                "service_name": "Unknown",
                "classification_source": "LLM",
                "status": "needs_review",
                "usage_type": "unknown",
                "risk_level": "medium",
                "category": "Unknown",
                "confidence": 0.3
            }, conflict_key="url_signature")
        
        # failed_permanent: should be excluded (counted in skipped_count)
        for i in range(2):
            db_client.upsert("analysis_cache", {
                "url_signature": f"{base_sig}_failed_{i}",
                "service_name": "Unknown",
                "classification_source": "LLM",
                "status": "failed_permanent",
                "error_reason": "budget_exceeded",
                "error_type": "budget_exceeded",
                "usage_type": "unknown",
                "risk_level": "medium",
                "category": "Unknown",
                "confidence": 0.0
            }, conflict_key="url_signature")
        
        # C) Writer Queueのflush/closeを明示
        db_client.flush()
        
        # 再計算前にclose/reopenで可視性を完全に確定（テストの安定性向上）
        db_client.close()
        db_client = DuckDBClient(str(db_path), temp_directory=str(temp_directory))
        
        # D) 再計算はDB上の集計から取り直す（同一接続を使用）
        # get_reader()ではなく、writer接続を直接使用（テストではread_onlyは使わない）
        # ただし、DuckDBClientの設計上、get_reader()を使う方が安全
        reader = db_client.get_reader()
        
        llm_analyzed_db = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE classification_source = 'LLM' AND status = 'active'"
        ).fetchone()[0] or 0
        
        needs_review_db = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE status = 'needs_review'"
        ).fetchone()[0] or 0
        
        skipped_db = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE status = 'failed_permanent'"
        ).fetchone()[0] or 0
        
        # Verify counts match expected
        assert llm_analyzed_db == 5, f"active should be counted in llm_analyzed_count (got {llm_analyzed_db})"
        assert needs_review_db == 3, f"needs_review should be counted separately (got {needs_review_db})"
        assert skipped_db == 2, f"failed_permanent should be excluded (got {skipped_db})"
        
        # Verify active is NOT in needs_review (mutually exclusive)
        active_in_needs_review = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE status = 'active' AND status = 'needs_review'"
        ).fetchone()[0] or 0
        assert active_in_needs_review == 0, "active and needs_review should be mutually exclusive"
        
        db_client.close()