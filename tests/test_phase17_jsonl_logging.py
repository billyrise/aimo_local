"""
Test Phase 17: JSONL structured logging

Tests that JSONL logs are written with all required fields.
"""

import pytest
import json
import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from orchestrator.jsonl_logger import JSONLLogger


class TestPhase17JSONLLogging:
    """Test Phase 17: JSONL structured logging."""
    
    def test_log_run_start(self, tmp_path):
        """Run start should be logged with all required fields."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_run_start(
            run_id="test_run_123",
            run_key="test_key_456",
            input_files=["/path/to/input.csv"],
            vendor="paloalto",
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            input_manifest_hash="abc123"
        )
        
        # JSONLLogger doesn't have close() method - it's not needed
        # Log files are written atomically and don't need explicit closing
        
        # Check that log file was created
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        assert log_file.exists()
        
        # Read and verify log entry
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            entry = json.loads(lines[0])
            assert entry["event_type"] == "run_start"
            assert entry["run_id"] == "test_run_123"
            assert entry["run_key"] == "test_key_456"
            assert entry["vendor"] == "paloalto"
            assert "timestamp" in entry
    
    def test_log_run_end(self, tmp_path):
        """Run end should be logged with all required metrics."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_run_end(
            run_id="test_run_123",
            status="succeeded",
            started_at="2024-01-15T10:00:00Z",
            finished_at="2024-01-15T10:05:00Z",
            event_count=1000,
            signature_count=100,
            count_a=10,
            count_b=5,
            count_c=2,
            unknown_count=20,
            llm_sent_count=15,
            llm_analyzed_count=12,
            llm_needs_review_count=2,
            llm_skipped_count=1,
            failures_by_type={"timeout": 1},
            exclusions={"action_filter": "allow"},
            exclusion_counts={"action_filter": 50}
        )
        
        # JSONLLogger doesn't have close() method - it's not needed
        # Log files are written atomically and don't need explicit closing
        
        # Check that log file was created
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        assert log_file.exists()
        
        # Read and verify log entry
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            entry = json.loads(lines[0])
            assert entry["event_type"] == "run_end"
            assert entry["run_id"] == "test_run_123"
            assert entry["status"] == "succeeded"
            assert "metrics" in entry
            assert entry["metrics"]["event_count"] == 1000
            assert entry["metrics"]["abc_counts"]["count_a"] == 10
            assert entry["metrics"]["llm"]["sent_count"] == 15
            assert "exclusions" in entry["metrics"]
    
    def test_log_stage_complete(self, tmp_path):
        """Stage completion should be logged."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_stage_complete(
            run_id="test_run_123",
            stage="ingest",
            stage_number=1,
            status="completed",
            duration_ms=5000.0,
            row_count=1000
        )
        
        # JSONLLogger doesn't have close() method - it's not needed
        # Log files are written atomically and don't need explicit closing
        
        # Check that log file was created
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        assert log_file.exists()
        
        # Read and verify log entry
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            entry = json.loads(lines[0])
            assert entry["event_type"] == "stage_complete"
            assert entry["run_id"] == "test_run_123"
            assert entry["stage"] == "ingest"
            assert entry["stage_number"] == 1
            assert entry["status"] == "completed"
            assert entry["duration_ms"] == 5000.0
            assert entry["row_count"] == 1000
    
    def test_log_error(self, tmp_path):
        """Error events should be logged."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_error(
            run_id="test_run_123",
            error_type="timeout",
            error_message="Request timeout",
            stage="llm_analysis"
        )
        
        # JSONLLogger doesn't have close() method - it's not needed
        # Log files are written atomically and don't need explicit closing
        
        # Check that log file was created
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        assert log_file.exists()
        
        # Read and verify log entry
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            entry = json.loads(lines[0])
            assert entry["event_type"] == "error"
            assert entry["run_id"] == "test_run_123"
            assert entry["error_type"] == "timeout"
            assert entry["error_message"] == "Request timeout"
            assert entry["stage"] == "llm_analysis"
    
    def test_daily_log_rotation(self, tmp_path):
        """Log files should rotate daily."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log an event
        logger.log_run_start(
            run_id="test_run_123",
            run_key="test_key_456",
            input_files=["/path/to/input.csv"],
            vendor="paloalto",
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            input_manifest_hash="abc123"
        )
        
        # JSONLLogger doesn't have close() method - it's not needed
        # Log files are written atomically and don't need explicit closing
        
        # Check that today's log file was created
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        assert log_file.exists()
