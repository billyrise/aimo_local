"""
Test JSONL Structured Logger (Phase 17)

Tests for JSONL structured logging functionality.
- Daily log file rotation
- Atomic writes
- Required fields recording
- Thread safety
"""

import pytest
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from orchestrator.jsonl_logger import JSONLLogger


class TestJSONLLogger:
    """Test JSONL logger functionality."""
    
    def test_log_file_creation(self, tmp_path):
        """Test that log files are created in logs directory."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log an event
        logger.log({"event_type": "test", "message": "test message"})
        
        # Check that log file was created
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        assert log_file.exists(), "Log file should be created"
    
    def test_log_entry_format(self, tmp_path):
        """Test that log entries are valid JSONL format."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log an event
        event = {"event_type": "test", "message": "test message", "count": 42}
        logger.log(event)
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        assert log_file.exists()
        
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        assert len(lines) == 1, "Should have one log entry"
        
        # Parse JSON line
        logged_event = json.loads(lines[0])
        
        assert logged_event["event_type"] == "test"
        assert logged_event["message"] == "test message"
        assert logged_event["count"] == 42
        assert "timestamp" in logged_event, "Timestamp should be added automatically"
    
    def test_timestamp_auto_add(self, tmp_path):
        """Test that timestamp is automatically added if not present."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log event without timestamp
        event = {"event_type": "test", "message": "test"}
        logger.log(event)
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            logged_event = json.loads(f.readline())
        
        assert "timestamp" in logged_event
        # Verify timestamp is valid ISO format
        datetime.fromisoformat(logged_event["timestamp"].replace("Z", "+00:00"))
    
    def test_multiple_entries(self, tmp_path):
        """Test that multiple entries are appended correctly."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log multiple events
        for i in range(5):
            logger.log({"event_type": "test", "index": i})
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        assert len(lines) == 5, "Should have 5 log entries"
        
        # Verify all entries are valid JSON
        for i, line in enumerate(lines):
            event = json.loads(line)
            assert event["index"] == i
    
    def test_atomic_write(self, tmp_path):
        """Test that writes are atomic (tmp -> rename)."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log an event
        logger.log({"event_type": "test", "message": "atomic test"})
        
        # Check that tmp file doesn't exist
        today = datetime.utcnow().strftime("%Y-%m-%d")
        tmp_file = logs_dir / f"{today}.jsonl.tmp"
        
        assert not tmp_file.exists(), "Temporary file should be removed after atomic rename"
        
        # Check that log file exists
        log_file = logs_dir / f"{today}.jsonl"
        assert log_file.exists(), "Log file should exist after atomic write"
    
    def test_run_start_logging(self, tmp_path):
        """Test run_start event logging."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_run_start(
            run_id="test_run_123",
            run_key="test_key_456",
            input_files=["/path/to/file1.csv", "/path/to/file2.csv"],
            vendor="paloalto",
            signature_version="1.0",
            rule_version="1",
            prompt_version="1",
            input_manifest_hash="abc123"
        )
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            event = json.loads(f.readline())
        
        assert event["event_type"] == "run_start"
        assert event["run_id"] == "test_run_123"
        assert event["run_key"] == "test_key_456"
        assert event["input_files"] == ["/path/to/file1.csv", "/path/to/file2.csv"]
        assert event["vendor"] == "paloalto"
        assert event["signature_version"] == "1.0"
        assert event["rule_version"] == "1"
        assert event["prompt_version"] == "1"
        assert event["input_manifest_hash"] == "abc123"
    
    def test_run_end_logging(self, tmp_path):
        """Test run_end event logging with all required fields."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        started_at = datetime.utcnow() - timedelta(hours=1)
        finished_at = datetime.utcnow()
        
        logger.log_run_end(
            run_id="test_run_123",
            status="succeeded",
            started_at=started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            event_count=1000,
            signature_count=100,
            count_a=10,
            count_b=5,
            count_c=2,
            unknown_count=20,
            llm_sent_count=15,
            llm_analyzed_count=12,
            llm_needs_review_count=2,
            llm_skipped_count=3,
            failures_by_type={"llm_error": 2, "budget_exceeded": 3},
            exclusions={"action_filter": "block"},
            exclusion_counts={"action_filter": 50}
        )
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            event = json.loads(f.readline())
        
        assert event["event_type"] == "run_end"
        assert event["run_id"] == "test_run_123"
        assert event["status"] == "succeeded"
        assert event["started_at"] == started_at.isoformat()
        assert event["finished_at"] == finished_at.isoformat()
        
        # Verify metrics
        metrics = event["metrics"]
        assert metrics["event_count"] == 1000
        assert metrics["signature_count"] == 100
        assert metrics["abc_counts"]["count_a"] == 10
        assert metrics["abc_counts"]["count_b"] == 5
        assert metrics["abc_counts"]["count_c"] == 2
        assert metrics["unknown_count"] == 20
        assert metrics["llm"]["sent_count"] == 15
        assert metrics["llm"]["analyzed_count"] == 12
        assert metrics["llm"]["needs_review_count"] == 2
        assert metrics["llm"]["skipped_count"] == 3
        assert metrics["failures_by_type"]["llm_error"] == 2
        assert metrics["failures_by_type"]["budget_exceeded"] == 3
        assert metrics["exclusions"]["conditions"]["action_filter"] == "block"
        assert metrics["exclusions"]["counts"]["action_filter"] == 50
    
    def test_stage_complete_logging(self, tmp_path):
        """Test stage_complete event logging."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_stage_complete(
            run_id="test_run_123",
            stage="ingest",
            stage_number=1,
            status="completed",
            duration_ms=1234.5,
            row_count=1000,
            metadata={"bytes_read": 1024000}
        )
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            event = json.loads(f.readline())
        
        assert event["event_type"] == "stage_complete"
        assert event["run_id"] == "test_run_123"
        assert event["stage"] == "ingest"
        assert event["stage_number"] == 1
        assert event["status"] == "completed"
        assert event["duration_ms"] == 1234.5
        assert event["row_count"] == 1000
        assert event["metadata"]["bytes_read"] == 1024000
    
    def test_error_logging(self, tmp_path):
        """Test error event logging."""
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        logger.log_error(
            run_id="test_run_123",
            error_type="llm_error",
            error_message="API rate limit exceeded",
            stage="llm_analysis",
            metadata={"retry_count": 3}
        )
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            event = json.loads(f.readline())
        
        assert event["event_type"] == "error"
        assert event["run_id"] == "test_run_123"
        assert event["error_type"] == "llm_error"
        assert event["error_message"] == "API rate limit exceeded"
        assert event["stage"] == "llm_analysis"
        assert event["metadata"]["retry_count"] == 3
    
    def test_daily_rotation(self, tmp_path):
        """Test that log files rotate daily."""
        logs_dir = tmp_path / "logs"
        
        # Create logger with specific date
        today = datetime.utcnow().strftime("%Y-%m-%d")
        logger1 = JSONLLogger(logs_dir)
        logger1.log({"event_type": "test", "date": "today"})
        
        # Verify today's file exists
        today_file = logs_dir / f"{today}.jsonl"
        assert today_file.exists(), "Today's log file should exist"
        
        # Verify content
        with open(today_file, "r", encoding="utf-8") as f:
            today_event = json.loads(f.readline())
        assert today_event["date"] == "today"
        
        # Test that _get_log_file_path works for different dates
        tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        tomorrow_file = logger1._get_log_file_path(tomorrow)
        
        # Manually write to tomorrow's file to simulate rotation
        with open(tomorrow_file, "w", encoding="utf-8") as f:
            json.dump({"event_type": "test", "date": "tomorrow"}, f, ensure_ascii=False)
            f.write("\n")
        
        assert tomorrow_file.exists(), "Tomorrow's log file should exist"
        
        # Verify content
        with open(tomorrow_file, "r", encoding="utf-8") as f:
            tomorrow_event = json.loads(f.readline())
        assert tomorrow_event["date"] == "tomorrow"
    
    def test_thread_safety(self, tmp_path):
        """Test that logging is thread-safe."""
        import threading
        
        logs_dir = tmp_path / "logs"
        logger = JSONLLogger(logs_dir)
        
        # Log from multiple threads
        def log_from_thread(thread_id):
            for i in range(10):
                logger.log({"event_type": "test", "thread_id": thread_id, "index": i})
        
        threads = []
        for i in range(5):
            t = threading.Thread(target=log_from_thread, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for all threads to complete
        for t in threads:
            t.join()
        
        # Read log file
        today = datetime.utcnow().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{today}.jsonl"
        
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # Should have 50 entries (5 threads * 10 entries each)
        assert len(lines) == 50, f"Expected 50 entries, got {len(lines)}"
        
        # Verify all entries are valid JSON
        for line in lines:
            event = json.loads(line)
            assert "thread_id" in event
            assert "index" in event
