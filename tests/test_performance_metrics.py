"""
Test performance metrics recording (Phase 13).

Tests that performance metrics are correctly recorded to DuckDB for each stage.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from datetime import datetime
import time

from db.duckdb_client import DuckDBClient
from orchestrator.metrics import MetricsRecorder


class TestPerformanceMetrics:
    """Test performance metrics recording."""
    
    def test_metrics_recorder_initialization(self, tmp_path):
        """Test MetricsRecorder initialization."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        assert recorder.db_client == client
        assert recorder.run_id == run_id
    
    def test_record_stage_duration(self, tmp_path):
        """Test recording stage duration."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        # Record a stage with context manager
        with recorder.record_stage(MetricsRecorder.STAGE_INGEST):
            time.sleep(0.1)  # Simulate processing
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ?",
            [run_id]
        ).fetchall()
        
        # Should have duration_ms metric
        duration_metrics = [m for m in metrics if m[1] == "duration_ms"]
        assert len(duration_metrics) > 0
        
        duration_metric = duration_metrics[0]
        assert duration_metric[0] == MetricsRecorder.STAGE_INGEST
        assert duration_metric[1] == "duration_ms"
        assert duration_metric[2] > 0  # Should be positive
        assert duration_metric[3] == "ms"
    
    def test_record_stage_with_row_count(self, tmp_path):
        """Test recording stage with row count (rows_per_sec calculation)."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        row_count = 1000
        
        # Record a stage with row count
        with recorder.record_stage(MetricsRecorder.STAGE_NORMALIZE, row_count=row_count):
            time.sleep(0.1)  # Simulate processing
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ?",
            [run_id]
        ).fetchall()
        
        # Should have duration_ms, rows_per_sec, and row_count metrics
        metric_dict = {m[1]: (m[2], m[3]) for m in metrics}
        
        assert "duration_ms" in metric_dict
        assert "rows_per_sec" in metric_dict
        assert "row_count" in metric_dict
        
        # Verify row_count
        assert metric_dict["row_count"][0] == float(row_count)
        assert metric_dict["row_count"][1] == "rows"
        
        # Verify rows_per_sec is positive
        assert metric_dict["rows_per_sec"][0] > 0
        assert metric_dict["rows_per_sec"][1] == "rows/sec"
    
    def test_record_custom_metric(self, tmp_path):
        """Test recording custom metrics."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        # Record custom metrics
        recorder.record_metric(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            "llm_analyzed_count",
            42.0,
            "signatures"
        )
        
        recorder.record_metric(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            "cache_hit_rate",
            0.85,
            "ratio"
        )
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ?",
            [run_id]
        ).fetchall()
        
        metric_dict = {m[1]: (m[2], m[3]) for m in metrics}
        
        assert "llm_analyzed_count" in metric_dict
        assert metric_dict["llm_analyzed_count"][0] == 42.0
        assert metric_dict["llm_analyzed_count"][1] == "signatures"
        
        assert "cache_hit_rate" in metric_dict
        assert metric_dict["cache_hit_rate"][0] == 0.85
        assert metric_dict["cache_hit_rate"][1] == "ratio"
    
    def test_record_stage_with_io_metrics(self, tmp_path):
        """Test recording stage with I/O metrics."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        bytes_read = 1024 * 1024  # 1MB
        bytes_written = 512 * 1024  # 512KB
        
        # Record a stage with I/O metrics
        with recorder.record_stage(
            MetricsRecorder.STAGE_INGEST,
            row_count=1000,
            bytes_read=bytes_read,
            bytes_written=bytes_written
        ):
            time.sleep(0.1)  # Simulate processing
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ?",
            [run_id]
        ).fetchall()
        
        metric_dict = {m[1]: (m[2], m[3]) for m in metrics}
        
        assert "bytes_read" in metric_dict
        assert metric_dict["bytes_read"][0] == float(bytes_read)
        assert metric_dict["bytes_read"][1] == "bytes"
        
        assert "bytes_written" in metric_dict
        assert metric_dict["bytes_written"][0] == float(bytes_written)
        assert metric_dict["bytes_written"][1] == "bytes"
    
    def test_all_stages_recorded(self, tmp_path):
        """Test that all stage types can be recorded."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        # Record metrics for all stages
        stages = [
            MetricsRecorder.STAGE_INGEST,
            MetricsRecorder.STAGE_NORMALIZE,
            MetricsRecorder.STAGE_ABC_CACHE,
            MetricsRecorder.STAGE_RULE_CLASSIFICATION,
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            MetricsRecorder.STAGE_REPORTING
        ]
        
        for stage in stages:
            with recorder.record_stage(stage, row_count=100):
                time.sleep(0.01)  # Simulate processing
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify all stages have metrics
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT DISTINCT stage FROM performance_metrics WHERE run_id = ?",
            [run_id]
        ).fetchall()
        
        recorded_stages = {m[0] for m in metrics}
        
        for stage in stages:
            assert stage in recorded_stages, f"Stage {stage} not recorded"
    
    def test_metrics_timestamps(self, tmp_path):
        """Test that timestamps are correctly recorded."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        before = datetime.utcnow()
        
        # Record a stage
        with recorder.record_stage(MetricsRecorder.STAGE_INGEST):
            time.sleep(0.1)
        
        after = datetime.utcnow()
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify timestamps
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT started_at, finished_at, recorded_at FROM performance_metrics WHERE run_id = ? AND metric_name = 'duration_ms'",
            [run_id]
        ).fetchone()
        
        assert metrics is not None
        started_at_val, finished_at_val, recorded_at_val = metrics
        
        # Parse timestamps (DuckDB may return datetime objects or strings)
        if isinstance(started_at_val, str):
            started_at = datetime.fromisoformat(started_at_val.replace('Z', '+00:00'))
        else:
            started_at = started_at_val
        
        if isinstance(finished_at_val, str):
            finished_at = datetime.fromisoformat(finished_at_val.replace('Z', '+00:00'))
        else:
            finished_at = finished_at_val
        
        # Verify timestamps are within expected range
        assert before <= started_at <= after
        assert before <= finished_at <= after
        assert started_at <= finished_at
    
    def test_multiple_runs_separate_metrics(self, tmp_path):
        """Test that metrics from different runs are kept separate."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        run_id_1 = "test_run_123"
        run_id_2 = "test_run_456"
        
        recorder_1 = MetricsRecorder(client, run_id_1)
        recorder_2 = MetricsRecorder(client, run_id_2)
        
        # Record metrics for both runs
        with recorder_1.record_stage(MetricsRecorder.STAGE_INGEST, row_count=100):
            time.sleep(0.01)
        
        with recorder_2.record_stage(MetricsRecorder.STAGE_INGEST, row_count=200):
            time.sleep(0.01)
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics are separate
        reader = client.get_reader()
        
        metrics_1 = reader.execute(
            "SELECT COUNT(*) FROM performance_metrics WHERE run_id = ?",
            [run_id_1]
        ).fetchone()[0]
        
        metrics_2 = reader.execute(
            "SELECT COUNT(*) FROM performance_metrics WHERE run_id = ?",
            [run_id_2]
        ).fetchone()[0]
        
        assert metrics_1 > 0
        assert metrics_2 > 0
        assert metrics_1 == metrics_2  # Should have same number of metrics
        
        # Verify row counts are different
        row_count_1 = reader.execute(
            "SELECT value FROM performance_metrics WHERE run_id = ? AND metric_name = 'row_count'",
            [run_id_1]
        ).fetchone()[0]
        
        row_count_2 = reader.execute(
            "SELECT value FROM performance_metrics WHERE run_id = ? AND metric_name = 'row_count'",
            [run_id_2]
        ).fetchone()[0]
        
        assert row_count_1 == 100.0
        assert row_count_2 == 200.0
    
    def test_record_memory_usage(self, tmp_path):
        """Test recording memory usage (Phase 16)."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        # Record a stage (memory usage should be recorded automatically if psutil is available)
        with recorder.record_stage(MetricsRecorder.STAGE_INGEST):
            time.sleep(0.1)  # Simulate processing
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ?",
            [run_id]
        ).fetchall()
        
        metric_dict = {m[1]: (m[2], m[3]) for m in metrics}
        
        # Memory metrics may or may not be present depending on psutil availability
        # If psutil is available, memory_mb should be recorded
        if "memory_mb" in metric_dict:
            assert metric_dict["memory_mb"][0] > 0  # Should be positive
            assert metric_dict["memory_mb"][1] == "MB"
            
            # If start memory was recorded, memory_delta_mb should also be present
            if "memory_delta_mb" in metric_dict:
                assert metric_dict["memory_delta_mb"][1] == "MB"
    
    def test_record_llm_cost_and_budget(self, tmp_path):
        """Test recording LLM cost and budget consumption (Phase 16)."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        # Insert some API costs for this run
        reader = client.get_reader()
        
        # Insert test API costs using reader connection
        reader.execute(
            "INSERT INTO api_costs (id, run_id, provider, model, request_count, input_tokens, output_tokens, cost_usd_estimated) VALUES (nextval('seq_api_costs_id'), ?, ?, ?, ?, ?, ?, ?)",
            [run_id, "openai", "gpt-4", 5, 1000, 2000, 0.15]
        )
        reader.execute(
            "INSERT INTO api_costs (id, run_id, provider, model, request_count, input_tokens, output_tokens, cost_usd_estimated) VALUES (nextval('seq_api_costs_id'), ?, ?, ?, ?, ?, ?, ?)",
            [run_id, "openai", "gpt-4", 3, 500, 1000, 0.075]
        )
        
        client.flush()
        time.sleep(0.2)
        
        # Create a mock budget controller
        from llm.budget import BudgetController
        
        budget_controller = BudgetController(daily_limit_usd=10.0)
        budget_controller.record_spending(0.225)  # Record spending to match API costs
        
        # Record LLM cost and budget
        recorder.record_llm_cost_and_budget(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            budget_controller=budget_controller
        )
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ? AND stage = ?",
            [run_id, MetricsRecorder.STAGE_LLM_ANALYSIS]
        ).fetchall()
        
        metric_dict = {m[1]: (m[2], m[3]) for m in metrics}
        
        # Should have LLM cost metric
        assert "llm_cost_usd" in metric_dict
        assert abs(metric_dict["llm_cost_usd"][0] - 0.225) < 0.001  # Should match total cost
        assert metric_dict["llm_cost_usd"][1] == "USD"
        
        # Should have budget consumption percentage
        assert "budget_consumed_pct" in metric_dict
        assert 0.0 <= metric_dict["budget_consumed_pct"][0] <= 100.0  # Should be 0-100%
        assert metric_dict["budget_consumed_pct"][1] == "%"
        
        # Should have budget status details
        assert "budget_daily_limit_usd" in metric_dict
        assert metric_dict["budget_daily_limit_usd"][0] == 10.0
        assert metric_dict["budget_daily_limit_usd"][1] == "USD"
        
        assert "budget_daily_spent_usd" in metric_dict
        assert abs(metric_dict["budget_daily_spent_usd"][0] - 0.225) < 0.001
        assert metric_dict["budget_daily_spent_usd"][1] == "USD"
        
        assert "budget_remaining_usd" in metric_dict
        assert metric_dict["budget_remaining_usd"][0] >= 0.0
        assert metric_dict["budget_remaining_usd"][1] == "USD"
    
    def test_record_llm_cost_without_budget_controller(self, tmp_path):
        """Test recording LLM cost without budget controller (Phase 16)."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        run_id = "test_run_123"
        
        recorder = MetricsRecorder(client, run_id)
        
        # Insert some API costs for this run
        reader = client.get_reader()
        
        reader.execute(
            "INSERT INTO api_costs (id, run_id, provider, model, request_count, input_tokens, output_tokens, cost_usd_estimated) VALUES (nextval('seq_api_costs_id'), ?, ?, ?, ?, ?, ?, ?)",
            [run_id, "openai", "gpt-4", 2, 500, 1000, 0.05]
        )
        
        client.flush()
        time.sleep(0.2)
        
        # Record LLM cost without budget controller
        recorder.record_llm_cost_and_budget(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            budget_controller=None
        )
        
        # Flush to ensure writes are completed
        client.flush()
        time.sleep(0.2)  # Wait for writer thread
        
        # Verify metrics were recorded
        reader = client.get_reader()
        metrics = reader.execute(
            "SELECT stage, metric_name, value, unit FROM performance_metrics WHERE run_id = ? AND stage = ?",
            [run_id, MetricsRecorder.STAGE_LLM_ANALYSIS]
        ).fetchall()
        
        metric_dict = {m[1]: (m[2], m[3]) for m in metrics}
        
        # Should have LLM cost metric
        assert "llm_cost_usd" in metric_dict
        assert abs(metric_dict["llm_cost_usd"][0] - 0.05) < 0.001
        
        # Should NOT have budget metrics (since budget_controller was None)
        assert "budget_consumed_pct" not in metric_dict
        assert "budget_daily_limit_usd" not in metric_dict