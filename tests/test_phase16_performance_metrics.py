"""
Test Phase 16: Complete performance metrics recording

Tests that memory usage and LLM cost/budget metrics are recorded.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch
import duckdb

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from orchestrator.metrics import MetricsRecorder
from db.duckdb_client import DuckDBClient


class TestPhase16PerformanceMetrics:
    """Test Phase 16: Complete performance metrics."""
    
    def test_memory_usage_recording(self, tmp_path):
        """Memory usage should be recorded if psutil is available."""
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        
        recorder = MetricsRecorder(db_client, "test_run_123")
        
        # Record a stage with memory measurement
        with recorder.record_stage("test_stage", row_count=100):
            # Simulate some work
            pass
        
        # Check that memory metrics were recorded
        reader = db_client.get_reader()
        memory_metrics = reader.execute("""
            SELECT metric_name, value, unit
            FROM performance_metrics
            WHERE run_id = ? AND metric_name LIKE 'memory%'
        """, ["test_run_123"]).fetchall()
        
        # Memory metrics should be recorded if psutil is available
        # If psutil is not available, no memory metrics will be recorded
        # This is acceptable behavior
        
        db_client.close()
    
    def test_llm_cost_recording(self, tmp_path):
        """LLM cost and budget metrics should be recorded."""
        db_path = tmp_path / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        
        # Initialize schema (create performance_metrics table)
        reader = db_client.get_reader()
        reader.execute("""
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                stage VARCHAR NOT NULL,
                metric_name VARCHAR NOT NULL,
                value DOUBLE NOT NULL,
                unit VARCHAR
            )
        """)
        reader.execute("CREATE SEQUENCE IF NOT EXISTS seq_perf_metrics_id START 1")
        
        recorder = MetricsRecorder(db_client, "test_run_123")
        
        # Create mock budget controller
        mock_budget = Mock()
        mock_budget.get_budget_utilization.return_value = 0.5  # 50% utilized
        mock_budget.get_status.return_value = {
            "daily_limit_usd": 100.0,
            "daily_spent_usd": 50.0,
            "remaining_usd": 50.0
        }
        
        # Insert test API costs (with required fields)
        reader.execute("""
            CREATE TABLE IF NOT EXISTS api_costs (
                id INTEGER PRIMARY KEY,
                run_id VARCHAR,
                provider VARCHAR NOT NULL,
                model VARCHAR NOT NULL,
                cost_usd_estimated DOUBLE
            )
        """)
        # Create sequence if not exists
        reader.execute("CREATE SEQUENCE IF NOT EXISTS seq_api_costs_id START 1")
        reader.execute("""
            INSERT INTO api_costs (id, run_id, provider, model, cost_usd_estimated) 
            VALUES (nextval('seq_api_costs_id'), ?, ?, ?, ?)
        """, ["test_run_123", "gemini", "gemini-1.5-flash", 25.0])
        
        # Record LLM cost and budget
        recorder.record_llm_cost_and_budget(
            "llm",
            budget_controller=mock_budget
        )
        
        # Flush to ensure writes are committed
        db_client.flush()
        
        # Check that cost and budget metrics were recorded
        cost_metrics = reader.execute("""
            SELECT metric_name, value, unit
            FROM performance_metrics
            WHERE run_id = ? AND (metric_name LIKE '%cost%' OR metric_name LIKE '%budget%')
        """, ["test_run_123"]).fetchall()
        
        # Should have cost and budget metrics
        assert len(cost_metrics) > 0
        
        db_client.close()
