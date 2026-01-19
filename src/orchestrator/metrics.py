"""
AIMO Analysis Engine - Performance Metrics Recorder

Records stage-level performance metrics to DuckDB for observability.
"""

import time
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import contextmanager

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

from db.duckdb_client import DuckDBClient


class MetricsRecorder:
    """
    Records performance metrics for each stage.
    
    Features:
    - Stage-level timing (duration_ms)
    - Throughput metrics (rows_per_sec)
    - I/O metrics (bytes_read, bytes_written) if available
    - Memory usage (if available)
    """
    
    # Stage names (matching schema.sql)
    STAGE_INGEST = "ingest"
    STAGE_NORMALIZE = "normalize"
    STAGE_ABC_CACHE = "abc_cache"
    STAGE_RULE_CLASSIFICATION = "rule_classification"
    STAGE_LLM_ANALYSIS = "llm"
    STAGE_REPORTING = "report"
    
    def __init__(self, db_client: DuckDBClient, run_id: str):
        """
        Initialize metrics recorder.
        
        Args:
            db_client: DuckDB client instance
            run_id: Current run ID
        """
        self.db_client = db_client
        self.run_id = run_id
        self._stage_start_times: Dict[str, float] = {}
        self._stage_start_memory: Dict[str, float] = {}
    
    @contextmanager
    def record_stage(self, stage: str, row_count: Optional[int] = None,
                     bytes_read: Optional[int] = None,
                     bytes_written: Optional[int] = None):
        """
        Context manager for recording stage metrics.
        
        Usage:
            with metrics_recorder.record_stage("ingest", row_count=1000):
                # Stage processing code
                pass
        
        Args:
            stage: Stage name (e.g., "ingest", "normalize")
            row_count: Number of rows processed (for rows_per_sec calculation)
            bytes_read: Bytes read during stage (optional)
            bytes_written: Bytes written during stage (optional)
        """
        started_at = datetime.utcnow()
        start_time = time.time()
        
        self._stage_start_times[stage] = start_time
        
        # Record memory usage at start (if psutil is available)
        start_memory_mb = None
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                start_memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
                self._stage_start_memory[stage] = start_memory_mb
            except Exception:
                # If memory measurement fails, continue without it
                pass
        
        try:
            yield
        finally:
            finished_at = datetime.utcnow()
            end_time = time.time()
            
            # Calculate duration
            duration_ms = (end_time - start_time) * 1000.0
            
            # Record duration metric
            self._record_metric(
                stage=stage,
                metric_name="duration_ms",
                value=duration_ms,
                unit="ms",
                started_at=started_at,
                finished_at=finished_at
            )
            
            # Record memory usage at end (if psutil is available)
            if PSUTIL_AVAILABLE:
                try:
                    process = psutil.Process()
                    memory_info = process.memory_info()
                    end_memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
                    
                    # Record end memory
                    self._record_metric(
                        stage=stage,
                        metric_name="memory_mb",
                        value=end_memory_mb,
                        unit="MB",
                        started_at=started_at,
                        finished_at=finished_at
                    )
                    
                    # Record memory delta if start memory was recorded
                    if start_memory_mb is not None:
                        memory_delta_mb = end_memory_mb - start_memory_mb
                        self._record_metric(
                            stage=stage,
                            metric_name="memory_delta_mb",
                            value=memory_delta_mb,
                            unit="MB",
                            started_at=started_at,
                            finished_at=finished_at
                        )
                except Exception:
                    # If memory measurement fails, continue without it
                    pass
            
            # Record throughput if row_count is provided
            if row_count is not None and row_count > 0:
                rows_per_sec = row_count / (end_time - start_time) if (end_time - start_time) > 0 else 0.0
                self._record_metric(
                    stage=stage,
                    metric_name="rows_per_sec",
                    value=rows_per_sec,
                    unit="rows/sec",
                    started_at=started_at,
                    finished_at=finished_at
                )
                
                # Record row count
                self._record_metric(
                    stage=stage,
                    metric_name="row_count",
                    value=float(row_count),
                    unit="rows",
                    started_at=started_at,
                    finished_at=finished_at
                )
            
            # Record I/O metrics if provided
            if bytes_read is not None:
                self._record_metric(
                    stage=stage,
                    metric_name="bytes_read",
                    value=float(bytes_read),
                    unit="bytes",
                    started_at=started_at,
                    finished_at=finished_at
                )
            
            if bytes_written is not None:
                self._record_metric(
                    stage=stage,
                    metric_name="bytes_written",
                    value=float(bytes_written),
                    unit="bytes",
                    started_at=started_at,
                    finished_at=finished_at
                )
    
    def record_metric(self, stage: str, metric_name: str, value: float,
                     unit: Optional[str] = None,
                     started_at: Optional[datetime] = None,
                     finished_at: Optional[datetime] = None):
        """
        Record a custom metric.
        
        Args:
            stage: Stage name
            metric_name: Metric name (e.g., "duration_ms", "rows_per_sec")
            value: Metric value
            unit: Unit (e.g., "ms", "rows/sec", "bytes")
            started_at: Stage start time (optional)
            finished_at: Stage end time (optional)
        """
        self._record_metric(
            stage=stage,
            metric_name=metric_name,
            value=value,
            unit=unit,
            started_at=started_at,
            finished_at=finished_at
        )
    
    def _record_metric(self, stage: str, metric_name: str, value: float,
                      unit: Optional[str] = None,
                      started_at: Optional[datetime] = None,
                      finished_at: Optional[datetime] = None):
        """
        Internal method to record a metric to DuckDB.
        
        Args:
            stage: Stage name
            metric_name: Metric name
            value: Metric value
            unit: Unit
            started_at: Stage start time
            finished_at: Stage end time
        """
        # Get next ID from sequence
        reader = self.db_client.get_reader()
        id_result = reader.execute(
            "SELECT nextval('seq_perf_metrics_id')"
        ).fetchone()
        metric_id = id_result[0] if id_result else None
        
        if metric_id is None:
            raise RuntimeError("Failed to get next ID from sequence seq_perf_metrics_id")
        
        # Prepare data for insert
        data = {
            "id": metric_id,
            "run_id": self.run_id,
            "stage": stage,
            "metric_name": metric_name,
            "value": value,
            "unit": unit,
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None
        }
        
        # Insert metric
        self.db_client.insert("performance_metrics", data, ignore_conflict=False)
    
    def record_llm_cost_and_budget(self, stage: str, budget_controller=None):
        """
        Record LLM cost and budget consumption for a stage.
        
        Args:
            stage: Stage name (typically STAGE_LLM_ANALYSIS)
            budget_controller: BudgetController instance (optional)
        """
        reader = self.db_client.get_reader()
        
        # Get total cost from api_costs table for this run
        cost_result = reader.execute(
            "SELECT SUM(cost_usd_estimated) FROM api_costs WHERE run_id = ?",
            [self.run_id]
        ).fetchone()
        total_cost_usd = cost_result[0] if cost_result and cost_result[0] else 0.0
        
        # Record total cost
        if total_cost_usd > 0:
            self.record_metric(
                stage=stage,
                metric_name="llm_cost_usd",
                value=total_cost_usd,
                unit="USD"
            )
        
        # Record budget consumption percentage if budget_controller is provided
        if budget_controller is not None:
            try:
                budget_utilization = budget_controller.get_budget_utilization()
                budget_consumed_pct = budget_utilization * 100.0  # Convert to percentage
                
                self.record_metric(
                    stage=stage,
                    metric_name="budget_consumed_pct",
                    value=budget_consumed_pct,
                    unit="%"
                )
                
                # Also record budget status details
                budget_status = budget_controller.get_status()
                self.record_metric(
                    stage=stage,
                    metric_name="budget_daily_limit_usd",
                    value=budget_status.get("daily_limit_usd", 0.0),
                    unit="USD"
                )
                self.record_metric(
                    stage=stage,
                    metric_name="budget_daily_spent_usd",
                    value=budget_status.get("daily_spent_usd", 0.0),
                    unit="USD"
                )
                self.record_metric(
                    stage=stage,
                    metric_name="budget_remaining_usd",
                    value=budget_status.get("remaining_usd", 0.0),
                    unit="USD"
                )
            except Exception as e:
                # If budget recording fails, log but don't fail
                print(f"Warning: Failed to record budget metrics: {e}", file=__import__("sys").stderr)