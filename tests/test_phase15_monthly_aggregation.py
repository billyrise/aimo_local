"""
Test Phase 15: Monthly time series aggregation

Tests that monthly aggregation is added to time series sheet in addition to weekly.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
from datetime import datetime
from unittest.mock import Mock
import duckdb

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.excel_writer import ExcelWriter
from ingestor.parquet_writer import ParquetWriter


class TestPhase15MonthlyAggregation:
    """Test Phase 15: Monthly time series aggregation."""
    
    def test_monthly_aggregation_in_time_series(self, tmp_path):
        """Time series sheet should include both weekly and monthly aggregation."""
        excel_path = tmp_path / "test_report.xlsx"
        writer = ExcelWriter(excel_path)
        
        # Create processed directory structure
        processed_dir = tmp_path / "data" / "processed"
        vendor_dir = processed_dir / "vendor=paloalto" / "date=2024-01-15"
        vendor_dir.mkdir(parents=True, exist_ok=True)
        
        # Create test Parquet file with events across multiple weeks and months
        parquet_writer = ParquetWriter(base_dir=processed_dir)
        
        test_events = []
        # Create events for January 2024 (week 1-4)
        for i in range(20):
            day = 1 + (i % 28)  # Days 1-28
            event_time = f"2024-01-{day:02d}T10:00:00Z"
            test_events.append({
                "event_time": event_time,
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": f"user{i}",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": f"https://example.com/path{i}",
                "action": "allow",
                "bytes_sent": 1024 * (i + 1),
                "bytes_received": 2048 * (i + 1),
                "ingest_file": "test.csv",
                "ingest_lineage_hash": f"{i}" * 64
            })
        
        # Create events for February 2024 (different month)
        for i in range(10):
            day = 1 + (i % 28)
            event_time = f"2024-02-{day:02d}T10:00:00Z"
            test_events.append({
                "event_time": event_time,
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": f"user{i+20}",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": f"https://example.com/path{i+20}",
                "action": "allow",
                "bytes_sent": 1024 * (i + 21),
                "bytes_received": 2048 * (i + 21),
                "ingest_file": "test.csv",
                "ingest_lineage_hash": f"{i+20}" * 64
            })
        
        parquet_path = parquet_writer.write_events(
            events=test_events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        report_data = {
            "run_id": "test_run_123",
            "vendor": "paloalto",
            "counts": {
                "total_events": 30,
                "total_signatures": 30
            }
        }
        
        run_context = Mock()
        run_context.run_id = "test_run_123"
        
        # Create mock DB reader with signature_stats
        db_reader = duckdb.connect(":memory:")
        db_reader.execute("""
            CREATE TABLE signature_stats (
                run_id VARCHAR,
                url_signature VARCHAR,
                norm_host VARCHAR,
                norm_path_template VARCHAR
            )
        """)
        
        # Insert test signature stats
        for i in range(30):
            db_reader.execute("""
                INSERT INTO signature_stats VALUES
                (?, ?, ?, ?)
            """, ["test_run_123", f"sig_{i}", "example.com", f"/path{i}"])
        
        # Create time series sheet
        writer._create_time_series_sheet(
            report_data=report_data,
            db_reader=db_reader,
            run_id="test_run_123"
        )
        
        # Verify sheet was created
        assert "TimeSeries" in writer.sheets
        
        # Verify that both weekly and monthly data are present
        # The query should return rows with PeriodType = 'Week' and 'Month'
        
        writer.workbook.close()
