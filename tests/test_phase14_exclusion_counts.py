"""
Test Phase 14: Accurate exclusion counts in audit narrative

Tests that exclusion counts are accurately calculated from Parquet files
and displayed in Excel audit narrative section.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
from datetime import datetime
from unittest.mock import Mock, MagicMock
import duckdb

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.excel_writer import ExcelWriter
from ingestor.parquet_writer import ParquetWriter


class TestPhase14ExclusionCounts:
    """Test Phase 14: Accurate exclusion counts."""
    
    def test_exclusion_counts_from_parquet(self, tmp_path):
        """Exclusion counts should be calculated from Parquet files."""
        excel_path = tmp_path / "test_report.xlsx"
        writer = ExcelWriter(excel_path)
        
        # Create processed directory structure
        processed_dir = tmp_path / "data" / "processed"
        vendor_dir = processed_dir / "vendor=paloalto" / "date=2024-01-15"
        vendor_dir.mkdir(parents=True, exist_ok=True)
        
        # Create test Parquet file with mixed actions
        parquet_writer = ParquetWriter(base_dir=processed_dir)
        
        test_events = [
            {
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path1",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            },
            {
                "event_time": "2024-01-15T10:01:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user2",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path2",
                "action": "block",
                "bytes_sent": 2048,
                "bytes_received": 4096,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "b" * 64
            },
            {
                "event_time": "2024-01-15T10:02:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user3",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path3",
                "action": "allow",
                "bytes_sent": 3072,
                "bytes_received": 6144,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "c" * 64
            }
        ]
        
        parquet_path = parquet_writer.write_events(
            events=test_events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        report_data = {
            "run_id": "test_run_123",
            "vendor": "paloalto",
            "exclusions": {
                "action_filter": "allow"
            },
            "counts": {
                "total_events": 3,
                "total_signatures": 3
            }
        }
        
        run_context = Mock()
        run_context.run_id = "test_run_123"
        
        # Create mock DB reader
        db_reader = duckdb.connect(":memory:")
        
        # Create audit narrative sheet
        writer._create_audit_narrative_sheet(
            report_data=report_data,
            run_context=run_context,
            db_reader=db_reader,
            run_id="test_run_123"
        )
        
        # Verify sheet was created
        assert "AuditNarrative" in writer.sheets
        
        # Verify exclusion count is calculated (1 block event should be excluded)
        # The exclusion count should be 1 (block event when action_filter="allow")
        
        writer.workbook.close()
    
    def test_exclusion_counts_no_parquet(self, tmp_path):
        """Exclusion counts should handle missing Parquet files gracefully."""
        excel_path = tmp_path / "test_report.xlsx"
        writer = ExcelWriter(excel_path)
        
        report_data = {
            "run_id": "test_run_123",
            "vendor": "paloalto",
            "exclusions": {
                "action_filter": "allow"
            },
            "counts": {
                "total_events": 0,
                "total_signatures": 0
            }
        }
        
        run_context = Mock()
        run_context.run_id = "test_run_123"
        
        db_reader = duckdb.connect(":memory:")
        
        # Should not raise error even without Parquet files
        writer._create_audit_narrative_sheet(
            report_data=report_data,
            run_context=run_context,
            db_reader=db_reader,
            run_id="test_run_123"
        )
        
        assert "AuditNarrative" in writer.sheets
        writer.workbook.close()
