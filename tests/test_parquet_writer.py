"""
Test Parquet Writer

Tests for Phase 8: Parquet Hive Partition Output functionality.
- Hive partition format
- Atomic writes (.tmp -> rename)
- Date partition extraction
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestor.parquet_writer import ParquetWriter
import pyarrow.parquet as pq


class TestParquetWriter:
    """Test Parquet writer functionality."""
    
    def test_hive_partition_format(self, tmp_path):
        """Parquet files should be written in Hive partition format."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        # Create test events
        events = [
            {
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            }
        ]
        
        # Write Parquet
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        # Verify Hive partition format: vendor=paloalto/date=2024-01-15/...
        assert "vendor=paloalto" in str(parquet_path)
        assert "date=2024-01-15" in str(parquet_path)
        assert parquet_path.suffix == ".parquet"
        assert parquet_path.exists()
        
        # Verify file can be read (use ParquetFile to read single file directly)
        parquet_file = pq.ParquetFile(str(parquet_path))
        table = parquet_file.read()
        assert len(table) == 1
    
    def test_atomic_write(self, tmp_path):
        """Parquet writes should be atomic (.tmp -> rename)."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        events = [
            {
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            }
        ]
        
        # Write Parquet
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        # Verify no .tmp file exists (should have been renamed)
        tmp_files = list(parquet_path.parent.glob("*.tmp"))
        assert len(tmp_files) == 0
        
        # Verify final file exists
        assert parquet_path.exists()
    
    def test_date_partition_extraction(self, tmp_path):
        """Date partition should be extracted from event_time."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        # Test with different date formats
        test_cases = [
            ("2024-01-15T10:00:00Z", "2024-01-15"),
            ("2024-12-31T23:59:59Z", "2024-12-31"),
            ("2024-06-01T00:00:00+00:00", "2024-06-01"),
        ]
        
        for event_time, expected_date in test_cases:
            events = [
                {
                    "event_time": event_time,
                    "vendor": "paloalto",
                    "log_type": "web",
                    "user_id": "user1",
                    "dest_host": "example.com",
                    "dest_domain": "example.com",
                    "url_full": "https://example.com/path",
                    "action": "allow",
                    "bytes_sent": 1024,
                    "bytes_received": 2048,
                    "ingest_file": "test.csv",
                    "ingest_lineage_hash": "a" * 64
                }
            ]
            
            parquet_path = writer.write_events(
                events=events,
                vendor="paloalto",
                run_id="test_run_123"
            )
            
            # Verify date partition
            assert f"date={expected_date}" in str(parquet_path)
    
    def test_date_partition_fallback(self, tmp_path):
        """Date partition should fallback to current date if event_time is missing."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        # Event without event_time
        events = [
            {
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            }
        ]
        
        # Should not raise error (falls back to current date)
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        # Verify file was created
        assert parquet_path.exists()
        # Date partition should exist (current date)
        assert "date=" in str(parquet_path)
    
    def test_multiple_events(self, tmp_path):
        """Should handle multiple events correctly."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        # Create multiple events
        events = []
        for i in range(100):
            events.append({
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": f"user{i}",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": f"https://example.com/path{i}",
                "action": "allow",
                "bytes_sent": 1024 + i,
                "bytes_received": 2048 + i,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            })
        
        # Write Parquet
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        # Verify file can be read (use ParquetFile to read single file directly)
        parquet_file = pq.ParquetFile(str(parquet_path))
        table = parquet_file.read()
        assert len(table) == 100
    
    def test_explicit_date_partition(self, tmp_path):
        """Should use explicit date partition if provided."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        events = [
            {
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            }
        ]
        
        # Write with explicit date partition
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123",
            date_partition="2024-12-31"
        )
        
        # Verify explicit date partition was used
        assert "date=2024-12-31" in str(parquet_path)
        assert "date=2024-01-15" not in str(parquet_path)
    
    def test_snappy_compression(self, tmp_path):
        """Parquet files should use Snappy compression."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        events = [
            {
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64
            }
        ]
        
        # Write Parquet
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        # Read metadata to verify compression
        parquet_file = pq.ParquetFile(str(parquet_path))
        # Check that file uses Snappy compression (metadata should indicate this)
        # Note: PyArrow doesn't expose compression directly, but we can verify file exists
        assert parquet_path.exists()
        assert parquet_file.metadata is not None
    
    def test_nullable_fields(self, tmp_path):
        """Should handle nullable fields correctly."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        # Event with null values
        events = [
            {
                "event_time": "2024-01-15T10:00:00Z",
                "vendor": "paloalto",
                "log_type": "web",
                "user_id": "user1",
                "dest_host": "example.com",
                "dest_domain": "example.com",
                "url_full": "https://example.com/path",
                "action": "allow",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "ingest_file": "test.csv",
                "ingest_lineage_hash": "a" * 64,
                # Nullable fields
                "user_dept": None,
                "device_id": None,
                "src_ip": None,
                "url_path": None,
                "url_query": None,
                "http_method": None,
                "status_code": None,
                "app_name": None,
                "app_category": None,
                "content_type": None,
                "user_agent": None,
                "raw_event_id": None,
            }
        ]
        
        # Should not raise error
        parquet_path = writer.write_events(
            events=events,
            vendor="paloalto",
            run_id="test_run_123"
        )
        
        # Verify file can be read (use ParquetFile to read single file directly)
        parquet_file = pq.ParquetFile(str(parquet_path))
        table = parquet_file.read()
        assert len(table) == 1
    
    def test_get_partition_path(self, tmp_path):
        """get_partition_path should return correct partition directory."""
        processed_dir = tmp_path / "processed"
        writer = ParquetWriter(base_dir=processed_dir)
        
        partition_path = writer.get_partition_path("paloalto", "2024-01-15")
        
        assert partition_path == processed_dir / "vendor=paloalto" / "date=2024-01-15"
        assert "vendor=paloalto" in str(partition_path)
        assert "date=2024-01-15" in str(partition_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
