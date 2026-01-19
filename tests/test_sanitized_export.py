"""
Tests for Sanitized Export (Phase 10)

Tests that sanitized CSV exports:
- Hash PII fields irreversibly
- Only include allowed columns
- Are safe for external sharing
"""

import pytest
import os
import csv
import hashlib
import sys
import re
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, MagicMock

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporting.sanitized_export import SanitizedExporter


class TestSanitizedExport:
    """Tests for sanitized CSV export."""
    
    @pytest.fixture
    def salt(self):
        """Test salt for hashing."""
        return "test_salt_12345"
    
    @pytest.fixture
    def exporter(self, salt):
        """Create exporter with test salt."""
        return SanitizedExporter(salt=salt)
    
    def test_anonymize_hashes_value(self, exporter):
        """Anonymize should hash values irreversibly."""
        original = "user123"
        hashed = exporter.anonymize(original)
        
        # Should be 16 characters (first 16 of SHA256)
        assert len(hashed) == 16
        assert hashed.isalnum()
        
        # Should be deterministic with same salt
        hashed2 = exporter.anonymize(original)
        assert hashed == hashed2
        
        # Should be different for different values
        hashed3 = exporter.anonymize("user456")
        assert hashed != hashed3
    
    def test_anonymize_empty_value(self, exporter):
        """Anonymize should return empty string for empty/None values."""
        assert exporter.anonymize("") == ""
        assert exporter.anonymize(None) == ""
    
    def test_anonymize_uses_salt(self, exporter, salt):
        """Anonymize should use salt in hashing."""
        original = "test_user"
        hashed1 = exporter.anonymize(original)
        
        # Create new exporter with different salt
        exporter2 = SanitizedExporter(salt="different_salt")
        hashed2 = exporter2.anonymize(original)
        
        # Should produce different hashes
        assert hashed1 != hashed2
    
    def test_export_csv_from_events_basic(self, exporter, tmp_path):
        """Test basic CSV export from events."""
        # Create mock events
        events = [
            {
                "event_time": "2024-01-17T10:00:00Z",
                "dest_domain": "example.com",
                "user_id": "user123",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "action": "allow",
                "ingest_lineage_hash": "hash1"
            },
            {
                "event_time": "2024-01-17T10:01:00Z",
                "dest_domain": "test.com",
                "user_id": "user456",
                "bytes_sent": 512,
                "bytes_received": 1024,
                "action": "block",
                "ingest_lineage_hash": "hash2"
            }
        ]
        
        # Create mock signatures dict
        signatures = {
            "sig1": {
                "signature": {"url_signature": "sig1"},
                "events": [events[0]]
            },
            "sig2": {
                "signature": {"url_signature": "sig2"},
                "events": [events[1]]
            }
        }
        
        # Create mock DB reader
        mock_reader = Mock()
        mock_reader.execute.return_value.fetchall.return_value = [
            ("sig1", "Service1", "genai", "high", "AI"),
            ("sig2", "Service2", "business", "low", "SaaS")
        ]
        
        output_path = tmp_path / "sanitized.csv"
        
        # Export
        row_count = exporter.export_csv_from_events(
            events=events,
            signatures=signatures,
            db_reader=mock_reader,
            run_id="test_run",
            output_path=output_path,
            max_rows=100
        )
        
        # Check file was created
        assert output_path.exists()
        assert row_count == 2
        
        # Read and verify CSV
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 2
        
        # Check columns
        expected_columns = ['ts', 'dest_domain', 'url_signature', 'service_name',
                           'usage_type', 'risk_level', 'category', 'bytes_sent',
                           'bytes_received', 'action', 'user_hash']
        assert set(rows[0].keys()) == set(expected_columns)
        
        # Check PII is hashed
        assert rows[0]['user_hash'] != "user123"
        assert len(rows[0]['user_hash']) == 16
        assert rows[0]['user_hash'].isalnum()
    
    def test_export_csv_from_events_no_signatures(self, exporter, tmp_path):
        """Test export when signatures dict is empty (fallback to DB)."""
        events = [
            {
                "event_time": "2024-01-17T10:00:00Z",
                "dest_domain": "example.com",
                "user_id": "user123",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "action": "allow",
                "ingest_lineage_hash": "hash1"
            }
        ]
        
        # Empty signatures dict
        signatures = {}
        
        # Create mock DB reader
        mock_reader = Mock()
        # First query: get signatures
        mock_reader.execute.return_value.fetchall.side_effect = [
            [("sig1", "example.com", "example.com", "/path")],  # signature_stats
            [("sig1", "Service1", "genai", "high", "AI")]  # analysis_cache
        ]
        
        output_path = tmp_path / "sanitized.csv"
        
        # Export should work with empty signatures
        row_count = exporter.export_csv_from_events(
            events=events,
            signatures=signatures,
            db_reader=mock_reader,
            run_id="test_run",
            output_path=output_path,
            max_rows=100
        )
        
        assert output_path.exists()
        assert row_count == 1
    
    def test_export_csv_atomic_write(self, exporter, tmp_path):
        """Test that CSV write is atomic (uses .tmp then rename)."""
        events = [
            {
                "event_time": "2024-01-17T10:00:00Z",
                "dest_domain": "example.com",
                "user_id": "user123",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "action": "allow",
                "ingest_lineage_hash": "hash1"
            }
        ]
        
        signatures = {
            "sig1": {
                "signature": {"url_signature": "sig1"},
                "events": [events[0]]
            }
        }
        
        mock_reader = Mock()
        mock_reader.execute.return_value.fetchall.return_value = [
            ("sig1", "Service1", "genai", "high", "AI")
        ]
        
        output_path = tmp_path / "sanitized.csv"
        
        # Export
        exporter.export_csv_from_events(
            events=events,
            signatures=signatures,
            db_reader=mock_reader,
            run_id="test_run",
            output_path=output_path,
            max_rows=100
        )
        
        # .tmp file should not exist (renamed)
        tmp_file = output_path.with_suffix(output_path.suffix + ".tmp")
        assert not tmp_file.exists()
        
        # Final file should exist
        assert output_path.exists()
    
    def test_validate_sanitized_no_forbidden_columns(self, exporter, tmp_path):
        """Test validation rejects forbidden columns."""
        # Create CSV with forbidden column
        csv_path = tmp_path / "test.csv"
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['ts', 'user_id', 'dest_domain'])
            writer.writeheader()
            writer.writerow({'ts': '2024-01-17T10:00:00Z', 'user_id': 'user123', 'dest_domain': 'example.com'})
        
        errors = exporter.validate_sanitized(csv_path)
        
        assert len(errors) > 0
        assert any('user_id' in error for error in errors)
    
    def test_validate_sanitized_no_email_patterns(self, exporter, tmp_path):
        """Test validation rejects email patterns in data."""
        # Create CSV with email in data
        csv_path = tmp_path / "test.csv"
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['ts', 'dest_domain', 'service_name'])
            writer.writeheader()
            writer.writerow({
                'ts': '2024-01-17T10:00:00Z',
                'dest_domain': 'user@example.com',  # Email pattern
                'service_name': 'Service1'
            })
        
        errors = exporter.validate_sanitized(csv_path)
        
        assert len(errors) > 0
        assert any('Email pattern' in error for error in errors)
    
    def test_validate_sanitized_valid_csv(self, exporter, tmp_path):
        """Test validation passes for valid sanitized CSV."""
        # Create valid sanitized CSV
        csv_path = tmp_path / "test.csv"
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['ts', 'dest_domain', 'url_signature', 'user_hash'])
            writer.writeheader()
            writer.writerow({
                'ts': '2024-01-17T10:00:00Z',
                'dest_domain': 'example.com',
                'url_signature': 'sig123',
                'user_hash': 'a1b2c3d4e5f6g7h8'
            })
        
        errors = exporter.validate_sanitized(csv_path)
        
        assert len(errors) == 0
    
    def test_export_respects_max_rows(self, exporter, tmp_path):
        """Test that export respects max_rows limit."""
        # Create many events
        events = [
            {
                "event_time": f"2024-01-17T10:{i:02d}:00Z",
                "dest_domain": "example.com",
                "user_id": f"user{i}",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "action": "allow",
                "ingest_lineage_hash": f"hash{i}"
            }
            for i in range(150)  # More than max_rows
        ]
        
        signatures = {}
        for i, event in enumerate(events):
            sig_key = f"sig{i}"
            signatures[sig_key] = {
                "signature": {"url_signature": sig_key},
                "events": [event]
            }
        
        mock_reader = Mock()
        mock_reader.execute.return_value.fetchall.return_value = []
        
        output_path = tmp_path / "sanitized.csv"
        
        # Export with max_rows=100
        row_count = exporter.export_csv_from_events(
            events=events,
            signatures=signatures,
            db_reader=mock_reader,
            run_id="test_run",
            output_path=output_path,
            max_rows=100
        )
        
        # Should only export 100 rows
        assert row_count == 100
        
        # Verify file has 100 data rows + 1 header
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 101  # 1 header + 100 rows
    
    def test_export_handles_missing_classification(self, exporter, tmp_path):
        """Test export handles missing classification data gracefully."""
        events = [
            {
                "event_time": "2024-01-17T10:00:00Z",
                "dest_domain": "example.com",
                "user_id": "user123",
                "bytes_sent": 1024,
                "bytes_received": 2048,
                "action": "allow",
                "ingest_lineage_hash": "hash1"
            }
        ]
        
        signatures = {
            "sig1": {
                "signature": {"url_signature": "sig1"},
                "events": [events[0]]
            }
        }
        
        # Mock DB reader returns empty classification (not in cache)
        mock_reader = Mock()
        mock_reader.execute.return_value.fetchall.return_value = []
        
        output_path = tmp_path / "sanitized.csv"
        
        # Export should work, using 'unknown' defaults
        row_count = exporter.export_csv_from_events(
            events=events,
            signatures=signatures,
            db_reader=mock_reader,
            run_id="test_run",
            output_path=output_path,
            max_rows=100
        )
        
        assert row_count == 1
        
        # Read and verify defaults
        with open(output_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert rows[0]['service_name'] == 'unknown'
        assert rows[0]['usage_type'] == 'unknown'
        assert rows[0]['risk_level'] == 'unknown'


class TestSanitizedExportEnvironment:
    """Tests for environment variable handling."""
    
    def test_exporter_requires_salt(self, monkeypatch):
        """Exporter should require SANITIZE_SALT if not provided."""
        # Remove env var
        monkeypatch.delenv("SANITIZE_SALT", raising=False)
        
        # Should raise error if salt not provided
        with pytest.raises(ValueError, match="SANITIZE_SALT"):
            SanitizedExporter()
    
    def test_exporter_uses_env_salt(self, monkeypatch):
        """Exporter should use SANITIZE_SALT from environment."""
        test_salt = "env_salt_12345"
        monkeypatch.setenv("SANITIZE_SALT", test_salt)
        
        exporter = SanitizedExporter()
        
        # Should use env salt
        hashed = exporter.anonymize("test")
        assert len(hashed) == 16
    
    def test_exporter_prefers_provided_salt(self, monkeypatch):
        """Exporter should prefer provided salt over env var."""
        env_salt = "env_salt"
        provided_salt = "provided_salt"
        
        monkeypatch.setenv("SANITIZE_SALT", env_salt)
        
        exporter = SanitizedExporter(salt=provided_salt)
        
        # Should use provided salt
        hashed1 = exporter.anonymize("test")
        
        # Create another with same provided salt
        exporter2 = SanitizedExporter(salt=provided_salt)
        hashed2 = exporter2.anonymize("test")
        
        assert hashed1 == hashed2
        
        # But different from env salt
        exporter3 = SanitizedExporter()  # Uses env salt
        hashed3 = exporter3.anonymize("test")
        assert hashed1 != hashed3
