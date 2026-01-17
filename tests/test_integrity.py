"""
Test Database Integrity Checks

External key constraints are removed from DDL for test stability.
This module provides application-level integrity checks as a replacement.
"""

import pytest
import tempfile
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duckdb_client import DuckDBClient


class TestIntegrityChecks:
    """Test application-level integrity checks (replacement for FK constraints)."""
    
    def test_no_orphan_run_ids_in_signature_stats(self, tmp_path):
        """
        Check that signature_stats does not contain run_id that doesn't exist in runs.
        This replaces the FK constraint: signature_stats.run_id -> runs.run_id
        """
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # Create a run
        run_data = {
            "run_id": "test_run_123",
            "run_key": "test_key_456",
            "started_at": "2024-01-01T00:00:00",
            "status": "running",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "input_manifest_hash": "test_hash"
        }
        client.upsert("runs", run_data, conflict_key="run_id")
        client.flush()
        
        # Try to create signature_stats with non-existent run_id (should be caught by check)
        stats_data = {
            "run_id": "non_existent_run_id",
            "url_signature": "test_sig_123",
            "norm_host": "example.com",
            "norm_path_template": "/path",
            "bytes_sent_bucket": "M",
            "access_count": 10,
            "unique_users": 5
        }
        client.upsert("signature_stats", stats_data, conflict_key="run_id")
        client.flush()
        
        import time
        time.sleep(0.2)
        
        # Check for orphan run_ids
        result = client._writer_conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM signature_stats ss
            LEFT JOIN runs r ON ss.run_id = r.run_id
            WHERE r.run_id IS NULL
        """).fetchone()
        
        client.close()
        
        # Should have 1 orphan (non_existent_run_id)
        assert result[0] == 1, "Integrity check: Found orphan run_id in signature_stats"
    
    def test_no_orphan_run_ids_in_input_files(self, tmp_path):
        """
        Check that input_files does not contain run_id that doesn't exist in runs.
        This replaces the FK constraint: input_files.run_id -> runs.run_id
        
        Note: input_files.file_id is PK, so we use insert() instead of upsert()
        to test orphan detection.
        """
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # Create a run
        run_data = {
            "run_id": "test_run_123",
            "run_key": "test_key_456",
            "started_at": "2024-01-01T00:00:00",
            "status": "running",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "input_manifest_hash": "test_hash"
        }
        client.upsert("runs", run_data, conflict_key="run_id")
        client.flush()
        
        # Try to create input_files with non-existent run_id using insert()
        # (upsert() would fail because run_id is part of the data but not updatable)
        file_data = {
            "file_id": "test_file_123",
            "run_id": "non_existent_run_id",
            "file_path": "/path/to/file.csv",
            "file_size": 1000,
            "file_hash": "abc123",
            "vendor": "paloalto",
            "log_type": "firewall",
            "row_count": 100
        }
        client.insert("input_files", file_data)
        client.flush()
        
        import time
        time.sleep(0.2)
        
        # Check for orphan run_ids
        result = client._writer_conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM input_files if
            LEFT JOIN runs r ON if.run_id = r.run_id
            WHERE r.run_id IS NULL
        """).fetchone()
        
        client.close()
        
        # Should have 1 orphan (non_existent_run_id)
        assert result[0] == 1, "Integrity check: Found orphan run_id in input_files"
    
    def test_referential_integrity_sql(self, tmp_path):
        """
        Test SQL queries for referential integrity checks.
        These can be run in CI/CD to ensure data integrity.
        """
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # Create valid data
        run_data = {
            "run_id": "test_run_123",
            "run_key": "test_key_456",
            "started_at": "2024-01-01T00:00:00",
            "status": "running",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "input_manifest_hash": "test_hash"
        }
        client.upsert("runs", run_data, conflict_key="run_id")
        client.flush()
        
        import time
        time.sleep(0.2)
        
        # Run integrity check queries
        checks = [
            # Check signature_stats.run_id references runs.run_id
            """
            SELECT COUNT(*) as orphan_count
            FROM signature_stats ss
            LEFT JOIN runs r ON ss.run_id = r.run_id
            WHERE r.run_id IS NULL
            """,
            # Check input_files.run_id references runs.run_id
            """
            SELECT COUNT(*) as orphan_count
            FROM input_files if
            LEFT JOIN runs r ON if.run_id = r.run_id
            WHERE r.run_id IS NULL
            """,
            # Check api_costs.run_id references runs.run_id
            """
            SELECT COUNT(*) as orphan_count
            FROM api_costs ac
            LEFT JOIN runs r ON ac.run_id = r.run_id
            WHERE r.run_id IS NULL
            """,
            # Check performance_metrics.run_id references runs.run_id
            """
            SELECT COUNT(*) as orphan_count
            FROM performance_metrics pm
            LEFT JOIN runs r ON pm.run_id = r.run_id
            WHERE r.run_id IS NULL
            """,
            # Check pii_audit.run_id references runs.run_id
            """
            SELECT COUNT(*) as orphan_count
            FROM pii_audit pa
            LEFT JOIN runs r ON pa.run_id = r.run_id
            WHERE r.run_id IS NULL
            """,
        ]
        
        for check_sql in checks:
            result = client._writer_conn.execute(check_sql).fetchone()
            # With valid data, all should be 0
            assert result[0] == 0, f"Integrity check failed: {check_sql}"
        
        client.close()
    
    def test_analysis_cache_signature_reference(self, tmp_path):
        """
        Check that analysis_cache.url_signature can be referenced from signature_stats.
        This is a logical integrity check (not enforced by FK, but should be consistent).
        """
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # Create run
        run_data = {
            "run_id": "test_run_123",
            "run_key": "test_key_456",
            "started_at": "2024-01-01T00:00:00",
            "status": "running",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "input_manifest_hash": "test_hash"
        }
        client.upsert("runs", run_data, conflict_key="run_id")
        
        # Create analysis_cache entry
        cache_data = {
            "url_signature": "test_sig_123",
            "service_name": "Test Service",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.95,
            "rationale_short": "Test",
            "classification_source": "RULE",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "status": "active",
            "is_human_verified": False
        }
        client.upsert("analysis_cache", cache_data, conflict_key="url_signature")
        
        # Create signature_stats with same url_signature
        stats_data = {
            "run_id": "test_run_123",
            "url_signature": "test_sig_123",
            "norm_host": "example.com",
            "norm_path_template": "/path",
            "bytes_sent_bucket": "M",
            "access_count": 10,
            "unique_users": 5
        }
        client.upsert("signature_stats", stats_data, conflict_key="run_id")
        client.flush()
        
        import time
        time.sleep(0.2)
        
        # Check that signature_stats.url_signature references analysis_cache.url_signature
        # (logical check, not enforced by FK)
        result = client._writer_conn.execute("""
            SELECT COUNT(*) as missing_cache_count
            FROM signature_stats ss
            LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
            WHERE ac.url_signature IS NULL
        """).fetchone()
        
        client.close()
        
        # Should be 0 (all signatures should have cache entries)
        assert result[0] == 0, "Integrity check: Found signature_stats.url_signature without analysis_cache entry"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
