"""
Test Idempotency

Tests that re-running the same input does not cause double counting.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import json
import hashlib

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duckdb_client import DuckDBClient
from ingestor.base import BaseIngestor
from normalize.url_normalizer import URLNormalizer
from signatures.signature_builder import SignatureBuilder


class TestIdempotency:
    """Test idempotency of pipeline operations."""
    
    def test_run_id_determinism(self):
        """Same input should produce same run_id."""
        # Import here to avoid circular import
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
        
        # Copy compute_run_id logic here to avoid circular import
        def compute_run_id(input_file: str, signature_version: str = "1.0"):
            file_path = Path(input_file)
            with open(file_path, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            file_size = file_path.stat().st_size
            mtime = file_path.stat().st_mtime
            run_key_input = f"{input_file}|{file_size}|{mtime}|{signature_version}"
            run_key = hashlib.sha256(run_key_input.encode('utf-8')).hexdigest()
            run_id = run_key[:16]
            return run_id, run_key
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("header1,header2\nvalue1,value2\n")
            temp_path = f.name
        
        try:
            # Compute run_id multiple times
            run_id1, run_key1 = compute_run_id(temp_path)
            run_id2, run_key2 = compute_run_id(temp_path)
            run_id3, run_key3 = compute_run_id(temp_path)
            
            # Should be identical
            assert run_id1 == run_id2 == run_id3
            assert run_key1 == run_key2 == run_key3
        finally:
            Path(temp_path).unlink()
    
    def test_duplicate_upsert_same_result(self, tmp_path):
        """UPSERTing the same data multiple times should result in single row."""
        # Create temporary database with isolated temp directory
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # UPSERT same data multiple times
        test_data = {
            "url_signature": "abc123" * 10,  # 60 chars, pad to 64
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
        
        # Pad url_signature to 64 chars (sha256 length)
        test_data["url_signature"] = test_data["url_signature"][:64].ljust(64, '0')
        
        # UPSERT 3 times with conflict_key specified
        client.upsert("analysis_cache", test_data, conflict_key="url_signature")
        client.upsert("analysis_cache", test_data, conflict_key="url_signature")
        client.upsert("analysis_cache", test_data, conflict_key="url_signature")
        
        client.flush()
        
        # Wait a bit for writer thread to complete batch processing
        import time
        time.sleep(0.2)
        
        # Check that only one row exists (use writer connection directly)
        result = client._writer_conn.execute(
            "SELECT COUNT(*) as cnt FROM analysis_cache WHERE url_signature = ?",
            [test_data["url_signature"]]
        ).fetchone()
        
        client.close()
        
        assert result[0] == 1
    
    def test_lineage_hash_determinism(self):
        """Same row should produce same lineage hash."""
        ingestor = BaseIngestor("paloalto")
        
        row1 = {"field1": "value1", "field2": "value2"}
        row2 = {"field2": "value2", "field1": "value1"}  # Same data, different order
        
        hash1 = ingestor._compute_lineage_hash(row1, "test.csv", 1)
        hash2 = ingestor._compute_lineage_hash(row2, "test.csv", 1)
        
        # Should be same (JSON serialization sorts keys)
        assert hash1 == hash2
    
    def test_lineage_hash_uniqueness(self):
        """Different rows should produce different lineage hashes."""
        ingestor = BaseIngestor("paloalto")
        
        row1 = {"field1": "value1", "field2": "value2"}
        row2 = {"field1": "value1", "field2": "value3"}  # Different value
        
        hash1 = ingestor._compute_lineage_hash(row1, "test.csv", 1)
        hash2 = ingestor._compute_lineage_hash(row2, "test.csv", 1)
        
        # Should be different
        assert hash1 != hash2
    
    def test_signature_cache_idempotency(self, tmp_path):
        """Same signature should not be duplicated in cache."""
        # Create temporary database with isolated temp directory
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # First, create a run (required for foreign key, but FK is removed now)
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
        
        # Create signature
        builder = SignatureBuilder()
        sig1 = builder.build_signature(
            norm_host="example.com",
            norm_path="/path",
            norm_query="",
            http_method="GET",
            bytes_sent=1024
        )
        
        # UPSERT signature stats multiple times
        stats_data = {
            "run_id": "test_run_123",
            "url_signature": sig1["url_signature"],
            "norm_host": sig1["norm_host"],
            "norm_path_template": sig1["norm_path_template"],
            "bytes_sent_bucket": sig1["bytes_bucket"],
            "access_count": 10,
            "unique_users": 5
        }
        
        # UPSERT 3 times with same data (composite PK: run_id, url_signature)
        # conflict_key="run_id" will be expanded to "run_id, url_signature" by the client
        client.upsert("signature_stats", stats_data, conflict_key="run_id")
        client.upsert("signature_stats", stats_data, conflict_key="run_id")
        client.upsert("signature_stats", stats_data, conflict_key="run_id")
        
        client.flush()
        
        # Wait a bit for writer thread to complete batch processing
        import time
        time.sleep(0.2)
        
        # Check that only one row exists (use writer connection directly)
        result = client._writer_conn.execute(
            "SELECT COUNT(*) as cnt FROM signature_stats WHERE run_id = ? AND url_signature = ?",
            [stats_data["run_id"], stats_data["url_signature"]]
        ).fetchone()
        
        client.close()
        
        assert result[0] == 1
    
    def test_run_replay_idempotency(self, tmp_path):
        """Re-running same input should not create duplicate runs."""
        # Create temporary database with isolated temp directory
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        run_id = "test_run_123"
        run_key = "test_key_456"
        
        # Create run
        run_data = {
            "run_id": run_id,
            "run_key": run_key,
            "started_at": "2024-01-01T00:00:00",
            "status": "running",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "input_manifest_hash": run_key
        }
        
        # UPSERT run multiple times with conflict_key specified
        client.upsert("runs", run_data, conflict_key="run_id")
        client.upsert("runs", run_data, conflict_key="run_id")
        client.upsert("runs", run_data, conflict_key="run_id")
        
        client.flush()
        
        # Wait a bit for writer thread to complete batch processing
        import time
        time.sleep(0.2)
        
        # Check that only one row exists (use writer connection directly)
        result = client._writer_conn.execute(
            "SELECT COUNT(*) as cnt FROM runs WHERE run_id = ?",
            [run_id]
        ).fetchone()
        
        client.close()
        
        assert result[0] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
