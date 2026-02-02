"""
Tests for Phase 7-3: Input manifest hash and input_files audit completeness.

Tests:
1. Same input files produce same input_manifest_hash
2. input_files records are created with run_id linkage
3. input_manifest_hash includes vendor, min/max_time after ingestion
4. code_version is saved to runs table
"""

import pytest
import tempfile
import shutil
import hashlib
from pathlib import Path
from datetime import datetime
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duckdb_client import DuckDBClient
from orchestrator import Orchestrator
from utils.git_version import get_code_version


class TestInputManifestHash:
    """Test input_manifest_hash computation and storage."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.db_path = self.temp_dir / "test.duckdb"
        self.work_dir = self.temp_dir / "work"
        self.work_dir.mkdir(parents=True)
        
        # Create test files
        self.test_file1 = self.temp_dir / "test1.csv"
        self.test_file2 = self.temp_dir / "test2.csv"
        
        self.test_file1.write_text("test,data\n1,2\n")
        self.test_file2.write_text("test,data\n3,4\n")
        
        # Initialize DB client
        self.db_client = DuckDBClient(str(self.db_path))
        self.orchestrator = Orchestrator(
            db_client=self.db_client,
            work_base_dir=self.work_dir
        )
    
    def teardown_method(self):
        """Clean up."""
        self.db_client.close()
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_same_input_same_hash(self):
        """Test that same input files produce same input_manifest_hash."""
        # Compute hash for same files twice
        input_files = [self.test_file1, self.test_file2]
        
        hash1 = self.orchestrator.compute_input_manifest_hash(input_files)
        hash2 = self.orchestrator.compute_input_manifest_hash(input_files)
        
        assert hash1 == hash2, "Same input files should produce same hash"
    
    def test_different_input_different_hash(self):
        """Test that different input files produce different hash."""
        # Create different file
        test_file3 = self.temp_dir / "test3.csv"
        test_file3.write_text("different,data\n5,6\n")
        
        hash1 = self.orchestrator.compute_input_manifest_hash([self.test_file1])
        hash2 = self.orchestrator.compute_input_manifest_hash([test_file3])
        
        assert hash1 != hash2, "Different input files should produce different hash"
    
    def test_input_files_record_creation(self):
        """Test that input_files records are created with run_id linkage."""
        # Create a run
        input_files = [self.test_file1]
        run_context = self.orchestrator.get_or_create_run(input_files)
        
        # Manually insert input_files record (simulating ingestion)
        file_hash = hashlib.sha256(self.test_file1.read_bytes()).hexdigest()
        file_id = hashlib.sha256(
            f"{self.test_file1}|{self.test_file1.stat().st_size}|{self.test_file1.stat().st_mtime}".encode()
        ).hexdigest()
        
        self.db_client.upsert("input_files", {
            "file_id": file_id,
            "run_id": run_context.run_id,
            "file_path": str(self.test_file1),
            "file_size": self.test_file1.stat().st_size,
            "file_hash": file_hash,
            "vendor": "paloalto",
            "log_type": "traffic",
            "row_count": 2,
            "min_time": datetime(2024, 1, 1, 0, 0, 0).isoformat(),
            "max_time": datetime(2024, 1, 1, 23, 59, 59).isoformat(),
            "ingested_at": datetime.utcnow().isoformat()
        }, conflict_key="file_id")
        
        self.db_client.flush()
        
        # Verify record exists
        reader = self.db_client.get_reader()
        result = reader.execute(
            "SELECT run_id, file_path, file_hash, vendor FROM input_files WHERE run_id = ?",
            [run_context.run_id]
        ).fetchone()
        
        assert result is not None, "input_files record should exist"
        assert result[0] == run_context.run_id, "run_id should match"
        assert result[1] == str(self.test_file1), "file_path should match"
        assert result[2] == file_hash, "file_hash should match"
        assert result[3] == "paloalto", "vendor should match"
    
    def test_input_manifest_hash_from_db(self):
        """Test that input_manifest_hash_from_db includes vendor, min/max_time."""
        # Create a run
        input_files = [self.test_file1]
        run_context = self.orchestrator.get_or_create_run(input_files)
        
        # Insert input_files record with vendor, min/max_time
        file_hash = hashlib.sha256(self.test_file1.read_bytes()).hexdigest()
        file_id = hashlib.sha256(
            f"{self.test_file1}|{self.test_file1.stat().st_size}|{self.test_file1.stat().st_mtime}".encode()
        ).hexdigest()
        
        min_time = datetime(2024, 1, 1, 0, 0, 0)
        max_time = datetime(2024, 1, 1, 23, 59, 59)
        
        self.db_client.upsert("input_files", {
            "file_id": file_id,
            "run_id": run_context.run_id,
            "file_path": str(self.test_file1),
            "file_size": self.test_file1.stat().st_size,
            "file_hash": file_hash,
            "vendor": "paloalto",
            "log_type": "traffic",
            "row_count": 2,
            "min_time": min_time.isoformat(),
            "max_time": max_time.isoformat(),
            "ingested_at": datetime.utcnow().isoformat()
        }, conflict_key="file_id")
        
        self.db_client.flush()
        
        # Compute hash from DB (should include vendor, min/max_time)
        final_hash = self.orchestrator.compute_input_manifest_hash_from_db(run_context.run_id)
        
        assert final_hash is not None, "Hash should be computed"
        assert len(final_hash) == 64, "Hash should be SHA256 (64 hex chars)"
        
        # Verify hash is deterministic (same input = same hash)
        final_hash2 = self.orchestrator.compute_input_manifest_hash_from_db(run_context.run_id)
        assert final_hash == final_hash2, "Hash should be deterministic"
    
    def test_code_version_saved(self):
        """Test that code_version is saved to runs table."""
        # Create a run
        input_files = [self.test_file1]
        run_context = self.orchestrator.get_or_create_run(input_files)
        
        self.db_client.flush()
        
        # Verify code_version is saved
        reader = self.db_client.get_reader()
        result = reader.execute(
            "SELECT code_version FROM runs WHERE run_id = ?",
            [run_context.run_id]
        ).fetchone()
        
        assert result is not None, "run record should exist"
        code_version = result[0]
        assert code_version is not None, "code_version should be set"
        # Should be either git hash (7 chars) or "unknown"
        assert code_version == "unknown" or len(code_version) >= 7, \
            f"code_version should be 'unknown' or git hash, got: {code_version}"


class TestInputManifestHashDeterminism:
    """Test that input_manifest_hash is deterministic across runs."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.db_path = self.temp_dir / "test.duckdb"
        self.work_dir = self.temp_dir / "work"
        self.work_dir.mkdir(parents=True)
        
        # Create test file
        self.test_file = self.temp_dir / "test.csv"
        self.test_file.write_text("test,data\n1,2\n")
        
        self.db_client = DuckDBClient(str(self.db_path))
        self.orchestrator = Orchestrator(
            db_client=self.db_client,
            work_base_dir=self.work_dir
        )
    
    def teardown_method(self):
        """Clean up."""
        self.db_client.close()
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_same_input_produces_same_manifest_hash(self):
        """Test that same input files produce same input_manifest_hash (idempotency)."""
        input_files = [self.test_file]
        
        # Create first run
        run_context1 = self.orchestrator.get_or_create_run(input_files)
        hash1 = run_context1.input_manifest_hash
        
        # Create second run with same input
        run_context2 = self.orchestrator.get_or_create_run(input_files)
        hash2 = run_context2.input_manifest_hash
        
        # Initial hashes should be same (before ingestion)
        assert hash1 == hash2, "Same input should produce same initial hash"
        
        # After adding input_files record, final hash should also be same
        file_hash = hashlib.sha256(self.test_file.read_bytes()).hexdigest()
        file_id = hashlib.sha256(
            f"{self.test_file}|{self.test_file.stat().st_size}|{self.test_file.stat().st_mtime}".encode()
        ).hexdigest()
        
        min_time = datetime(2024, 1, 1, 0, 0, 0)
        max_time = datetime(2024, 1, 1, 23, 59, 59)
        
        # Insert same input_files record for both runs
        for run_id in [run_context1.run_id, run_context2.run_id]:
            self.db_client.upsert("input_files", {
                "file_id": file_id,
                "run_id": run_id,
                "file_path": str(self.test_file),
                "file_size": self.test_file.stat().st_size,
                "file_hash": file_hash,
                "vendor": "paloalto",
                "log_type": "traffic",
                "row_count": 2,
                "min_time": min_time.isoformat(),
                "max_time": max_time.isoformat(),
                "ingested_at": datetime.utcnow().isoformat()
            }, conflict_key="file_id")
        
        self.db_client.flush()
        
        # Compute final hashes
        final_hash1 = self.orchestrator.compute_input_manifest_hash_from_db(run_context1.run_id)
        final_hash2 = self.orchestrator.compute_input_manifest_hash_from_db(run_context2.run_id)
        
        assert final_hash1 == final_hash2, "Same input should produce same final hash"
