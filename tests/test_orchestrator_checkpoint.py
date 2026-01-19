"""
Test Orchestrator and Checkpoint Resume

Tests for Phase 7: Orchestrator + Checkpoint Resume functionality.
- Deterministic run_id generation
- Checkpoint recording and reading
- Stage skip logic
- Resume from last completed stage
- Idempotency on re-run
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import sys
import hashlib
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duckdb_client import DuckDBClient
from orchestrator import Orchestrator, RunContext


class TestOrchestratorCheckpoint:
    """Test orchestrator checkpoint and resume functionality."""
    
    def test_run_id_determinism(self, tmp_path):
        """Same input should produce same run_id."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create temporary file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context multiple times
        run1 = orchestrator.get_or_create_run([test_file])
        run2 = orchestrator.get_or_create_run([test_file])
        run3 = orchestrator.get_or_create_run([test_file])
        
        # Should produce same run_id and run_key
        assert run1.run_id == run2.run_id == run3.run_id
        assert run1.run_key == run2.run_key == run3.run_key
        assert run1.input_manifest_hash == run2.input_manifest_hash == run3.input_manifest_hash
        
        client.close()
    
    def test_input_manifest_hash_determinism(self, tmp_path):
        """Same files should produce same input_manifest_hash."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create temporary files
        test_file1 = tmp_path / "test1.csv"
        test_file1.write_text("header1,header2\nvalue1,value2\n")
        
        test_file2 = tmp_path / "test2.csv"
        test_file2.write_text("header3,header4\nvalue3,value4\n")
        
        # Compute manifest hash multiple times
        hash1 = orchestrator.compute_input_manifest_hash([test_file1, test_file2])
        hash2 = orchestrator.compute_input_manifest_hash([test_file1, test_file2])
        
        # Should be identical
        assert hash1 == hash2
        
        # Different order should produce different hash (files are sorted by path)
        # But if paths are same, order doesn't matter (already sorted)
        hash3 = orchestrator.compute_input_manifest_hash([test_file2, test_file1])
        # Since files are sorted by path, order in list doesn't matter
        assert hash1 == hash3
        
        client.close()
    
    def test_run_key_includes_versions(self, tmp_path):
        """run_key should include signature_version, rule_version, prompt_version."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Create orchestrator with default versions
        orchestrator1 = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create orchestrator with different versions
        orchestrator2 = Orchestrator(
            db_client=client,
            work_base_dir=work_dir,
            signature_version="2.0",
            rule_version="2",
            prompt_version="2"
        )
        
        # Compute input_manifest_hash (same for both)
        manifest_hash = orchestrator1.compute_input_manifest_hash([test_file])
        
        # Compute run_keys
        run_key1 = orchestrator1.compute_run_key(manifest_hash)
        run_key2 = orchestrator2.compute_run_key(manifest_hash)
        
        # Should be different (different versions)
        assert run_key1 != run_key2
        
        client.close()
    
    def test_checkpoint_recording(self, tmp_path):
        """Checkpoint should be recorded after stage completion."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context
        run = orchestrator.get_or_create_run([test_file])
        
        # Initial checkpoint should be 0
        assert run.last_completed_stage == 0
        
        # Update checkpoint to stage 1
        orchestrator.update_checkpoint(Orchestrator.STAGE_1_INGESTION)
        client.flush()
        
        # Wait a bit for writer thread to complete
        import time
        time.sleep(0.2)
        
        # Verify checkpoint was recorded
        reader = client.get_reader()
        result = reader.execute(
            "SELECT last_completed_stage, status FROM runs WHERE run_id = ?",
            [run.run_id]
        ).fetchone()
        
        assert result[0] == Orchestrator.STAGE_1_INGESTION
        assert result[1] == "running"
        
        client.close()
    
    def test_stage_skip_logic(self, tmp_path):
        """should_skip_stage should return True for completed stages."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context
        run = orchestrator.get_or_create_run([test_file])
        
        # Initially, no stages should be skipped
        assert not orchestrator.should_skip_stage(Orchestrator.STAGE_1_INGESTION)
        assert not orchestrator.should_skip_stage(Orchestrator.STAGE_2_NORMALIZATION)
        
        # Update checkpoint to stage 2
        orchestrator.update_checkpoint(Orchestrator.STAGE_2_NORMALIZATION)
        client.flush()
        
        # Stage 1 and 2 should be skipped
        assert orchestrator.should_skip_stage(Orchestrator.STAGE_1_INGESTION)
        assert orchestrator.should_skip_stage(Orchestrator.STAGE_2_NORMALIZATION)
        assert not orchestrator.should_skip_stage(Orchestrator.STAGE_3_RULE_CLASSIFICATION)
        
        client.close()
    
    def test_resume_from_checkpoint(self, tmp_path):
        """Re-running should resume from last_completed_stage."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # First run: complete stage 1 and 2
        run1 = orchestrator.get_or_create_run([test_file])
        orchestrator.update_checkpoint(Orchestrator.STAGE_1_INGESTION)
        orchestrator.update_checkpoint(Orchestrator.STAGE_2_NORMALIZATION)
        client.flush()
        
        # Wait a bit for writer thread to complete
        import time
        time.sleep(0.2)
        
        # Close and recreate orchestrator (simulating re-run)
        client.close()
        client2 = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator2 = Orchestrator(
            db_client=client2,
            work_base_dir=work_dir
        )
        
        # Get run context again (should resume from checkpoint)
        run2 = orchestrator2.get_or_create_run([test_file])
        
        # Should have same run_id
        assert run2.run_id == run1.run_id
        
        # Should resume from stage 2
        assert run2.last_completed_stage == Orchestrator.STAGE_2_NORMALIZATION
        
        # Stage 1 and 2 should be skipped
        assert orchestrator2.should_skip_stage(Orchestrator.STAGE_1_INGESTION)
        assert orchestrator2.should_skip_stage(Orchestrator.STAGE_2_NORMALIZATION)
        assert not orchestrator2.should_skip_stage(Orchestrator.STAGE_3_RULE_CLASSIFICATION)
        
        client2.close()
    
    def test_idempotency_on_rerun(self, tmp_path):
        """Re-running same input should not create duplicate runs."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context multiple times
        run1 = orchestrator.get_or_create_run([test_file])
        run2 = orchestrator.get_or_create_run([test_file])
        run3 = orchestrator.get_or_create_run([test_file])
        
        # All should have same run_id
        assert run1.run_id == run2.run_id == run3.run_id
        
        # Verify only one run record exists in DB
        reader = client.get_reader()
        result = reader.execute(
            "SELECT COUNT(*) FROM runs WHERE run_id = ?",
            [run1.run_id]
        ).fetchone()
        
        assert result[0] == 1
        
        client.close()
    
    def test_work_directory_creation(self, tmp_path):
        """Work directory should be created for each run."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context
        run = orchestrator.get_or_create_run([test_file])
        
        # Work directory should exist
        assert run.work_dir.exists()
        assert run.work_dir == work_dir / run.run_id
        
        # Raw directory should exist
        raw_dir = orchestrator.get_raw_dir()
        assert raw_dir.exists()
        assert raw_dir == run.work_dir / "raw"
        
        client.close()
    
    def test_finalize_run(self, tmp_path):
        """finalize_run should mark run as succeeded."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context
        run = orchestrator.get_or_create_run([test_file])
        
        # Finalize run
        orchestrator.finalize_run("succeeded")
        client.flush()
        
        # Wait a bit for writer thread to complete
        import time
        time.sleep(0.2)
        
        # Verify run status
        reader = client.get_reader()
        result = reader.execute(
            "SELECT status, finished_at, last_completed_stage FROM runs WHERE run_id = ?",
            [run.run_id]
        ).fetchone()
        
        assert result[0] == "succeeded"
        assert result[1] is not None  # finished_at should be set
        assert result[2] == Orchestrator.STAGE_5_REPORTING
        
        client.close()
    
    def test_multiple_files_manifest_hash(self, tmp_path):
        """input_manifest_hash should handle multiple files correctly."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create multiple test files
        test_file1 = tmp_path / "test1.csv"
        test_file1.write_text("header1,header2\nvalue1,value2\n")
        
        test_file2 = tmp_path / "test2.csv"
        test_file2.write_text("header3,header4\nvalue3,value4\n")
        
        test_file3 = tmp_path / "test3.csv"
        test_file3.write_text("header5,header6\nvalue5,value6\n")
        
        # Compute manifest hash
        hash1 = orchestrator.compute_input_manifest_hash([test_file1, test_file2, test_file3])
        hash2 = orchestrator.compute_input_manifest_hash([test_file1, test_file2, test_file3])
        
        # Should be identical
        assert hash1 == hash2
        
        # Different files should produce different hash
        test_file4 = tmp_path / "test4.csv"
        test_file4.write_text("different,content\n")
        hash3 = orchestrator.compute_input_manifest_hash([test_file1, test_file2, test_file4])
        
        assert hash1 != hash3
        
        client.close()
    
    def test_run_key_collision_detection(self, tmp_path):
        """Run key collision should be detected and raise error."""
        db_path = tmp_path / "aimo_test.duckdb"
        temp_dir = tmp_path / "duckdb_tmp"
        work_dir = tmp_path / "work"
        
        client = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator = Orchestrator(
            db_client=client,
            work_base_dir=work_dir
        )
        
        # Create test file
        test_file = tmp_path / "test_input.csv"
        test_file.write_text("header1,header2\nvalue1,value2\n")
        
        # Get run context
        run1 = orchestrator.get_or_create_run([test_file])
        
        # Manually update the run record with different run_key (simulating collision)
        # This should not happen in practice, but we test the safety check
        # First, we need to delete the existing run and insert a conflicting one
        client._writer_conn.execute(
            "DELETE FROM runs WHERE run_id = ?",
            [run1.run_id]
        )
        client.flush()
        import time
        time.sleep(0.2)
        
        # Insert conflicting run record
        client.insert("runs", {
            "run_id": run1.run_id,
            "run_key": "different_key_that_should_not_match",
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
            "signature_version": "1.0",
            "rule_version": "1",
            "prompt_version": "1",
            "input_manifest_hash": "different_hash"
        }, ignore_conflict=True)
        client.flush()
        time.sleep(0.2)
        
        # Close and recreate orchestrator
        client.close()
        client2 = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
        orchestrator2 = Orchestrator(
            db_client=client2,
            work_base_dir=work_dir
        )
        
        # Getting run context should detect collision and raise error
        with pytest.raises(ValueError, match="Run ID collision detected"):
            orchestrator2.get_or_create_run([test_file])
        
        client2.close()
        
        client2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
