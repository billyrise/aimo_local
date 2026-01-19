"""
Tests for Phase 11: Box Sync File Stabilization

Tests file stabilization, copying, and integration with main pipeline.
"""

import pytest
import tempfile
import shutil
import time
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from orchestrator.file_stabilizer import FileStabilizer


class TestFileStabilizer:
    """Test file stabilization functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create temporary directories
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.temp_dir / "input"
        self.work_dir = self.temp_dir / "work"
        self.config_dir = self.temp_dir / "config"
        
        # Create directories
        self.input_dir.mkdir(parents=True)
        self.work_dir.mkdir(parents=True)
        self.config_dir.mkdir(parents=True)
        
        # Create test config file
        self.config_path = self.config_dir / "box_sync.yaml"
        self._create_test_config()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def _create_test_config(self):
        """Create test configuration file."""
        config_content = """# Test Box Sync Configuration
enabled: false
fallback_input_path: "{input_path}"

stabilization:
  wait_seconds: 2  # Short wait for testing
  poll_interval_seconds: 0.5
  max_wait_seconds: 10

file_handling:
  include_patterns:
    - "*.csv"
    - "*.txt"
  exclude_patterns:
    - "*.tmp"
    - ".*"

work:
  base_path: "{work_path}"
""".format(
            input_path=str(self.input_dir),
            work_path=str(self.work_dir)
        )
        
        with open(self.config_path, 'w') as f:
            f.write(config_content)
    
    def test_find_input_files(self):
        """Test finding input files."""
        # Create test files
        test_file1 = self.input_dir / "test1.csv"
        test_file2 = self.input_dir / "test2.txt"
        test_file3 = self.input_dir / "hidden.tmp"  # Should be excluded
        
        test_file1.write_text("test data 1")
        test_file2.write_text("test data 2")
        test_file3.write_text("test data 3")
        
        # Initialize stabilizer with test config
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Find files
        found_files = stabilizer.find_input_files()
        
        # Should find test1.csv and test2.txt, but not hidden.tmp
        assert len(found_files) == 2
        assert test_file1 in found_files
        assert test_file2 in found_files
        assert test_file3 not in found_files
    
    def test_wait_for_stable(self):
        """Test file stabilization waiting."""
        # Create test file
        test_file = self.input_dir / "test_stable.csv"
        test_file.write_text("initial content")
        
        # Initialize stabilizer with short wait time
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # File should be stable immediately (no changes)
        start_time = time.time()
        result = stabilizer.wait_for_stable(test_file)
        elapsed = time.time() - start_time
        
        assert result is True
        # Should wait at least wait_seconds (2 seconds in test config)
        assert elapsed >= 2.0
    
    def test_wait_for_stable_with_changes(self):
        """Test stabilization with file changes."""
        # Create test file
        test_file = self.input_dir / "test_changing.csv"
        test_file.write_text("initial")
        
        # Initialize stabilizer
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Start stabilization in background (simulate file being written)
        import threading
        
        def modify_file():
            time.sleep(0.5)
            test_file.write_text("modified")
            time.sleep(0.5)
            test_file.write_text("final")
        
        thread = threading.Thread(target=modify_file)
        thread.start()
        
        # Wait for stabilization (should wait longer due to changes)
        start_time = time.time()
        result = stabilizer.wait_for_stable(test_file)
        elapsed = time.time() - start_time
        
        thread.join()
        
        assert result is True
        # Should wait longer due to file changes
        assert elapsed >= 3.0  # At least 2 seconds after last change
    
    def test_copy_to_work_dir(self):
        """Test copying file to work directory."""
        # Create test file
        test_file = self.input_dir / "test_copy.csv"
        test_file.write_text("test content")
        
        # Initialize stabilizer
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Copy to work directory
        run_work_dir = self.work_dir / "run_123"
        copied_path = stabilizer.copy_to_work_dir(test_file, run_work_dir)
        
        # Check copied file exists
        assert copied_path.exists()
        assert copied_path.name == test_file.name
        assert copied_path.read_text() == "test content"
        
        # Check original file unchanged
        assert test_file.exists()
        assert test_file.read_text() == "test content"
        
        # Check structure: data/work/run_id/raw/filename
        assert copied_path.parent.name == "raw"
        assert copied_path.parent.parent.name == "run_123"
    
    def test_stabilize_and_copy(self):
        """Test complete stabilize and copy workflow."""
        # Create test file
        test_file = self.input_dir / "test_complete.csv"
        test_file.write_text("test data")
        
        # Initialize stabilizer
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Stabilize and copy
        run_work_dir = self.work_dir / "run_456"
        copied_path = stabilizer.stabilize_and_copy(test_file, run_work_dir)
        
        # Check result
        assert copied_path is not None
        assert copied_path.exists()
        assert copied_path.read_text() == "test data"
        
        # Original file should be unchanged
        assert test_file.exists()
    
    def test_process_input_files(self):
        """Test processing multiple input files."""
        # Create multiple test files
        test_files = []
        for i in range(3):
            test_file = self.input_dir / f"test_{i}.csv"
            test_file.write_text(f"test data {i}")
            test_files.append(test_file)
        
        # Initialize stabilizer
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Process all files
        run_work_dir = self.work_dir / "run_789"
        copied_files = stabilizer.process_input_files(run_work_dir)
        
        # Check all files were copied
        assert len(copied_files) == 3
        
        # Check each file
        for i, test_file in enumerate(test_files):
            copied_file = run_work_dir / "raw" / test_file.name
            assert copied_file.exists()
            assert copied_file.read_text() == f"test data {i}"
            
            # Original file unchanged
            assert test_file.exists()
    
    def test_file_pattern_filtering(self):
        """Test file pattern filtering."""
        # Create files with different extensions
        files = {
            "include1.csv": True,
            "include2.txt": True,
            "exclude1.tmp": False,
            "exclude2.hidden": False,
            "exclude3.DS_Store": False,
        }
        
        for filename, should_include in files.items():
            (self.input_dir / filename).write_text("test")
        
        # Initialize stabilizer
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Find files
        found_files = stabilizer.find_input_files()
        found_names = {f.name for f in found_files}
        
        # Check filtering
        for filename, should_include in files.items():
            if should_include:
                assert filename in found_names, f"{filename} should be included"
            else:
                assert filename not in found_names, f"{filename} should be excluded"


class TestFileStabilizerIntegration:
    """Integration tests for file stabilizer with main pipeline."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.input_dir = self.temp_dir / "input"
        self.work_dir = self.temp_dir / "work"
        self.config_dir = self.temp_dir / "config"
        
        self.input_dir.mkdir(parents=True)
        self.work_dir.mkdir(parents=True)
        self.config_dir.mkdir(parents=True)
        
        # Create test config
        self.config_path = self.config_dir / "box_sync.yaml"
        config_content = """enabled: false
fallback_input_path: "{input_path}"
stabilization:
  wait_seconds: 1
  poll_interval_seconds: 0.2
  max_wait_seconds: 5
file_handling:
  include_patterns: ["*.csv"]
work:
  base_path: "{work_path}"
""".format(
            input_path=str(self.input_dir),
            work_path=str(self.work_dir)
        )
        with open(self.config_path, 'w') as f:
            f.write(config_content)
    
    def teardown_method(self):
        """Clean up."""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def test_work_directory_structure(self):
        """Test that work directory structure is correct."""
        # Create test file
        test_file = self.input_dir / "integration_test.csv"
        test_file.write_text("test,data\n1,2\n")
        
        # Initialize stabilizer
        stabilizer = FileStabilizer(config_path=self.config_path)
        
        # Process file
        run_work_dir = self.work_dir / "test_run"
        copied_files = stabilizer.process_input_files(run_work_dir)
        
        # Check structure: work/run_id/raw/filename
        assert len(copied_files) == 1
        copied_file = copied_files[0]
        
        assert copied_file.parent.name == "raw"
        assert copied_file.parent.parent.name == "test_run"
        assert copied_file.name == "integration_test.csv"
        
        # Verify original file is unchanged (read-only treatment)
        assert test_file.exists()
        assert test_file.read_text() == "test,data\n1,2\n"
