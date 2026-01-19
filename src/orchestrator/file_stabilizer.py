"""
AIMO Analysis Engine - File Stabilizer

Handles Box sync file stabilization and copying to work directory.
Ensures atomicity by waiting for files to stabilize before processing.
"""

import time
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import yaml
import fnmatch
import logging

# Configure logging (use print for now, can be enhanced later)
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class FileStabilizer:
    """
    Stabilizes files from Box sync folder before processing.
    
    Features:
    - Monitors file size and modification time
    - Waits for stabilization (no changes for N seconds)
    - Copies stable files to work directory
    - Filters files by include/exclude patterns
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize file stabilizer.
        
        Args:
            config_path: Path to box_sync.yaml (default: config/box_sync.yaml)
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "box_sync.yaml"
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Get stabilization settings
        stabilization = self.config.get("stabilization", {})
        self.wait_seconds = stabilization.get("wait_seconds", 60)
        self.poll_interval_seconds = stabilization.get("poll_interval_seconds", 5)
        self.max_wait_seconds = stabilization.get("max_wait_seconds", 600)
        
        # Get file handling settings
        file_handling = self.config.get("file_handling", {})
        self.include_patterns = file_handling.get("include_patterns", ["*.csv", "*.json", "*.log", "*.txt"])
        self.exclude_patterns = file_handling.get("exclude_patterns", [".*", "*.tmp", "*.partial"])
        
        # Get sync paths
        self.enabled = self.config.get("enabled", False)
        if self.enabled:
            self.input_path = Path(self.config.get("local_sync_path", "./data/input"))
        else:
            self.input_path = Path(self.config.get("fallback_input_path", "./data/input"))
        
        # Work directory base path
        work_config = self.config.get("work", {})
        self.work_base_path = Path(work_config.get("base_path", "./data/work"))
    
    def _matches_pattern(self, filename: str, patterns: List[str]) -> bool:
        """
        Check if filename matches any pattern in patterns list.
        
        Args:
            filename: Filename to check
            patterns: List of glob patterns
            
        Returns:
            True if filename matches any pattern
        """
        for pattern in patterns:
            if fnmatch.fnmatch(filename, pattern):
                return True
        return False
    
    def _should_process_file(self, file_path: Path) -> bool:
        """
        Check if file should be processed based on include/exclude patterns.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if file should be processed
        """
        filename = file_path.name
        
        # Check exclude patterns first (higher priority)
        if self._matches_pattern(filename, self.exclude_patterns):
            return False
        
        # Check include patterns
        if self.include_patterns:
            return self._matches_pattern(filename, self.include_patterns)
        
        # If no include patterns, accept all (except excluded)
        return True
    
    def find_input_files(self) -> List[Path]:
        """
        Find input files in sync folder that match processing criteria.
        
        Returns:
            List of file paths to process
        """
        if not self.input_path.exists():
            logger.warning(f"Input path does not exist: {self.input_path}")
            return []
        
        input_files = []
        
        # Recursively search for files
        for file_path in self.input_path.rglob("*"):
            if file_path.is_file() and self._should_process_file(file_path):
                input_files.append(file_path)
        
        return sorted(input_files)  # Sort for determinism
    
    def _get_file_stats(self, file_path: Path) -> Dict[str, Any]:
        """
        Get file statistics (size, mtime).
        
        Args:
            file_path: Path to file
            
        Returns:
            Dict with 'size' and 'mtime' keys
        """
        stat = file_path.stat()
        return {
            "size": stat.st_size,
            "mtime": stat.st_mtime
        }
    
    def wait_for_stable(self, file_path: Path) -> bool:
        """
        Wait for file to stabilize (no changes for wait_seconds).
        
        Args:
            file_path: Path to file to stabilize
            
        Returns:
            True if file stabilized, False if max_wait exceeded
        """
        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            return False
        
        start_time = time.time()
        last_stats = None
        stable_since = None
        
        print(f"  Waiting for file to stabilize: {file_path.name} (max {self.max_wait_seconds}s)")
        
        while True:
            # Check if max wait time exceeded
            elapsed = time.time() - start_time
            if elapsed > self.max_wait_seconds:
                logger.warning(
                    f"Max wait time ({self.max_wait_seconds}s) exceeded for {file_path.name}. "
                    f"File may be locked or still syncing."
                )
                return False
            
            # Get current file stats
            try:
                current_stats = self._get_file_stats(file_path)
            except (OSError, FileNotFoundError):
                # File may have been deleted or moved
                logger.warning(f"File disappeared during stabilization: {file_path}")
                return False
            
            # Check if stats changed
            if last_stats is None:
                # First check
                last_stats = current_stats
                stable_since = time.time()
            elif (current_stats["size"] != last_stats["size"] or 
                  current_stats["mtime"] != last_stats["mtime"]):
                # Stats changed - reset stable timer
                last_stats = current_stats
                stable_since = time.time()
                print(f"    File changed: {file_path.name} (size: {current_stats['size']}, resetting timer...)")
            else:
                # Stats unchanged - check if stable long enough
                stable_duration = time.time() - stable_since
                remaining = self.wait_seconds - stable_duration
                if remaining > 0:
                    print(f"    Stable for {stable_duration:.1f}s, need {remaining:.1f}s more...", end='\r')
                if stable_duration >= self.wait_seconds:
                    print(f"  File stabilized: {file_path.name} (stable for {stable_duration:.1f}s)")
                    return True
            
            # Wait before next check
            time.sleep(self.poll_interval_seconds)
    
    def copy_to_work_dir(self, file_path: Path, work_dir: Path) -> Path:
        """
        Copy stabilized file to work directory.
        
        Args:
            file_path: Source file path (in input/sync folder)
            work_dir: Destination work directory (data/work/run_id/raw/)
            
        Returns:
            Path to copied file in work directory
        """
        # Create raw subdirectory
        raw_dir = work_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Destination path (preserve original filename)
        dest_path = raw_dir / file_path.name
        
        # Copy file (atomic: use shutil.copy2 to preserve metadata)
        print(f"  Copying {file_path.name} to work directory...")
        shutil.copy2(file_path, dest_path)
        
        print(f"  Copied to: {dest_path}")
        return dest_path
    
    def stabilize_and_copy(self, file_path: Path, work_dir: Path) -> Optional[Path]:
        """
        Stabilize file and copy to work directory.
        
        Args:
            file_path: Source file path
            work_dir: Destination work directory
            
        Returns:
            Path to copied file, or None if stabilization failed
        """
        # Wait for file to stabilize
        if not self.wait_for_stable(file_path):
            logger.error(f"Failed to stabilize file: {file_path}")
            return None
        
        # Copy to work directory
        try:
            copied_path = self.copy_to_work_dir(file_path, work_dir)
            return copied_path
        except Exception as e:
            logger.error(f"Failed to copy file {file_path}: {e}")
            return None
    
    def process_input_files(self, work_dir: Path) -> List[Path]:
        """
        Find, stabilize, and copy all input files to work directory.
        
        Args:
            work_dir: Work directory for current run (data/work/run_id/)
            
        Returns:
            List of copied file paths in work directory
        """
        # Find input files
        input_files = self.find_input_files()
        
        if not input_files:
            print(f"No input files found in {self.input_path}")
            return []
        
        print(f"Found {len(input_files)} input file(s) to process")
        
        # Stabilize and copy each file
        copied_files = []
        for input_file in input_files:
            print(f"Processing: {input_file}")
            copied_path = self.stabilize_and_copy(input_file, work_dir)
            if copied_path:
                copied_files.append(copied_path)
            else:
                print(f"  Warning: Skipping {input_file.name} (stabilization/copy failed)")
        
        print(f"Successfully copied {len(copied_files)} file(s) to work directory")
        return copied_files
