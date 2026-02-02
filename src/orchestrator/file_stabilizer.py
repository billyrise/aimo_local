"""
AIMO Analysis Engine - File Stabilizer

Handles Box sync file stabilization and copying to work directory.
Ensures atomicity by waiting for files to stabilize before processing.
"""

import time
import shutil
import os
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
    
    def __init__(self, config_path: Optional[Path] = None, jsonl_logger: Optional[Any] = None):
        """
        Initialize file stabilizer.
        
        Args:
            config_path: Path to box_sync.yaml (default: config/box_sync.yaml)
            jsonl_logger: Optional JSONL logger for audit logging
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "box_sync.yaml"
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Get stabilization settings
        # Priority: 1) Environment variable, 2) Config file, 3) Default (60)
        stabilization = self.config.get("stabilization", {})
        env_stability_seconds = os.getenv("STABILITY_SECONDS")
        if env_stability_seconds:
            try:
                self.wait_seconds = int(env_stability_seconds)
            except ValueError:
                logger.warning(f"Invalid STABILITY_SECONDS environment variable: {env_stability_seconds}, using config/default")
                self.wait_seconds = stabilization.get("wait_seconds", 60)
        else:
            self.wait_seconds = stabilization.get("wait_seconds", 60)
        
        self.poll_interval_seconds = stabilization.get("poll_interval_seconds", 5)
        self.max_wait_seconds = stabilization.get("max_wait_seconds", 600)
        
        # JSONL logger for audit logging
        self.jsonl_logger = jsonl_logger
        
        # Get file handling settings
        file_handling = self.config.get("file_handling", {})
        self.include_patterns = file_handling.get("include_patterns", ["*.csv", "*.json", "*.log", "*.txt"])
        self.exclude_patterns = file_handling.get("exclude_patterns", [".*", "*.tmp", "*.partial"])
        
        # Get sync paths
        # Box sync is now mandatory (P0: prevent incomplete file processing)
        # Always use Box sync path regardless of enabled setting
        self.enabled = True  # Always enabled (mandatory)
        self.input_path = Path(self.config.get("local_sync_path", "./data/input"))
        
        # Fallback path is only used if local_sync_path doesn't exist
        # This allows graceful degradation for local testing
        fallback_path = Path(self.config.get("fallback_input_path", "./data/input"))
        if not self.input_path.exists() and fallback_path.exists():
            logger.warning(f"Box sync path does not exist: {self.input_path}, using fallback: {fallback_path}")
            self.input_path = fallback_path
        
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
    
    def wait_for_stable(self, file_path: Path) -> Dict[str, Any]:
        """
        Wait for file to stabilize (no changes for wait_seconds).
        
        Args:
            file_path: Path to file to stabilize
            
        Returns:
            Dict with 'success' (bool) and 'metadata' (dict) containing:
            - initial_size, initial_mtime
            - final_size, final_mtime
            - wait_duration_seconds
            - change_count (number of times file changed during stabilization)
        """
        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            return {"success": False, "metadata": {"error": "File does not exist"}}
        
        start_time = time.time()
        last_stats = None
        stable_since = None
        change_count = 0
        initial_stats = None
        
        print(f"  Waiting for file to stabilize: {file_path.name} (max {self.max_wait_seconds}s)")
        
        while True:
            # Check if max wait time exceeded
            elapsed = time.time() - start_time
            if elapsed > self.max_wait_seconds:
                logger.warning(
                    f"Max wait time ({self.max_wait_seconds}s) exceeded for {file_path.name}. "
                    f"File may be locked or still syncing."
                )
                metadata = {
                    "error": "Max wait time exceeded",
                    "initial_size": initial_stats["size"] if initial_stats else None,
                    "initial_mtime": initial_stats["mtime"] if initial_stats else None,
                    "wait_duration_seconds": elapsed,
                    "change_count": change_count
                }
                return {"success": False, "metadata": metadata}
            
            # Get current file stats
            try:
                current_stats = self._get_file_stats(file_path)
            except (OSError, FileNotFoundError):
                # File may have been deleted or moved
                logger.warning(f"File disappeared during stabilization: {file_path}")
                metadata = {
                    "error": "File disappeared",
                    "initial_size": initial_stats["size"] if initial_stats else None,
                    "initial_mtime": initial_stats["mtime"] if initial_stats else None,
                    "wait_duration_seconds": time.time() - start_time,
                    "change_count": change_count
                }
                return {"success": False, "metadata": metadata}
            
            # Check if stats changed
            if last_stats is None:
                # First check
                initial_stats = current_stats.copy()
                last_stats = current_stats
                stable_since = time.time()
            elif (current_stats["size"] != last_stats["size"] or 
                  current_stats["mtime"] != last_stats["mtime"]):
                # Stats changed - reset stable timer
                change_count += 1
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
                    wait_duration = time.time() - start_time
                    metadata = {
                        "initial_size": initial_stats["size"],
                        "initial_mtime": initial_stats["mtime"],
                        "final_size": current_stats["size"],
                        "final_mtime": current_stats["mtime"],
                        "wait_duration_seconds": wait_duration,
                        "stable_duration_seconds": stable_duration,
                        "change_count": change_count
                    }
                    return {"success": True, "metadata": metadata}
            
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
        
        # Check if already exists (idempotent: reuse existing copy)
        if dest_path.exists():
            print(f"  File already exists in work directory: {dest_path.name} (reusing)")
            return dest_path
        
        # Copy file (atomic: use shutil.copy2 to preserve metadata)
        print(f"  Copying {file_path.name} to work directory...")
        shutil.copy2(file_path, dest_path)
        
        print(f"  Copied to: {dest_path}")
        return dest_path
    
    def stabilize_and_copy(self, file_path: Path, work_dir: Path, run_id: Optional[str] = None) -> Optional[Path]:
        """
        Stabilize file and copy to work directory.
        
        Args:
            file_path: Source file path
            work_dir: Destination work directory
            run_id: Optional run_id for audit logging
            
        Returns:
            Path to copied file, or None if stabilization failed
        """
        # Wait for file to stabilize
        stabilization_result = self.wait_for_stable(file_path)
        if not stabilization_result["success"]:
            logger.error(f"Failed to stabilize file: {file_path}")
            # Log failure to audit log
            if self.jsonl_logger:
                self.jsonl_logger.log({
                    "event_type": "file_stabilization_failed",
                    "timestamp": datetime.utcnow().isoformat(),
                    "run_id": run_id,
                    "source_path": str(file_path),
                    "error": stabilization_result["metadata"].get("error", "Unknown error"),
                    "metadata": stabilization_result["metadata"]
                })
            return None
        
        metadata = stabilization_result["metadata"]
        
        # Copy to work directory
        try:
            copied_path = self.copy_to_work_dir(file_path, work_dir)
            
            # Log successful stabilization to audit log
            if self.jsonl_logger:
                self.jsonl_logger.log({
                    "event_type": "file_stabilized",
                    "timestamp": datetime.utcnow().isoformat(),
                    "run_id": run_id,
                    "source_path": str(file_path),
                    "dest_path": str(copied_path),
                    "initial_size": metadata["initial_size"],
                    "initial_mtime": metadata["initial_mtime"],
                    "final_size": metadata["final_size"],
                    "final_mtime": metadata["final_mtime"],
                    "wait_duration_seconds": metadata["wait_duration_seconds"],
                    "stable_duration_seconds": metadata["stable_duration_seconds"],
                    "change_count": metadata["change_count"],
                    "stability_seconds": self.wait_seconds
                })
            
            return copied_path
        except Exception as e:
            logger.error(f"Failed to copy file {file_path}: {e}")
            # Log copy failure to audit log
            if self.jsonl_logger:
                self.jsonl_logger.log({
                    "event_type": "file_copy_failed",
                    "timestamp": datetime.utcnow().isoformat(),
                    "run_id": run_id,
                    "source_path": str(file_path),
                    "error": str(e),
                    "metadata": metadata
                })
            return None
    
    def process_input_files(self, work_dir: Path, run_id: Optional[str] = None) -> List[Path]:
        """
        Find, stabilize, and copy all input files to work directory.
        
        Args:
            work_dir: Work directory for current run (data/work/run_id/)
            run_id: Optional run_id for audit logging
            
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
            copied_path = self.stabilize_and_copy(input_file, work_dir, run_id=run_id)
            if copied_path:
                copied_files.append(copied_path)
            else:
                print(f"  Warning: Skipping {input_file.name} (stabilization/copy failed)")
        
        print(f"Successfully copied {len(copied_files)} file(s) to work directory")
        return copied_files
    
    def prepare_input_file(self, input_file: Path, work_dir: Path, run_id: Optional[str] = None) -> Optional[Path]:
        """
        Prepare a single input file: stabilize if needed and copy to work directory.
        
        This method handles the case where input_file is specified directly (e.g., from command line).
        If the file is in data/input, it will be stabilized and copied.
        If the file is already in work directory, it will be reused (idempotent).
        
        Args:
            input_file: Path to input file (may be in data/input or already in work directory)
            work_dir: Work directory for current run (data/work/run_id/)
            run_id: Optional run_id for audit logging
            
        Returns:
            Path to file in work directory, or None if preparation failed
        """
        input_file = Path(input_file).resolve()
        work_dir = Path(work_dir).resolve()
        
        # Check if file is already in work directory (idempotent: reuse)
        try:
            if str(input_file).startswith(str(work_dir)):
                print(f"File already in work directory: {input_file} (reusing)")
                return input_file
        except Exception:
            pass
        
        # Check if file is in input directory (needs stabilization)
        input_path = Path(self.input_path).resolve()
        try:
            if str(input_file).startswith(str(input_path)):
                # File is in input directory - stabilize and copy
                print(f"File is in input directory, stabilizing and copying: {input_file}")
                return self.stabilize_and_copy(input_file, work_dir, run_id=run_id)
        except Exception:
            pass
        
        # File is outside both input and work directories
        # For direct file specification, copy directly without stabilization
        # (assumes file is already stable)
        print(f"File is outside input/work directories, copying directly: {input_file}")
        try:
            copied_path = self.copy_to_work_dir(input_file, work_dir)
            # Log direct copy (no stabilization)
            if self.jsonl_logger:
                stat = input_file.stat()
                self.jsonl_logger.log({
                    "event_type": "file_copied_direct",
                    "timestamp": datetime.utcnow().isoformat(),
                    "run_id": run_id,
                    "source_path": str(input_file),
                    "dest_path": str(copied_path),
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "note": "File copied directly (not from input directory, no stabilization)"
                })
            return copied_path
        except Exception as e:
            logger.error(f"Failed to copy file {input_file}: {e}")
            return None