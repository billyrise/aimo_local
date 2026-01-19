"""
AIMO Analysis Engine - JSONL Structured Logger

Implements structured logging in JSONL format for operational monitoring.
Logs are written to logs/ directory with daily rotation.

Features:
- JSONL format (one JSON object per line)
- Daily log file rotation (logs/YYYY-MM-DD.jsonl)
- Atomic writes (.tmp -> rename())
- Required fields: run start/end, input files, counts, errors, exclusions
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from threading import Lock


class JSONLLogger:
    """
    Structured logger that writes JSONL format logs.
    
    Features:
    - Daily log file rotation
    - Atomic writes (tmp -> rename)
    - Thread-safe logging
    - Required audit fields
    """
    
    def __init__(self, logs_dir: Path):
        """
        Initialize JSONL logger.
        
        Args:
            logs_dir: Base directory for log files (e.g., ./logs)
        """
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._current_log_file: Optional[Path] = None
        self._current_date: Optional[str] = None
    
    def _get_log_file_path(self, date: Optional[str] = None) -> Path:
        """
        Get log file path for a given date.
        
        Args:
            date: Date string (YYYY-MM-DD), or None for today
            
        Returns:
            Path to log file
        """
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")
        
        return self.logs_dir / f"{date}.jsonl"
    
    def _ensure_log_file(self) -> Path:
        """
        Ensure log file is open for current date.
        
        Returns:
            Path to current log file
        """
        today = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Check if we need to rotate
        if self._current_date != today:
            self._current_date = today
            self._current_log_file = self._get_log_file_path(today)
        
        return self._current_log_file
    
    def log(self, event: Dict[str, Any]):
        """
        Write a log event to JSONL file.
        
        Args:
            event: Dictionary containing log event data
        """
        with self._lock:
            log_file = self._ensure_log_file()
            
            # Add timestamp if not present
            if "timestamp" not in event:
                event["timestamp"] = datetime.utcnow().isoformat()
            
            # Write to temporary file first (atomic write)
            tmp_file = log_file.with_suffix(".jsonl.tmp")
            
            try:
                # Read existing content if file exists
                existing_content = ""
                if log_file.exists():
                    try:
                        with open(log_file, "r", encoding="utf-8") as f:
                            existing_content = f.read()
                    except Exception:
                        # If read fails, start fresh
                        existing_content = ""
                
                # Write to temporary file
                with open(tmp_file, "w", encoding="utf-8") as f:
                    # Write existing content
                    if existing_content:
                        f.write(existing_content)
                    
                    # Write new log entry
                    json_line = json.dumps(event, ensure_ascii=False)
                    f.write(json_line + "\n")
                
                # Atomic rename
                tmp_file.replace(log_file)
                
            except Exception as e:
                # Clean up tmp file on error
                if tmp_file.exists():
                    try:
                        tmp_file.unlink()
                    except Exception:
                        pass
                # Re-raise to allow caller to handle
                raise RuntimeError(f"Failed to write log entry: {e}") from e
    
    def log_run_start(self, 
                     run_id: str,
                     run_key: str,
                     input_files: List[str],
                     vendor: str,
                     signature_version: str,
                     rule_version: str,
                     prompt_version: str,
                     input_manifest_hash: str):
        """
        Log run start event.
        
        Args:
            run_id: Run ID
            run_key: Run key (deterministic hash)
            input_files: List of input file paths
            vendor: Vendor name
            signature_version: Signature version
            rule_version: Rule version
            prompt_version: Prompt version
            input_manifest_hash: Input manifest hash
        """
        event = {
            "event_type": "run_start",
            "run_id": run_id,
            "run_key": run_key,
            "input_files": input_files,
            "vendor": vendor,
            "signature_version": signature_version,
            "rule_version": rule_version,
            "prompt_version": prompt_version,
            "input_manifest_hash": input_manifest_hash
        }
        self.log(event)
    
    def log_run_end(self,
                   run_id: str,
                   status: str,
                   started_at: str,
                   finished_at: str,
                   event_count: int,
                   signature_count: int,
                   count_a: int,
                   count_b: int,
                   count_c: int,
                   unknown_count: int,
                   llm_sent_count: int,
                   llm_analyzed_count: int,
                   llm_needs_review_count: int,
                   llm_skipped_count: int,
                   failures_by_type: Dict[str, int],
                   exclusions: Dict[str, Any],
                   exclusion_counts: Dict[str, int]):
        """
        Log run end event with all required metrics.
        
        Args:
            run_id: Run ID
            status: Run status (succeeded/failed/partial)
            started_at: Start timestamp (ISO format)
            finished_at: End timestamp (ISO format)
            event_count: Total event count
            signature_count: Total signature count
            count_a: A count
            count_b: B count
            count_c: C count
            unknown_count: Unknown signature count
            llm_sent_count: LLM requests sent
            llm_analyzed_count: LLM analyzed count
            llm_needs_review_count: LLM needs review count
            llm_skipped_count: LLM skipped count
            failures_by_type: Failures by error type
            exclusions: Exclusion conditions
            exclusion_counts: Exclusion counts by type
        """
        event = {
            "event_type": "run_end",
            "run_id": run_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "metrics": {
                "event_count": event_count,
                "signature_count": signature_count,
                "abc_counts": {
                    "count_a": count_a,
                    "count_b": count_b,
                    "count_c": count_c
                },
                "unknown_count": unknown_count,
                "llm": {
                    "sent_count": llm_sent_count,
                    "analyzed_count": llm_analyzed_count,
                    "needs_review_count": llm_needs_review_count,
                    "skipped_count": llm_skipped_count
                },
                "failures_by_type": failures_by_type,
                "exclusions": {
                    "conditions": exclusions,
                    "counts": exclusion_counts
                }
            }
        }
        self.log(event)
    
    def log_stage_complete(self,
                          run_id: str,
                          stage: str,
                          stage_number: int,
                          status: str,
                          duration_ms: Optional[float] = None,
                          row_count: Optional[int] = None,
                          metadata: Optional[Dict[str, Any]] = None):
        """
        Log stage completion event.
        
        Args:
            run_id: Run ID
            stage: Stage name
            stage_number: Stage number
            status: Stage status (completed/failed/skipped)
            duration_ms: Duration in milliseconds
            row_count: Row count processed
            metadata: Additional metadata
        """
        event = {
            "event_type": "stage_complete",
            "run_id": run_id,
            "stage": stage,
            "stage_number": stage_number,
            "status": status
        }
        
        if duration_ms is not None:
            event["duration_ms"] = duration_ms
        if row_count is not None:
            event["row_count"] = row_count
        if metadata:
            event["metadata"] = metadata
        
        self.log(event)
    
    def log_error(self,
                 run_id: str,
                 error_type: str,
                 error_message: str,
                 stage: Optional[str] = None,
                 metadata: Optional[Dict[str, Any]] = None):
        """
        Log error event.
        
        Args:
            run_id: Run ID
            error_type: Error type
            error_message: Error message
            stage: Stage where error occurred
            metadata: Additional metadata
        """
        event = {
            "event_type": "error",
            "run_id": run_id,
            "error_type": error_type,
            "error_message": error_message
        }
        
        if stage:
            event["stage"] = stage
        if metadata:
            event["metadata"] = metadata
        
        self.log(event)
