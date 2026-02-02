"""
AIMO Analysis Engine - Orchestrator

Manages execution lifecycle, checkpointing, and resume logic.
Implements deterministic run_id generation and stage-based checkpointing.

Standard Integration (v0.1.7+):
- Each run records the AIMO Standard version, commit, and SHA256 checksums
- This ensures audit reproducibility across Standard updates
"""

import hashlib
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field

from db.duckdb_client import DuckDBClient
from signatures.signature_builder import SignatureBuilder


@dataclass
class StandardInfo:
    """AIMO Standard version information for audit trail."""
    version: str  # e.g., "0.1.7"
    commit: str  # Full git commit hash
    tag: str  # e.g., "v0.1.7"
    artifacts_dir_sha256: str  # SHA256 of artifacts directory
    artifacts_zip_sha256: Optional[str] = None  # SHA256 of zip if exists
    
    def to_dict(self) -> dict:
        """Convert to dict for serialization."""
        return {
            "aimo_standard_version": self.version,
            "aimo_standard_commit": self.commit,
            "aimo_standard_tag": self.tag,
            "aimo_standard_artifacts_dir_sha256": self.artifacts_dir_sha256,
            "aimo_standard_artifacts_zip_sha256": self.artifacts_zip_sha256,
        }


@dataclass
class RunContext:
    """Execution context for a single run."""
    run_id: str
    run_key: str
    work_dir: Path
    input_manifest_hash: str
    signature_version: str
    rule_version: str
    prompt_version: str
    started_at: datetime
    last_completed_stage: int = 0
    status: str = "running"
    # Standard info (required for v0.1.7+ integration)
    standard_info: Optional[StandardInfo] = None


class Orchestrator:
    """
    Orchestrates execution lifecycle with checkpointing and resume support.
    
    Features:
    - Deterministic run_id generation from input manifest
    - Stage-based checkpointing (last_completed_stage)
    - Resume from last completed stage on re-run
    - Work directory management (data/work/run_id/)
    """
    
    # Stage definitions (matching spec 3.1)
    STAGE_0_ORCHESTRATOR = 0
    STAGE_1_INGESTION = 1
    STAGE_2_NORMALIZATION = 2
    STAGE_2B_ABC_DETECTION = 2  # Same stage as normalization
    STAGE_2C_CACHE = 2  # Same stage as normalization
    STAGE_3_RULE_CLASSIFICATION = 3
    STAGE_4_LLM_ANALYSIS = 4
    STAGE_5_REPORTING = 5
    
    def __init__(self, 
                 db_client: DuckDBClient,
                 work_base_dir: Path,
                 signature_version: Optional[str] = None,
                 rule_version: str = "1",
                 prompt_version: str = "1",
                 taxonomy_version: str = "1.0",
                 evidence_pack_version: str = "1.0",
                 engine_spec_version: str = "1.5",
                 aimo_standard_version: str = "0.1.7",
                 resolve_standard: bool = True):
        """
        Initialize orchestrator.
        
        Args:
            db_client: DuckDB client instance
            work_base_dir: Base directory for work files (e.g., ./data/work)
            signature_version: Signature version (default: from config)
            rule_version: Rule version
            prompt_version: Prompt version
            taxonomy_version: Taxonomy version (for Taxonomyセット)
            evidence_pack_version: Evidence Pack version
            engine_spec_version: Engine spec version (v1.5)
            aimo_standard_version: AIMO Standard version (default: 0.1.7)
            resolve_standard: Whether to resolve Standard artifacts (default: True)
        """
        self.db_client = db_client
        self.work_base_dir = Path(work_base_dir)
        self.work_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Get signature version from SignatureBuilder if not provided
        if signature_version is None:
            signature_builder = SignatureBuilder()
            signature_version = signature_builder.signature_version
        
        self.signature_version = signature_version
        self.rule_version = rule_version
        self.prompt_version = prompt_version
        self.taxonomy_version = taxonomy_version
        self.evidence_pack_version = evidence_pack_version
        self.engine_spec_version = engine_spec_version
        self.aimo_standard_version = aimo_standard_version
        
        self.current_run: Optional[RunContext] = None
        self.standard_info: Optional[StandardInfo] = None
        
        # Resolve AIMO Standard artifacts (for audit trail)
        if resolve_standard:
            self._resolve_standard(aimo_standard_version)
    
    def _resolve_standard(self, version: str):
        """
        Resolve AIMO Standard artifacts and record version info.
        
        Args:
            version: Standard version to resolve (e.g., "0.1.7")
        """
        try:
            from standard_adapter.resolver import resolve_standard_artifacts
            
            artifacts = resolve_standard_artifacts(version=version)
            
            self.standard_info = StandardInfo(
                version=artifacts.standard_version,
                commit=artifacts.standard_commit,
                tag=artifacts.standard_tag,
                artifacts_dir_sha256=artifacts.artifacts_dir_sha256,
                artifacts_zip_sha256=artifacts.artifacts_zip_sha256
            )
            
            # Update taxonomy_version to match Standard
            self.taxonomy_version = version
            
        except Exception as e:
            # Log warning but don't fail - Standard resolution is important but not blocking
            print(f"  WARNING: Failed to resolve AIMO Standard v{version}: {e}", flush=True)
            self.standard_info = None
    
    def compute_input_manifest_hash(self, input_files: List[Path]) -> str:
        """
        Compute input_manifest_hash from input files.
        
        Args:
            input_files: List of input file paths
            
        Returns:
            SHA256 hash of normalized manifest
        """
        # Sort files by path for determinism
        sorted_files = sorted([str(f.absolute()) for f in input_files])
        
        manifest_entries = []
        for file_path in sorted_files:
            path = Path(file_path)
            if not path.exists():
                raise FileNotFoundError(f"Input file not found: {file_path}")
            
            # Compute file hash
            with open(path, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            
            # Build manifest entry: path|size|mtime|hash
            file_size = path.stat().st_size
            mtime = path.stat().st_mtime
            
            manifest_entry = f"{file_path}|{file_size}|{mtime}|{file_hash}"
            manifest_entries.append(manifest_entry)
        
        # Join entries with newline for determinism
        manifest_str = "\n".join(manifest_entries)
        
        # Hash the manifest
        manifest_hash = hashlib.sha256(manifest_str.encode('utf-8')).hexdigest()
        
        return manifest_hash
    
    def compute_input_manifest_hash_from_db(self, run_id: str) -> str:
        """
        Compute input_manifest_hash from input_files table (Phase 7-3: with vendor, min/max_time).
        
        This is the final hash that includes all audit fields:
        - file_hash (sha256 of file content)
        - vendor
        - min_time, max_time (from ingestion)
        
        Args:
            run_id: Run ID to compute hash for
            
        Returns:
            SHA256 hash of normalized manifest (file_hash|vendor|min_time|max_time, sorted)
        """
        reader = self.db_client.get_reader()
        
        # Get all input files for this run
        input_files_rows = reader.execute(
            """
            SELECT file_hash, vendor, min_time, max_time
            FROM input_files
            WHERE run_id = ?
            ORDER BY file_hash, vendor, min_time, max_time
            """,
            [run_id]
        ).fetchall()
        
        if not input_files_rows:
            # Fallback: use initial hash if no input_files records yet
            return self.current_run.input_manifest_hash if self.current_run else ""
        
        # Build manifest entries: file_hash|vendor|min_time|max_time
        manifest_entries = []
        for row in input_files_rows:
            file_hash, vendor, min_time, max_time = row
            
            # Format: file_hash|vendor|min_time|max_time
            # Use empty string for None values
            min_time_str = min_time.isoformat() if min_time else ""
            max_time_str = max_time.isoformat() if max_time else ""
            vendor_str = vendor or ""
            
            manifest_entry = f"{file_hash}|{vendor_str}|{min_time_str}|{max_time_str}"
            manifest_entries.append(manifest_entry)
        
        # Join entries with newline for determinism
        manifest_str = "\n".join(manifest_entries)
        
        # Hash the manifest
        manifest_hash = hashlib.sha256(manifest_str.encode('utf-8')).hexdigest()
        
        return manifest_hash
    
    def compute_run_key(self, 
                       input_manifest_hash: str,
                       target_range_start: Optional[str] = None,
                       target_range_end: Optional[str] = None) -> str:
        """
        Compute deterministic run_key from input manifest and versions.
        
        The run_key includes AIMO Standard version and artifacts hash to ensure
        cache coherence: if the Standard changes, the run_key changes, preventing
        cache mixing between different Standard versions.
        
        Args:
            input_manifest_hash: Hash of input file manifest
            target_range_start: Optional date range start (YYYY-MM-DD)
            target_range_end: Optional date range end (YYYY-MM-DD)
            
        Returns:
            SHA256 hash (run_key)
        """
        # Build run_key input (matching spec 3.2.1, extended for Standard pinning)
        target_range = ""
        if target_range_start and target_range_end:
            target_range = f"{target_range_start}|{target_range_end}"
        
        # AIMO Standard version and hash (for cache coherence)
        standard_version = self.aimo_standard_version
        standard_artifacts_sha = ""
        if self.standard_info:
            standard_artifacts_sha = self.standard_info.artifacts_dir_sha256 or ""
        
        run_key_input = (
            f"{input_manifest_hash}|"
            f"{target_range}|"
            f"{self.signature_version}|"
            f"{self.rule_version}|"
            f"{self.prompt_version}|"
            f"{self.taxonomy_version}|"
            f"{self.evidence_pack_version}|"
            f"{self.engine_spec_version}|"
            f"{standard_version}|"
            f"{standard_artifacts_sha}"
        )
        
        run_key = hashlib.sha256(run_key_input.encode('utf-8')).hexdigest()
        return run_key
    
    def compute_run_id(self, run_key: str) -> str:
        """
        Compute short run_id from run_key.
        
        Args:
            run_key: Full run_key (SHA256 hash)
            
        Returns:
            Short run_id (first 16 chars)
        """
        return run_key[:16]
    
    def get_or_create_run(self,
                         input_files: List[Path],
                         target_range_start: Optional[str] = None,
                         target_range_end: Optional[str] = None) -> RunContext:
        """
        Get existing run or create new run context.
        
        This method implements idempotency:
        - If run with same input_manifest_hash exists, return existing run
        - Otherwise, create new run context
        
        Args:
            input_files: List of input file paths
            target_range_start: Optional date range start
            target_range_end: Optional date range end
            
        Returns:
            RunContext instance
        """
        # Compute input_manifest_hash
        input_manifest_hash = self.compute_input_manifest_hash(input_files)
        
        # Compute run_key
        run_key = self.compute_run_key(input_manifest_hash, target_range_start, target_range_end)
        
        # Compute run_id
        run_id = self.compute_run_id(run_key)
        
        # Check if run exists in DB
        reader = self.db_client.get_reader()
        existing_run = reader.execute(
            "SELECT run_id, run_key, last_completed_stage, status FROM runs WHERE run_id = ?",
            [run_id]
        ).fetchone()
        
        if existing_run:
            # Existing run: resume from last_completed_stage
            existing_run_id, existing_run_key, last_stage, status = existing_run
            
            # Verify run_key matches (safety check)
            if existing_run_key != run_key:
                raise ValueError(
                    f"Run ID collision detected: run_id={run_id} exists with different run_key. "
                    f"This should not happen with deterministic hashing."
                )
            
            # Create work directory
            work_dir = self.work_base_dir / run_id
            work_dir.mkdir(parents=True, exist_ok=True)
            
            # Get started_at from DB
            started_at_row = reader.execute(
                "SELECT started_at FROM runs WHERE run_id = ?",
                [run_id]
            ).fetchone()
            
            started_at_str = started_at_row[0] if started_at_row else datetime.utcnow().isoformat()
            if isinstance(started_at_str, str):
                started_at = datetime.fromisoformat(started_at_str)
            else:
                started_at = started_at_str
            
            # Handle None values from DB
            last_stage = last_stage if last_stage is not None else 0
            status = status if status else "running"
            
            run_context = RunContext(
                run_id=run_id,
                run_key=run_key,
                work_dir=work_dir,
                input_manifest_hash=input_manifest_hash,
                signature_version=self.signature_version,
                rule_version=self.rule_version,
                prompt_version=self.prompt_version,
                started_at=started_at,
                last_completed_stage=last_stage,
                status=status
            )
            # Note: Taxonomy versions are stored in DB, not in RunContext
        else:
            # New run: create context and insert into DB
            work_dir = self.work_base_dir / run_id
            work_dir.mkdir(parents=True, exist_ok=True)
            
            started_at = datetime.utcnow()
            
            run_context = RunContext(
                run_id=run_id,
                run_key=run_key,
                work_dir=work_dir,
                input_manifest_hash=input_manifest_hash,
                signature_version=self.signature_version,
                rule_version=self.rule_version,
                prompt_version=self.prompt_version,
                started_at=started_at,
                last_completed_stage=0,
                status="running"
            )
            
            # Get code_version (git commit hash)
            from utils.git_version import get_code_version
            repo_root = Path(__file__).parent.parent.parent
            code_version = get_code_version(repo_root)
            
            # Build run record
            run_record = {
                "run_id": run_id,
                "run_key": run_key,
                "started_at": started_at.isoformat(),
                "status": "running",
                "last_completed_stage": 0,
                "code_version": code_version,
                "signature_version": self.signature_version,
                "rule_version": self.rule_version,
                "prompt_version": self.prompt_version,
                "taxonomy_version": self.taxonomy_version,
                "evidence_pack_version": self.evidence_pack_version,
                "engine_spec_version": self.engine_spec_version,
                "input_manifest_hash": input_manifest_hash,
                "target_range_start": target_range_start,
                "target_range_end": target_range_end,
            }
            
            # Add AIMO Standard info (required for audit trail)
            if self.standard_info:
                run_record["aimo_standard_version"] = self.standard_info.version
                run_record["aimo_standard_commit"] = self.standard_info.commit
                run_record["aimo_standard_artifacts_dir_sha256"] = self.standard_info.artifacts_dir_sha256
                if self.standard_info.artifacts_zip_sha256:
                    run_record["aimo_standard_artifacts_zip_sha256"] = self.standard_info.artifacts_zip_sha256
                
                # Also set standard_info in run context
                run_context.standard_info = self.standard_info
            
            # Insert run record (idempotent: ON CONFLICT DO NOTHING)
            self.db_client.insert("runs", run_record, ignore_conflict=True)
            
            # Flush to ensure run record is written
            self.db_client.flush()
        
        self.current_run = run_context
        return run_context
    
    def update_checkpoint(self, stage: int, status: str = "running"):
        """
        Update checkpoint after stage completion.
        
        Args:
            stage: Completed stage number
            status: Run status (running/succeeded/failed/partial)
        """
        if not self.current_run:
            raise RuntimeError("No active run context")
        
        # Update last_completed_stage first (no indexed column)
        self.db_client.update("runs", {
            "last_completed_stage": stage
        }, where_clause="run_id = ?", where_values=[self.current_run.run_id])
        
        # Update status separately if it changed (indexed column - DuckDB limitation)
        # DuckDB cannot update indexed columns via ON CONFLICT DO UPDATE
        # P0: Use Writer Queue for all DB writes (even indexed columns)
        if status != self.current_run.status:
            try:
                # Use Writer Queue (execute_sql) instead of direct connection
                self.db_client.execute_sql(
                    "UPDATE runs SET status = ? WHERE run_id = ?",
                    [status, self.current_run.run_id]
                )
            except Exception as e:
                # DuckDB may have issues with indexed column updates
                # Log warning but continue (status update is not critical for checkpoint functionality)
                print(f"  WARNING: Failed to update status to '{status}': {e}", flush=True)
                print(f"  Checkpoint (last_completed_stage) was updated successfully.", flush=True)
        
        # Flush to ensure checkpoint is written
        self.db_client.flush()
        
        # Update current run context
        self.current_run.last_completed_stage = stage
        self.current_run.status = status
    
    def finalize_run(self, status: str = "succeeded"):
        """
        Finalize run (mark as succeeded/failed).
        
        Args:
            status: Final status (succeeded/failed/partial)
        """
        if not self.current_run:
            raise RuntimeError("No active run context")
        
        finished_at = datetime.utcnow()
        
        # DuckDB limitation: Cannot update indexed columns (status) via ON CONFLICT DO UPDATE
        # P0: Use Writer Queue for all DB writes (even indexed columns and index operations)
        # Workaround: Temporarily drop index, update using Writer Queue, then recreate index
        try:
            # Drop index temporarily (via Writer Queue)
            self.db_client.execute_sql("DROP INDEX IF EXISTS idx_runs_status")
            self.db_client.flush()
            
            # Update all columns including status using Writer Queue
            self.db_client.execute_sql(
                "UPDATE runs SET status = ?, finished_at = ?, last_completed_stage = ? WHERE run_id = ?",
                [status, finished_at.isoformat(), self.STAGE_5_REPORTING, self.current_run.run_id]
            )
            self.db_client.flush()
            
            # Recreate index (via Writer Queue)
            self.db_client.execute_sql("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            self.db_client.flush()
        except Exception as e:
            # If index drop/update fails, try to recreate index and log warning
            try:
                self.db_client.execute_sql("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
                self.db_client.flush()
            except Exception:
                pass
            print(f"  WARNING: Failed to update status to '{status}': {e}", flush=True)
            print(f"  Checkpoint (last_completed_stage) was updated successfully.", flush=True)
        
        self.db_client.flush()
        
        self.current_run.status = status
    
    def should_skip_stage(self, stage: int) -> bool:
        """
        Check if stage should be skipped (already completed).
        
        Args:
            stage: Stage number to check
            
        Returns:
            True if stage should be skipped (already completed)
        """
        if not self.current_run:
            return False
        
        return self.current_run.last_completed_stage >= stage
    
    def get_work_dir(self) -> Path:
        """Get work directory for current run."""
        if not self.current_run:
            raise RuntimeError("No active run context")
        
        return self.current_run.work_dir
    
    def get_raw_dir(self) -> Path:
        """Get raw input directory for current run (data/work/run_id/raw/)."""
        raw_dir = self.get_work_dir() / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        return raw_dir
