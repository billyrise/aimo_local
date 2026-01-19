"""
AIMO Analysis Engine - Main Entry Point

Minimal E2E pipeline: Ingest → Normalize → Signature → Cache → Report (stub)

Usage:
    python src/main.py <input_file> [--vendor <vendor_name>]
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import hashlib
import json
import warnings
import shutil
from dotenv import load_dotenv

# File locking for process-level duplicate execution prevention
try:
    from filelock import FileLock, Timeout
    FILELOCK_AVAILABLE = True
except ImportError:
    FILELOCK_AVAILABLE = False

# Suppress urllib3 SSL warnings for LibreSSL compatibility
# urllib3 1.x works with LibreSSL, but may show warnings in some environments
warnings.filterwarnings('ignore', category=UserWarning, module='urllib3')

# Load environment variables from .env.local
env_local_path = Path(__file__).parent.parent / ".env.local"
if env_local_path.exists():
    load_dotenv(env_local_path, override=True)
else:
    # Fallback to .env if .env.local doesn't exist
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from ingestor.base import BaseIngestor
from ingestor.parquet_writer import ParquetWriter
from normalize.url_normalizer import URLNormalizer
from signatures.signature_builder import SignatureBuilder
from detectors.abc_detector import ABCDetector
from classifiers.rule_classifier import RuleClassifier
from reporting.excel_writer import ExcelWriter
from llm.client import LLMClient
from reporting.report_builder import ReportBuilder
from reporting.sanitized_export import SanitizedExporter
from db.duckdb_client import DuckDBClient
# Import Orchestrator from src/orchestrator.py (not from orchestrator module)
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from orchestrator import Orchestrator, RunContext
from orchestrator.file_stabilizer import FileStabilizer
from orchestrator.metrics import MetricsRecorder
from orchestrator.jsonl_logger import JSONLLogger


def compute_run_id(input_file: str, signature_version: str = "1.0") -> str:
    """
    Compute deterministic run_id from input file.
    
    Args:
        input_file: Path to input file
        signature_version: Signature version
    
    Returns:
        Short run_id (first 16 chars of base32-encoded hash)
    """
    file_path = Path(input_file)
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Compute file hash
    with open(file_path, 'rb') as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
    
    # Build run_key
    file_size = file_path.stat().st_size
    mtime = file_path.stat().st_mtime
    
    run_key_input = f"{input_file}|{file_size}|{mtime}|{signature_version}"
    run_key = hashlib.sha256(run_key_input.encode('utf-8')).hexdigest()
    
    # Short run_id (first 16 chars)
    run_id = run_key[:16]
    
    return run_id, run_key


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="AIMO Analysis Engine - E2E Pipeline")
    parser.add_argument("input_file", nargs="?", help="Path to input log file (optional if --use-box-sync)")
    parser.add_argument("--vendor", default="paloalto", 
                       help="Vendor name (default: paloalto)")
    parser.add_argument("--db-path", default="./data/cache/aimo.duckdb",
                       help="Path to DuckDB database")
    parser.add_argument("--output-dir", default="./data/output",
                       help="Output directory for reports")
    parser.add_argument("--enable-user-dimension", action="store_true",
                       help="Enable user×signature dimension for A/B/C detection")
    parser.add_argument("--skip-lock", action="store_true",
                       help="Skip process-level locking (use only if called from wrapper script)")
    parser.add_argument("--use-box-sync", action="store_true",
                       help="Use Box sync file stabilization (detect files from config/box_sync.yaml)")
    
    args = parser.parse_args()
    
    # Process-level locking (prevent duplicate execution)
    # Note: If called from ops/bin/run_aimo.sh, the wrapper script already handles locking
    # This is a fallback for direct execution
    if not args.skip_lock and FILELOCK_AVAILABLE:
        db_path = Path(args.db_path)
        lock_file = db_path.parent / "aimo.lock"
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        
        lock = FileLock(str(lock_file), timeout=1)  # Non-blocking: fail immediately if locked
        try:
            lock.acquire()
        except Timeout:
            print(f"ERROR: Another AIMO process is already running (lock file: {lock_file})", file=sys.stderr)
            print("       If this is incorrect, remove the lock file manually.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"WARNING: Failed to acquire lock: {e}", file=sys.stderr)
            print("         Continuing without lock (not recommended for production)", file=sys.stderr)
    
    try:
        _main_internal(args)
    finally:
        # Release lock on exit
        if not args.skip_lock and FILELOCK_AVAILABLE:
            try:
                lock.release()
            except Exception:
                pass  # Ignore errors during cleanup


def _main_internal(args):
    """Internal main function (called after lock acquisition)."""
    # Initialize components
    print("Initializing components...")
    db_client = DuckDBClient(args.db_path)
    
    # Log DuckDB configuration (規約固定)
    print(f"DuckDB path: {db_client.db_path}")
    print(f"DuckDB temp_directory: {db_client.temp_directory}")
    
    # Initialize orchestrator
    work_base_dir = Path(args.output_dir).parent / "work"
    orchestrator = Orchestrator(
        db_client=db_client,
        work_base_dir=work_base_dir
    )
    
    # Handle input file detection and stabilization
    input_files = []
    
    if args.use_box_sync:
        # Phase 11: Box sync file stabilization
        print("Using Box sync file stabilization...")
        stabilizer = FileStabilizer()
        
        # Step 1: Find input files in sync folder
        sync_input_files = stabilizer.find_input_files()
        
        if not sync_input_files:
            print("Error: No input files found in sync folder", file=sys.stderr)
            sys.exit(1)
        
        print(f"Found {len(sync_input_files)} file(s) in sync folder")
        
        # Step 2: Stabilize and copy files to temporary location
        # We need to stabilize before computing run_id (which requires file content)
        temp_work_dir = work_base_dir / "temp_stabilization"
        temp_work_dir.mkdir(parents=True, exist_ok=True)
        
        copied_files = []
        for sync_file in sync_input_files:
            print(f"Stabilizing: {sync_file.name}")
            copied_path = stabilizer.stabilize_and_copy(sync_file, temp_work_dir)
            if copied_path:
                copied_files.append(copied_path)
            else:
                print(f"Warning: Failed to stabilize {sync_file.name}, skipping", file=sys.stderr)
        
        if not copied_files:
            print("Error: No files were successfully stabilized", file=sys.stderr)
            sys.exit(1)
        
        print(f"Stabilized and copied {len(copied_files)} file(s)")
        
        # Step 3: Compute run_id from stabilized files
        # Use copied files to compute run_id (deterministic)
        input_files = copied_files
    else:
        # Legacy mode: use provided input_file
        if not args.input_file:
            print("Error: input_file is required when --use-box-sync is not specified", file=sys.stderr)
            sys.exit(1)
        
        input_path = Path(args.input_file)
        if not input_path.exists():
            print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
            sys.exit(1)
        
        input_files = [input_path]
    
    # Get or create run context (idempotent)
    # This computes run_id from input_files (stabilized files for Box sync mode)
    run_context = orchestrator.get_or_create_run(input_files)
    
    # If using Box sync, move files from temp_stabilization to actual run_id directory
    if args.use_box_sync:
        actual_work_dir = run_context.work_dir
        raw_dir = actual_work_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Move files from temp to actual work directory
        temp_raw_dir = temp_work_dir / "raw"
        if temp_raw_dir.exists():
            for temp_file in temp_raw_dir.iterdir():
                if temp_file.is_file():
                    dest_file = raw_dir / temp_file.name
                    if not dest_file.exists():
                        shutil.move(str(temp_file), str(dest_file))
                        print(f"  Moved {temp_file.name} to work directory")
            
            # Clean up temp directory
            try:
                shutil.rmtree(temp_work_dir)
            except Exception:
                pass
        
        # Update input_files to point to actual work directory
        input_files = list(raw_dir.iterdir())
        input_files = [f for f in input_files if f.is_file()]
    
    # Use first input file for processing (or all files if multi-file support is needed)
    input_path = input_files[0] if input_files else None
    if not input_path or not input_path.exists():
        print(f"Error: No valid input file found", file=sys.stderr)
        sys.exit(1)
    
    print(f"Run ID: {run_context.run_id}")
    print(f"Input file: {args.input_file}")
    print(f"Vendor: {args.vendor}")
    print(f"Last completed stage: {run_context.last_completed_stage}")
    print()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create processed directory for Parquet files
    processed_dir = Path(args.output_dir).parent / "processed"
    
    # Initialize processing components
    ingestor = BaseIngestor(args.vendor)
    parquet_writer = ParquetWriter(base_dir=processed_dir)
    normalizer = URLNormalizer()
    signature_builder = SignatureBuilder()
    abc_detector = ABCDetector(run_id=run_context.run_id)
    rule_classifier = RuleClassifier()
    llm_client = LLMClient()
    report_builder = ReportBuilder()
    
    # Initialize metrics recorder
    metrics_recorder = MetricsRecorder(db_client, run_context.run_id)
    
    # Phase 17: Initialize JSONL logger
    logs_dir = Path(args.output_dir).parent / "logs"
    jsonl_logger = JSONLLogger(logs_dir)
    
    # Log run start
    input_file_paths = [str(f) for f in input_files]
    jsonl_logger.log_run_start(
        run_id=run_context.run_id,
        run_key=run_context.run_key,
        input_files=input_file_paths,
        vendor=args.vendor,
        signature_version=signature_builder.signature_version,
        rule_version=orchestrator.rule_version,
        prompt_version=orchestrator.prompt_version,
        input_manifest_hash=run_context.input_manifest_hash
    )
    
    # Stage 1: Ingestion
    if not orchestrator.should_skip_stage(Orchestrator.STAGE_1_INGESTION):
        with metrics_recorder.record_stage(MetricsRecorder.STAGE_INGEST):
            canonical_events, event_count, pii_count = _stage_1_ingestion(
                orchestrator, ingestor, input_path
            )
            
            # Write Parquet file (Hive partition format)
            if canonical_events:
                parquet_path = _stage_1_write_parquet(
                    orchestrator, parquet_writer, canonical_events, args.vendor
                )
                print(f"  Written Parquet: {parquet_path}")
                print()
        
        # Record metrics with row count
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_INGEST,
            "row_count",
            float(event_count),
            "rows"
        )
        
        # Phase 17: Log stage completion
        jsonl_logger.log_stage_complete(
            run_id=run_context.run_id,
            stage="ingest",
            stage_number=1,
            status="completed",
            row_count=event_count
        )
        
        orchestrator.update_checkpoint(Orchestrator.STAGE_1_INGESTION)
    else:
        print("Stage 1: Ingestion (skipped - already completed)")
        # Load from work directory or reconstruct from DB if needed
        # For now, we'll re-ingest (can be optimized later)
        canonical_events, event_count, pii_count = _stage_1_ingestion(
            orchestrator, ingestor, input_path
        )
        print()
    
    # Stage 2: Normalization & Signature
    if not orchestrator.should_skip_stage(Orchestrator.STAGE_2_NORMALIZATION):
        with metrics_recorder.record_stage(MetricsRecorder.STAGE_NORMALIZE, row_count=len(canonical_events)):
            signatures, signature_count = _stage_2_normalization(
                orchestrator, canonical_events, normalizer, signature_builder
            )
        
        # Record signature count
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_NORMALIZE,
            "signature_count",
            float(signature_count),
            "signatures"
        )
        
        # Phase 17: Log stage completion
        jsonl_logger.log_stage_complete(
            run_id=run_context.run_id,
            stage="normalize",
            stage_number=2,
            status="completed",
            row_count=len(canonical_events),
            metadata={"signature_count": signature_count}
        )
        
        orchestrator.update_checkpoint(Orchestrator.STAGE_2_NORMALIZATION)
    else:
        print("Stage 2: Normalization & Signature (skipped - already completed)")
        # Reconstruct signatures from DB if needed
        reader = db_client.get_reader()
        signature_stats_rows = reader.execute(
            "SELECT url_signature FROM signature_stats WHERE run_id = ?",
            [run_context.run_id]
        ).fetchall()
        signature_count = len(signature_stats_rows)
        signatures = {}  # Will be reconstructed from DB if needed
        print(f"  Found {signature_count} signatures in cache")
        print()
    
    # Stage 2b: A/B/C Extraction & Stage 2c: Cache
    if not orchestrator.should_skip_stage(Orchestrator.STAGE_2C_CACHE):
        with metrics_recorder.record_stage(MetricsRecorder.STAGE_ABC_CACHE, row_count=event_count):
            abc_results, event_flags, signals, metadata, count_a, count_b, count_c, thresholds, cache_hit_count = _stage_2b_2c_abc_cache(
                orchestrator, canonical_events, signatures, abc_detector, db_client, 
                signature_builder, input_path, args.vendor, ingestor, event_count, signature_count
            )
        
        # Record A/B/C counts
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_ABC_CACHE,
            "count_a",
            float(count_a),
            "signatures"
        )
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_ABC_CACHE,
            "count_b",
            float(count_b),
            "signatures"
        )
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_ABC_CACHE,
            "count_c",
            float(count_c),
            "signatures"
        )
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_ABC_CACHE,
            "cache_hit_count",
            float(cache_hit_count),
            "signatures"
        )
        
        # Phase 17: Log stage completion
        jsonl_logger.log_stage_complete(
            run_id=run_context.run_id,
            stage="abc_cache",
            stage_number=2,
            status="completed",
            row_count=event_count,
            metadata={
                "count_a": count_a,
                "count_b": count_b,
                "count_c": count_c,
                "cache_hit_count": cache_hit_count
            }
        )
        
        orchestrator.update_checkpoint(Orchestrator.STAGE_2C_CACHE)
    else:
        print("Stage 2b-2c: A/B/C Extraction & Cache (skipped - already completed)")
        # Load metadata from DB or reconstruct
        reader = db_client.get_reader()
        # Reconstruct basic counts
        count_a = 0
        count_b = 0
        count_c = 0
        thresholds = {}
        event_flags = []
        signals = {}
        metadata = {}
        cache_hit_count = 0
        print()
    
    # Stage 3: Rule-based Classification
    if not orchestrator.should_skip_stage(Orchestrator.STAGE_3_RULE_CLASSIFICATION):
        with metrics_recorder.record_stage(MetricsRecorder.STAGE_RULE_CLASSIFICATION, row_count=signature_count):
            rule_classified_count, newly_classified_count, unknown_count = _stage_3_rule_classification(
                orchestrator, db_client, rule_classifier, signature_builder, signature_count
            )
        
        # Record classification counts
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_RULE_CLASSIFICATION,
            "rule_classified_count",
            float(rule_classified_count),
            "signatures"
        )
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_RULE_CLASSIFICATION,
            "unknown_count",
            float(unknown_count),
            "signatures"
        )
        
        # Phase 17: Log stage completion
        jsonl_logger.log_stage_complete(
            run_id=run_context.run_id,
            stage="rule_classification",
            stage_number=3,
            status="completed",
            metadata={
                "rule_classified_count": rule_classified_count,
                "unknown_count": unknown_count
            }
        )
        
        orchestrator.update_checkpoint(Orchestrator.STAGE_3_RULE_CLASSIFICATION)
    else:
        print("Stage 3: Rule-based Classification (skipped - already completed)")
        # Load counts from DB
        reader = db_client.get_reader()
        rule_classified_count = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE classification_source = 'RULE' AND status = 'active'"
        ).fetchone()[0] or 0
        unknown_count = max(0, signature_count - rule_classified_count)
        newly_classified_count = 0
        print(f"  Rule-classified: {rule_classified_count}")
        print(f"  Unknown (needs LLM): {unknown_count}")
        print()
    
    # Stage 4: LLM Analysis (unknown only)
    if not orchestrator.should_skip_stage(Orchestrator.STAGE_4_LLM_ANALYSIS):
        with metrics_recorder.record_stage(MetricsRecorder.STAGE_LLM_ANALYSIS):
            llm_analyzed_count, llm_needs_review_count, llm_skipped_count, total_retry_summary = _stage_4_llm_analysis(
                orchestrator, db_client, llm_client, signature_builder
            )
        
        # Record LLM analysis counts
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            "llm_analyzed_count",
            float(llm_analyzed_count),
            "signatures"
        )
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            "llm_needs_review_count",
            float(llm_needs_review_count),
            "signatures"
        )
        metrics_recorder.record_metric(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            "llm_skipped_count",
            float(llm_skipped_count),
            "signatures"
        )
        
        # Record LLM cost and budget consumption
        metrics_recorder.record_llm_cost_and_budget(
            MetricsRecorder.STAGE_LLM_ANALYSIS,
            budget_controller=llm_client.budget_controller
        )
        
        # Phase 17: Log stage completion
        jsonl_logger.log_stage_complete(
            run_id=run_context.run_id,
            stage="llm_analysis",
            stage_number=4,
            status="completed",
            metadata={
                "llm_analyzed_count": llm_analyzed_count,
                "llm_needs_review_count": llm_needs_review_count,
                "llm_skipped_count": llm_skipped_count
            }
        )
        
        orchestrator.update_checkpoint(Orchestrator.STAGE_4_LLM_ANALYSIS)
    else:
        print("Stage 4: LLM Analysis (skipped - already completed)")
        # Load counts from DB
        reader = db_client.get_reader()
        llm_analyzed_count = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE classification_source = 'LLM' AND status = 'active'"
        ).fetchone()[0] or 0
        llm_needs_review_count = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE status = 'needs_review'"
        ).fetchone()[0] or 0
        llm_skipped_count = reader.execute(
            "SELECT COUNT(*) FROM analysis_cache WHERE error_reason LIKE '%budget_exceeded%'"
        ).fetchone()[0] or 0
        total_retry_summary = {
            "attempts": 0,
            "backoff_ms_total": 0,
            "last_error_code": None,
            "rate_limit_events": 0
        }
        print(f"  LLM-analyzed: {llm_analyzed_count}")
        print(f"  Needs review: {llm_needs_review_count}")
        print(f"  Skipped (budget): {llm_skipped_count}")
        print()
    
    # Stage 5: Reporting (audit-ready)
    run_status = "succeeded"
    try:
        with metrics_recorder.record_stage(MetricsRecorder.STAGE_REPORTING):
            report_path = _stage_5_reporting(
                orchestrator, canonical_events, signatures, db_client, report_builder, output_dir,
                args.vendor, abc_detector, llm_client, signature_builder,
                event_count, signature_count, count_a, count_b, count_c,
                metadata, thresholds, rule_classified_count, unknown_count,
                llm_analyzed_count, llm_needs_review_count, llm_skipped_count,
                cache_hit_count, total_retry_summary, input_path
            )
        # Phase 17: Log stage completion
        jsonl_logger.log_stage_complete(
            run_id=run_context.run_id,
            stage="reporting",
            stage_number=5,
            status="completed"
        )
        
        orchestrator.update_checkpoint(Orchestrator.STAGE_5_REPORTING)
        orchestrator.finalize_run("succeeded")
    except Exception as e:
        print(f"ERROR in Stage 5: {e}", file=sys.stderr)
        run_status = "failed"
        orchestrator.finalize_run("failed")
        # Phase 17: Log error
        jsonl_logger.log_error(
            run_id=run_context.run_id,
            error_type="stage_failure",
            error_message=str(e),
            stage="reporting"
        )
        raise
    finally:
        # Phase 17: Collect failure counts by error type for logging
        reader = db_client.get_reader()
        failures_by_type = {}
        
        # Query failures from analysis_cache
        failure_rows = reader.execute("""
            SELECT error_type, COUNT(*) as count
            FROM analysis_cache
            WHERE error_type IS NOT NULL
            GROUP BY error_type
        """).fetchall()
        
        for error_type, count in failure_rows:
            failures_by_type[error_type] = int(count)
        
        # Phase 14: Get exclusion conditions and accurate counts from Parquet files
        exclusions = metadata.get("exclusions", {})
        exclusion_counts = {}
        
        # Calculate exclusion counts from Parquet files if available
        if isinstance(exclusions, dict):
            processed_dir = Path(args.output_dir).parent / "processed"
            vendor_dir = processed_dir / f"vendor={args.vendor}"
            
            parquet_files = []
            if vendor_dir.exists():
                for date_dir in vendor_dir.iterdir():
                    if date_dir.is_dir() and date_dir.name.startswith("date="):
                        for parquet_file in date_dir.glob("*.parquet"):
                            parquet_files.append(str(parquet_file))
            
            if parquet_files:
                for excl_type, condition in exclusions.items():
                    try:
                        if excl_type == "action_filter" and condition:
                            # Query excluded events from Parquet files
                            escaped_paths = [p.replace("'", "''") for p in parquet_files]
                            parquet_paths_str = "', '".join(escaped_paths)
                            
                            excl_query = f"""
                            SELECT COUNT(*) 
                            FROM read_parquet(['{parquet_paths_str}'])
                            WHERE action IS NOT NULL 
                                AND action != ?
                                AND action != ''
                            """
                            result = reader.execute(excl_query, [str(condition)]).fetchone()
                            exclusion_counts[excl_type] = result[0] if result and result[0] else 0
                        else:
                            exclusion_counts[excl_type] = None
                    except Exception:
                        exclusion_counts[excl_type] = None
            else:
                # No Parquet files available
                for excl_type in exclusions.keys():
                    exclusion_counts[excl_type] = None
        
        # Get LLM sent count from retry summary
        llm_sent_count = total_retry_summary.get("attempts", 0)
        
        # Log run end
        finished_at = datetime.utcnow()
        jsonl_logger.log_run_end(
            run_id=run_context.run_id,
            status=run_status,
            started_at=run_context.started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            event_count=event_count,
            signature_count=signature_count,
            count_a=count_a,
            count_b=count_b,
            count_c=count_c,
            unknown_count=unknown_count,
            llm_sent_count=llm_sent_count,
            llm_analyzed_count=llm_analyzed_count,
            llm_needs_review_count=llm_needs_review_count,
            llm_skipped_count=llm_skipped_count,
            failures_by_type=failures_by_type,
            exclusions=exclusions,
            exclusion_counts=exclusion_counts
        )
        
        # Flush metrics before closing
        db_client.flush()
        db_client.close()
    
    # Final KPI summary (定型KPI)
    print("Pipeline completed successfully!")
    print()
    print("=== Execution KPI ===")
    print(f"Run ID: {run_context.run_id}")
    print(f"rows_in: {event_count}")
    print(f"rows_out: {len(canonical_events)}")
    print(f"unique_signatures: {signature_count}")
    print(f"cache_hit: {cache_hit_count}")
    print(f"pii_count: {pii_count}")  # TODO: Implement PII counting
    print(f"abc_count_a: {count_a}")
    print(f"abc_count_b: {count_b}")
    print(f"abc_count_c: {count_c}")
    print(f"duckdb_path: {db_client.db_path}")
    print(f"temp_directory: {db_client.temp_directory}")
    print(f"Report: {report_path}")


# Stage functions (refactored for checkpointing support)

def _stage_1_ingestion(orchestrator: Orchestrator, ingestor: BaseIngestor, input_path: Path):
    """Stage 1: Ingestion"""
    print("Stage 1: Ingestion...")
    event_count = 0
    canonical_events = []
    pii_count = 0  # Track PII detections
    
    for event in ingestor.ingest_file(str(input_path)):
        canonical_events.append(event)
        event_count += 1
        
        if event_count % 1000 == 0:
            print(f"  Processed {event_count} events...")
    
    print(f"  Ingested {event_count} events (rows_in: {event_count})")
    print()
    
    return canonical_events, event_count, pii_count


def _stage_1_write_parquet(orchestrator: Orchestrator, parquet_writer: ParquetWriter,
                          canonical_events: List[Dict[str, Any]], vendor: str) -> Path:
    """Stage 1: Write Parquet file (Hive partition format)"""
    run_context = orchestrator.current_run
    
    # Write events to Parquet
    parquet_path = parquet_writer.write_events(
        events=canonical_events,
        vendor=vendor,
        run_id=run_context.run_id
    )
    
    return parquet_path


def _stage_2_normalization(orchestrator: Orchestrator, canonical_events: list, 
                          normalizer: URLNormalizer, signature_builder: SignatureBuilder):
    """Stage 2: Normalization & Signature"""
    print("Stage 2: Normalization & Signature...")
    signature_count = 0
    signatures = {}
    
    for event in canonical_events:
        # Normalize URL
        url_full = event.get("url_full", "")
        if not url_full:
            continue
        
        norm_result = normalizer.normalize(url_full)
        
        # Build signature
        sig = signature_builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method=event.get("http_method"),
            bytes_sent=event.get("bytes_sent", 0)
        )
        
        # Store signature
        url_sig = sig["url_signature"]
        if url_sig not in signatures:
            signatures[url_sig] = {
                "signature": sig,
                "events": []
            }
            signature_count += 1
        
        signatures[url_sig]["events"].append(event)
    
    print(f"  Generated {signature_count} unique signatures")
    print()
    
    return signatures, signature_count


def _stage_2b_2c_abc_cache(orchestrator: Orchestrator, canonical_events: list, signatures: dict,
                           abc_detector: ABCDetector, db_client: DuckDBClient,
                           signature_builder: SignatureBuilder, input_path: Path,
                           vendor: str, ingestor: BaseIngestor, event_count: int, signature_count: int):
    """Stage 2b: A/B/C Extraction & Stage 2c: Cache"""
    run_context = orchestrator.current_run
    
    # Stage 2b: A/B/C Extraction
    print("Stage 2b: A/B/C Extraction...")
    
    # Validate prerequisites: required fields for A/B/C detection
    required_fields = ["user_id", "dest_domain", "event_time", "bytes_sent", "action"]
    missing_fields = []
    for event in canonical_events:
        for field in required_fields:
            if field not in event or event[field] is None:
                if field not in missing_fields:
                    missing_fields.append(field)
    
    if missing_fields:
        print(f"  Warning: Some events missing required fields: {missing_fields}")
        print(f"  Proceeding with available fields...")
    
    # Prepare events with signatures for A/B/C detection
    events_with_signatures = []
    for url_sig, data in signatures.items():
        for event in data["events"]:
            # Add url_signature to event for A/B/C detection
            event_with_sig = event.copy()
            event_with_sig["url_signature"] = url_sig
            events_with_signatures.append(event_with_sig)
    
    # Detect A/B/C signals
    abc_results = abc_detector.detect(events_with_signatures)
    
    # Extract results
    event_flags = abc_results.get("event_flags", [])
    signals = abc_results.get("signals", {})
    metadata = abc_results.get("metadata", {})
    
    count_a = len(signals.get("A", []))
    count_b = len(signals.get("B", []))
    count_c = len(signals.get("C", []))
    
    print(f"  Detected A signals: {count_a}")
    print(f"  Detected B signals: {count_b}")
    print(f"  Detected C signals: {count_c}")
    
    # Print thresholds from metadata
    thresholds = metadata.get("thresholds_used", {})
    print(f"  A threshold: {thresholds.get('A_min_bytes', 'N/A')} bytes")
    print(f"  B burst threshold: {thresholds.get('B_burst_count', 'N/A')} events within {thresholds.get('B_burst_window_seconds', 'N/A')}s window")
    print(f"  B cumulative threshold: {thresholds.get('B_cumulative_bytes', 'N/A')} bytes per UTC day")
    print(f"  C sample rate: {thresholds.get('C_sample_rate', 'N/A')} (seed: {metadata.get('sample', {}).get('seed', 'N/A')})")
    print()
    
    # Stage 2c: Cache (DuckDB)
    print("Stage 2c: Cache (DuckDB)...")
    
    # Record input file
    file_hash = hashlib.sha256(open(input_path, 'rb').read()).hexdigest()
    file_id = hashlib.sha256(f"{input_path}|{input_path.stat().st_size}|{input_path.stat().st_mtime}".encode()).hexdigest()
    db_client.upsert("input_files", {
        "file_id": file_id,
        "run_id": run_context.run_id,
        "file_path": str(input_path),
        "file_size": input_path.stat().st_size,
        "file_hash": file_hash,
        "vendor": vendor,
        "log_type": ingestor.mapping.get("event_type", "unknown"),
        "row_count": event_count
    }, conflict_key="file_id")
    
    # Store signature stats with candidate_flags from A/B/C detection
    # Build map from lineage_hash to candidate_flags
    lineage_to_flags = {ef["ingest_lineage_hash"]: ef["candidate_flags"] 
                        for ef in event_flags if ef.get("ingest_lineage_hash")}
    
    # Build map from url_signature to events with lineage_hash
    sig_to_lineage = {}
    for url_sig, data in signatures.items():
        for event in data["events"]:
            lineage_hash = event.get("ingest_lineage_hash")
            if lineage_hash:
                if url_sig not in sig_to_lineage:
                    sig_to_lineage[url_sig] = []
                sig_to_lineage[url_sig].append(lineage_hash)
    
    for url_sig, data in signatures.items():
        sig = data["signature"]
        events = data["events"]
        
        # Aggregate candidate_flags for this signature
        # Collect all unique flags from events with this signature
        flags_set = set()
        lineage_hashes = sig_to_lineage.get(url_sig, [])
        for lineage_hash in lineage_hashes:
            flags = lineage_to_flags.get(lineage_hash)
            if flags:
                # Split pipe-separated flags and add to set
                for flag in flags.split("|"):
                    if flag:
                        flags_set.add(flag)
        
        candidate_flags = "|".join(sorted(flags_set)) if flags_set else None
        
        db_client.upsert("signature_stats", {
            "run_id": run_context.run_id,
            "url_signature": url_sig,
            "norm_host": sig["norm_host"],
            "norm_path_template": sig["norm_path_template"],
            "bytes_sent_bucket": sig["bytes_bucket"],
            "access_count": len(events),
            "unique_users": len(set(e.get("user_id") for e in events)),
            "bytes_sent_sum": sum(e.get("bytes_sent", 0) for e in events),
            "bytes_sent_max": max((e.get("bytes_sent", 0) for e in events), default=0),
            "candidate_flags": candidate_flags
        }, conflict_key="run_id, url_signature")  # Composite PK (run_id, url_signature)
    
    # Update run metrics
    db_client.update("runs", {
        "total_events": event_count,
        "unique_signatures": signature_count
    }, where_clause="run_id = ?", where_values=[run_context.run_id])
    
    # Flush writes
    db_client.flush()
    
    # Count cache hits (signatures that already exist in analysis_cache)
    import time
    time.sleep(0.2)  # Wait for writer thread
    cache_hit_count = 0
    try:
        existing_sigs = db_client._writer_conn.execute(
            "SELECT url_signature FROM analysis_cache"
        ).fetchall()
        existing_sig_set = {row[0] for row in existing_sigs}
        cache_hit_count = sum(1 for url_sig in signatures.keys() if url_sig in existing_sig_set)
    except Exception:
        pass  # Ignore if table doesn't exist yet
    
    print(f"  Cached {signature_count} signatures")
    print(f"  Cache hits: {cache_hit_count}")
    print()
    
    return abc_results, event_flags, signals, metadata, count_a, count_b, count_c, thresholds, cache_hit_count


def _stage_3_rule_classification(orchestrator: Orchestrator, db_client: DuckDBClient,
                                 rule_classifier: RuleClassifier, signature_builder: SignatureBuilder,
                                 signature_count: int):
    """Stage 3: Rule-based Classification"""
    print("Stage 3: Rule-based Classification...")
    run_context = orchestrator.current_run
    
    # Get signature stats from DB for classification
    # We need norm_host and norm_path_template from signature_stats
    reader = db_client.get_reader()
    signature_stats_query = f"""
        SELECT url_signature, norm_host, norm_path_template
        FROM signature_stats
        WHERE run_id = ?
    """
    signature_stats_rows = reader.execute(signature_stats_query, [run_context.run_id]).fetchall()
    
    # Classify signatures
    # Note: rule_hit includes both newly classified and cache hits (both are rule-classified)
    rule_classified_count = 0
    newly_classified_count = 0
    
    for row in signature_stats_rows:
        url_sig, norm_host, norm_path_template = row
        
        # Check if already in cache (count as rule_hit if exists)
        cache_check = reader.execute(
            "SELECT url_signature, classification_source FROM analysis_cache WHERE url_signature = ?",
            [url_sig]
        ).fetchone()
        
        if cache_check:
            # Cache hit: count as rule_hit if it was classified by RULE
            cache_source = cache_check[1] if len(cache_check) > 1 else None
            if cache_source == "RULE":
                rule_classified_count += 1
            # Continue to next signature (already classified)
            continue
        
        # Classify using rules
        classification = rule_classifier.classify(
            url_signature=url_sig,
            norm_host=norm_host or "",
            norm_path_template=norm_path_template
        )
        
        if classification:
            # Save to analysis_cache
            db_client.upsert("analysis_cache", {
                "url_signature": url_sig,
                "service_name": classification["service_name"],
                "category": classification["category"],
                "usage_type": classification["usage_type"],
                "risk_level": classification["default_risk"],
                "confidence": classification["confidence"],
                "rationale_short": classification.get("rationale_short", "")[:400],
                "classification_source": "RULE",
                "signature_version": signature_builder.signature_version,
                "rule_version": str(classification["rule_version"]),
                "prompt_version": "1",
                "status": "active",
                "is_human_verified": False,
                "analysis_date": datetime.utcnow().isoformat()
            }, conflict_key="url_signature")
            rule_classified_count += 1
            newly_classified_count += 1
    
    # Calculate unknown_count: total_signatures - rule_hit (audit integrity)
    # This ensures rule_hit + unknown_count == total_signatures
    unknown_count = max(0, signature_count - rule_classified_count)
    
    # Flush writes
    db_client.flush()
    
    print(f"  Rule-classified: {rule_classified_count} (newly: {newly_classified_count}, cache hits: {rule_classified_count - newly_classified_count})")
    print(f"  Unknown (needs LLM): {unknown_count}")
    print(f"  Total signatures: {signature_count} (rule_hit + unknown = {rule_classified_count + unknown_count})")
    print()
    
    return rule_classified_count, newly_classified_count, unknown_count


def _stage_4_llm_analysis(orchestrator: Orchestrator, db_client: DuckDBClient,
                         llm_client: LLMClient, signature_builder: SignatureBuilder):
    """Stage 4: LLM Analysis (unknown only)"""
    print("Stage 4: LLM Analysis (unknown only)...")
    run_context = orchestrator.current_run
    
    # Get unknown signatures (not in analysis_cache)
    # Re-query signature_stats to ensure we have the latest data (in case of UPSERT errors)
    reader = db_client.get_reader()
    signature_stats_query = """
        SELECT url_signature, norm_host, norm_path_template, candidate_flags
        FROM signature_stats
        WHERE run_id = ?
        ORDER BY url_signature
    """
    signature_stats_rows = reader.execute(signature_stats_query, [run_context.run_id]).fetchall()
    print(f"  Total signature_stats rows: {len(signature_stats_rows)}")
    
    unknown_signatures = []
    for row in signature_stats_rows:
        url_sig, norm_host, norm_path_template, candidate_flags = row
        
        # Check if already classified
        # State transitions:
        # - active: Classification confirmed (exclude from LLM analysis)
        # - needs_review: Retry candidate (include in LLM analysis, with backoff/limit)
        # - failed_permanent: Permanent failure (exclude from LLM analysis, human review only)
        cache_check = reader.execute(
            "SELECT url_signature, status, failure_count FROM analysis_cache WHERE url_signature = ? AND status IN ('active', 'failed_permanent')",
            [url_sig]
        ).fetchone()
        
        # Check if needs_review has exceeded retry limit
        if not cache_check:
            needs_review_check = reader.execute(
                "SELECT url_signature, status, failure_count FROM analysis_cache WHERE url_signature = ? AND status = 'needs_review'",
                [url_sig]
            ).fetchone()
            if needs_review_check:
                failure_count = needs_review_check[2] or 0
                # Max retries for needs_review: 3 attempts
                if failure_count >= 3:
                    # Mark as failed_permanent after max retries
                    db_client.upsert("analysis_cache", {
                        "url_signature": url_sig,
                        "status": "failed_permanent",
                        "error_type": "llm_error",
                        "error_reason": "Max retries exceeded for needs_review",
                        "failure_count": failure_count,
                        "last_error_at": datetime.utcnow().isoformat()
                    }, conflict_key="url_signature")
                    cache_check = (url_sig, "failed_permanent", failure_count)
        
        if not cache_check:
            # Get additional stats for LLM prompt
            stats_row = reader.execute(
                "SELECT access_count, bytes_sent_sum FROM signature_stats WHERE run_id = ? AND url_signature = ?",
                [run_context.run_id, url_sig]
            ).fetchone()
            
            access_count = stats_row[0] if stats_row else 0
            bytes_sent_sum = stats_row[1] if stats_row else 0
            
            unknown_signatures.append({
                "url_signature": url_sig,
                "norm_host": norm_host or "",
                "norm_path_template": norm_path_template or "",
                "access_count": access_count,
                "bytes_sent_sum": bytes_sent_sum,
                "candidate_flags": candidate_flags  # Include candidate_flags for priority filtering
            })
    
    # Phase 12: Priority-based budget filtering
    # Filter signatures by priority before batching
    # Estimate cost per signature for filtering
    estimated_input_tokens_per_sig = 100
    estimated_output_tokens_per_sig = 200
    estimated_cost_per_sig = llm_client._estimate_cost(
        estimated_input_tokens_per_sig, 
        estimated_output_tokens_per_sig
    )
    
    # Filter by priority using budget controller
    budget_controller = llm_client.budget_controller
    to_analyze, skipped = budget_controller.filter_by_priority(
        unknown_signatures,
        estimated_cost_per_sig
    )
    
    # Log filtering results
    print(f"  Unknown signatures to analyze: {len(unknown_signatures)}")
    print(f"  After priority filtering: {len(to_analyze)} to analyze, {len(skipped)} skipped")
    
    # Log skipped signatures by reason
    skip_reasons = {}
    for sig in skipped:
        reason = sig.get("skip_reason", "unknown")
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
    if skip_reasons:
        print(f"  Skip reasons: {skip_reasons}")
    
    # Mark skipped signatures in analysis_cache
    for sig in skipped:
        url_sig = sig["url_signature"]
        skip_reason = sig.get("skip_reason", "unknown")
        db_client.upsert("analysis_cache", {
            "url_signature": url_sig,
            "status": "failed_permanent",
            "error_type": "budget_exceeded",
            "error_reason": f"Budget exhausted: {skip_reason}",
            "failure_count": 0,
            "last_error_at": datetime.utcnow().isoformat(),
            "classification_source": "LLM",
            "signature_version": signature_builder.signature_version,
            "rule_version": "1",
            "prompt_version": "1"
        }, conflict_key="url_signature")
    
    # Batch filtered signatures for LLM analysis
    max_per_batch = llm_client.batching_config.get("max_signatures_per_request", 20)
    llm_analyzed_count = 0
    llm_needs_review_count = 0
    llm_skipped_count = len(skipped)  # Count skipped due to budget
    
    # Track retry summary and rate limit events across all batches
    total_retry_summary = {
        "attempts": 0,
        "backoff_ms_total": 0,
        "last_error_code": None,
        "rate_limit_events": 0
    }
    
    # Process filtered signatures
    for i in range(0, len(to_analyze), max_per_batch):
        batch = to_analyze[i:i + max_per_batch]
        
        try:
            # Analyze batch (returns classifications and retry_summary)
            classifications, batch_retry_summary = llm_client.analyze_batch(batch, initial_batch_size=max_per_batch)
            
            # Aggregate retry summary
            total_retry_summary["attempts"] = max(total_retry_summary["attempts"], batch_retry_summary["attempts"])
            total_retry_summary["backoff_ms_total"] += batch_retry_summary["backoff_ms_total"]
            if batch_retry_summary["last_error_code"]:
                total_retry_summary["last_error_code"] = batch_retry_summary["last_error_code"]
            total_retry_summary["rate_limit_events"] += batch_retry_summary["rate_limit_events"]
            
            # Save to analysis_cache
            for sig_data, classification in zip(batch, classifications):
                url_sig = sig_data["url_signature"]
                
                db_client.upsert("analysis_cache", {
                    "url_signature": url_sig,
                    "service_name": classification["service_name"],
                    "category": classification["category"],
                    "usage_type": classification["usage_type"],
                    "risk_level": classification["risk_level"],
                    "confidence": classification["confidence"],
                    "rationale_short": classification.get("rationale_short", "")[:400],
                    "classification_source": "LLM",
                    "signature_version": signature_builder.signature_version,
                    "rule_version": "1",
                    "prompt_version": "1",
                    "model": llm_client.provider_config.get("model", "unknown"),
                    "status": "active",
                    "is_human_verified": False,
                    "analysis_date": datetime.utcnow().isoformat()
                }, conflict_key="url_signature")
                
                llm_analyzed_count += 1
        
        except Exception as e:
            # Mark batch as needs_review or failed_permanent based on error type
            error_str = str(e)
            # Use LLMClient's error classification method
            error_type, is_permanent = llm_client._classify_error(e)
            
            for sig_data in batch:
                url_sig = sig_data["url_signature"]
                
                # Get current failure count
                current_status = reader.execute(
                    "SELECT status, failure_count FROM analysis_cache WHERE url_signature = ?",
                    [url_sig]
                ).fetchone()
                current_failure_count = (current_status[1] if current_status and current_status[1] else 0) if current_status else 0
                new_failure_count = current_failure_count + 1
                
                # Determine status based on error type and retry count
                if is_permanent or new_failure_count >= 3:
                    # Permanent error or max retries exceeded -> failed_permanent
                    status = "failed_permanent"
                    if "budget_exceeded" in error_str:
                        llm_skipped_count += 1
                    else:
                        llm_needs_review_count += 1  # Count as needs_review for reporting
                else:
                    # Transient error with retries remaining -> needs_review
                    status = "needs_review"
                    llm_needs_review_count += 1
                
                db_client.upsert("analysis_cache", {
                    "url_signature": url_sig,
                    "status": status,
                    "error_type": "llm_error",
                    "error_reason": error_str[:500],  # Max length
                    "failure_count": new_failure_count,
                    "last_error_at": datetime.utcnow().isoformat(),
                    "classification_source": "LLM",
                    "signature_version": signature_builder.signature_version,
                    "rule_version": "1",
                    "prompt_version": "1"
                }, conflict_key="url_signature")
    
    # Flush writes
    db_client.flush()
    
    print(f"  LLM-analyzed: {llm_analyzed_count}")
    print(f"  Needs review: {llm_needs_review_count}")
    print(f"  Skipped (budget): {llm_skipped_count}")
    print()
    
    return llm_analyzed_count, llm_needs_review_count, llm_skipped_count, total_retry_summary


def _stage_5_reporting(orchestrator: Orchestrator, canonical_events: list, signatures: dict,
                      db_client: DuckDBClient, report_builder: ReportBuilder, output_dir: Path, vendor: str,
                      abc_detector: ABCDetector, llm_client: LLMClient, signature_builder: SignatureBuilder,
                      event_count: int, signature_count: int, count_a: int, count_b: int, count_c: int,
                      metadata: dict, thresholds: dict, rule_classified_count: int, unknown_count: int,
                      llm_analyzed_count: int, llm_needs_review_count: int, llm_skipped_count: int,
                      cache_hit_count: int, total_retry_summary: dict, input_path: Path):
    """Stage 5: Reporting (audit-ready)"""
    print("Stage 5: Reporting (audit-ready)...")
    run_context = orchestrator.current_run
    
    finished_at = datetime.utcnow()
    
    # Get additional counts from DB
    reader = db_client.get_reader()
    
    # Get burst_hit and cumulative_hit from metadata (from ABC detection)
    burst_hit = metadata.get("burst_hit", 0)
    cumulative_hit = metadata.get("cumulative_hit", 0)
    
    # Recalculate LLM analyzed count from database (for accuracy)
    # This ensures we count all LLM-classified signatures, including cache hits
    llm_analyzed_db = reader.execute(
        "SELECT COUNT(*) FROM analysis_cache WHERE classification_source = 'LLM' AND status = 'active'"
    ).fetchone()
    llm_analyzed_count_final = llm_analyzed_db[0] if llm_analyzed_db else llm_analyzed_count
    
    # Get cache hit rate
    total_classified = rule_classified_count + llm_analyzed_count_final + cache_hit_count
    cache_hit_rate = cache_hit_count / signature_count if signature_count > 0 else 0.0
    
    # Build report
    report = report_builder.build_report(
        run_id=run_context.run_id,
        run_key=run_context.run_key,
        started_at=run_context.started_at,
        finished_at=finished_at,
        input_file=str(input_path),
        vendor=vendor,
        thresholds_used={
            "A_min_bytes": thresholds.get("A_min_bytes", 0),
            "B_burst_count": thresholds.get("B_burst_count", 0),
            "B_burst_window_seconds": thresholds.get("B_burst_window_seconds", 0),
            "B_cumulative_bytes": thresholds.get("B_cumulative_bytes", 0),
            "C_sample_rate": thresholds.get("C_sample_rate", 0.0)
        },
        counts={
            "total_events": event_count,
            "total_signatures": signature_count,
            "unique_users": len(set(e.get("user_id") for e in canonical_events)),
            "unique_domains": len(set(e.get("dest_domain") for e in canonical_events)),
            "abc_count_a": count_a,
            "abc_count_b": count_b,
            "abc_count_c": count_c,
            "burst_hit": burst_hit,
            "cumulative_hit": cumulative_hit
        },
        sample={
            "sample_rate": thresholds.get("C_sample_rate", 0.0),
            "sample_method": "deterministic_hash",
            "seed": run_context.run_id
        },
        rule_coverage={
            "rule_hit": rule_classified_count,
            "unknown_count": unknown_count
        },
        llm_coverage={
            "llm_analyzed_count": llm_analyzed_count_final,
            "needs_review_count": llm_needs_review_count,
            "cache_hit_rate": cache_hit_rate,
            "skipped_count": llm_skipped_count,
            # Audit trail: LLM provider and model info
            "llm_provider": llm_client.provider_name,
            "llm_model": llm_client.provider_config.get("model", "unknown"),
            "structured_output": llm_client.provider_config.get("structured_output", False),
            "schema_sanitized": True,  # We always sanitize schema for Gemini
            # Retry summary for audit
            "retry_summary": total_retry_summary,
            "rate_limit_events": total_retry_summary["rate_limit_events"]
        },
        signature_version=signature_builder.signature_version,
        rule_version="1",
        prompt_version="1",
        exclusions={
            "action_filter": abc_detector.action_filter
        }
    )
    
    # Save report
    report_path = output_dir / f"run_{run_context.run_id}_summary.json"
    report_builder.save_report(report, report_path)
    
    print(f"  Generated report: {report_path}")
    print(f"  Report validated against schema")
    
    # Generate Excel report
    excel_path = output_dir / f"run_{run_context.run_id}_report.xlsx"
    try:
        excel_writer = ExcelWriter(excel_path)
        excel_writer.generate_excel(
            run_id=run_context.run_id,
            report_data=report,
            db_reader=reader,
            run_context=run_context
        )
        print(f"  Generated Excel report: {excel_path}")
    except Exception as e:
        print(f"  WARNING: Failed to generate Excel report: {e}")
        print(f"  Continuing with JSON report only...")
    
    # Generate sanitized CSV export
    sanitized_path = output_dir / f"run_{run_context.run_id}_sanitized.csv"
    try:
        exporter = SanitizedExporter()
        row_count = exporter.export_csv_from_events(
            events=canonical_events,
            signatures=signatures,
            db_reader=reader,
            run_id=run_context.run_id,
            output_path=sanitized_path,
            max_rows=100000
        )
        
        # Validate sanitized output
        validation_errors = exporter.validate_sanitized(sanitized_path)
        if validation_errors:
            print(f"  WARNING: Sanitized CSV validation errors:")
            for error in validation_errors:
                print(f"    - {error}")
        else:
            print(f"  Generated sanitized CSV: {sanitized_path} ({row_count} rows)")
    except Exception as e:
        print(f"  WARNING: Failed to generate sanitized CSV: {e}")
        print(f"  Continuing with other reports...")
    
    print()
    
    return report_path
