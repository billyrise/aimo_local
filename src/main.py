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
import hashlib
import json
import warnings
from dotenv import load_dotenv

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
from normalize.url_normalizer import URLNormalizer
from signatures.signature_builder import SignatureBuilder
from detectors.abc_detector import ABCDetector
from classifiers.rule_classifier import RuleClassifier
from llm.client import LLMClient
from reporting.report_builder import ReportBuilder
from db.duckdb_client import DuckDBClient


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
    parser.add_argument("input_file", help="Path to input log file")
    parser.add_argument("--vendor", default="paloalto", 
                       help="Vendor name (default: paloalto)")
    parser.add_argument("--db-path", default="./data/cache/aimo.duckdb",
                       help="Path to DuckDB database")
    parser.add_argument("--output-dir", default="./data/output",
                       help="Output directory for reports")
    parser.add_argument("--enable-user-dimension", action="store_true",
                       help="Enable user×signature dimension for A/B/C detection")
    
    args = parser.parse_args()
    
    # Validate input
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    
    # Compute run_id first (needed for ABCDetector)
    run_id, run_key = compute_run_id(str(input_path))
    run_started_at = datetime.utcnow()
    
    print(f"Run ID: {run_id}")
    print(f"Input file: {args.input_file}")
    print(f"Vendor: {args.vendor}")
    print()
    
    # Initialize components
    print("Initializing components...")
    ingestor = BaseIngestor(args.vendor)
    normalizer = URLNormalizer()
    signature_builder = SignatureBuilder()
    abc_detector = ABCDetector(run_id=run_id)
    rule_classifier = RuleClassifier()
    llm_client = LLMClient()
    report_builder = ReportBuilder()
    db_client = DuckDBClient(args.db_path)
    
    # Log DuckDB configuration (規約固定)
    print(f"DuckDB path: {db_client.db_path}")
    print(f"DuckDB temp_directory: {db_client.temp_directory}")
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Stage 1: Ingestion
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
    
    # Stage 2: Normalization & Signature
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
    
    # Record run
    db_client.upsert("runs", {
        "run_id": run_id,
        "run_key": run_key,
        "started_at": datetime.utcnow().isoformat(),
        "status": "running",
        "last_completed_stage": 2,
        "signature_version": signature_builder.signature_version,
        "rule_version": "1",
        "prompt_version": "1",
        "input_manifest_hash": run_key,
        "total_events": event_count,
        "unique_signatures": signature_count
    }, conflict_key="run_id")
    
    # Record input file
    file_hash = hashlib.sha256(open(input_path, 'rb').read()).hexdigest()
    file_id = hashlib.sha256(f"{input_path}|{input_path.stat().st_size}|{input_path.stat().st_mtime}".encode()).hexdigest()
    db_client.upsert("input_files", {
        "file_id": file_id,
        "run_id": run_id,
        "file_path": str(input_path),
        "file_size": input_path.stat().st_size,
        "file_hash": file_hash,
        "vendor": args.vendor,
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
            "run_id": run_id,
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
    
    # Stage 3: Rule-based Classification
    print("Stage 3: Rule-based Classification...")
    
    # Get signature stats from DB for classification
    # We need norm_host and norm_path_template from signature_stats
    reader = db_client.get_reader()
    signature_stats_query = f"""
        SELECT url_signature, norm_host, norm_path_template
        FROM signature_stats
        WHERE run_id = ?
    """
    signature_stats_rows = reader.execute(signature_stats_query, [run_id]).fetchall()
    
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
    
    # Stage 4: LLM Analysis (unknown only)
    print("Stage 4: LLM Analysis (unknown only)...")
    
    # Get unknown signatures (not in analysis_cache)
    # Re-query signature_stats to ensure we have the latest data (in case of UPSERT errors)
    signature_stats_query = """
        SELECT url_signature, norm_host, norm_path_template
        FROM signature_stats
        WHERE run_id = ?
        ORDER BY url_signature
    """
    signature_stats_rows = reader.execute(signature_stats_query, [run_id]).fetchall()
    print(f"  Total signature_stats rows: {len(signature_stats_rows)}")
    
    unknown_signatures = []
    for row in signature_stats_rows:
        url_sig, norm_host, norm_path_template = row
        
        # Check if already classified
        cache_check = reader.execute(
            "SELECT url_signature FROM analysis_cache WHERE url_signature = ?",
            [url_sig]
        ).fetchone()
        
        if not cache_check:
            # Get additional stats for LLM prompt
            stats_row = reader.execute(
                "SELECT access_count, bytes_sent_sum FROM signature_stats WHERE run_id = ? AND url_signature = ?",
                [run_id, url_sig]
            ).fetchone()
            
            access_count = stats_row[0] if stats_row else 0
            bytes_sent_sum = stats_row[1] if stats_row else 0
            
            unknown_signatures.append({
                "url_signature": url_sig,
                "norm_host": norm_host or "",
                "norm_path_template": norm_path_template or "",
                "access_count": access_count,
                "bytes_sent_sum": bytes_sent_sum
            })
    
    # Batch unknown signatures for LLM analysis
    max_per_batch = llm_client.batching_config.get("max_signatures_per_request", 20)
    llm_analyzed_count = 0
    llm_needs_review_count = 0
    llm_skipped_count = 0
    
    print(f"  Unknown signatures to analyze: {len(unknown_signatures)}")
    
    for i in range(0, len(unknown_signatures), max_per_batch):
        batch = unknown_signatures[i:i + max_per_batch]
        
        try:
            # Analyze batch
            classifications = llm_client.analyze_batch(batch)
            
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
            # Mark batch as needs_review
            error_str = str(e)
            for sig_data in batch:
                url_sig = sig_data["url_signature"]
                
                db_client.upsert("analysis_cache", {
                    "url_signature": url_sig,
                    "status": "needs_review",
                    "error_type": "llm_error",
                    "error_reason": error_str[:500],  # Max length
                    "failure_count": 1,
                    "last_error_at": datetime.utcnow().isoformat(),
                    "classification_source": "LLM",
                    "signature_version": signature_builder.signature_version,
                    "rule_version": "1",
                    "prompt_version": "1"
                }, conflict_key="url_signature")
                
                if "budget_exceeded" in error_str:
                    llm_skipped_count += 1
                else:
                    llm_needs_review_count += 1
    
    # Flush writes
    db_client.flush()
    
    print(f"  LLM-analyzed: {llm_analyzed_count}")
    print(f"  Needs review: {llm_needs_review_count}")
    print(f"  Skipped (budget): {llm_skipped_count}")
    print()
    
    # Stage 5: Reporting (audit-ready)
    print("Stage 5: Reporting (audit-ready)...")
    
    finished_at = datetime.utcnow()
    
    # Get additional counts from DB
    reader = db_client.get_reader()
    
    # Get burst_hit and cumulative_hit from metadata (from ABC detection)
    burst_hit = metadata.get("burst_hit", 0)
    cumulative_hit = metadata.get("cumulative_hit", 0)
    
    # Get run started_at from DB
    run_row = reader.execute(
        "SELECT started_at FROM runs WHERE run_id = ?",
        [run_id]
    ).fetchone()
    if run_row and run_row[0]:
        if isinstance(run_row[0], str):
            run_started_at = datetime.fromisoformat(run_row[0])
        elif isinstance(run_row[0], datetime):
            run_started_at = run_row[0]
        else:
            # Fallback to current value
            pass
    
    # Get cache hit rate
    total_classified = rule_classified_count + llm_analyzed_count + cache_hit_count
    cache_hit_rate = cache_hit_count / signature_count if signature_count > 0 else 0.0
    
    # Build report
    report = report_builder.build_report(
        run_id=run_id,
        run_key=run_key,
        started_at=datetime.fromisoformat(run_started_at) if isinstance(run_started_at, str) else run_started_at,
        finished_at=finished_at,
        input_file=str(input_path),
        vendor=args.vendor,
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
            "seed": run_id
        },
        rule_coverage={
            "rule_hit": rule_classified_count,
            "unknown_count": unknown_count
        },
        llm_coverage={
            "llm_analyzed_count": llm_analyzed_count,
            "needs_review_count": llm_needs_review_count,
            "cache_hit_rate": cache_hit_rate,
            "skipped_count": llm_skipped_count
        },
        signature_version=signature_builder.signature_version,
        rule_version="1",
        prompt_version="1",
        exclusions={
            "action_filter": abc_detector.action_filter
        }
    )
    
    # Save report
    report_path = output_dir / f"run_{run_id}_summary.json"
    report_builder.save_report(report, report_path)
    
    print(f"  Generated report: {report_path}")
    print(f"  Report validated against schema")
    print()
    
    # Update run status (use upsert to avoid PK constraint issues)
    # Note: started_at is required NOT NULL, so we need to include it
    started_at_str = run_started_at.isoformat() if isinstance(run_started_at, datetime) else run_started_at
    db_client.upsert("runs", {
        "run_id": run_id,
        "run_key": run_key,  # Required NOT NULL field
        "started_at": started_at_str,  # Required NOT NULL field
        "status": "succeeded",
        "finished_at": datetime.utcnow().isoformat(),
        "last_completed_stage": 5,
        "signature_version": signature_builder.signature_version,
        "rule_version": "1",
        "prompt_version": "1",
        "input_manifest_hash": run_key,
        "total_events": event_count,
        "unique_signatures": signature_count
    }, conflict_key="run_id")
    
    db_client.flush()
    db_client.close()
    
    # Final KPI summary (定型KPI)
    print("Pipeline completed successfully!")
    print()
    print("=== Execution KPI ===")
    print(f"Run ID: {run_id}")
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


if __name__ == "__main__":
    main()
