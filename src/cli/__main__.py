"""
AIMO Analysis Engine CLI

Provides command-line interface for querying run status, history, and cache statistics.

Usage:
    python -m src.cli status --last
    python -m src.cli runs --limit 5
    python -m src.cli cache-stats
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.duckdb_client import DuckDBClient


def get_default_db_path() -> Path:
    """Get default DuckDB database path."""
    repo_root = Path(__file__).parent.parent.parent
    db_path = repo_root / "data" / "cache" / "aimo.duckdb"
    return db_path


def cmd_status(args):
    """Show last run status."""
    db_path = Path(args.db_path) if args.db_path else get_default_db_path()
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        print("       Run the engine at least once to create the database.", file=sys.stderr)
        sys.exit(1)
    
    db_client = DuckDBClient(str(db_path))
    reader = db_client.get_reader()
    
    try:
        # Get last run
        query = """
            SELECT 
                run_id,
                status,
                started_at,
                finished_at,
                total_events,
                unique_signatures,
                cache_hit_count,
                llm_sent_count
            FROM runs
            ORDER BY started_at DESC
            LIMIT 1
        """
        result = reader.execute(query).fetchone()
        
        if not result:
            print("No runs found in database.")
            return
        
        run_id, status, started_at, finished_at, total_events, unique_signatures, cache_hit_count, llm_sent_count = result
        
        print(f"Last Run Status:")
        print(f"  Run ID: {run_id}")
        print(f"  Status: {status}")
        print(f"  Started: {started_at}")
        if finished_at:
            print(f"  Finished: {finished_at}")
            if started_at and finished_at:
                duration = finished_at - started_at
                print(f"  Duration: {duration}")
        print(f"  Total Events: {total_events or 0}")
        print(f"  Unique Signatures: {unique_signatures or 0}")
        print(f"  Cache Hits: {cache_hit_count or 0}")
        print(f"  LLM Requests: {llm_sent_count or 0}")
        
    finally:
        db_client.close_reader(reader)
        db_client.close()


def cmd_runs(args):
    """List recent runs."""
    db_path = Path(args.db_path) if args.db_path else get_default_db_path()
    limit = args.limit or 10
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        print("       Run the engine at least once to create the database.", file=sys.stderr)
        sys.exit(1)
    
    db_client = DuckDBClient(str(db_path))
    reader = db_client.get_reader()
    
    try:
        query = """
            SELECT 
                run_id,
                status,
                started_at,
                finished_at,
                total_events,
                unique_signatures,
                cache_hit_count,
                llm_sent_count
            FROM runs
            ORDER BY started_at DESC
            LIMIT ?
        """
        results = reader.execute(query, [limit]).fetchall()
        
        if not results:
            print("No runs found in database.")
            return
        
        print(f"Recent Runs (limit: {limit}):")
        print()
        print(f"{'Run ID':<16} {'Status':<12} {'Started':<20} {'Events':<10} {'Signatures':<12} {'Cache Hits':<12} {'LLM Req':<10}")
        print("-" * 100)
        
        for row in results:
            run_id, status, started_at, finished_at, total_events, unique_signatures, cache_hit_count, llm_sent_count = row
            started_str = started_at.strftime("%Y-%m-%d %H:%M:%S") if started_at else "N/A"
            print(f"{run_id:<16} {status:<12} {started_str:<20} {total_events or 0:<10} {unique_signatures or 0:<12} {cache_hit_count or 0:<12} {llm_sent_count or 0:<10}")
        
    finally:
        db_client.close_reader(reader)
        db_client.close()


def cmd_cache_stats(args):
    """Show cache statistics."""
    db_path = Path(args.db_path) if args.db_path else get_default_db_path()
    
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        print("       Run the engine at least once to create the database.", file=sys.stderr)
        sys.exit(1)
    
    db_client = DuckDBClient(str(db_path))
    reader = db_client.get_reader()
    
    try:
        # Total cache entries
        total_query = "SELECT COUNT(*) FROM analysis_cache"
        total_count = reader.execute(total_query).fetchone()[0] or 0
        
        # Active entries
        active_query = "SELECT COUNT(*) FROM analysis_cache WHERE status = 'active'"
        active_count = reader.execute(active_query).fetchone()[0] or 0
        
        # Classification source breakdown
        source_query = """
            SELECT 
                classification_source,
                COUNT(*) as count
            FROM analysis_cache
            WHERE status = 'active'
            GROUP BY classification_source
            ORDER BY count DESC
        """
        source_results = reader.execute(source_query).fetchall()
        
        # Usage type breakdown
        usage_query = """
            SELECT 
                usage_type,
                COUNT(*) as count
            FROM analysis_cache
            WHERE status = 'active'
            GROUP BY usage_type
            ORDER BY count DESC
        """
        usage_results = reader.execute(usage_query).fetchall()
        
        # Status breakdown
        status_query = """
            SELECT 
                status,
                COUNT(*) as count
            FROM analysis_cache
            GROUP BY status
            ORDER BY count DESC
        """
        status_results = reader.execute(status_query).fetchall()
        
        # Signature stats summary
        sig_stats_query = """
            SELECT 
                COUNT(DISTINCT run_id) as run_count,
                COUNT(DISTINCT url_signature) as unique_signatures,
                SUM(access_count) as total_accesses
            FROM signature_stats
        """
        sig_stats_result = reader.execute(sig_stats_query).fetchone()
        run_count = sig_stats_result[0] or 0 if sig_stats_result else 0
        unique_sigs = sig_stats_result[1] or 0 if sig_stats_result else 0
        total_accesses = sig_stats_result[2] or 0 if sig_stats_result else 0
        
        print("Cache Statistics:")
        print()
        print(f"Analysis Cache:")
        print(f"  Total entries: {total_count}")
        print(f"  Active entries: {active_count}")
        print()
        
        if status_results:
            print("  Status breakdown:")
            for status, count in status_results:
                print(f"    {status}: {count}")
            print()
        
        if source_results:
            print("  Classification source (active only):")
            for source, count in source_results:
                print(f"    {source or 'NULL'}: {count}")
            print()
        
        if usage_results:
            print("  Usage type (active only):")
            for usage_type, count in usage_results:
                print(f"    {usage_type or 'NULL'}: {count}")
            print()
        
        print(f"Signature Stats:")
        print(f"  Runs: {run_count}")
        print(f"  Unique signatures: {unique_sigs}")
        print(f"  Total accesses: {total_accesses}")
        
    finally:
        db_client.close_reader(reader)
        db_client.close()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="AIMO Analysis Engine CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--db-path",
        type=str,
        help="Path to DuckDB database (default: data/cache/aimo.duckdb)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # status command
    status_parser = subparsers.add_parser("status", help="Show last run status")
    status_parser.add_argument("--last", action="store_true", help="Show last run (default)")
    status_parser.set_defaults(func=cmd_status)
    
    # runs command
    runs_parser = subparsers.add_parser("runs", help="List recent runs")
    runs_parser.add_argument("--limit", type=int, help="Maximum number of runs to show (default: 10)")
    runs_parser.set_defaults(func=cmd_runs)
    
    # cache-stats command
    cache_stats_parser = subparsers.add_parser("cache-stats", help="Show cache statistics")
    cache_stats_parser.set_defaults(func=cmd_cache_stats)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        if args.db_path or True:  # Always show traceback for debugging
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
