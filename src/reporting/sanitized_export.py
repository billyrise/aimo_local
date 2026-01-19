"""
Sanitized Export for AIMO Analysis Engine

Generates fully anonymized CSV exports for external sharing and debugging.
All PII fields are irreversibly hashed using SHA256 with a salt.
"""

import os
import csv
import hashlib
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime


class SanitizedExporter:
    """
    Exporter for fully anonymized CSV data.
    
    All PII fields (user_id, src_ip, device_id) are irreversibly hashed.
    URL PII is already masked in normalization (using url_signature).
    """
    
    def __init__(self, salt: Optional[str] = None):
        """
        Initialize sanitized exporter.
        
        Args:
            salt: Salt for hashing (default: from SANITIZE_SALT env var)
        """
        if salt is None:
            salt = os.getenv("SANITIZE_SALT", "")
            if not salt:
                raise ValueError(
                    "SANITIZE_SALT environment variable must be set for sanitized exports. "
                    "Set a random string in .env.local (never commit it)."
                )
        
        self.salt = salt
        
        # Email pattern for validation
        self.email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    def anonymize(self, value: Optional[str]) -> str:
        """
        Irreversibly hash a PII value.
        
        Args:
            value: PII value to hash
            
        Returns:
            First 16 characters of SHA256(salt + value)
        """
        if not value:
            return ""
        
        hash_input = f"{self.salt}{value}".encode('utf-8')
        hash_output = hashlib.sha256(hash_input).hexdigest()
        return hash_output[:16]
    
    def export_csv(self,
                   db_reader,
                   run_id: str,
                   output_path: Path,
                   max_rows: int = 100000) -> int:
        """
        Export sanitized CSV from DuckDB.
        
        Args:
            db_reader: DuckDB reader connection
            run_id: Run ID to export
            output_path: Path to output CSV file
            max_rows: Maximum rows to export (default: 100000)
            
        Returns:
            Number of rows exported
        """
        # Query to join canonical events with signature stats and analysis cache
        # We need: event_time, dest_domain, url_signature, service_name, usage_type,
        # risk_level, category, bytes_sent, bytes_received, action, user_id (to hash)
        query = """
            SELECT DISTINCT
                e.event_time as ts,
                e.dest_domain,
                ss.url_signature,
                COALESCE(ac.service_name, 'unknown') as service_name,
                COALESCE(ac.usage_type, 'unknown') as usage_type,
                COALESCE(ac.risk_level, 'unknown') as risk_level,
                COALESCE(ac.category, '') as category,
                e.bytes_sent,
                e.bytes_received,
                e.action,
                e.user_id
            FROM signature_stats ss
            INNER JOIN (
                SELECT DISTINCT
                    event_time,
                    dest_domain,
                    url_signature,
                    bytes_sent,
                    bytes_received,
                    action,
                    user_id
                FROM (
                    SELECT
                        event_time,
                        dest_domain,
                        url_signature,
                        bytes_sent,
                        bytes_received,
                        action,
                        user_id
                    FROM signature_stats ss2
                    INNER JOIN (
                        SELECT DISTINCT
                            ingest_lineage_hash,
                            event_time,
                            dest_domain,
                            bytes_sent,
                            bytes_received,
                            action,
                            user_id
                        FROM input_files if2
                        WHERE if2.run_id = ?
                    ) events ON 1=1
                    WHERE ss2.run_id = ?
                )
                LIMIT ?
            ) e ON ss.url_signature = e.url_signature
            LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
            WHERE ss.run_id = ?
            ORDER BY e.ts
        """
        
        # Simplified query: Get events from signature_stats and join with analysis_cache
        # We'll need to reconstruct from canonical events stored in Parquet or work directory
        # For now, use a simpler approach: query signature_stats and get sample events
        
        # Alternative: Query from signature_stats with aggregated data
        # Since we need individual events, we should query from a source that has events
        # For Phase 10, we'll export signature-level data with aggregated stats
        
        # Get signature-level data with classification
        signature_query = """
            SELECT
                ss.url_signature,
                ss.dest_domain,
                COALESCE(ac.service_name, 'unknown') as service_name,
                COALESCE(ac.usage_type, 'unknown') as usage_type,
                COALESCE(ac.risk_level, 'unknown') as risk_level,
                COALESCE(ac.category, '') as category,
                ss.access_count,
                ss.bytes_sent_sum as bytes_sent,
                ss.unique_users,
                ss.first_seen as ts,
                ss.candidate_flags
            FROM signature_stats ss
            LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
            WHERE ss.run_id = ?
            ORDER BY ss.first_seen
            LIMIT ?
        """
        
        rows = db_reader.execute(signature_query, [run_id, max_rows]).fetchall()
        
        if not rows:
            # No data to export
            return 0
        
        # Prepare output directory
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temp file first (atomic write)
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        
        row_count = 0
        with open(temp_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'ts', 'dest_domain', 'url_signature', 'service_name',
                'usage_type', 'risk_level', 'category', 'bytes_sent',
                'bytes_received', 'action', 'user_hash'
            ])
            
            # Write rows
            for row in rows:
                url_sig, dest_domain, service_name, usage_type, risk_level, category, \
                    access_count, bytes_sent, unique_users, ts, candidate_flags = row
                
                # For signature-level export, we use aggregated values
                # bytes_received is not available in signature_stats, use 0
                bytes_received = 0
                
                # action is not in signature_stats, use 'allow' as default
                action = 'allow'
                
                # user_hash: Since we're at signature level, we can't hash individual users
                # Use a hash of the signature + "users" to indicate multiple users
                # For proper event-level export, we'd need to query from Parquet files
                user_hash = self.anonymize(f"{url_sig}:users")
                
                # Format timestamp
                if ts:
                    if isinstance(ts, str):
                        ts_str = ts
                    else:
                        ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                else:
                    ts_str = ""
                
                writer.writerow([
                    ts_str,
                    dest_domain or '',
                    url_sig,
                    service_name or 'unknown',
                    usage_type or 'unknown',
                    risk_level or 'unknown',
                    category or '',
                    bytes_sent or 0,
                    bytes_received,
                    action,
                    user_hash
                ])
                row_count += 1
        
        # Atomic rename
        temp_path.replace(output_path)
        
        return row_count
    
    def export_csv_from_events(self,
                               events: List[Dict[str, Any]],
                               signatures: Dict[str, Dict[str, Any]],
                               db_reader,
                               run_id: str,
                               output_path: Path,
                               max_rows: int = 100000) -> int:
        """
        Export sanitized CSV from canonical events list.
        
        This method joins events with signatures and analysis_cache to get
        classification information, then exports with PII hashed.
        
        Args:
            events: List of canonical event dictionaries (should have url_signature added)
            signatures: Dictionary mapping url_signature to signature data (may be empty if stage skipped)
            db_reader: DuckDB reader connection
            run_id: Run ID
            output_path: Path to output CSV file
            max_rows: Maximum rows to export (default: 100000)
            
        Returns:
            Number of rows exported
        """
        if not events:
            return 0
        
        # Build event -> url_signature mapping from signatures dict
        # signatures structure: {url_signature: {"signature": {...}, "events": [event1, event2, ...]}}
        event_to_sig = {}
        if signatures:
            for url_sig, sig_data in signatures.items():
                for event in sig_data.get("events", []):
                    # Use ingest_lineage_hash as key to match events
                    lineage_hash = event.get("ingest_lineage_hash")
                    if lineage_hash:
                        event_to_sig[lineage_hash] = url_sig
        
        # If signatures dict is empty (e.g., stage was skipped), try to reconstruct from DB
        if not event_to_sig:
            # Build mapping from DB: query signature_stats and match by dest_domain + url pattern
            # This is a fallback when signatures dict is not available
            sig_query = """
                SELECT url_signature, dest_domain, norm_host, norm_path_template
                FROM signature_stats
                WHERE run_id = ?
            """
            sig_rows = db_reader.execute(sig_query, [run_id]).fetchall()
            
            # Build a simple mapping by dest_domain (first match)
            # Note: This is a simplified matching - in production, you'd match by normalized URL
            sig_by_domain = {}
            for url_sig, dest_domain, norm_host, norm_path_template in sig_rows:
                if dest_domain:
                    if dest_domain not in sig_by_domain:
                        sig_by_domain[dest_domain] = url_sig
            
            # Create a simple mapping function
            def get_sig_for_event(event):
                dest_domain = event.get('dest_domain', '')
                return sig_by_domain.get(dest_domain, '')
        else:
            # Use the mapping we built from signatures dict
            def get_sig_for_event(event):
                lineage_hash = event.get('ingest_lineage_hash')
                return event_to_sig.get(lineage_hash, '') if lineage_hash else ''
        
        # Get classification data from analysis_cache
        cache_query = """
            SELECT url_signature, service_name, usage_type, risk_level, category
            FROM analysis_cache
            WHERE url_signature IN (
                SELECT url_signature FROM signature_stats WHERE run_id = ?
            )
        """
        cache_rows = db_reader.execute(cache_query, [run_id]).fetchall()
        cache_by_sig = {
            url_sig: {
                'service_name': service_name or 'unknown',
                'usage_type': usage_type or 'unknown',
                'risk_level': risk_level or 'unknown',
                'category': category or ''
            }
            for url_sig, service_name, usage_type, risk_level, category in cache_rows
        }
        
        # Prepare output directory
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temp file first (atomic write)
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        
        row_count = 0
        with open(temp_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'ts', 'dest_domain', 'url_signature', 'service_name',
                'usage_type', 'risk_level', 'category', 'bytes_sent',
                'bytes_received', 'action', 'user_hash'
            ])
            
            # Write rows
            for event in events[:max_rows]:
                # Get url_signature from mapping
                url_signature = get_sig_for_event(event)
                
                # Fallback: try to get from event directly (if added in Stage 2)
                if not url_signature:
                    url_signature = event.get('url_signature', '')
                
                # Get classification
                classification = cache_by_sig.get(url_signature, {
                    'service_name': 'unknown',
                    'usage_type': 'unknown',
                    'risk_level': 'unknown',
                    'category': ''
                })
                
                # Hash user_id
                user_id = event.get('user_id', '')
                user_hash = self.anonymize(user_id)
                
                # Format timestamp
                ts = event.get('event_time', '')
                if isinstance(ts, datetime):
                    ts_str = ts.isoformat()
                elif isinstance(ts, str):
                    ts_str = ts
                else:
                    ts_str = str(ts) if ts else ''
                
                writer.writerow([
                    ts_str,
                    event.get('dest_domain', ''),
                    url_signature,
                    classification['service_name'],
                    classification['usage_type'],
                    classification['risk_level'],
                    classification['category'],
                    event.get('bytes_sent', 0),
                    event.get('bytes_received', 0),
                    event.get('action', 'allow'),
                    user_hash
                ])
                row_count += 1
        
        # Atomic rename
        temp_path.replace(output_path)
        
        return row_count
    
    def validate_sanitized(self, csv_path: Path) -> List[str]:
        """
        Validate that sanitized CSV contains no PII.
        
        Args:
            csv_path: Path to sanitized CSV file
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        if not csv_path.exists():
            errors.append(f"CSV file not found: {csv_path}")
            return errors
        
        # Read CSV and check for forbidden columns
        forbidden_columns = ['user_id', 'src_ip', 'device_id', 'url_full', 'url_path', 'url_query']
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            
            # Check for forbidden columns
            for col in forbidden_columns:
                if col in columns:
                    errors.append(f"Forbidden column found: {col}")
            
            # Check for email patterns in string columns
            for row in reader:
                for col, value in row.items():
                    if value and isinstance(value, str):
                        if self.email_pattern.search(value):
                            errors.append(f"Email pattern found in column {col}: {value[:50]}")
                            break  # Only report first occurrence per column
        
        return errors
