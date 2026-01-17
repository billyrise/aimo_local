"""
Base Ingestor for AIMO Analysis Engine

Reads vendor-specific log files and normalizes them to Canonical Event format.
Uses mapping.yaml files to map vendor fields to canonical schema.
"""

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Iterator, Tuple
import yaml
import polars as pl
from dateutil import parser as date_parser
import tldextract


class BaseIngestor:
    """
    Base class for vendor-specific log ingestion.
    
    Subclasses should implement vendor-specific parsing logic.
    This base class provides common utilities and canonical event construction.
    """
    
    def __init__(self, vendor: str, mapping_path: Optional[str] = None):
        """
        Initialize ingestor.
        
        Args:
            vendor: Vendor name (e.g., "paloalto", "zscaler")
            mapping_path: Path to mapping.yaml (default: schemas/vendors/{vendor}/mapping.yaml)
        """
        self.vendor = vendor
        
        if mapping_path is None:
            mapping_path = Path(__file__).parent.parent.parent / "schemas" / "vendors" / vendor / "mapping.yaml"
        
        with open(mapping_path, 'r', encoding='utf-8') as f:
            self.mapping = yaml.safe_load(f)
        
        # Initialize domain extractor (Public Suffix List)
        psl_path = Path(__file__).parent.parent.parent / "data" / "psl" / "public_suffix_list.dat"
        if psl_path.exists():
            # Use local file with file:// protocol
            suffix_list_urls = [f"file://{psl_path.absolute()}"]
        else:
            # Use default URLs if local file doesn't exist
            suffix_list_urls = None
        
        self.domain_extractor = tldextract.TLDExtract(
            suffix_list_urls=suffix_list_urls,
            fallback_to_snapshot=True  # Fallback to snapshot if download fails
        )
    
    def ingest_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """
        Ingest a log file and yield canonical events.
        
        Args:
            file_path: Path to input log file
        
        Yields:
            Canonical event dictionaries
        """
        # Detect file format and parse
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            yield from self._ingest_csv(file_path)
        elif file_ext == '.json':
            yield from self._ingest_json(file_path)
        elif file_ext == '.jsonl':
            yield from self._ingest_jsonl(file_path)
        else:
            # Try CSV as default
            yield from self._ingest_csv(file_path)
    
    def _ingest_csv(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """Ingest CSV file."""
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            # Try to detect delimiter
            sample = f.read(1024)
            f.seek(0)
            
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
            except:
                dialect = csv.excel  # Default
            
            reader = csv.DictReader(f, dialect=dialect)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                try:
                    event = self._parse_row(row, file_path, row_num)
                    if event:
                        yield event
                except Exception as e:
                    # Log parse error but continue
                    print(f"Parse error at row {row_num}: {e}", flush=True)
                    continue
    
    def _ingest_json(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """Ingest JSON file (array of objects)."""
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
            if isinstance(data, list):
                for idx, row in enumerate(data, start=1):
                    try:
                        event = self._parse_row(row, file_path, idx)
                        if event:
                            yield event
                    except Exception as e:
                        print(f"Parse error at index {idx}: {e}", flush=True)
                        continue
            else:
                # Single object
                try:
                    event = self._parse_row(data, file_path, 1)
                    if event:
                        yield event
                except Exception as e:
                    print(f"Parse error: {e}", flush=True)
    
    def _ingest_jsonl(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """Ingest JSONL file (one JSON object per line)."""
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    row = json.loads(line)
                    event = self._parse_row(row, file_path, line_num)
                    if event:
                        yield event
                except Exception as e:
                    print(f"Parse error at line {line_num}: {e}", flush=True)
                    continue
    
    def _parse_row(self, row: Dict[str, Any], file_path: str, row_num: int) -> Optional[Dict[str, Any]]:
        """
        Parse a single row into canonical event.
        
        Args:
            row: Raw row dictionary
            file_path: Source file path
            row_num: Row number (for lineage)
        
        Returns:
            Canonical event dictionary or None if row should be skipped
        """
        # Extract fields using mapping
        event_time = self._extract_timestamp(row)
        if not event_time:
            return None  # Skip rows without timestamp
        
        # Extract bytes
        bytes_sent = self._extract_bytes_sent(row)
        bytes_received = self._extract_bytes_received(row)
        
        # Extract URL
        url_full, dest_host = self._extract_url(row)
        if not dest_host:
            return None  # Skip rows without destination
        
        # Extract domain (eTLD+1)
        dest_domain = self._extract_domain(dest_host)
        
        # Extract identity
        user_id = self._extract_user_id(row)
        if not user_id:
            return None  # Skip rows without user
        
        user_dept = self._extract_user_dept(row)
        device_id = self._extract_device_id(row)
        src_ip = self._extract_src_ip(row)
        
        # Extract action
        action = self._extract_action(row)
        
        # Extract other fields
        http_method = self._extract_method(row)
        status_code = self._extract_status_code(row)
        app_name = self._extract_app_name(row)
        app_category = self._extract_category(row)
        content_type = self._extract_content_type(row)
        user_agent = self._extract_user_agent(row)
        raw_event_id = self._extract_raw_event_id(row)
        
        # Parse URL components
        url_path, url_query = self._parse_url_components(url_full)
        
        # Build canonical event
        canonical = {
            "event_time": event_time.isoformat() if isinstance(event_time, datetime) else str(event_time),
            "vendor": self.vendor,
            "log_type": self.mapping.get("event_type", "unknown"),
            "user_id": user_id,
            "user_dept": user_dept,
            "device_id": device_id,
            "src_ip": src_ip,
            "dest_host": dest_host.lower(),
            "dest_domain": dest_domain,
            "url_full": url_full,
            "url_path": url_path,
            "url_query": url_query,
            "http_method": http_method,
            "status_code": status_code,
            "action": action,
            "app_name": app_name,
            "app_category": app_category,
            "bytes_sent": bytes_sent,
            "bytes_received": bytes_received,
            "content_type": content_type,
            "user_agent": user_agent,
            "raw_event_id": raw_event_id,
            "ingest_file": Path(file_path).name,
            "ingest_lineage_hash": self._compute_lineage_hash(row, file_path, row_num)
        }
        
        return canonical
    
    def _extract_timestamp(self, row: Dict[str, Any]) -> Optional[datetime]:
        """Extract event timestamp."""
        timestamp_config = self.mapping.get("timestamp", {})
        candidates = timestamp_config.get("candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = row[candidate]
                if not value:
                    continue
                
                # Try to parse
                try:
                    # Try ISO8601 first
                    return date_parser.parse(str(value))
                except:
                    try:
                        # Try epoch
                        if isinstance(value, (int, float)):
                            return datetime.fromtimestamp(float(value))
                    except:
                        pass
        
        return None
    
    def _extract_bytes_sent(self, row: Dict[str, Any]) -> int:
        """Extract bytes_sent (upload equivalent)."""
        bytes_config = self.mapping.get("bytes", {})
        candidates = bytes_config.get("sent_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                try:
                    value = int(row[candidate])
                    if value >= 0:
                        return value
                except:
                    pass
        
        return 0
    
    def _extract_bytes_received(self, row: Dict[str, Any]) -> int:
        """Extract bytes_received (download equivalent)."""
        bytes_config = self.mapping.get("bytes", {})
        candidates = bytes_config.get("recv_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                try:
                    value = int(row[candidate])
                    if value >= 0:
                        return value
                except:
                    pass
        
        return 0
    
    def _extract_url(self, row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """Extract URL and host."""
        url_config = self.mapping.get("url", {})
        candidates = url_config.get("full_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                url = str(row[candidate]).strip()
                if url:
                    # Extract host from URL
                    host = self._extract_host_from_url(url)
                    return url, host
        
        return None, None
    
    def _extract_host_from_url(self, url: str) -> Optional[str]:
        """Extract hostname from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url if '://' in url else f"https://{url}")
            return parsed.netloc or parsed.path.split('/', 1)[0] if parsed.path else None
        except:
            return None
    
    def _extract_domain(self, host: str) -> str:
        """Extract eTLD+1 domain from hostname."""
        try:
            extracted = self.domain_extractor(host)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return host
        except:
            return host
    
    def _extract_user_id(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract user ID."""
        identity_config = self.mapping.get("identity", {})
        candidates = identity_config.get("user_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_user_dept(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract user department."""
        identity_config = self.mapping.get("identity", {})
        candidates = identity_config.get("dept_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_device_id(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract device ID."""
        identity_config = self.mapping.get("identity", {})
        candidates = identity_config.get("device_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_src_ip(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract source IP."""
        identity_config = self.mapping.get("identity", {})
        candidates = identity_config.get("src_ip_candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_action(self, row: Dict[str, Any]) -> str:
        """Extract action (allow/block/etc)."""
        action_config = self.mapping.get("action", {})
        candidates = action_config.get("field_candidates", [])
        mapping = action_config.get("map", {})
        default = action_config.get("default", "unknown")
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip().lower()
                
                # Check mapping
                for mapped_key, mapped_values in mapping.items():
                    if value in [v.lower() for v in mapped_values]:
                        return mapped_key
        
        return default
    
    def _extract_method(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract HTTP method."""
        method_config = self.mapping.get("method", {})
        candidates = method_config.get("candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip().upper()
                if value:
                    return value
        
        return None
    
    def _extract_status_code(self, row: Dict[str, Any]) -> Optional[int]:
        """Extract HTTP status code."""
        status_config = self.mapping.get("status", {})
        candidates = status_config.get("candidates", ["status_code", "status", "http_status"])
        
        for candidate in candidates:
            if candidate in row:
                try:
                    value = int(row[candidate])
                    if 100 <= value <= 599:
                        return value
                except:
                    pass
        
        return None
    
    def _extract_app_name(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract application name."""
        app_config = self.mapping.get("app", {})
        candidates = app_config.get("candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_category(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract category."""
        category_config = self.mapping.get("category", {})
        candidates = category_config.get("candidates", [])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_content_type(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract content type."""
        content_type_config = self.mapping.get("content_type", {})
        candidates = content_type_config.get("candidates", ["content_type", "content-type"])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_user_agent(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract user agent."""
        ua_config = self.mapping.get("user_agent", {})
        candidates = ua_config.get("candidates", ["user_agent", "user-agent", "ua"])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _extract_raw_event_id(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract raw event ID if available."""
        id_config = self.mapping.get("raw_event_id", {})
        candidates = id_config.get("candidates", ["event_id", "id", "log_id"])
        
        for candidate in candidates:
            if candidate in row:
                value = str(row[candidate]).strip()
                if value:
                    return value
        
        return None
    
    def _parse_url_components(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse URL into path and query components."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url if '://' in url else f"https://{url}")
            return parsed.path or None, parsed.query or None
        except:
            return None, None
    
    def _compute_lineage_hash(self, row: Dict[str, Any], file_path: str, row_num: int) -> str:
        """
        Compute lineage hash for idempotency.
        
        Args:
            row: Raw row dictionary
            file_path: Source file path
            row_num: Row number
        
        Returns:
            sha256 hex digest
        """
        # Create canonical representation
        canonical_repr = json.dumps({
            "file": file_path,
            "row": row_num,
            "data": sorted(row.items())
        }, sort_keys=True)
        
        return hashlib.sha256(canonical_repr.encode('utf-8')).hexdigest()


# Example usage:
# if __name__ == "__main__":
#     ingestor = BaseIngestor("paloalto")
#     
#     for event in ingestor.ingest_file("sample_logs/paloalto_sample.csv"):
#         print(json.dumps(event, indent=2))
#         break  # Just show first event
