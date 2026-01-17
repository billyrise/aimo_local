"""
A/B/C Detector for AIMO Analysis Engine

Extracts three types of risk signals from canonical events:
- A: Single large transfer (bytes_sent >= threshold)
- B: Write method to AI/Unknown OR burst behavior OR cumulative daily transfer
- C: Deterministic sample (2%) from B candidates with bytes_sent < 1MB

All operations are deterministic and idempotent, using UTC timestamps (RFC 3339).
C sampling uses deterministic hash (seed=run_id) for reproducibility.

Required aggregations:
- Cumulative: user_id × dest_domain × day → sum(bytes_sent)
- Burst: user_id × dest_domain × 5min window → count(write_methods)
"""

import yaml
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
from collections import defaultdict
import warnings


class ABCDetector:
    """
    Detector for A/B/C risk signals.
    
    All timestamps are in UTC (RFC 3339 format).
    Day boundaries are UTC 00:00:00 (no DST ambiguity).
    5-minute windows are aligned to UTC boundaries (floor_to_5min).
    """
    
    def __init__(self, 
                 thresholds_path: Optional[str] = None,
                 run_id: Optional[str] = None,
                 action_filter: str = "allow"):
        """
        Initialize A/B/C detector.
        
        Args:
            thresholds_path: Path to thresholds.yaml (default: config/thresholds.yaml)
            run_id: Run ID for deterministic C sampling (required for C sampling)
            action_filter: Action to filter for A/B detection (default: "allow")
        """
        if thresholds_path is None:
            thresholds_path = Path(__file__).parent.parent.parent / "config" / "thresholds.yaml"
        
        with open(thresholds_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        candidates = self.config.get("candidates", {})
        method_groups = self.config.get("method_groups", {})
        
        # A: Single large transfer
        self.A_min_bytes_sent = candidates.get("A", {}).get("min_bytes_sent", 1048576)  # 1MB default
        
        # B: Burst behavior
        burst_config = candidates.get("B", {}).get("burst", {})
        self.B_window_seconds = burst_config.get("window_seconds", 300)  # 5 minutes default
        self.B_burst_count = burst_config.get("min_count", 20)  # 20 events default
        
        # B: Cumulative daily transfer
        cumulative_config = candidates.get("B", {}).get("cumulative", {})
        self.B_cumulative_bytes = cumulative_config.get("min_bytes_sent", 20971520)  # 20MB default
        
        # B: High-risk categories
        self.B_high_risk_categories = candidates.get("B", {}).get("high_risk_categories", ["GenAI", "AI", "Unknown", "Uncategorized"])
        
        # Write methods for B detection
        self.write_methods = set(candidates.get("B", {}).get("write_methods", ["POST", "PUT", "PATCH"]))
        
        # C: Sample rate
        c_config = candidates.get("C", {})
        self.C_sample_rate = c_config.get("sample_rate", 0.02)  # 2% default
        
        # Run ID for deterministic sampling
        self.run_id = run_id or "default_run"
        
        self.action_filter = action_filter
    
    def detect(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Detect A/B/C signals from canonical events.
        
        Args:
            events: List of canonical events (must have event_time, user_id, dest_domain, 
                   bytes_sent, http_method, app_category, action, url_signature, ingest_lineage_hash)
        
        Returns:
            Dictionary with:
            - 'event_flags': List of dicts with event-level flags (candidate_flags)
            - 'signals': Dict with 'A', 'B', 'C' lists of detected signals
            - 'metadata': Audit metadata (counts, thresholds, sample info)
        """
        # Validate and normalize events
        normalized_events = self._normalize_events(events)
        
        if not normalized_events:
            return {
                "event_flags": [],
                "signals": {"A": [], "B": [], "C": []},
                "metadata": self._create_empty_metadata()
            }
        
        # Sort by timestamp for deterministic processing
        normalized_events.sort(key=lambda e: (
            e["ts"],
            e.get("user_id", ""),
            e.get("dest_domain", ""),
            e.get("url_signature", ""),
            e.get("ingest_lineage_hash", "")
        ))
        
        # Step 1: Compute required aggregations
        # Cumulative: user_id × dest_domain × day → sum(bytes_sent)
        cumulative_agg = self._compute_cumulative(normalized_events)
        
        # Burst: user_id × dest_domain × 5min window → count(write_methods)
        burst_agg = self._compute_burst(normalized_events)
        
        # Step 2: Detect A signals (bytes_sent >= 1MB)
        a_events = self._detect_A(normalized_events)
        
        # Step 3: Detect B candidates
        # B: write_method AND (AI/Unknown OR burst>=20 OR cumulative>=20MB)
        b_candidates = self._detect_B_candidates(normalized_events, cumulative_agg, burst_agg)
        
        # Step 4: Detect C signals (deterministic sample from B candidates with bytes_sent < 1MB)
        c_events = self._detect_C(b_candidates, normalized_events)
        
        # Step 5: Build event-level flags
        event_flags = self._build_event_flags(normalized_events, a_events, b_candidates, c_events, cumulative_agg, burst_agg)
        
        # Step 6: Build signals (for backward compatibility and reporting)
        signals = {
            "A": self._build_A_signals(a_events, normalized_events),
            "B": self._build_B_signals(b_candidates, normalized_events, cumulative_agg, burst_agg),
            "C": self._build_C_signals(c_events, normalized_events)
        }
        
        # Step 7: Build audit metadata
        metadata = self._build_metadata(a_events, b_candidates, c_events, len(normalized_events))
        
        return {
            "event_flags": event_flags,
            "signals": signals,
            "metadata": metadata
        }
    
    def _normalize_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize events: ensure ts is UTC datetime, validate required fields.
        
        Returns:
            List of normalized events with 'ts' field (datetime object)
        """
        normalized = []
        parse_warnings = []
        
        for event in events:
            # Extract timestamp
            ts_raw = event.get("event_time") or event.get("ts")
            if not ts_raw:
                parse_warnings.append("Missing timestamp in event")
                continue
            
            # Handle datetime objects directly
            if isinstance(ts_raw, datetime):
                ts = ts_raw
                # Convert to UTC naive datetime if timezone-aware
                if ts.tzinfo is not None:
                    from dateutil.tz import UTC
                    ts = ts.astimezone(UTC).replace(tzinfo=None)
                ts_str = ts.isoformat() + "Z"
            else:
                ts_str = str(ts_raw)
                try:
                    ts = self._parse_timestamp(ts_str)
                except Exception as e:
                    parse_warnings.append(f"Invalid timestamp format: {ts_str} ({e})")
                    continue
            
            # Validate required fields
            if not event.get("user_id") or not event.get("dest_domain"):
                continue  # Skip events without required aggregation keys
            
            # Create normalized event
            norm_event = {
                "ts": ts,
                "ts_str": ts_str,
                "user_id": event.get("user_id", ""),
                "dest_domain": event.get("dest_domain", ""),
                "url_signature": event.get("url_signature", ""),
                "action": event.get("action", ""),
                "http_method": event.get("http_method"),
                "app_category": event.get("app_category"),
                "bytes_sent": int(event.get("bytes_sent", 0)),
                "bytes_received": int(event.get("bytes_received", 0)),
                "ingest_lineage_hash": event.get("ingest_lineage_hash", "")
            }
            
            normalized.append(norm_event)
        
        if parse_warnings:
            warnings.warn(f"Parse warnings: {len(parse_warnings)} events had timestamp issues")
        
        return normalized
    
    def _parse_timestamp(self, ts_str: str) -> datetime:
        """
        Parse timestamp string to UTC datetime.
        
        Supports RFC 3339 / ISO 8601 formats.
        All timestamps are normalized to UTC (naive datetime in UTC).
        
        Args:
            ts_str: Timestamp string (RFC 3339 format expected)
        
        Returns:
            datetime object (naive, representing UTC)
        """
        from dateutil import parser as date_parser
        from dateutil.tz import UTC
        
        dt = date_parser.isoparse(ts_str)
        
        # Convert to UTC if timezone-aware
        if dt.tzinfo is not None:
            dt = dt.astimezone(UTC)
            dt = dt.replace(tzinfo=None)  # Make naive (UTC)
        
        return dt
    
    def _floor_to_5min(self, dt: datetime) -> datetime:
        """
        Floor timestamp to 5-minute boundary (UTC).
        
        Args:
            dt: UTC datetime
        
        Returns:
            Floored datetime (e.g., 10:23:45 → 10:20:00)
        """
        minutes = dt.minute
        floored_minutes = (minutes // 5) * 5
        return dt.replace(minute=floored_minutes, second=0, microsecond=0)
    
    def _floor_to_day(self, dt: datetime) -> datetime:
        """
        Floor timestamp to UTC day boundary (00:00:00).
        
        Args:
            dt: UTC datetime
        
        Returns:
            Floored datetime (e.g., 2024-01-15 10:23:45 → 2024-01-15 00:00:00)
        """
        return datetime(dt.year, dt.month, dt.day)
    
    def _compute_cumulative(self, events: List[Dict[str, Any]]) -> Dict[Tuple[str, str, datetime], int]:
        """
        Compute cumulative aggregation: user_id × dest_domain × day → sum(bytes_sent).
        
        Args:
            events: List of normalized events
        
        Returns:
            Dictionary mapping (user_id, dest_domain, day_start) → sum(bytes_sent)
        """
        cumulative = defaultdict(int)
        
        for event in events:
            if event["action"] != self.action_filter:
                continue
            
            user_id = event["user_id"]
            dest_domain = event["dest_domain"]
            day_start = self._floor_to_day(event["ts"])
            bytes_sent = event["bytes_sent"]
            
            key = (user_id, dest_domain, day_start)
            cumulative[key] += bytes_sent
        
        return dict(cumulative)
    
    def _compute_burst(self, events: List[Dict[str, Any]]) -> Dict[Tuple[str, str, datetime], int]:
        """
        Compute burst aggregation: user_id × dest_domain × 5min window → count(write_methods).
        
        Args:
            events: List of normalized events
        
        Returns:
            Dictionary mapping (user_id, dest_domain, window_start) → count(write_methods)
        """
        burst = defaultdict(int)
        
        for event in events:
            if event["action"] != self.action_filter:
                continue
            
            # Check if write method
            http_method = event.get("http_method")
            if not http_method or http_method.upper() not in self.write_methods:
                continue
            
            user_id = event["user_id"]
            dest_domain = event["dest_domain"]
            window_start = self._floor_to_5min(event["ts"])
            
            key = (user_id, dest_domain, window_start)
            burst[key] += 1
        
        return dict(burst)
    
    def _is_write_method(self, http_method: Optional[str]) -> bool:
        """Check if HTTP method is a write method."""
        if not http_method:
            return False
        return http_method.upper() in self.write_methods
    
    def _is_high_risk_category(self, app_category: Optional[str]) -> bool:
        """Check if app_category is high-risk (AI/Unknown)."""
        if not app_category:
            return False
        return app_category in self.B_high_risk_categories
    
    def _detect_A(self, events: List[Dict[str, Any]]) -> Set[str]:
        """
        Detect A signals: bytes_sent >= 1MB.
        
        Args:
            events: List of normalized events
        
        Returns:
            Set of ingest_lineage_hash values for A events
        """
        a_events = set()
        
        for event in events:
            if event["action"] != self.action_filter:
                continue
            
            if event["bytes_sent"] >= self.A_min_bytes_sent:
                lineage_hash = event.get("ingest_lineage_hash")
                if lineage_hash:
                    a_events.add(lineage_hash)
        
        return a_events
    
    def _detect_B_candidates(self, 
                             events: List[Dict[str, Any]],
                             cumulative_agg: Dict[Tuple[str, str, datetime], int],
                             burst_agg: Dict[Tuple[str, str, datetime], int]) -> Set[str]:
        """
        Detect B candidates: write_method AND (AI/Unknown OR burst>=20 OR cumulative>=20MB).
        
        Args:
            events: List of normalized events
            cumulative_agg: Cumulative aggregation results
            burst_agg: Burst aggregation results
        
        Returns:
            Set of ingest_lineage_hash values for B candidate events
        """
        b_candidates = set()
        
        for event in events:
            if event["action"] != self.action_filter:
                continue
            
            # Must be write method
            if not self._is_write_method(event.get("http_method")):
                continue
            
            # Check conditions: AI/Unknown OR burst>=20 OR cumulative>=20MB
            is_high_risk = self._is_high_risk_category(event.get("app_category"))
            
            user_id = event["user_id"]
            dest_domain = event["dest_domain"]
            ts = event["ts"]
            
            # Check burst
            window_start = self._floor_to_5min(ts)
            burst_key = (user_id, dest_domain, window_start)
            burst_count = burst_agg.get(burst_key, 0)
            is_burst = burst_count >= self.B_burst_count
            
            # Check cumulative
            day_start = self._floor_to_day(ts)
            cumulative_key = (user_id, dest_domain, day_start)
            cumulative_bytes = cumulative_agg.get(cumulative_key, 0)
            is_cumulative = cumulative_bytes >= self.B_cumulative_bytes
            
            # B candidate if any condition is met
            if is_high_risk or is_burst or is_cumulative:
                lineage_hash = event.get("ingest_lineage_hash")
                if lineage_hash:
                    b_candidates.add(lineage_hash)
        
        return b_candidates
    
    def _detect_C(self, 
                  b_candidates: Set[str],
                  events: List[Dict[str, Any]]) -> Set[str]:
        """
        Detect C signals: deterministic sample (2%) from B candidates with bytes_sent < 1MB.
        
        Uses deterministic hash sampling: hash = sha256(run_id + "|" + ingest_lineage_hash)
        Sample if (hash % 10000) < (sample_rate * 10000)
        
        Args:
            b_candidates: Set of ingest_lineage_hash values for B candidate events
            events: List of normalized events
        
        Returns:
            Set of ingest_lineage_hash values for C events
        """
        c_events = set()
        
        # Build map from lineage_hash to event
        event_map = {e.get("ingest_lineage_hash"): e for e in events if e.get("ingest_lineage_hash")}
        
        for lineage_hash in b_candidates:
            event = event_map.get(lineage_hash)
            if not event:
                continue
            
            # C applies only to B candidates with bytes_sent < 1MB
            if event["bytes_sent"] >= self.A_min_bytes_sent:
                continue
            
            # Deterministic hash sampling
            sample_input = f"{self.run_id}|{lineage_hash}"
            hash_bytes = hashlib.sha256(sample_input.encode('utf-8')).digest()
            # Convert first 8 bytes to integer
            hash_int = int.from_bytes(hash_bytes[:8], byteorder='big')
            # Sample if hash % 10000 < sample_rate * 10000
            if (hash_int % 10000) < int(self.C_sample_rate * 10000):
                c_events.add(lineage_hash)
        
        return c_events
    
    def _build_event_flags(self,
                           events: List[Dict[str, Any]],
                           a_events: Set[str],
                           b_candidates: Set[str],
                           c_events: Set[str],
                           cumulative_agg: Dict[Tuple[str, str, datetime], int],
                           burst_agg: Dict[Tuple[str, str, datetime], int]) -> List[Dict[str, Any]]:
        """
        Build event-level flags (candidate_flags).
        
        Flags format: "A|B|C|burst|cumulative|sampled" (pipe-separated)
        
        Args:
            events: List of normalized events
            a_events: Set of A event lineage hashes
            b_candidates: Set of B candidate lineage hashes
            c_events: Set of C event lineage hashes
            cumulative_agg: Cumulative aggregation results
            burst_agg: Burst aggregation results
        
        Returns:
            List of dicts with event flags
        """
        event_flags = []
        
        for event in events:
            flags = []
            lineage_hash = event.get("ingest_lineage_hash", "")
            
            # A flag
            if lineage_hash in a_events:
                flags.append("A")
            
            # B flag
            if lineage_hash in b_candidates:
                flags.append("B")
            
            # C flag
            if lineage_hash in c_events:
                flags.append("C")
            
            # Burst flag (if event contributed to burst)
            user_id = event["user_id"]
            dest_domain = event["dest_domain"]
            ts = event["ts"]
            window_start = self._floor_to_5min(ts)
            burst_key = (user_id, dest_domain, window_start)
            if burst_agg.get(burst_key, 0) >= self.B_burst_count:
                flags.append("burst")
            
            # Cumulative flag (if event contributed to cumulative)
            day_start = self._floor_to_day(ts)
            cumulative_key = (user_id, dest_domain, day_start)
            if cumulative_agg.get(cumulative_key, 0) >= self.B_cumulative_bytes:
                flags.append("cumulative")
            
            # Sampled flag (for C events)
            if lineage_hash in c_events:
                flags.append("sampled")
            
            candidate_flags = "|".join(flags) if flags else None
            
            event_flags.append({
                "ingest_lineage_hash": lineage_hash,
                "candidate_flags": candidate_flags,
                "user_id": user_id,
                "dest_domain": dest_domain,
                "event_time": event["ts_str"],
                "bytes_sent": event["bytes_sent"]
            })
        
        return event_flags
    
    def _build_A_signals(self, a_events: Set[str], events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build A signals for reporting."""
        signals = []
        event_map = {e.get("ingest_lineage_hash"): e for e in events if e.get("ingest_lineage_hash")}
        
        for lineage_hash in a_events:
            event = event_map.get(lineage_hash)
            if not event:
                continue
            
            signals.append({
                "ingest_lineage_hash": lineage_hash,
                "user_id": event["user_id"],
                "dest_domain": event["dest_domain"],
                "url_signature": event.get("url_signature", ""),
                "event_time": event["ts_str"],
                "bytes_sent": event["bytes_sent"]
            })
        
        return signals
    
    def _build_B_signals(self,
                         b_candidates: Set[str],
                         events: List[Dict[str, Any]],
                         cumulative_agg: Dict[Tuple[str, str, datetime], int],
                         burst_agg: Dict[Tuple[str, str, datetime], int]) -> List[Dict[str, Any]]:
        """Build B signals for reporting."""
        signals = []
        event_map = {e.get("ingest_lineage_hash"): e for e in events if e.get("ingest_lineage_hash")}
        
        for lineage_hash in b_candidates:
            event = event_map.get(lineage_hash)
            if not event:
                continue
            
            user_id = event["user_id"]
            dest_domain = event["dest_domain"]
            ts = event["ts"]
            
            # Determine which condition triggered B
            is_high_risk = self._is_high_risk_category(event.get("app_category"))
            window_start = self._floor_to_5min(ts)
            burst_key = (user_id, dest_domain, window_start)
            burst_count = burst_agg.get(burst_key, 0)
            day_start = self._floor_to_day(ts)
            cumulative_key = (user_id, dest_domain, day_start)
            cumulative_bytes = cumulative_agg.get(cumulative_key, 0)
            
            signals.append({
                "ingest_lineage_hash": lineage_hash,
                "user_id": user_id,
                "dest_domain": dest_domain,
                "url_signature": event.get("url_signature", ""),
                "event_time": event["ts_str"],
                "bytes_sent": event["bytes_sent"],
                "trigger": {
                    "high_risk_category": is_high_risk,
                    "burst_count": burst_count,
                    "cumulative_bytes": cumulative_bytes
                }
            })
        
        return signals
    
    def _build_C_signals(self, c_events: Set[str], events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build C signals for reporting."""
        signals = []
        event_map = {e.get("ingest_lineage_hash"): e for e in events if e.get("ingest_lineage_hash")}
        
        for lineage_hash in c_events:
            event = event_map.get(lineage_hash)
            if not event:
                continue
            
            signals.append({
                "ingest_lineage_hash": lineage_hash,
                "user_id": event["user_id"],
                "dest_domain": event["dest_domain"],
                "url_signature": event.get("url_signature", ""),
                "event_time": event["ts_str"],
                "bytes_sent": event["bytes_sent"]
            })
        
        return signals
    
    def _build_metadata(self,
                       a_events: Set[str],
                       b_candidates: Set[str],
                       c_events: Set[str],
                       total_events: int) -> Dict[str, Any]:
        """
        Build audit metadata.
        
        Returns:
            Dictionary with counts, thresholds, sample info
        """
        # Count B candidates that hit burst/cumulative thresholds
        # (This is approximate - full count would require re-aggregation)
        burst_hit_count = 0  # Will be computed from signals
        cumulative_hit_count = 0  # Will be computed from signals
        
        return {
            "thresholds_used": {
                "A_min_bytes": self.A_min_bytes_sent,
                "B_burst_count": self.B_burst_count,
                "B_burst_window_seconds": self.B_window_seconds,
                "B_cumulative_bytes": self.B_cumulative_bytes,
                "B_high_risk_categories": self.B_high_risk_categories,
                "B_write_methods": list(self.write_methods),
                "C_sample_rate": self.C_sample_rate
            },
            "counts": {
                "A_count": len(a_events),
                "B_count": len(b_candidates),
                "C_count": len(c_events),
                "total_events": total_events
            },
            "sample": {
                "sample_rate": self.C_sample_rate,
                "sample_method": "deterministic_hash",
                "seed": self.run_id
            },
            "exclusions": []  # No exclusions implemented yet
        }
    
    def _create_empty_metadata(self) -> Dict[str, Any]:
        """Create empty metadata structure."""
        return {
            "thresholds_used": {
                "A_min_bytes": self.A_min_bytes_sent,
                "B_burst_count": self.B_burst_count,
                "B_burst_window_seconds": self.B_window_seconds,
                "B_cumulative_bytes": self.B_cumulative_bytes,
                "B_high_risk_categories": self.B_high_risk_categories,
                "B_write_methods": list(self.write_methods),
                "C_sample_rate": self.C_sample_rate
            },
            "counts": {
                "A_count": 0,
                "B_count": 0,
                "C_count": 0,
                "total_events": 0
            },
            "sample": {
                "sample_rate": self.C_sample_rate,
                "sample_method": "deterministic_hash",
                "seed": self.run_id
            },
            "exclusions": []
        }
