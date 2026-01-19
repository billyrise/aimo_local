"""
Parquet Writer for AIMO Analysis Engine

Writes canonical events to Parquet files in Hive partition format.
Implements atomic writes (.tmp -> rename) for crash safety.
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetWriter:
    """
    Writes canonical events to Parquet files in Hive partition format.
    
    Partition structure: data/processed/vendor=<v>/date=<YYYY-MM-DD>/...snappy.parquet
    """
    
    def __init__(self, base_dir: Path):
        """
        Initialize Parquet writer.
        
        Args:
            base_dir: Base directory for processed files (e.g., ./data/processed)
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def write_events(self,
                    events: List[Dict[str, Any]],
                    vendor: str,
                    run_id: str,
                    date_partition: Optional[str] = None) -> Path:
        """
        Write events to Parquet file in Hive partition format.
        
        Args:
            events: List of canonical event dictionaries
            vendor: Vendor name (e.g., "paloalto")
            run_id: Run ID for file naming
            date_partition: Date partition (YYYY-MM-DD). If None, extracted from events.
            
        Returns:
            Path to written Parquet file
        """
        if not events:
            raise ValueError("Cannot write empty event list")
        
        # Extract date partition from events if not provided
        if date_partition is None:
            date_partition = self._extract_date_partition(events)
        
        # Build partition path: vendor=<v>/date=<YYYY-MM-DD>
        partition_path = self.base_dir / f"vendor={vendor}" / f"date={date_partition}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Build output filename: run_<run_id>_<timestamp>.snappy.parquet
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"run_{run_id}_{timestamp}.snappy.parquet"
        output_path = partition_path / filename
        
        # Write to temporary file first (atomic write)
        tmp_path = output_path.with_suffix('.parquet.tmp')
        
        try:
            # Convert events to PyArrow Table
            table = self._events_to_table(events)
            
            # Write Parquet file with Snappy compression
            # Note: use_dictionary=False to avoid type conflicts when reading as dataset
            pq.write_table(
                table,
                str(tmp_path),
                compression='snappy',
                use_dictionary=False,  # Disable dictionary encoding to avoid type conflicts
                write_statistics=True,  # Enable statistics for partition pruning
                row_group_size=100000  # 100k rows per row group (good balance)
            )
            
            # Atomic rename: .tmp -> final file
            tmp_path.rename(output_path)
            
            return output_path
        
        except Exception as e:
            # Clean up temp file on error
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            raise RuntimeError(f"Failed to write Parquet file: {e}") from e
    
    def _extract_date_partition(self, events: List[Dict[str, Any]]) -> str:
        """
        Extract date partition from events.
        
        Uses the first event's event_time to determine the partition.
        If event_time is missing or invalid, uses current date.
        
        Args:
            events: List of canonical events
            
        Returns:
            Date partition string (YYYY-MM-DD)
        """
        if not events:
            # Fallback to current date
            return datetime.utcnow().strftime("%Y-%m-%d")
        
        # Try to extract date from first event
        first_event = events[0]
        event_time = first_event.get("event_time")
        
        if event_time:
            try:
                # Parse ISO-8601 datetime string
                if isinstance(event_time, str):
                    dt = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
                elif isinstance(event_time, datetime):
                    dt = event_time
                else:
                    raise ValueError(f"Unexpected event_time type: {type(event_time)}")
                
                # Return date partition (YYYY-MM-DD)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                # If parsing fails, fall back to current date
                pass
        
        # Fallback to current date
        return datetime.utcnow().strftime("%Y-%m-%d")
    
    def _events_to_table(self, events: List[Dict[str, Any]]) -> pa.Table:
        """
        Convert list of event dictionaries to PyArrow Table.
        
        Args:
            events: List of canonical event dictionaries
            
        Returns:
            PyArrow Table
        """
        # Define schema based on canonical_event.schema.json
        schema = pa.schema([
            # Required fields
            pa.field("event_time", pa.string()),  # ISO-8601 string
            pa.field("vendor", pa.string()),
            pa.field("log_type", pa.string()),
            pa.field("user_id", pa.string()),
            pa.field("dest_host", pa.string()),
            pa.field("dest_domain", pa.string()),
            pa.field("url_full", pa.string()),
            pa.field("action", pa.string()),
            pa.field("bytes_sent", pa.int64()),
            pa.field("bytes_received", pa.int64()),
            pa.field("ingest_file", pa.string()),
            pa.field("ingest_lineage_hash", pa.string()),
            
            # Optional fields
            pa.field("user_dept", pa.string()),
            pa.field("device_id", pa.string()),
            pa.field("src_ip", pa.string()),
            pa.field("url_path", pa.string()),
            pa.field("url_query", pa.string()),
            pa.field("http_method", pa.string()),
            pa.field("status_code", pa.int64()),  # Nullable
            pa.field("app_name", pa.string()),
            pa.field("app_category", pa.string()),
            pa.field("content_type", pa.string()),
            pa.field("user_agent", pa.string()),
            pa.field("raw_event_id", pa.string()),
        ])
        
        # Convert events to arrays
        arrays = []
        for field in schema:
            field_name = field.name
            values = []
            
            for event in events:
                value = event.get(field_name)
                
                # Handle None values
                if value is None:
                    values.append(None)
                elif field.type == pa.int64():
                    # Convert to int64
                    try:
                        values.append(int(value) if value is not None else None)
                    except (ValueError, TypeError):
                        values.append(None)
                else:
                    # Convert to string
                    values.append(str(value) if value is not None else None)
            
            # Create array with null handling
            if field.type == pa.int64():
                # For nullable int64, use list with None
                array = pa.array(values, type=pa.int64())
            else:
                # For string fields, use list with None
                array = pa.array(values, type=pa.string())
            
            arrays.append(array)
        
        # Create table from arrays
        table = pa.Table.from_arrays(arrays, schema=schema)
        
        return table
    
    def get_partition_path(self, vendor: str, date_partition: str) -> Path:
        """
        Get partition path for given vendor and date.
        
        Args:
            vendor: Vendor name
            date_partition: Date partition (YYYY-MM-DD)
            
        Returns:
            Partition directory path
        """
        return self.base_dir / f"vendor={vendor}" / f"date={date_partition}"
