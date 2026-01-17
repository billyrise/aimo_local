"""
DuckDB Client for AIMO Analysis Engine

Provides single-writer connection management and UPSERT helpers.
All DuckDB writes must go through this client to ensure single-writer semantics.
"""

import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
from queue import Queue, Empty
import duckdb
from datetime import datetime


class DuckDBClient:
    """
    DuckDB client with single-writer guarantee.
    
    This class implements a Writer Queue pattern:
    - All writes are serialized through a single writer thread
    - Multiple readers can connect concurrently
    - UPSERT operations use ON CONFLICT DO UPDATE (INSERT OR REPLACE is prohibited)
    - Duplicate keys within same batch are deduplicated (last one wins)
    """
    
    def __init__(self, db_path: str, temp_directory: Optional[str] = None):
        """
        Initialize DuckDB client.
        
        Args:
            db_path: Path to DuckDB database file
            temp_directory: Optional temp directory for DuckDB (default: DBと同じディレクトリ配下)
                           Must be writable and on same filesystem as DB for performance.
        """
        self.db_path = Path(db_path).absolute()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # temp_directory規約: DBと同一の書込み可能領域（ローカルSSD）配下に固定
        if temp_directory is None:
            # デフォルト: DBと同じディレクトリ配下のduckdb_tmp
            temp_directory = str(self.db_path.parent / "duckdb_tmp")
        
        self.temp_directory = Path(temp_directory).absolute()
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        
        # ログ出力: temp_directoryのパスを明示
        print(f"DuckDB temp_directory: {self.temp_directory}", flush=True)
        
        # Writer connection (single thread)
        self._writer_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._writer_thread: Optional[threading.Thread] = None
        self._write_queue: Queue = Queue()
        self._writer_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        
        # Reader connections (can be multiple)
        self._reader_conns: List[duckdb.DuckDBPyConnection] = []
        self._reader_lock = threading.Lock()
        
        # Initialize database schema if needed
        self._init_schema()
    
    def _init_schema(self):
        """Initialize database schema from schema.sql"""
        schema_path = Path(__file__).parent.parent.parent / "src" / "db" / "schema.sql"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        # Use a temporary connection to initialize schema
        conn = duckdb.connect(str(self.db_path))
        try:
            # 起動時に必ずSET temp_directoryを実行（規約固定）
            conn.execute(f"SET temp_directory = '{self.temp_directory}'")
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            conn.execute(schema_sql)
            conn.commit()
        finally:
            conn.close()
    
    def get_reader(self) -> duckdb.DuckDBPyConnection:
        """
        Get a reader connection (can be called from any thread).
        
        Note: For tests, use the writer connection directly instead of read_only.
        Read-only connections are only for multi-process scenarios.
        
        Returns:
            DuckDB connection for read operations
        """
        # For single-process scenarios (like tests), use writer connection
        # Read-only connections can cause conflicts with temp_directory settings
        if self._writer_conn:
            return self._writer_conn
        
        # Fallback: create a regular connection (not read_only)
        conn = duckdb.connect(str(self.db_path))
        # 起動時に必ずSET temp_directoryを実行（規約固定）
        conn.execute(f"SET temp_directory = '{self.temp_directory}'")
        
        with self._reader_lock:
            self._reader_conns.append(conn)
        return conn
    
    def close_reader(self, conn: duckdb.DuckDBPyConnection):
        """Close a reader connection."""
        try:
            conn.close()
        except Exception:
            pass
        with self._reader_lock:
            if conn in self._reader_conns:
                self._reader_conns.remove(conn)
    
    def _start_writer(self):
        """Start the writer thread (internal)."""
        with self._writer_lock:
            if self._writer_thread is not None and self._writer_thread.is_alive():
                return
            
            self._writer_conn = duckdb.connect(str(self.db_path))
            
            # 起動時に必ずSET temp_directoryを実行（規約固定）
            self._writer_conn.execute(f"SET temp_directory = '{self.temp_directory}'")
            
            self._shutdown_event.clear()
            
            def writer_loop():
                """Writer thread main loop."""
                batch: List[Dict[str, Any]] = []
                batch_size = 50
                
                while not self._shutdown_event.is_set():
                    try:
                        # Get item from queue with timeout
                        item = self._write_queue.get(timeout=1.0)
                        batch.append(item)
                        
                        # Process batch when full or queue is empty
                        if len(batch) >= batch_size:
                            self._process_batch(batch)
                            batch = []
                    except Empty:
                        # Process remaining batch if any
                        if batch:
                            self._process_batch(batch)
                            batch = []
                    except Exception as e:
                        # Log error and continue
                        print(f"Writer error: {e}", flush=True)
                        batch = []
            
            self._writer_thread = threading.Thread(target=writer_loop, daemon=True)
            self._writer_thread.start()
    
    def _process_batch(self, batch: List[Dict[str, Any]]):
        """
        Process a batch of write operations (internal).
        
        ON CONFLICTを使うUPSERTは「同一バッチ内の重複キー」を必ず事前dedup。
        DuckDBは同一コマンド内で同一キーを二度更新しようとするとエラーになる場合があるため。
        """
        if not self._writer_conn:
            return
        
        # 同一バッチ内の重複キーを事前dedup（最後の1件だけ残す）
        deduplicated_batch = self._deduplicate_batch(batch)
        
        try:
            for item in deduplicated_batch:
                op_type = item.get("op")
                if op_type == "upsert":
                    self._execute_upsert(
                        table=item["table"],
                        data=item["data"],
                        conflict_key=item.get("conflict_key"),
                        update_columns=item.get("update_columns")
                    )
                elif op_type == "insert":
                    self._execute_insert(
                        table=item["table"],
                        data=item["data"]
                    )
                elif op_type == "update":
                    self._execute_update(
                        table=item["table"],
                        data=item["data"],
                        where_clause=item["where_clause"]
                    )
            
            self._writer_conn.commit()
        except Exception as e:
            print(f"Batch processing error: {e}", flush=True)
            try:
                self._writer_conn.rollback()
            except Exception:
                # Ignore rollback errors (transaction may already be committed or not started)
                pass
            raise
    
    def _deduplicate_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate batch by primary key (keep last occurrence).
        
        ON CONFLICTを使うUPSERTは同一バッチ内で同一キーを複数回更新しようとすると
        エラーになる場合があるため、primary_keyごとに最後の1件だけ残す。
        """
        # UPSERT操作のみを対象にdedup
        upsert_items: Dict[str, Dict[str, Any]] = {}  # key: (table, pk_value), value: item
        
        for item in batch:
            if item.get("op") == "upsert":
                table = item["table"]
                data = item["data"]
                conflict_key = item.get("conflict_key")
                
                # conflict_keyを決定
                if not conflict_key:
                    if "run_id" in data:
                        conflict_key = "run_id"
                    elif "url_signature" in data:
                        conflict_key = "url_signature"
                    elif "file_id" in data:
                        conflict_key = "file_id"
                    else:
                        # conflict_keyが不明な場合はdedupしない
                        continue
                
                # 複合PKの処理
                # signature_statsは常に複合PK (run_id, url_signature)
                if table == "signature_stats":
                    # 複合PK: (run_id, url_signature)
                    pk_value = (table, data.get("run_id"), data.get("url_signature"))
                else:
                    # 単一PK
                    pk_value = (table, data.get(conflict_key))
                
                # 最後の1件だけ残す（上書き）
                upsert_items[pk_value] = item
        
        # dedupされたUPSERT + その他の操作（insert, update）を結合
        result = []
        upsert_seen = set()
        
        for item in batch:
            if item.get("op") == "upsert":
                table = item["table"]
                data = item["data"]
                conflict_key = item.get("conflict_key")
                
                if not conflict_key:
                    if "run_id" in data:
                        conflict_key = "run_id"
                    elif "url_signature" in data:
                        conflict_key = "url_signature"
                    elif "file_id" in data:
                        conflict_key = "file_id"
                    else:
                        result.append(item)  # conflict_key不明はそのまま
                        continue
                
                # signature_statsは常に複合PK (run_id, url_signature)
                if table == "signature_stats":
                    pk_value = (table, data.get("run_id"), data.get("url_signature"))
                else:
                    pk_value = (table, data.get(conflict_key))
                
                # 最後の1件だけ追加
                if pk_value not in upsert_seen:
                    result.append(upsert_items[pk_value])
                    upsert_seen.add(pk_value)
            else:
                # insert, updateはそのまま
                result.append(item)
        
        return result
    
    def _execute_upsert(self, table: str, data: Dict[str, Any], 
                       conflict_key: Optional[str] = None,
                       update_columns: Optional[List[str]] = None):
        """
        Execute UPSERT using ON CONFLICT DO UPDATE (INSERT OR REPLACE is prohibited).
        
        Args:
            table: Table name
            data: Dictionary of column: value
            conflict_key: Primary key column name (required)
            update_columns: Columns to update on conflict (if None, updates all non-PK columns)
        """
        if not self._writer_conn:
            raise RuntimeError("Writer connection not initialized")
        
        # Determine conflict key (primary key)
        if not conflict_key:
            # Try to infer from common PK names
            if "run_id" in data:
                conflict_key = "run_id"
            elif "url_signature" in data:
                conflict_key = "url_signature"
            elif "file_id" in data:
                conflict_key = "file_id"
            else:
                raise ValueError(f"conflict_key must be specified for table {table}")
        
        # Handle composite primary keys
        # For signature_stats, PK is (run_id, url_signature)
        # DuckDB's ON CONFLICT can handle composite keys
        if table == "signature_stats":
            # Always use composite key for signature_stats
            if conflict_key != "run_id, url_signature":
                conflict_key = "run_id, url_signature"
        
        # Build column list
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        
        # Determine update columns (all non-PK columns if not specified)
        # For composite PK, exclude all PK columns
        pk_columns = [col.strip() for col in conflict_key.split(",")]
        if update_columns is None:
            # Exclude all PK columns from update
            update_columns = [col for col in columns if col not in pk_columns]
        
        # Ensure PK columns are never in update_columns
        update_columns = [col for col in update_columns if col not in pk_columns]
        
        # DuckDB limitation: ON CONFLICT DO UPDATE SET cannot update columns
        # that are referenced by indexes. Exclude indexed columns from update.
        # Known indexed columns per table (from schema.sql):
        indexed_columns = {
            "analysis_cache": ["status", "usage_type", "updated_at", "is_human_verified"],  # All indexed columns
            "runs": ["status", "started_at"],  # idx_runs_status, idx_runs_started
        }
        
        if table in indexed_columns:
            # Exclude indexed columns from update_columns
            update_columns = [col for col in update_columns if col not in indexed_columns[table]]
        
        # INSERT OR REPLACEのフォールバックは全面禁止（例外なし）
        # 監査・整合性・来歴の観点でDELETE→INSERT相当が再発するため。
        # インデックス付きカラムは更新対象から除外済み。更新対象が空の場合は設計ミスとして扱う。
        if not update_columns:
            raise ValueError(
                f"Cannot UPSERT {table}: No updatable columns (all columns are PK or indexed). "
                f"Indexed columns are immutable by design. If you need to update indexed columns, "
                f"consider: (1) Make indexed columns immutable, (2) Use history table pattern, "
                f"or (3) Remove index from updatable columns."
            )
        
        # ON CONFLICT DO UPDATE (preserves PK, updates specified columns)
        # INSERT OR REPLACEは使用禁止
        values = [data.get(col) for col in columns]
        update_clause = ", ".join([f"{col} = ?" for col in update_columns])
        update_values = [data.get(col) for col in update_columns]
        
        sql = f"""
            INSERT INTO {table} ({column_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}
        """
        
        self._writer_conn.execute(sql, values + update_values)
    
    def _execute_insert(self, table: str, data: Dict[str, Any]):
        """Execute INSERT (internal)."""
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        values = [data.get(col) for col in columns]
        
        sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
        self._writer_conn.execute(sql, values)
    
    def _execute_update(self, table: str, data: Dict[str, Any], where_clause: str):
        """Execute UPDATE (internal)."""
        set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
        values = [data.get(col) for col in data.keys()]
        
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self._writer_conn.execute(sql, values)
    
    def upsert(self, table: str, data: Dict[str, Any],
               conflict_key: Optional[str] = None,
               update_columns: Optional[List[str]] = None):
        """
        Queue an UPSERT operation (non-blocking).
        
        Args:
            table: Table name
            data: Dictionary of column: value
            conflict_key: Primary key column name
            update_columns: Columns to update on conflict (None = replace entire row)
        """
        self._start_writer()
        
        self._write_queue.put({
            "op": "upsert",
            "table": table,
            "data": data,
            "conflict_key": conflict_key,
            "update_columns": update_columns
        })
    
    def insert(self, table: str, data: Dict[str, Any]):
        """
        Queue an INSERT operation (non-blocking).
        
        Args:
            table: Table name
            data: Dictionary of column: value
        """
        self._start_writer()
        
        self._write_queue.put({
            "op": "insert",
            "table": table,
            "data": data
        })
    
    def update(self, table: str, data: Dict[str, Any], where_clause: str):
        """
        Queue an UPDATE operation (non-blocking).
        
        Args:
            table: Table name
            data: Dictionary of column: value
            where_clause: WHERE clause (without "WHERE" keyword)
        """
        self._start_writer()
        
        self._write_queue.put({
            "op": "update",
            "table": table,
            "data": data,
            "where_clause": where_clause
        })
    
    def flush(self, timeout: float = 30.0):
        """
        Wait for all queued writes to complete.
        
        Args:
            timeout: Maximum time to wait (seconds)
        """
        import time
        start = time.time()
        
        while not self._write_queue.empty():
            if time.time() - start > timeout:
                raise TimeoutError("Flush timeout")
            time.sleep(0.1)
        
        # Wait a bit more to ensure batch processing completes
        # Writer thread processes batches with timeout, so we need to wait
        time.sleep(1.0)
    
    def close(self):
        """Close all connections and stop writer thread."""
        # Signal shutdown
        self._shutdown_event.set()
        
        # Wait for writer thread to finish
        if self._writer_thread and self._writer_thread.is_alive():
            self.flush()
            self._writer_thread.join(timeout=5.0)
        
        # Close writer connection
        if self._writer_conn:
            try:
                self._writer_conn.close()
            except Exception:
                pass
            self._writer_conn = None
        
        # Close reader connections
        with self._reader_lock:
            for conn in self._reader_conns:
                try:
                    conn.close()
                except Exception:
                    pass
            self._reader_conns.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Example usage:
# if __name__ == "__main__":
#     client = DuckDBClient("./data/cache/aimo.duckdb")
#     
#     # Queue UPSERT
#     client.upsert("analysis_cache", {
#         "url_signature": "abc123...",
#         "service_name": "Test Service",
#         "usage_type": "business",
#         "risk_level": "low",
#         "category": "Collaboration",
#         "confidence": 0.95,
#         "rationale_short": "Test",
#         "classification_source": "RULE",
#         "signature_version": "1.0",
#         "rule_version": "1",
#         "prompt_version": "1",
#         "status": "active",
#         "is_human_verified": False,
#         "analysis_date": datetime.utcnow().isoformat()
#     })
#     
#     # Flush and close
#     client.flush()
#     client.close()
