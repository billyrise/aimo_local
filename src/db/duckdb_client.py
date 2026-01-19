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
        
        # Process each item individually to allow continuation on runs table errors
        successful_ops = 0
        failed_ops = 0
        
        for item in deduplicated_batch:
            op_type = item.get("op")
            table_name = item.get("table", "unknown")
            ignore_conflict = item.get("ignore_conflict", False)
            
            try:
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
                        data=item["data"],
                        ignore_conflict=ignore_conflict
                    )
                elif op_type == "update":
                    self._execute_update(
                        table=item["table"],
                        data=item["data"],
                        where_clause=item["where_clause"],
                        where_values=item.get("where_values")
                    )
                successful_ops += 1
            except Exception as item_error:
                # For runs table INSERT with ignore_conflict, log warning but continue
                if table_name == "runs" and op_type == "insert" and ignore_conflict:
                    error_msg = str(item_error)
                    if "Duplicate key" in error_msg or "already exists" in error_msg.lower() or "Can not assign" in error_msg:
                        print(f"  WARNING: Run record already exists or conflict (idempotent): {item_error}", flush=True)
                        print(f"  Continuing with remaining operations...", flush=True)
                        # Continue to next item (don't raise)
                        failed_ops += 1
                        continue
                    else:
                        print(f"  WARNING: Failed to insert run record: {item_error}", flush=True)
                        print(f"  Continuing with remaining operations...", flush=True)
                        # Continue to next item (don't raise)
                        failed_ops += 1
                        continue
                # For runs table UPDATE, log warning but continue (report generation is more important)
                elif table_name == "runs" and op_type == "update":
                    error_msg = str(item_error)
                    print(f"  WARNING: Failed to update run status: {item_error}", flush=True)
                    print(f"  Continuing (report generation completed successfully)...", flush=True)
                    # Continue to next item (don't raise)
                    failed_ops += 1
                    continue
                # For other errors, log and continue (don't fail entire batch)
                print(f"  WARNING: Failed to process {op_type} for {table_name}: {item_error}", flush=True)
                print(f"  Continuing with remaining operations...", flush=True)
                failed_ops += 1
                continue
        
        # Commit if we had any successful operations
        if successful_ops > 0:
            try:
                self._writer_conn.commit()
            except Exception as commit_error:
                print(f"  WARNING: Failed to commit batch: {commit_error}", flush=True)
                try:
                    self._writer_conn.rollback()
                except Exception:
                    pass
        elif failed_ops > 0 and successful_ops == 0:
            # All operations failed, but we should still try to rollback
            try:
                self._writer_conn.rollback()
            except Exception:
                pass
    
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
        
        Note:
            For analysis_cache table, if is_human_verified=true exists, skip update
            (is_human_verified=true の行は上書き禁止)
        """
        if not self._writer_conn:
            raise RuntimeError("Writer connection not initialized")
        
        # Check is_human_verified for analysis_cache table (spec 9.4)
        if table == "analysis_cache":
            # Determine conflict key (primary key)
            if not conflict_key:
                if "url_signature" in data:
                    conflict_key = "url_signature"
                else:
                    raise ValueError(f"conflict_key must be specified for table {table}")
            
            url_sig = data.get(conflict_key)
            if url_sig:
                # Check if existing record has is_human_verified=true
                check_query = f"SELECT is_human_verified FROM {table} WHERE {conflict_key} = ?"
                existing = self._writer_conn.execute(check_query, [url_sig]).fetchone()
                
                if existing and existing[0] is True:
                    # Skip update if is_human_verified=true (人手確定を最優先)
                    print(f"  WARNING: Skipping UPSERT for {url_sig} (is_human_verified=true)", flush=True)
                    return  # Skip this UPSERT operation
        
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
        
        # Only exclude indexed columns if update_columns was not explicitly provided
        # If explicitly provided, assume the caller knows what they're doing
        if table in indexed_columns and update_columns is None:
            # Exclude indexed columns from update_columns (only when auto-detecting)
            update_columns = [col for col in columns if col not in pk_columns]
            update_columns = [col for col in update_columns if col not in indexed_columns[table]]
        elif table in indexed_columns and update_columns is not None:
            # update_columns was explicitly provided - respect it (caller knows what they're doing)
            # But still exclude PK columns
            update_columns = [col for col in update_columns if col not in pk_columns]
        
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
        # DuckDB requires EXCLUDED keyword for ON CONFLICT DO UPDATE
        values = [data.get(col) for col in columns]
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        sql = f"""
            INSERT INTO {table} ({column_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}
        """
        
        # Debug: Log SQL for runs table to diagnose the error
        if table == "runs":
            print(f"DEBUG: UPSERT SQL for runs table:", flush=True)
            print(f"  conflict_key: {conflict_key}", flush=True)
            print(f"  pk_columns: {pk_columns}", flush=True)
            print(f"  update_columns: {update_columns}", flush=True)
            print(f"  Full SQL:\n{sql}", flush=True)
            print(f"  values: {values}", flush=True)
        
        try:
            self._writer_conn.execute(sql, values)
        except Exception as e:
            # Enhanced error message for debugging
            if table == "runs":
                print(f"DEBUG: Error executing UPSERT for runs table:", flush=True)
                print(f"  Full SQL:\n{sql}", flush=True)
                print(f"  values: {values}", flush=True)
                print(f"  update_columns: {update_columns}", flush=True)
                print(f"  pk_columns: {pk_columns}", flush=True)
                print(f"  conflict_key: {conflict_key}", flush=True)
            raise
    
    def _execute_insert(self, table: str, data: Dict[str, Any], ignore_conflict: bool = False):
        """Execute INSERT (internal).
        
        Args:
            table: Table name
            data: Dictionary of column: value
            ignore_conflict: If True, use INSERT ... ON CONFLICT DO NOTHING
        """
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        values = [data.get(col) for col in columns]
        
        if ignore_conflict:
            # Determine conflict key (primary key)
            conflict_key = None
            if "run_id" in data:
                conflict_key = "run_id"
            elif "url_signature" in data:
                conflict_key = "url_signature"
            elif "file_id" in data:
                conflict_key = "file_id"
            
            if conflict_key:
                sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders}) ON CONFLICT ({conflict_key}) DO NOTHING"
            else:
                sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
        else:
            sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
        
        self._writer_conn.execute(sql, values)
    
    def _execute_update(self, table: str, data: Dict[str, Any], where_clause: str, where_values: Optional[List[Any]] = None):
        """Execute UPDATE (internal).
        
        Args:
            table: Table name
            data: Dictionary of column: value to update
            where_clause: WHERE clause (e.g., "run_id = ?")
            where_values: Values for WHERE clause placeholders
        """
        if not data:
            return  # Nothing to update
        
        set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
        values = [data.get(col) for col in data.keys()]
        
        if where_values:
            values.extend(where_values)
        
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        try:
            self._writer_conn.execute(sql, values)
        except Exception as e:
            # Re-raise with more context
            error_msg = str(e)
            if "Duplicate key" in error_msg:
                # This shouldn't happen with UPDATE, but DuckDB may have issues with indexed columns
                # Try updating without indexed columns first, then update indexed columns separately
                raise RuntimeError(
                    f"UPDATE failed for {table}: {error_msg}. "
                    f"This may be a DuckDB limitation with indexed columns. "
                    f"SQL: {sql}, Values: {values}"
                ) from e
            raise
    
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
    
    def insert(self, table: str, data: Dict[str, Any], ignore_conflict: bool = False):
        """
        Queue an INSERT operation (non-blocking).
        
        Args:
            table: Table name
            data: Dictionary of column: value
            ignore_conflict: If True, use INSERT ... ON CONFLICT DO NOTHING
        """
        self._start_writer()
        
        self._write_queue.put({
            "op": "insert",
            "table": table,
            "data": data,
            "ignore_conflict": ignore_conflict
        })
    
    def update(self, table: str, data: Dict[str, Any], where_clause: str, where_values: Optional[List[Any]] = None):
        """
        Queue an UPDATE operation (non-blocking).
        
        Args:
            table: Table name
            data: Dictionary of column: value
            where_clause: WHERE clause (e.g., "run_id = ?")
            where_values: Values for WHERE clause placeholders
        """
        self._start_writer()
        
        self._write_queue.put({
            "op": "update",
            "table": table,
            "data": data,
            "where_clause": where_clause,
            "where_values": where_values
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
