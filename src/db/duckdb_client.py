"""
DuckDB Client for AIMO Analysis Engine

Provides single-writer connection management and UPSERT helpers.
All DuckDB writes must go through this client to ensure single-writer semantics.

UPSERT仕様（恒久対策として固定）:
- すべてのUPSERTで INSERT ... ON CONFLICT DO UPDATE SET ... を使用
- INSERT OR REPLACE は禁止（DELETE→INSERT相当のため監査・来歴が破壊される）
- UPDATE句の右辺は必ず EXCLUDED.<col> を使用
- 更新対象列（update_columns）から以下を強制除外:
  a) conflict_cols（衝突ターゲット列）
  b) PK列
  c) indexed_columns（インデックス付き列）
- 除外時はWARNログを出力
- 監査用にUPSERT情報をJSONログで記録

参考:
- DuckDB公式: https://duckdb.org/docs/sql/statements/insert#on-conflict-clause
- DuckDBインデックス制限: UPDATEがDELETE+INSERTに変換されるケースあり
"""

import os
import json
import threading
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from queue import Queue, Empty
import duckdb
from datetime import datetime


# Configure logger for UPSERT audit and is_human_verified protection
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# =============================================================================
# UPSERT許可リスト方式（恒久対策）
# 各テーブルごとに「更新して良い列」を明示的に定義
# これにより、誤って危険な列を更新しようとしても自動で弾く
# =============================================================================

# runs テーブル: 更新可能な列
RUNS_UPDATABLE_COLS: Set[str] = {
    "finished_at",
    "last_completed_stage",
    "total_events",
    "unique_signatures",
    "cache_hit_count",
    "llm_sent_count",
    "code_version",
    "psl_hash",
}

# signature_stats テーブル: 更新可能な列
SIGNATURE_STATS_UPDATABLE_COLS: Set[str] = {
    "norm_host",
    "norm_path_template",
    "dest_domain",
    "bytes_sent_bucket",
    "access_count",
    "unique_users",
    "bytes_sent_sum",
    "bytes_sent_max",
    "bytes_sent_p95",
    "bytes_received_sum",
    "burst_max_5min",
    "cumulative_user_domain_day_max",
    "candidate_flags",
    "sampled",
    # Legacy taxonomy columns (deprecated but kept for compatibility)
    "fs_uc_code",
    "dt_code",
    "ch_code",
    "rs_code",
    "ob_code",
    "ev_code",
    "taxonomy_version",
    # New 8-dimension taxonomy columns (v1.6+)
    "fs_code",
    "im_code",
    "uc_codes_json",
    "dt_codes_json",
    "ch_codes_json",
    "rs_codes_json",
    "ev_codes_json",
    "ob_codes_json",
    "taxonomy_schema_version",
    "first_seen",
    "last_seen",
}

# analysis_cache テーブル: 更新可能な列
# Note: status, usage_type, updated_at, is_human_verified はインデックス列なので除外
ANALYSIS_CACHE_UPDATABLE_COLS: Set[str] = {
    "service_name",
    "risk_level",
    "category",
    "confidence",
    "rationale_short",
    "classification_source",
    "signature_version",
    "rule_version",
    "prompt_version",
    "taxonomy_version",
    "model",
    # Legacy taxonomy columns (deprecated but kept for compatibility)
    "fs_uc_code",
    "dt_code",
    "ch_code",
    "rs_code",
    "ob_code",
    "ev_code",
    # New 8-dimension taxonomy columns (v1.6+)
    "fs_code",
    "im_code",
    "uc_codes_json",
    "dt_codes_json",
    "ch_codes_json",
    "rs_codes_json",
    "ev_codes_json",
    "ob_codes_json",
    "taxonomy_schema_version",
    "error_type",
    "error_reason",
    "retry_after",
    "failure_count",
    "last_error_at",
    "analysis_date",
    "created_at",
}

# input_files テーブル: 更新可能な列
INPUT_FILES_UPDATABLE_COLS: Set[str] = {
    "file_path",
    "file_size",
    "file_hash",
    "vendor",
    "log_type",
    "min_time",
    "max_time",
    "row_count",
    "parse_error_count",
    "ingested_at",
}

# テーブルごとの許可リストマップ
TABLE_UPDATABLE_COLS: Dict[str, Set[str]] = {
    "runs": RUNS_UPDATABLE_COLS,
    "signature_stats": SIGNATURE_STATS_UPDATABLE_COLS,
    "analysis_cache": ANALYSIS_CACHE_UPDATABLE_COLS,
    "input_files": INPUT_FILES_UPDATABLE_COLS,
}

# テーブルごとのインデックス列（更新禁止）
# DuckDB制限: ON CONFLICT DO UPDATE SET でインデックス列を更新するとエラー
TABLE_INDEXED_COLS: Dict[str, Set[str]] = {
    "analysis_cache": {"status", "usage_type", "updated_at", "is_human_verified"},
    "runs": {"status", "started_at"},
    "signature_stats": set(),  # run_id, url_signature は複合PKなので別管理
    "input_files": set(),
}

# テーブルごとのPK列
TABLE_PK_COLS: Dict[str, Set[str]] = {
    "runs": {"run_id"},
    "signature_stats": {"run_id", "url_signature"},  # 複合PK
    "analysis_cache": {"url_signature"},
    "input_files": {"file_id"},
}


class DuckDBClient:
    """
    DuckDB client with single-writer guarantee.
    
    This class implements a Writer Queue pattern:
    - All writes are serialized through a single writer thread
    - Multiple readers can connect concurrently
    - UPSERT operations use ON CONFLICT DO UPDATE (INSERT OR REPLACE is prohibited)
    - Duplicate keys within same batch are deduplicated (last one wins)
    - is_human_verified=true rows are never overwritten (P0: human verification protection)
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
        
        # temp_directory 設定ログ（監査・運用品質として固定）
        # ops/runbook.md と一致する形式で出力
        temp_dir_log = {
            "db_path": str(self.db_path),
            "temp_directory": str(self.temp_directory),
            "note": "DuckDB temp_directory is required for WAL and spill files"
        }
        logger.info(f"DuckDB initialized: {json.dumps(temp_dir_log)}")
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
        """Initialize database schema from schema.sql and apply migrations."""
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
            
            # Apply migrations (idempotent, safe to run on every init)
            self._apply_migrations(conn)
        finally:
            conn.close()
    
    def _apply_migrations(self, conn):
        """
        Apply database migrations.
        
        Migrations are idempotent: they check if changes are needed before applying.
        This ensures safe operation on every client initialization.
        """
        try:
            from db.migrations import apply_migrations, get_schema_version
            
            result = apply_migrations(conn)
            
            if result["applied"] > 0:
                logger.info(f"Applied {result['applied']} migration(s)")
                conn.commit()
            
            # Log current schema version
            schema_version = get_schema_version(conn)
            logger.debug(f"Database schema version: {schema_version}")
            
        except ImportError as e:
            # Migrations module not available (e.g., during testing)
            logger.debug(f"Migrations module not available: {e}")
        except Exception as e:
            # Log error but don't fail initialization
            logger.warning(f"Migration error (non-fatal): {e}")
    
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
                elif op_type == "execute_sql":
                    self._execute_sql(
                        sql=item["sql"],
                        params=item.get("params", [])
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
                # For execute_sql operations (UPDATE signature_stats, index operations, etc.)
                # Log warning but continue (some operations are non-critical)
                elif op_type == "execute_sql":
                    error_msg = str(item_error)
                    sql_preview = item.get("sql", "unknown")[:100]  # First 100 chars for logging
                    print(f"  WARNING: Failed to execute SQL: {item_error}", flush=True)
                    print(f"  SQL preview: {sql_preview}...", flush=True)
                    print(f"  Continuing with remaining operations...", flush=True)
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
        
        恒久対策として以下を仕様固定:
        - INSERT ... ON CONFLICT(<conflict_cols>) DO UPDATE SET ... を使用
        - UPDATE句の右辺は必ず EXCLUDED.<col> を使用（直接値埋め込み禁止）
        - 以下の列は強制除外:
          a) conflict_cols（衝突ターゲット列）
          b) PK列
          c) indexed_columns（インデックス付き列）
          d) 許可リスト外の列（TABLE_UPDATABLE_COLS）
        - 除外時はWARNログを出力
        - 監査用にUPSERT情報をJSONログで記録
        
        Args:
            table: Table name
            data: Dictionary of column: value
            conflict_key: Primary key column name (required)
            update_columns: Columns to update on conflict (if None, uses all updatable columns)
        
        Note:
            For analysis_cache table, if is_human_verified=true exists, skip update
            (is_human_verified=true の行は上書き禁止)
        """
        if not self._writer_conn:
            raise RuntimeError("Writer connection not initialized")
        
        # =========================================
        # Step 1: is_human_verified 保護チェック
        # =========================================
        if table == "analysis_cache":
            if not conflict_key:
                if "url_signature" in data:
                    conflict_key = "url_signature"
                else:
                    raise ValueError(f"conflict_key must be specified for table {table}")
            
            url_sig = data.get(conflict_key)
            if url_sig:
                check_query = f"SELECT is_human_verified, classification_source, service_name FROM {table} WHERE {conflict_key} = ?"
                existing = self._writer_conn.execute(check_query, [url_sig]).fetchone()
                
                if existing and existing[0] is True:
                    existing_source = existing[1] if len(existing) > 1 else "unknown"
                    existing_service = existing[2] if len(existing) > 2 else "unknown"
                    attempted_source = data.get("classification_source", "unknown")
                    attempted_service = data.get("service_name", "unknown")
                    
                    logger.warning(
                        f"Skipping UPSERT for url_signature={url_sig} "
                        f"(is_human_verified=true protection): "
                        f"existing=[source={existing_source}, service={existing_service}], "
                        f"attempted=[source={attempted_source}, service={attempted_service}]"
                    )
                    return  # Skip this UPSERT operation
        
        # =========================================
        # Step 2: conflict_key の決定
        # =========================================
        if not conflict_key:
            if table in TABLE_PK_COLS:
                pk_cols = TABLE_PK_COLS[table]
                if len(pk_cols) == 1:
                    conflict_key = next(iter(pk_cols))
                else:
                    conflict_key = ", ".join(sorted(pk_cols))
            elif "run_id" in data:
                conflict_key = "run_id"
            elif "url_signature" in data:
                conflict_key = "url_signature"
            elif "file_id" in data:
                conflict_key = "file_id"
            else:
                raise ValueError(f"conflict_key must be specified for table {table}")
        
        # signature_stats は常に複合PK
        if table == "signature_stats":
            conflict_key = "run_id, url_signature"
        
        # =========================================
        # Step 3: 各種除外列の計算
        # =========================================
        columns = list(data.keys())
        pk_columns = set(col.strip() for col in conflict_key.split(","))
        indexed_columns = TABLE_INDEXED_COLS.get(table, set())
        updatable_cols = TABLE_UPDATABLE_COLS.get(table, set())
        
        # 要求されたupdate_columnsを記録（監査用）
        requested_update_cols = set(update_columns) if update_columns else set(columns) - pk_columns
        
        # =========================================
        # Step 4: update_columns の決定（許可リスト方式）
        # =========================================
        if update_columns is None:
            # 自動検出: データ内の列から、許可リストに含まれるもののみ
            update_columns = [col for col in columns if col not in pk_columns]
        else:
            update_columns = list(update_columns)
        
        # 強制除外リスト
        excluded_cols: Dict[str, List[str]] = {
            "pk_columns": [],
            "indexed_columns": [],
            "not_in_allowlist": [],
        }
        
        applied_update_cols = []
        for col in update_columns:
            # a) PK列は除外
            if col in pk_columns:
                excluded_cols["pk_columns"].append(col)
                continue
            
            # b) インデックス列は除外
            if col in indexed_columns:
                excluded_cols["indexed_columns"].append(col)
                continue
            
            # c) 許可リストにない列は除外（テーブルが許可リストに定義されている場合のみ）
            if updatable_cols and col not in updatable_cols:
                excluded_cols["not_in_allowlist"].append(col)
                continue
            
            applied_update_cols.append(col)
        
        # =========================================
        # Step 5: 除外警告ログ
        # =========================================
        all_excluded = []
        for reason, cols in excluded_cols.items():
            if cols:
                all_excluded.extend(cols)
                logger.warning(
                    f"UPSERT {table}: Excluded columns from update ({reason}): {cols}"
                )
        
        # =========================================
        # Step 6: 更新列がない場合はエラー
        # =========================================
        if not applied_update_cols:
            raise ValueError(
                f"Cannot UPSERT {table}: No updatable columns after filtering. "
                f"Excluded: {all_excluded}. "
                f"PK columns: {pk_columns}. "
                f"Indexed columns: {indexed_columns}. "
                f"Allowed columns: {updatable_cols if updatable_cols else 'all (no allowlist)'}. "
                f"If you need to update excluded columns, review TABLE_UPDATABLE_COLS or TABLE_INDEXED_COLS."
            )
        
        # =========================================
        # Step 7: SQL構築（EXCLUDED を使用）
        # =========================================
        placeholders = ", ".join(["?" for _ in columns])
        column_list = ", ".join(columns)
        values = [data.get(col) for col in columns]
        
        # UPDATE句は必ず EXCLUDED.<col> を使用（直接値埋め込み禁止）
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in applied_update_cols])
        
        sql = f"""
            INSERT INTO {table} ({column_list})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}
        """
        
        # =========================================
        # Step 8: 監査ログ（JSON形式）
        # =========================================
        audit_log = {
            "table": table,
            "conflict_cols": list(pk_columns),
            "requested_update_cols": list(requested_update_cols),
            "applied_update_cols": applied_update_cols,
            "excluded_cols": {k: v for k, v in excluded_cols.items() if v},
            "row_count": 1,
        }
        logger.debug(f"UPSERT audit: {json.dumps(audit_log)}")
        
        # =========================================
        # Step 9: SQL実行
        # =========================================
        try:
            self._writer_conn.execute(sql, values)
        except Exception as e:
            # エラー時は詳細ログを出力（ただし値は除く：機密保護）
            error_log = {
                "table": table,
                "conflict_key": conflict_key,
                "applied_update_cols": applied_update_cols,
                "excluded_cols": {k: v for k, v in excluded_cols.items() if v},
                "error": str(e),
            }
            logger.error(f"UPSERT failed: {json.dumps(error_log)}")
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
    
    def execute_sql(self, sql: str, params: Optional[List[Any]] = None):
        """
        Queue a raw SQL execution (non-blocking, Writer Queue経由).
        
        P0: All DB writes must go through Writer Queue to prevent DB corruption
        in parallel execution scenarios.
        
        Args:
            sql: SQL statement to execute
            params: Optional list of parameters for parameterized query
        """
        self._start_writer()
        
        self._write_queue.put({
            "op": "execute_sql",
            "sql": sql,
            "params": params or []
        })
    
    def _execute_sql(self, sql: str, params: List[Any]):
        """
        Execute raw SQL statement (internal, Writer Queue経由).
        
        Args:
            sql: SQL statement
            params: Parameters for parameterized query
        """
        if not self._writer_conn:
            raise RuntimeError("Writer connection not initialized")
        
        try:
            if params:
                self._writer_conn.execute(sql, params)
            else:
                self._writer_conn.execute(sql)
        except Exception as e:
            # Re-raise with more context
            raise RuntimeError(
                f"SQL execution failed: {e}. "
                f"SQL: {sql}, Params: {params}"
            ) from e
    
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
