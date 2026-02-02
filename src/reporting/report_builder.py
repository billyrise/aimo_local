"""
Report Builder for AIMO Analysis Engine

Generates audit-ready JSON reports with all required metadata:
- Thresholds used
- Counts (A/B/C, events, signatures)
- Sample metadata (rate, method, seed)
- Rule coverage
- LLM coverage
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, TYPE_CHECKING
from datetime import datetime
import jsonschema

if TYPE_CHECKING:
    from db.duckdb_client import DuckDBClient


class ReportBuilder:
    """
    Builder for audit-ready analysis reports.
    
    All reports must include:
    - Thresholds used (for reproducibility)
    - Counts (for audit trail)
    - Sample metadata (for reproducibility)
    - Coverage metrics (rule/LLM)
    """
    
    def __init__(self, schema_path: Optional[str] = None):
        """
        Initialize report builder.
        
        Args:
            schema_path: Path to schemas/report_summary.schema.json (default: schemas/report_summary.schema.json)
        """
        if schema_path is None:
            schema_path = Path(__file__).parent.parent.parent / "schemas" / "report_summary.schema.json"
        
        self.schema_path = Path(schema_path)
        
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Report schema not found: {self.schema_path}")
        
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
    
    def build_report(self,
                     run_id: str,
                     run_key: str,
                     started_at: datetime,
                     finished_at: Optional[datetime],
                     input_file: str,
                     vendor: str,
                     thresholds_used: Dict[str, Any],
                     counts: Dict[str, Any],
                     sample: Dict[str, Any],
                     rule_coverage: Dict[str, int],
                     llm_coverage: Dict[str, Any],
                     signature_version: str,
                     rule_version: str,
                     prompt_version: str,
                     exclusions: Optional[Dict[str, Any]] = None,
                     code_version: Optional[str] = None,
                     input_manifest_hash: Optional[str] = None,
                     input_files_summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Build audit-ready report.
        
        Args:
            run_id: Short run identifier
            run_key: Deterministic run key
            started_at: Run start time
            finished_at: Run finish time (optional)
            input_file: Path to input file
            vendor: Vendor name
            thresholds_used: A/B/C thresholds dict
            counts: All counts dict
            sample: Sample metadata dict
            rule_coverage: Rule coverage dict
            llm_coverage: LLM coverage dict
            signature_version: Signature version
            rule_version: Rule version
            prompt_version: Prompt version
            exclusions: Exclusion criteria (optional)
        
        Returns:
            Report dict (validated against schema)
        """
        report = {
            "run_id": run_id,
            "run_key": run_key,
            "started_at": started_at.isoformat() if isinstance(started_at, datetime) else started_at,
            "finished_at": finished_at.isoformat() if finished_at and isinstance(finished_at, datetime) else (finished_at if finished_at else None),
            "input_file": input_file,
            "vendor": vendor,
            "thresholds_used": thresholds_used,
            "counts": counts,
            "sample": sample,
            "exclusions": exclusions or {},
            "rule_coverage": rule_coverage,
            "llm_coverage": llm_coverage,
            "signature_version": signature_version,
            "rule_version": rule_version,
            "prompt_version": prompt_version
        }
        
        # Phase 7-3: Add audit fields
        if code_version is not None:
            report["code_version"] = code_version
        if input_manifest_hash is not None:
            report["input_manifest_hash"] = input_manifest_hash
        if input_files_summary is not None:
            report["input_files_summary"] = input_files_summary
        
        # Validate against schema
        validator = jsonschema.Draft202012Validator(self.schema)
        errors = list(validator.iter_errors(report))
        if errors:
            error_messages = [f"{e.path}: {e.message}" for e in errors]
            raise ValueError(f"Report validation failed:\n" + "\n".join(error_messages))
        
        return report
    
    def save_report(self, report: Dict[str, Any], output_path: Path) -> None:
        """
        Save report to JSON file.
        
        Args:
            report: Report dict
            output_path: Path to output file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temp file first, then rename (atomic write)
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Atomic rename
        temp_path.replace(output_path)
    
    @staticmethod
    def compute_llm_coverage_from_db(db_reader: Any, run_id: str, unknown_count: int) -> Dict[str, Any]:
        """
        Compute LLM coverage metrics from database (audit-ready definition).
        
        This method provides the authoritative definition of LLM coverage metrics
        for audit purposes. All values are recalculated from the database state.
        
        Definitions (Phase 7-4: Audit-ready):
        - llm_analyzed_count: analysis_cache の status='active' かつ classification_source='LLM' の件数（署名単位）
        - needs_review_count: status='needs_review' の件数
        - failed_permanent_count: status='failed_permanent' の件数
        - cache_hit_rate: unknown候補に対して、LLM呼び出し無しでactiveが得られた割合
          (unknown候補 = unknown_count, cache_hit = analysis_cacheに既に存在してactiveな署名)
        
        Args:
            db_reader: DuckDB reader connection
            run_id: Run ID for filtering (optional, currently not used but kept for future)
            unknown_count: Number of unknown signatures (rule-classified以外)
        
        Returns:
            Dict with llm_coverage metrics
        """
        # llm_analyzed_count: analysis_cache の status='active' かつ classification_source='LLM' の件数
        llm_analyzed_result = db_reader.execute(
            """
            SELECT COUNT(*) 
            FROM analysis_cache 
            WHERE status = 'active' AND classification_source = 'LLM'
            """
        ).fetchone()
        llm_analyzed_count = int(llm_analyzed_result[0]) if llm_analyzed_result else 0
        
        # needs_review_count: status='needs_review' の件数
        needs_review_result = db_reader.execute(
            """
            SELECT COUNT(*) 
            FROM analysis_cache 
            WHERE status = 'needs_review'
            """
        ).fetchone()
        needs_review_count = int(needs_review_result[0]) if needs_review_result else 0
        
        # failed_permanent_count: status='failed_permanent' の件数
        failed_permanent_result = db_reader.execute(
            """
            SELECT COUNT(*) 
            FROM analysis_cache 
            WHERE status = 'failed_permanent'
            """
        ).fetchone()
        failed_permanent_count = int(failed_permanent_result[0]) if failed_permanent_result else 0
        
        # cache_hit_rate: unknown候補に対して、LLM呼び出し無しでactiveが得られた割合
        # 定義: unknown候補（unknown_count）のうち、analysis_cacheに既に存在してactiveな署名の割合
        # 計算: (analysis_cacheに既に存在してactiveな署名数) / unknown_count
        # 注意: これは「LLM呼び出し無しでactiveが得られた」という意味で、cache_hitの概念
        cache_hit_active_result = db_reader.execute(
            """
            SELECT COUNT(*) 
            FROM analysis_cache 
            WHERE status = 'active' AND classification_source IN ('LLM', 'RULE')
            """
        ).fetchone()
        cache_hit_active_count = int(cache_hit_active_result[0]) if cache_hit_active_result else 0
        
        # cache_hit_rate = (既存のactiveな署名数) / unknown_count
        # ただし、unknown_countが0の場合は0.0を返す
        cache_hit_rate = cache_hit_active_count / unknown_count if unknown_count > 0 else 0.0
        
        return {
            "llm_analyzed_count": llm_analyzed_count,
            "needs_review_count": needs_review_count,
            "failed_permanent_count": failed_permanent_count,
            "cache_hit_rate": cache_hit_rate
        }
    
    @staticmethod
    def compute_retry_summary_from_db(db_reader: Any, run_id: str) -> Dict[str, Any]:
        """
        Compute retry summary from database (run-level aggregation).
        
        This method aggregates retry metadata from analysis_cache for a given run.
        Currently, retry metadata is stored in-memory during LLM analysis,
        but this method provides a way to recompute from DB if needed.
        
        Note: Currently, retry_summary is computed during LLM analysis and passed
        to the report. This method is provided for future use or audit verification.
        
        Args:
            db_reader: DuckDB reader connection
            run_id: Run ID (currently not used, but kept for future filtering)
        
        Returns:
            Dict with retry_summary metrics (defaults to zeros if not available)
        """
        # For now, retry_summary is computed during LLM analysis.
        # This method returns default values as a placeholder for future DB-based aggregation.
        # TODO: If retry metadata is stored in DB in the future, implement aggregation here.
        return {
            "attempts": 0,
            "backoff_ms_total": 0,
            "last_error_code": None,
            "rate_limit_events": 0
        }