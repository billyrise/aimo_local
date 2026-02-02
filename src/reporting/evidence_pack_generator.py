"""
Evidence Pack Generator for AIMO Analysis Engine

LEGACY COMPATIBILITY MODULE
===========================
This module generates legacy Evidence Pack outputs (summary.json/xlsx).
For AIMO Standard v0.1.7+ compliant Evidence Bundles, use:
    from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator

This generator is now used as a derived output generator within the
StandardEvidenceBundleGenerator, placing outputs in the derived/ subdirectory.

Generates Evidence Pack output (legacy format):
- evidence_pack_summary.json (機械可読)
- evidence_pack_summary.xlsx or csv (人間可読)

All outputs include taxonomy codes and version information.
"""

import json
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import xlsxwriter


class EvidencePackGenerator:
    """
    Generator for Evidence Pack output (Taxonomyセット対応).
    
    Features:
    - Generates evidence_pack_summary.json (machine-readable)
    - Generates evidence_pack_summary.xlsx or csv (human-readable)
    - All outputs MUST include 7 codes + taxonomy_version (列欠落禁止)
    """
    
    def __init__(self, output_dir: Path):
        """
        Initialize Evidence Pack generator.
        
        Args:
            output_dir: Output directory (data/output/<run_id>/evidence_pack/)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_evidence_pack(self,
                               run_id: str,
                               db_reader,
                               taxonomy_version: str,
                               evidence_pack_version: str,
                               engine_spec_version: str) -> Dict[str, Path]:
        """
        Generate Evidence Pack output files.
        
        Args:
            run_id: Run identifier
            db_reader: DuckDB reader connection
            taxonomy_version: Taxonomy version
            evidence_pack_version: Evidence Pack version
            engine_spec_version: Engine spec version
        
        Returns:
            Dict with paths to generated files:
                - json_path: Path to evidence_pack_summary.json
                - xlsx_path: Path to evidence_pack_summary.xlsx
        """
        # Query evidence pack data from database
        # Join signature_stats with analysis_cache to get taxonomy codes
        query = """
        SELECT 
            ss.url_signature,
            ss.norm_host,
            ss.norm_path_template,
            ss.dest_domain,
            ss.bytes_sent_sum,
            ss.access_count,
            ss.unique_users,
            ss.candidate_flags,
            ac.service_name,
            ac.category,
            ac.usage_type,
            ac.risk_level,
            ac.confidence,
            ac.classification_source,
            ac.fs_uc_code,
            ac.dt_code,
            ac.ch_code,
            ac.im_code,
            ac.rs_code,
            ac.ob_code,
            ac.ev_code,
            ac.taxonomy_version
        FROM signature_stats ss
        LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ?
        ORDER BY ss.bytes_sent_sum DESC
        """
        
        rows = db_reader.execute(query, [run_id]).fetchall()
        
        # Build evidence pack data
        evidence_pack_data = []
        for row in rows:
            (url_sig, norm_host, norm_path_template, dest_domain, bytes_sent_sum,
             access_count, unique_users, candidate_flags, service_name, category,
             usage_type, risk_level, confidence, classification_source,
             fs_uc_code, dt_code, ch_code, im_code, rs_code, ob_code, ev_code,
             taxonomy_version_from_db) = row
            
            # Use taxonomy_version from DB if available, otherwise use parameter
            taxonomy_ver = taxonomy_version_from_db or taxonomy_version
            
            evidence_pack_data.append({
                "url_signature": url_sig or "",
                "norm_host": norm_host or "",
                "norm_path_template": norm_path_template or "",
                "dest_domain": dest_domain or "",
                "bytes_sent_sum": bytes_sent_sum or 0,
                "access_count": access_count or 0,
                "unique_users": unique_users or 0,
                "candidate_flags": candidate_flags or "",
                "service_name": service_name or "Unknown",
                "category": category or "",
                "usage_type": usage_type or "unknown",
                "risk_level": risk_level or "medium",
                "confidence": float(confidence) if confidence else 0.0,
                "classification_source": classification_source or "UNKNOWN",
                # Taxonomy codes (列欠落禁止: 空文字列でも必ず含める)
                "fs_uc_code": fs_uc_code or "",
                "dt_code": dt_code or "",
                "ch_code": ch_code or "",
                "im_code": im_code or "",
                "rs_code": rs_code or "",
                "ob_code": ob_code or "",
                "ev_code": ev_code or "",
                "taxonomy_version": taxonomy_ver or ""
            })
        
        # Generate JSON output (machine-readable)
        json_path = self.output_dir / "evidence_pack_summary.json"
        self._generate_json(json_path, evidence_pack_data, run_id, taxonomy_version,
                           evidence_pack_version, engine_spec_version)
        
        # Generate Excel output (human-readable)
        xlsx_path = self.output_dir / "evidence_pack_summary.xlsx"
        self._generate_xlsx(xlsx_path, evidence_pack_data, run_id, taxonomy_version,
                           evidence_pack_version, engine_spec_version)
        
        return {
            "json_path": json_path,
            "xlsx_path": xlsx_path
        }
    
    def _generate_json(self, output_path: Path, data: List[Dict[str, Any]],
                      run_id: str, taxonomy_version: str,
                      evidence_pack_version: str, engine_spec_version: str):
        """Generate JSON output (machine-readable)."""
        evidence_pack = {
            "metadata": {
                "run_id": run_id,
                "generated_at": datetime.utcnow().isoformat(),
                "engine_spec_version": engine_spec_version,
                "taxonomy_version": taxonomy_version,
                "evidence_pack_version": evidence_pack_version,
                "total_signatures": len(data)
            },
            "signatures": data
        }
        
        # Write to temp file first, then rename (atomic write)
        temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(evidence_pack, f, indent=2, ensure_ascii=False)
        
        # Atomic rename
        temp_path.replace(output_path)
    
    def _generate_xlsx(self, output_path: Path, data: List[Dict[str, Any]],
                      run_id: str, taxonomy_version: str,
                      evidence_pack_version: str, engine_spec_version: str):
        """Generate Excel output (human-readable) with constant_memory=True."""
        workbook = xlsxwriter.Workbook(
            str(output_path),
            {'constant_memory': True, 'default_date_format': 'yyyy-mm-dd hh:mm:ss'}
        )
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 11,
            'bg_color': '#1F4E79',
            'font_color': '#FFFFFF',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        
        data_format = workbook.add_format({
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        bytes_format = workbook.add_format({
            'num_format': '#,##0',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        # Create worksheet
        worksheet = workbook.add_worksheet("EvidencePack")
        
        # Write header
        # Required columns: 7 codes + taxonomy_version (列欠落禁止)
        columns = [
            "url_signature", "norm_host", "norm_path_template", "dest_domain",
            "bytes_sent_sum", "access_count", "unique_users", "candidate_flags",
            "service_name", "category", "usage_type", "risk_level", "confidence",
            "classification_source",
            # Taxonomy codes (列欠落禁止)
            "fs_uc_code", "dt_code", "ch_code", "im_code", "rs_code", "ob_code", "ev_code",
            "taxonomy_version"
        ]
        
        for col_idx, col_name in enumerate(columns):
            worksheet.write(0, col_idx, col_name, header_format)
        
        # Write data (chunked for constant_memory mode)
        chunk_size = 1000
        row = 1
        
        for chunk_start in range(0, len(data), chunk_size):
            chunk = data[chunk_start:chunk_start + chunk_size]
            
            for row_data in chunk:
                for col_idx, col_name in enumerate(columns):
                    value = row_data.get(col_name)
                    
                    # Apply format based on column type
                    if col_name == "bytes_sent_sum":
                        worksheet.write(row, col_idx, value, bytes_format)
                    else:
                        worksheet.write(row, col_idx, value, data_format)
                
                row += 1
        
        # Set column widths
        worksheet.set_column('A:A', 70)  # url_signature
        worksheet.set_column('B:B', 30)  # norm_host
        worksheet.set_column('C:C', 50)  # norm_path_template
        worksheet.set_column('D:D', 30)  # dest_domain
        worksheet.set_column('E:E', 15)  # bytes_sent_sum
        worksheet.set_column('F:F', 12)  # access_count
        worksheet.set_column('G:G', 12)  # unique_users
        worksheet.set_column('H:H', 20)  # candidate_flags
        worksheet.set_column('I:I', 30)  # service_name
        worksheet.set_column('J:J', 20)  # category
        worksheet.set_column('K:K', 15)  # usage_type
        worksheet.set_column('L:L', 12)  # risk_level
        worksheet.set_column('M:M', 12)  # confidence
        worksheet.set_column('N:N', 20)  # classification_source
        # Taxonomy codes columns
        worksheet.set_column('O:U', 15)  # fs_uc_code through ev_code
        worksheet.set_column('V:V', 20)  # taxonomy_version
        
        workbook.close()
    
    def generate_run_manifest(self,
                             run_id: str,
                             run_key: str,
                             started_at: datetime,
                             finished_at: Optional[datetime],
                             signature_version: str,
                             rule_version: str,
                             prompt_version: str,
                             taxonomy_version: str,
                             evidence_pack_version: str,
                             engine_spec_version: str) -> Path:
        """
        Generate run_manifest.json with all version information.
        
        Args:
            run_id: Run identifier
            run_key: Deterministic run key
            started_at: Run start time
            finished_at: Run finish time (optional)
            signature_version: Signature version
            rule_version: Rule version
            prompt_version: Prompt version
            taxonomy_version: Taxonomy version
            evidence_pack_version: Evidence Pack version
            engine_spec_version: Engine spec version
        
        Returns:
            Path to generated run_manifest.json
        """
        manifest = {
            "run_id": run_id,
            "run_key": run_key,
            "started_at": started_at.isoformat() if isinstance(started_at, datetime) else started_at,
            "finished_at": finished_at.isoformat() if finished_at and isinstance(finished_at, datetime) else (finished_at if finished_at else None),
            "versions": {
                "engine_spec_version": engine_spec_version,
                "taxonomy_version": taxonomy_version,
                "evidence_pack_version": evidence_pack_version,
                "signature_version": signature_version,
                "rule_version": rule_version,
                "prompt_version": prompt_version
            },
            "generated_at": datetime.utcnow().isoformat()
        }
        
        # Write to parent directory (data/output/<run_id>/)
        manifest_path = self.output_dir.parent / "run_manifest.json"
        
        # Write to temp file first, then rename (atomic write)
        temp_path = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        
        # Atomic rename
        temp_path.replace(manifest_path)
        
        return manifest_path
