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
from typing import Dict, Any, Optional
from datetime import datetime
import jsonschema


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
                     exclusions: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
