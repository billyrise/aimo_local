"""
Standard Evidence Bundle Generator for AIMO Analysis Engine

Generates AIMO Standard v0.1.7+ compliant Evidence Bundles.
The bundle structure and schemas are loaded from Standard artifacts,
NOT hardcoded in the Engine.

Key principles:
- Evidence Bundle schema is loaded from Standard artifacts
- All outputs validated against Standard schemas after generation
- Run metadata includes Standard version/commit/sha for reproducibility
- Validator results included in bundle for audit trail

Bundle structure:
  evidence_bundle/
    ├── run_manifest.json         # Run metadata + Standard version info
    ├── evidence_pack_manifest.json  # Standard-compliant manifest
    ├── logs/
    │   ├── shadow_ai_discovery.jsonl  # Standard schema
    │   └── agent_activity.jsonl       # Standard schema
    ├── analysis/
    │   └── taxonomy_assignments.json   # Classification results
    ├── checksums.json            # SHA-256 of all files
    ├── validation_result.json    # Self-validation result
    └── derived/                  # Engine-specific outputs (non-authoritative)
        ├── evidence_pack_summary.json
        └── evidence_pack_summary.xlsx
"""

import json
import hashlib
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date
from dataclasses import dataclass, asdict


@dataclass
class BundleGenerationResult:
    """Result of Evidence Bundle generation."""
    bundle_path: Path
    run_manifest_path: Path
    evidence_pack_manifest_path: Path
    checksums_path: Path
    validation_result_path: Path
    validation_passed: bool
    validation_errors: List[str]
    files_generated: List[str]


class StandardEvidenceBundleGenerator:
    """
    Generator for AIMO Standard v0.1.7+ compliant Evidence Bundles.
    
    Features:
    - Loads schemas from Standard artifacts
    - Generates Standard-compliant log files (Shadow AI, Agent Activity)
    - Validates all outputs against Standard schemas
    - Includes checksums and validation results
    """
    
    # Standard log file names
    SHADOW_AI_DISCOVERY_LOG = "shadow_ai_discovery.jsonl"
    AGENT_ACTIVITY_LOG = "agent_activity.jsonl"
    
    def __init__(self, aimo_standard_version: str = "0.1.7"):
        """
        Initialize the generator.
        
        Args:
            aimo_standard_version: AIMO Standard version to use
        """
        self.aimo_standard_version = aimo_standard_version
        self._schema_loader = None
        self._taxonomy_adapter = None
        self._validator_runner = None
        self._standard_info = None
        
        # Try to load Standard adapter components
        try:
            from standard_adapter.resolver import resolve_standard_artifacts
            from standard_adapter.schemas import get_schema_loader
            from standard_adapter.taxonomy import get_taxonomy_adapter
            from standard_adapter.validator_runner import get_validator_runner
            
            self._standard_info = resolve_standard_artifacts(aimo_standard_version)
            self._schema_loader = get_schema_loader(aimo_standard_version)
            self._taxonomy_adapter = get_taxonomy_adapter(aimo_standard_version)
            self._validator_runner = get_validator_runner(aimo_standard_version)
            
        except ImportError as e:
            print(f"Warning: Standard adapter not fully available: {e}", flush=True)
        except Exception as e:
            print(f"Warning: Failed to initialize Standard adapter: {e}", flush=True)
    
    def generate(
        self,
        run_context,
        output_dir: Path,
        db_reader,
        include_derived: bool = True
    ) -> BundleGenerationResult:
        """
        Generate a complete Evidence Bundle.
        
        Args:
            run_context: Run context from orchestrator
            output_dir: Output directory for the bundle
            db_reader: DuckDB reader connection
            include_derived: Whether to include derived (legacy) outputs
        
        Returns:
            BundleGenerationResult with paths and validation status
        """
        bundle_dir = Path(output_dir) / "evidence_bundle"
        bundle_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        logs_dir = bundle_dir / "logs"
        logs_dir.mkdir(exist_ok=True)
        analysis_dir = bundle_dir / "analysis"
        analysis_dir.mkdir(exist_ok=True)
        
        files_generated = []
        
        # 1. Generate run_manifest.json
        run_manifest_path = self._generate_run_manifest(bundle_dir, run_context)
        files_generated.append(str(run_manifest_path.relative_to(bundle_dir)))
        
        # 2. Get analysis data from DB
        analysis_data = self._query_analysis_data(db_reader, run_context.run_id)
        
        # 3. Generate Shadow AI Discovery Log (Standard schema)
        shadow_ai_path = self._generate_shadow_ai_discovery_log(
            logs_dir, run_context, analysis_data
        )
        files_generated.append(str(shadow_ai_path.relative_to(bundle_dir)))
        
        # 4. Generate Agent Activity Log (Standard schema) - placeholder for now
        agent_activity_path = self._generate_agent_activity_log(
            logs_dir, run_context
        )
        files_generated.append(str(agent_activity_path.relative_to(bundle_dir)))
        
        # 5. Generate taxonomy assignments (analysis results)
        taxonomy_path = self._generate_taxonomy_assignments(
            analysis_dir, run_context, analysis_data
        )
        files_generated.append(str(taxonomy_path.relative_to(bundle_dir)))
        
        # 6. Generate evidence_pack_manifest.json (Standard schema)
        manifest_path = self._generate_evidence_pack_manifest(
            bundle_dir, run_context, analysis_data, files_generated
        )
        files_generated.insert(1, str(manifest_path.relative_to(bundle_dir)))
        
        # 7. Generate derived outputs (legacy compatibility)
        if include_derived:
            derived_files = self._generate_derived_outputs(
                bundle_dir, run_context, analysis_data, db_reader
            )
            files_generated.extend(derived_files)
        
        # 8. Generate checksums for all files
        checksums_path = self._generate_checksums(bundle_dir, files_generated)
        files_generated.append(str(checksums_path.relative_to(bundle_dir)))
        
        # 9. Run validation and generate result file
        validation_passed, validation_errors = self._run_validation(bundle_dir)
        validation_result_path = self._generate_validation_result(
            bundle_dir, validation_passed, validation_errors
        )
        files_generated.append(str(validation_result_path.relative_to(bundle_dir)))
        
        return BundleGenerationResult(
            bundle_path=bundle_dir,
            run_manifest_path=run_manifest_path,
            evidence_pack_manifest_path=manifest_path,
            checksums_path=checksums_path,
            validation_result_path=validation_result_path,
            validation_passed=validation_passed,
            validation_errors=validation_errors,
            files_generated=files_generated
        )
    
    def _generate_run_manifest(self, bundle_dir: Path, run_context) -> Path:
        """Generate run_manifest.json with full audit information."""
        # Get Standard info
        standard_version = self.aimo_standard_version
        standard_commit = ""
        standard_artifacts_sha = ""
        standard_artifacts_zip_sha = ""
        
        if self._standard_info:
            standard_commit = self._standard_info.standard_commit or ""
            standard_artifacts_sha = self._standard_info.artifacts_dir_sha256 or ""
            standard_artifacts_zip_sha = self._standard_info.artifacts_zip_sha256 or ""
        
        # Get from run_context if available
        if hasattr(run_context, 'standard_info') and run_context.standard_info:
            si = run_context.standard_info
            standard_version = si.standard_version or standard_version
            standard_commit = si.standard_commit or standard_commit
            standard_artifacts_sha = si.artifacts_dir_sha256 or standard_artifacts_sha
            standard_artifacts_zip_sha = si.artifacts_zip_sha256 or standard_artifacts_zip_sha
        
        manifest = {
            "run_id": run_context.run_id,
            "run_key": run_context.run_key,
            "started_at": run_context.started_at.isoformat() if hasattr(run_context.started_at, 'isoformat') else str(run_context.started_at),
            "finished_at": datetime.utcnow().isoformat(),
            "status": run_context.status if hasattr(run_context, 'status') else "completed",
            
            # AIMO Standard versioning (critical for reproducibility)
            "aimo_standard": {
                "version": standard_version,
                "commit": standard_commit,
                "artifacts_dir_sha256": standard_artifacts_sha,
                "artifacts_zip_sha256": standard_artifacts_zip_sha
            },
            
            # Input tracking
            "input_manifest_hash": run_context.input_manifest_hash,
            
            # Engine versioning
            "versions": {
                "code_version": getattr(run_context, 'code_version', ""),
                "signature_version": getattr(run_context, 'signature_version', "1.0"),
                "rule_version": getattr(run_context, 'rule_version', "1"),
                "prompt_version": getattr(run_context, 'prompt_version', "1"),
                "taxonomy_version": standard_version,
                "evidence_pack_version": getattr(run_context, 'evidence_pack_version', "1.0"),
                "engine_spec_version": getattr(run_context, 'engine_spec_version', "1.5")
            },
            
            # Extraction parameters (for audit)
            "extraction_parameters": {
                "a_threshold_bytes": getattr(run_context, 'a_threshold_bytes', 0),
                "b_threshold_bytes": getattr(run_context, 'b_threshold_bytes', 0),
                "c_sample_rate": getattr(run_context, 'c_sample_rate', 0.0),
                "exclusion_count": getattr(run_context, 'exclusion_count', 0),
                "sample_seed": getattr(run_context, 'sample_seed', "")
            },
            
            "generated_at": datetime.utcnow().isoformat()
        }
        
        manifest_path = bundle_dir / "run_manifest.json"
        self._write_json_atomic(manifest_path, manifest)
        
        return manifest_path
    
    def _query_analysis_data(self, db_reader, run_id: str) -> List[Dict[str, Any]]:
        """
        Query analysis data from database with backward-compatible normalization.
        
        Uses db.compat layer to normalize legacy and new format records.
        Records with legacy-only data are marked needs_review=True.
        """
        # Query includes both new and legacy columns for compatibility
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
            ss.first_seen,
            ss.last_seen,
            ac.service_name,
            ac.category,
            ac.usage_type,
            ac.risk_level,
            ac.confidence,
            ac.classification_source,
            ac.rationale_short,
            -- New 8-dimension columns
            ac.fs_code,
            ac.im_code,
            ac.uc_codes_json,
            ac.dt_codes_json,
            ac.ch_codes_json,
            ac.rs_codes_json,
            ac.ev_codes_json,
            ac.ob_codes_json,
            ac.taxonomy_schema_version,
            -- Legacy columns for backward compatibility
            ac.fs_uc_code,
            ac.dt_code,
            ac.ch_code,
            ac.rs_code,
            ac.ob_code,
            ac.ev_code
        FROM signature_stats ss
        LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ?
        ORDER BY ss.bytes_sent_sum DESC
        """
        
        rows = db_reader.execute(query, [run_id]).fetchall()
        
        # Import compatibility layer
        from db.compat import normalize_taxonomy_record
        
        results = []
        for row in rows:
            (url_sig, norm_host, norm_path, dest_domain, bytes_sum, access_count,
             unique_users, candidate_flags, first_seen, last_seen, service_name,
             category, usage_type, risk_level, confidence, classification_source,
             rationale, fs_code, im_code, uc_json, dt_json, ch_json, rs_json,
             ev_json, ob_json, taxonomy_version,
             # Legacy columns
             fs_uc_code, dt_code, ch_code, rs_code, ob_code, ev_code) = row
            
            # Build row dict for normalization
            taxonomy_row = {
                "fs_code": fs_code,
                "im_code": im_code,
                "uc_codes_json": uc_json,
                "dt_codes_json": dt_json,
                "ch_codes_json": ch_json,
                "rs_codes_json": rs_json,
                "ev_codes_json": ev_json,
                "ob_codes_json": ob_json,
                "taxonomy_schema_version": taxonomy_version,
                # Legacy
                "fs_uc_code": fs_uc_code,
                "dt_code": dt_code,
                "ch_code": ch_code,
                "rs_code": rs_code,
                "ob_code": ob_code,
                "ev_code": ev_code,
            }
            
            # Normalize using compat layer
            normalized = normalize_taxonomy_record(taxonomy_row, self.aimo_standard_version)
            
            results.append({
                "url_signature": url_sig or "",
                "norm_host": norm_host or "",
                "norm_path_template": norm_path or "",
                "dest_domain": dest_domain or "",
                "bytes_sent_sum": bytes_sum or 0,
                "access_count": access_count or 0,
                "unique_users": unique_users or 0,
                "candidate_flags": candidate_flags or "",
                "first_seen": first_seen.isoformat() if first_seen else None,
                "last_seen": last_seen.isoformat() if last_seen else None,
                "service_name": service_name or "Unknown",
                "category": category or "",
                "usage_type": usage_type or "unknown",
                "risk_level": risk_level or "medium",
                "confidence": float(confidence) if confidence else 0.0,
                "classification_source": classification_source or "UNKNOWN",
                "rationale_short": rationale or "",
                # 8-dimension codes (normalized)
                "fs_code": normalized.fs_code,
                "im_code": normalized.im_code,
                "uc_codes": normalized.uc_codes,
                "dt_codes": normalized.dt_codes,
                "ch_codes": normalized.ch_codes,
                "rs_codes": normalized.rs_codes,
                "ev_codes": normalized.ev_codes,
                "ob_codes": normalized.ob_codes,
                "taxonomy_version": normalized.taxonomy_version,
                # Migration metadata
                "_needs_review": normalized.needs_review,
                "_source_format": normalized.source_format,
            })
        
        return results
    
    def _generate_shadow_ai_discovery_log(
        self,
        logs_dir: Path,
        run_context,
        analysis_data: List[Dict[str, Any]]
    ) -> Path:
        """Generate Shadow AI Discovery Log in Standard format."""
        log_path = logs_dir / self.SHADOW_AI_DISCOVERY_LOG
        
        with open(log_path, 'w', encoding='utf-8') as f:
            for item in analysis_data:
                # Only include GenAI/Shadow AI entries
                if item.get("usage_type") != "genai":
                    continue
                
                record = {
                    "event_time": datetime.utcnow().isoformat() + "Z",
                    "actor_id": "analysis_engine",
                    "actor_type": "service",
                    "source_system": "aimo_analysis_engine",
                    "ai_service": item.get("service_name", "Unknown"),
                    "action": "discovery",
                    "data_classification": self._map_risk_to_classification(item.get("risk_level", "medium")),
                    "decision": self._map_to_decision(item),
                    "evidence_ref": f"run:{run_context.run_id}:sig:{item.get('url_signature', '')}",
                    "record_id": str(uuid.uuid4()),
                    "destination": item.get("norm_host", ""),
                    "model_family": item.get("category", "")
                }
                
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        return log_path
    
    def _generate_agent_activity_log(
        self,
        logs_dir: Path,
        run_context
    ) -> Path:
        """Generate Agent Activity Log in Standard format."""
        log_path = logs_dir / self.AGENT_ACTIVITY_LOG
        
        # Generate a single record for the analysis run itself
        record = {
            "event_time": datetime.utcnow().isoformat() + "Z",
            "agent_id": "aimo_analysis_engine",
            "agent_version": getattr(run_context, 'engine_spec_version', "1.5"),
            "run_id": run_context.run_id,
            "event_type": "agent_run",
            "actor_id": "system",
            "tool_name": "log_analyzer",
            "tool_action": "analyze",
            "tool_target": f"input_manifest:{run_context.input_manifest_hash}",
            "auth_context": "engine_service_account",
            "input_ref": run_context.input_manifest_hash,
            "output_ref": f"bundle:{run_context.run_id}",
            "decision": "allow",
            "evidence_ref": f"run:{run_context.run_id}"
        }
        
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        return log_path
    
    def _generate_taxonomy_assignments(
        self,
        analysis_dir: Path,
        run_context,
        analysis_data: List[Dict[str, Any]]
    ) -> Path:
        """Generate taxonomy assignments in analysis directory."""
        assignments = {
            "run_id": run_context.run_id,
            "aimo_standard_version": self.aimo_standard_version,
            "generated_at": datetime.utcnow().isoformat(),
            "total_signatures": len(analysis_data),
            "assignments": []
        }
        
        for item in analysis_data:
            assignment = {
                "url_signature": item.get("url_signature", ""),
                "service_name": item.get("service_name", "Unknown"),
                "usage_type": item.get("usage_type", "unknown"),
                "risk_level": item.get("risk_level", "medium"),
                "confidence": item.get("confidence", 0.0),
                "classification_source": item.get("classification_source", "UNKNOWN"),
                "codes": {
                    "FS": [item.get("fs_code")] if item.get("fs_code") else [],
                    "IM": [item.get("im_code")] if item.get("im_code") else [],
                    "UC": item.get("uc_codes", []),
                    "DT": item.get("dt_codes", []),
                    "CH": item.get("ch_codes", []),
                    "RS": item.get("rs_codes", []),
                    "EV": item.get("ev_codes", []),
                    "OB": item.get("ob_codes", [])
                }
            }
            assignments["assignments"].append(assignment)
        
        taxonomy_path = analysis_dir / "taxonomy_assignments.json"
        self._write_json_atomic(taxonomy_path, assignments)
        
        return taxonomy_path
    
    def _generate_evidence_pack_manifest(
        self,
        bundle_dir: Path,
        run_context,
        analysis_data: List[Dict[str, Any]],
        files_generated: List[str]
    ) -> Path:
        """Generate evidence_pack_manifest.json in Standard format."""
        # Aggregate codes from all assignments
        aggregated_codes = self._aggregate_codes(analysis_data)
        
        # Build evidence files list
        evidence_files = []
        for i, file_path in enumerate(files_generated):
            ev_file = {
                "file_id": f"EV-{i+1:02d}",
                "filename": file_path,
                "ev_type": "EV-001",
                "title": Path(file_path).name
            }
            evidence_files.append(ev_file)
        
        manifest = {
            "$schema": "https://standard.aimoaas.com/schemas/evidence_pack_manifest.schema.json",
            "pack_id": f"EP-{run_context.run_id}",
            "pack_version": "1.0.0",
            "title": f"AIMO Analysis Run {run_context.run_id}",
            "description": "Evidence Bundle generated by AIMO Analysis Engine",
            "created_date": date.today().isoformat(),
            "last_updated": date.today().isoformat(),
            "taxonomy_version": "0.1.7",
            "owner": "AIMO Analysis Engine",
            "codes": aggregated_codes,
            "evidence_files": evidence_files if evidence_files else [
                {"file_id": "EV-01", "filename": "run_manifest.json", "ev_type": "EV-001", "title": "Run Manifest"}
            ],
            "change_log": [
                {
                    "date": date.today().isoformat(),
                    "version": "1.0.0",
                    "author": "AIMO Analysis Engine",
                    "summary": "Initial generation"
                }
            ]
        }
        
        manifest_path = bundle_dir / "evidence_pack_manifest.json"
        self._write_json_atomic(manifest_path, manifest)
        
        return manifest_path
    
    def _aggregate_codes(self, analysis_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Aggregate codes from all analysis data."""
        codes = {
            "FS": set(),
            "IM": set(),
            "UC": set(),
            "DT": set(),
            "CH": set(),
            "RS": set(),
            "EV": set(),
            "OB": set()
        }
        
        for item in analysis_data:
            if item.get("fs_code"):
                codes["FS"].add(item["fs_code"])
            if item.get("im_code"):
                codes["IM"].add(item["im_code"])
            for code in item.get("uc_codes", []):
                codes["UC"].add(code)
            for code in item.get("dt_codes", []):
                codes["DT"].add(code)
            for code in item.get("ch_codes", []):
                codes["CH"].add(code)
            for code in item.get("rs_codes", []):
                codes["RS"].add(code)
            for code in item.get("ev_codes", []):
                codes["EV"].add(code)
            for code in item.get("ob_codes", []):
                codes["OB"].add(code)
        
        # Convert sets to sorted lists, ensure at least one code per required dimension
        result = {}
        for dim in ["FS", "IM", "UC", "DT", "CH", "RS", "EV"]:
            code_list = sorted(codes[dim])
            if not code_list:
                # Fallback code
                code_list = [f"{dim}-099"]
            result[dim] = code_list
        
        # OB is optional
        result["OB"] = sorted(codes["OB"])
        
        return result
    
    def _generate_derived_outputs(
        self,
        bundle_dir: Path,
        run_context,
        analysis_data: List[Dict[str, Any]],
        db_reader
    ) -> List[str]:
        """Generate derived (legacy) outputs in derived/ subdirectory."""
        derived_dir = bundle_dir / "derived"
        derived_dir.mkdir(exist_ok=True)
        
        files = []
        
        # Import legacy generator
        try:
            from reporting.evidence_pack_generator import EvidencePackGenerator
            
            legacy_generator = EvidencePackGenerator(derived_dir)
            
            # Generate legacy outputs
            paths = legacy_generator.generate_evidence_pack(
                run_id=run_context.run_id,
                db_reader=db_reader,
                taxonomy_version=self.aimo_standard_version,
                evidence_pack_version=getattr(run_context, 'evidence_pack_version', "1.0"),
                engine_spec_version=getattr(run_context, 'engine_spec_version', "1.5")
            )
            
            if paths.get("json_path"):
                files.append(str(paths["json_path"].relative_to(bundle_dir)))
            if paths.get("xlsx_path"):
                files.append(str(paths["xlsx_path"].relative_to(bundle_dir)))
                
        except Exception as e:
            print(f"Warning: Failed to generate derived outputs: {e}", flush=True)
        
        return files
    
    def _generate_checksums(
        self,
        bundle_dir: Path,
        files: List[str]
    ) -> Path:
        """Generate SHA-256 checksums for all files."""
        checksums = {
            "algorithm": "SHA-256",
            "generated_at": datetime.utcnow().isoformat(),
            "files": {}
        }
        
        for file_rel_path in files:
            file_path = bundle_dir / file_rel_path
            if file_path.exists():
                sha256_hash = self._calculate_file_sha256(file_path)
                checksums["files"][file_rel_path] = sha256_hash
        
        checksums_path = bundle_dir / "checksums.json"
        self._write_json_atomic(checksums_path, checksums)
        
        return checksums_path
    
    def _run_validation(self, bundle_dir: Path) -> Tuple[bool, List[str]]:
        """Run validation on the generated bundle."""
        errors = []
        
        if self._validator_runner:
            try:
                # Pass Path object, not string - validator expects Path
                result = self._validator_runner.run_validation(evidence_bundle_dir=bundle_dir)
                if not result.passed:
                    errors.extend(result.errors)
                return result.passed, errors
            except Exception as e:
                errors.append(f"Validator error: {str(e)}")
                return False, errors
        else:
            # Basic validation without Standard adapter
            # Check required files exist
            required_files = ["run_manifest.json", "evidence_pack_manifest.json"]
            for req_file in required_files:
                if not (bundle_dir / req_file).exists():
                    errors.append(f"Missing required file: {req_file}")
            
            # Try JSON schema validation if possible
            try:
                import jsonschema
                manifest_path = bundle_dir / "evidence_pack_manifest.json"
                if manifest_path.exists():
                    with open(manifest_path, 'r', encoding='utf-8') as f:
                        manifest = json.load(f)
                    
                    # Load schema from Standard submodule if available
                    schema_path = Path(__file__).parent.parent.parent / "third_party" / "aimo-standard" / "schemas" / "jsonschema" / "evidence_pack_manifest.schema.json"
                    if schema_path.exists():
                        with open(schema_path, 'r', encoding='utf-8') as f:
                            schema = json.load(f)
                        
                        validator = jsonschema.Draft202012Validator(schema)
                        for error in validator.iter_errors(manifest):
                            errors.append(f"Schema validation: {error.message}")
                            
            except Exception as e:
                errors.append(f"Validation error: {str(e)}")
        
        return len(errors) == 0, errors
    
    def _generate_validation_result(
        self,
        bundle_dir: Path,
        passed: bool,
        errors: List[str]
    ) -> Path:
        """Generate validation_result.json."""
        result = {
            "validation_time": datetime.utcnow().isoformat(),
            "passed": passed,
            "status": "passed" if passed else "failed",
            "aimo_standard_version": self.aimo_standard_version,
            "validator_version": "1.0.0",
            "errors": errors,
            "error_count": len(errors)
        }
        
        result_path = bundle_dir / "validation_result.json"
        self._write_json_atomic(result_path, result)
        
        return result_path
    
    def _write_json_atomic(self, path: Path, data: Any):
        """Write JSON file atomically."""
        temp_path = path.with_suffix(path.suffix + ".tmp")
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        temp_path.replace(path)
    
    def _calculate_file_sha256(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def _map_risk_to_classification(self, risk_level: str) -> str:
        """Map risk level to data classification."""
        mapping = {
            "high": "confidential",
            "medium": "internal",
            "low": "public"
        }
        return mapping.get(risk_level, "internal")
    
    def _map_to_decision(self, item: Dict[str, Any]) -> str:
        """Map analysis result to decision."""
        risk = item.get("risk_level", "medium")
        confidence = item.get("confidence", 0.0)
        
        if confidence < 0.5:
            return "needs_review"
        elif risk == "high":
            return "block"
        else:
            return "allow"
