"""
Standard Evidence Bundle Generator for AIMO Analysis Engine

Generates AIMO Standard v0.1.1+ compliant Evidence Bundles.
The bundle structure and schemas are loaded from Standard artifacts,
NOT hardcoded in the Engine.

Key principles:
- Evidence Bundle schema is loaded from Standard artifacts
- All outputs validated against Standard schemas after generation
- Run metadata includes Standard version/commit/sha for reproducibility
- Validator results included in bundle for audit trail

Bundle structure (v0.1 root structure):
  evidence_bundle/
    ├── manifest.json             # Bundle root manifest (bundle_id, object_index, payload_index, hash_chain, signing)
    ├── objects/
    │   └── index.json             # Enumerated objects
    ├── payloads/
    │   ├── run_manifest.json      # Run metadata + Standard version info
    │   ├── evidence_pack_manifest.json
    │   ├── logs/
    │   │   ├── shadow_ai_discovery.jsonl
    │   │   └── agent_activity.jsonl
    │   ├── analysis/
    │   │   └── taxonomy_assignments.json
    │   ├── checksums.json
    │   ├── validation_result.json
    │   └── derived/               # Engine-specific (non-authoritative)
    ├── signatures/
    │   └── bundle.sig             # At least one signature targeting manifest.json
    └── hashes/
        └── chain.json            # Hash chain (covers manifest.json, objects/index.json)
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
    Generator for AIMO Standard v0.1.1+ compliant Evidence Bundles.
    
    Implements Evidence Bundle root structure (v0.1): manifest.json at root,
    objects/, payloads/, signatures/, hashes/. Payloads (run_manifest,
    evidence_pack_manifest, logs, analysis, etc.) live under payloads/.
    
    Features:
    - Loads schemas from Standard artifacts
    - Generates Standard-compliant log files (Shadow AI, Agent Activity)
    - Validates all outputs against Standard schemas
    - Includes checksums and validation results
    """
    
    # Standard log file names
    SHADOW_AI_DISCOVERY_LOG = "shadow_ai_discovery.jsonl"
    AGENT_ACTIVITY_LOG = "agent_activity.jsonl"
    
    def __init__(self, aimo_standard_version: str = "0.1.1"):
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
        
        # v0.1 root structure: objects/, payloads/, signatures/, hashes/
        payloads_dir = bundle_dir / "payloads"
        payloads_dir.mkdir(exist_ok=True)
        objects_dir = bundle_dir / "objects"
        objects_dir.mkdir(exist_ok=True)
        signatures_dir = bundle_dir / "signatures"
        signatures_dir.mkdir(exist_ok=True)
        hashes_dir = bundle_dir / "hashes"
        hashes_dir.mkdir(exist_ok=True)
        
        logs_dir = payloads_dir / "logs"
        logs_dir.mkdir(exist_ok=True)
        analysis_dir = payloads_dir / "analysis"
        analysis_dir.mkdir(exist_ok=True)
        
        # Paths relative to bundle_dir (for payload_index)
        files_generated = []
        
        # 1. Generate run_manifest.json under payloads/
        run_manifest_path = self._generate_run_manifest(payloads_dir, run_context)
        files_generated.append("payloads/" + run_manifest_path.name)
        
        # 2. Get analysis data from DB
        analysis_data = self._query_analysis_data(db_reader, run_context.run_id)
        
        # 3. Shadow AI Discovery Log
        shadow_ai_path = self._generate_shadow_ai_discovery_log(
            logs_dir, run_context, analysis_data
        )
        files_generated.append("payloads/logs/" + shadow_ai_path.name)
        
        # 4. Agent Activity Log
        agent_activity_path = self._generate_agent_activity_log(
            logs_dir, run_context
        )
        files_generated.append("payloads/logs/" + agent_activity_path.name)
        
        # 5. Taxonomy assignments
        taxonomy_path = self._generate_taxonomy_assignments(
            analysis_dir, run_context, analysis_data
        )
        files_generated.append("payloads/analysis/" + taxonomy_path.name)
        
        # 6. evidence_pack_manifest.json under payloads/
        pack_manifest_path = self._generate_evidence_pack_manifest(
            payloads_dir, run_context, analysis_data,
            [f.replace("payloads/", "") for f in files_generated]
        )
        files_generated.insert(1, "payloads/" + pack_manifest_path.name)
        
        # 6b. dictionary.json (Standard 0.1.1: required artifact)
        dict_path = self._add_dictionary_to_payloads(payloads_dir)
        if dict_path:
            files_generated.append("payloads/" + dict_path.name)
        
        # 6c. summary.json (1-page overview)
        summary_path = self._generate_summary(payloads_dir, run_context, analysis_data)
        files_generated.append("payloads/" + summary_path.name)
        
        # 6d. change_log.json (bundle change log)
        changelog_path = self._generate_change_log(payloads_dir, run_context)
        files_generated.append("payloads/" + changelog_path.name)
        
        # 7. Derived outputs under payloads/derived/
        if include_derived:
            derived_files = self._generate_derived_outputs(
                payloads_dir, run_context, analysis_data, db_reader
            )
            files_generated.extend("payloads/" + f for f in derived_files)
        
        # 8. Checksums (payloads/)
        checksums_path = self._generate_checksums(
            payloads_dir,
            [f.replace("payloads/", "") for f in files_generated]
        )
        files_generated.append("payloads/" + checksums_path.name)
        
        # 9. objects/index.json
        index_path = self._generate_objects_index(objects_dir, run_context)
        
        # 10. Build payload_index (path, sha256, mime, size) for all payload files so far
        payload_index = self._build_payload_index(bundle_dir, files_generated)
        
        # 11. Root manifest (bundle_id, object_index, payload_index, hash_chain, signing)
        #    First write with placeholder hash_chain.head; then write hashes/chain.json and update head
        root_manifest_path = self._write_root_manifest(
            bundle_dir, run_context, index_path, payload_index, signatures_dir
        )
        
        # 12. Run validation (validator expects root manifest.json)
        validation_passed, validation_errors = self._run_validation(bundle_dir)
        
        # 13. validation_result.json under payloads/
        validation_result_path = self._generate_validation_result(
            payloads_dir, validation_passed, validation_errors
        )
        files_generated.append("payloads/" + validation_result_path.name)
        
        # 14. Update root manifest: add validation_result to payload_index and refresh hash_chain
        payload_index = self._build_payload_index(bundle_dir, files_generated)
        self._update_root_manifest_after_validation(
            bundle_dir, index_path, payload_index, root_manifest_path
        )
        
        return BundleGenerationResult(
            bundle_path=bundle_dir,
            run_manifest_path=run_manifest_path,
            evidence_pack_manifest_path=pack_manifest_path,
            checksums_path=checksums_path,
            validation_result_path=validation_result_path,
            validation_passed=validation_passed,
            validation_errors=validation_errors,
            files_generated=files_generated
        )
    
    def _generate_objects_index(self, objects_dir: Path, run_context) -> Path:
        """Generate objects/index.json for v0.1 object_index."""
        index = {
            "bundle_run_id": run_context.run_id,
            "created_at": datetime.utcnow().isoformat() + "Z",
        }
        index_path = objects_dir / "index.json"
        self._write_json_atomic(index_path, index)
        return index_path

    def _build_payload_index(
        self, bundle_dir: Path, payload_rel_paths: List[str]
    ) -> List[Dict[str, Any]]:
        """Build payload_index entries: logical_id, path, sha256 (64 hex), mime, size."""
        payload_index = []
        for rel_path in payload_rel_paths:
            full_path = bundle_dir / rel_path
            if not full_path.exists():
                continue
            sha = self._calculate_file_sha256(full_path)
            size = full_path.stat().st_size
            mime = "application/json" if rel_path.endswith(".json") else "application/jsonl"
            if rel_path.endswith(".xlsx"):
                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            logical_id = rel_path.replace("/", "_").replace(" ", "_")
            payload_index.append({
                "logical_id": logical_id,
                "path": rel_path,
                "sha256": sha,
                "mime": mime,
                "size": size,
            })
        return payload_index

    def _write_root_manifest(
        self,
        bundle_dir: Path,
        run_context,
        objects_index_path: Path,
        payload_index: List[Dict[str, Any]],
        signatures_dir: Path,
    ) -> Path:
        """Write root manifest.json (v0.1) and hashes/chain.json; create signatures/ placeholder."""
        objects_rel = "objects/index.json"
        index_sha = self._calculate_file_sha256(objects_index_path)
        object_index = [
            {"id": "index", "type": "index", "path": objects_rel, "sha256": index_sha}
        ]
        # Placeholder head; will update after writing manifest and chain
        hash_chain = {
            "algorithm": "sha256",
            "head": "0" * 64,
            "path": "hashes/chain.json",
            "covers": ["manifest.json", "objects/index.json"],
        }
        # One signature targeting manifest.json (v0.1: existence and reference)
        sig_path_rel = "signatures/bundle.sig"
        sig_file = signatures_dir / "bundle.sig"
        sig_file.write_text(
            "Placeholder signature for v0.1; targets manifest.json. "
            "Cryptographic verification is out of scope for v0.1.\n",
            encoding="utf-8",
        )
        signing = {
            "signatures": [
                {
                    "signature_id": "SIG-001",
                    "path": sig_path_rel,
                    "targets": ["manifest.json"],
                    "algorithm": "unspecified",
                    "created_at": datetime.utcnow().isoformat() + "Z",
                }
            ]
        }
        root_manifest = {
            "bundle_id": str(uuid.uuid4()),
            "bundle_version": "1.0.0",
            "created_at": datetime.utcnow().isoformat() + "Z",
            "scope_ref": "SC-001",
            "object_index": object_index,
            "payload_index": payload_index,
            "hash_chain": hash_chain,
            "signing": signing,
        }
        root_manifest_path = bundle_dir / "manifest.json"
        self._write_json_atomic(root_manifest_path, root_manifest)
        # Hash chain: covers manifest.json and objects/index.json
        manifest_sha = self._calculate_file_sha256(root_manifest_path)
        chain_content = {
            "algorithm": "sha256",
            "covers": ["manifest.json", "objects/index.json"],
            "entries": [
                {"path": "manifest.json", "sha256": manifest_sha},
                {"path": "objects/index.json", "sha256": index_sha},
            ],
        }
        chain_path = bundle_dir / "hashes" / "chain.json"
        self._write_json_atomic(chain_path, chain_content)
        chain_sha = self._calculate_file_sha256(chain_path)
        root_manifest["hash_chain"]["head"] = chain_sha
        self._write_json_atomic(root_manifest_path, root_manifest)
        return root_manifest_path

    def _update_root_manifest_after_validation(
        self,
        bundle_dir: Path,
        objects_index_path: Path,
        payload_index: List[Dict[str, Any]],
        root_manifest_path: Path,
    ) -> None:
        """Update root manifest with final payload_index (including validation_result) and refresh hash_chain."""
        with open(root_manifest_path, "r", encoding="utf-8") as f:
            root_manifest = json.load(f)
        root_manifest["payload_index"] = payload_index
        self._write_json_atomic(root_manifest_path, root_manifest)
        manifest_sha = self._calculate_file_sha256(root_manifest_path)
        index_sha = self._calculate_file_sha256(objects_index_path)
        chain_path = bundle_dir / "hashes" / "chain.json"
        chain_content = {
            "algorithm": "sha256",
            "covers": ["manifest.json", "objects/index.json"],
            "entries": [
                {"path": "manifest.json", "sha256": manifest_sha},
                {"path": "objects/index.json", "sha256": index_sha},
            ],
        }
        self._write_json_atomic(chain_path, chain_content)
        chain_sha = self._calculate_file_sha256(chain_path)
        root_manifest["hash_chain"]["head"] = chain_sha
        self._write_json_atomic(root_manifest_path, root_manifest)

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
                "lg_codes": normalized.lg_codes,
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
                    "LG": item.get("lg_codes", []),
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
        # Standard 0.1.1: file_id EP-01..EP-07, ev_type = LG code
        ep_ids = ["EP-01", "EP-02", "EP-03", "EP-04", "EP-05", "EP-06", "EP-07"]
        for i, file_path in enumerate(files_generated):
            ev_file = {
                "file_id": ep_ids[i % len(ep_ids)],
                "filename": file_path,
                "ev_type": "LG-001",
                "title": Path(file_path).name
            }
            evidence_files.append(ev_file)
        
        manifest = {
            "$schema": "https://standard.aimoaas.com/0.1.1/schemas/evidence_pack_manifest.schema.json",
            "pack_id": f"EP-{run_context.run_id}",
            "pack_version": "1.0.0",
            "title": f"AIMO Analysis Run {run_context.run_id}",
            "description": "Evidence Bundle generated by AIMO Analysis Engine",
            "created_date": date.today().isoformat(),
            "last_updated": date.today().isoformat(),
            "taxonomy_version": "0.1.1",
            "owner": "AIMO Analysis Engine",
            "codes": aggregated_codes,
            "evidence_files": evidence_files if evidence_files else [
                {"file_id": "EP-01", "filename": "run_manifest.json", "ev_type": "LG-001", "title": "Run Manifest"}
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
    
    def _add_dictionary_to_payloads(self, payloads_dir: Path) -> Optional[Path]:
        """
        Copy Standard taxonomy dictionary to payloads/dictionary.json (Standard 0.1.1 required).
        Prefers examples/evidence_bundle_minimal/dictionary.json then source_pack/03_taxonomy/taxonomy_dictionary.json.
        """
        if not self._standard_info:
            return None
        artifacts_dir = getattr(self._standard_info, "artifacts_dir", None)
        submodule_dir = getattr(self._standard_info, "submodule_dir", None)
        for base_dir, rel_path in [
            (artifacts_dir, Path("examples") / "evidence_bundle_minimal" / "dictionary.json"),
            (submodule_dir, Path("source_pack") / "03_taxonomy" / "taxonomy_dictionary.json"),
        ]:
            if base_dir is None:
                continue
            src = Path(base_dir) / rel_path if isinstance(base_dir, Path) else Path(str(base_dir)) / rel_path
            if src.exists():
                dest = payloads_dir / "dictionary.json"
                content = src.read_bytes()
                temp_path = dest.with_suffix(dest.suffix + ".tmp")
                temp_path.write_bytes(content)
                temp_path.replace(dest)
                return dest
        return None
    
    def _generate_summary(
        self,
        payloads_dir: Path,
        run_context,
        analysis_data: List[Dict[str, Any]],
    ) -> Path:
        """Generate 1-page summary (summary.json) for Standard 0.1.1."""
        summary = {
            "title": f"AIMO Analysis Run Summary — {run_context.run_id}",
            "run_id": run_context.run_id,
            "aimo_standard_version": self.aimo_standard_version,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_signatures": len(analysis_data),
            "genai_count": sum(1 for i in analysis_data if i.get("usage_type") == "genai"),
            "status": getattr(run_context, "status", "completed"),
        }
        path = payloads_dir / "summary.json"
        self._write_json_atomic(path, summary)
        return path
    
    def _generate_change_log(self, payloads_dir: Path, run_context) -> Path:
        """Generate change_log.json for Standard 0.1.1 (bundle-level change log)."""
        changelog = {
            "bundle_run_id": run_context.run_id,
            "entries": [
                {
                    "date": date.today().isoformat(),
                    "version": "1.0.0",
                    "author": "AIMO Analysis Engine",
                    "summary": f"Initial Evidence Bundle for run {run_context.run_id}",
                }
            ],
        }
        path = payloads_dir / "change_log.json"
        self._write_json_atomic(path, changelog)
        return path
    
    def _aggregate_codes(self, analysis_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Aggregate codes from all analysis data."""
        codes = {
            "FS": set(),
            "IM": set(),
            "UC": set(),
            "DT": set(),
            "CH": set(),
            "RS": set(),
            "LG": set(),
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
            for code in item.get("lg_codes", []):
                codes["LG"].add(code)
            for code in item.get("ob_codes", []):
                codes["OB"].add(code)
        
        # Convert sets to sorted lists, ensure at least one code per required dimension (Standard 0.1.1: LG)
        result = {}
        for dim in ["FS", "IM", "UC", "DT", "CH", "RS", "LG"]:
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
