"""
Tests for AIMO Standard v0.1.1+ compliant Evidence Bundle generation.

Tests:
- Bundle can be generated from sample data
- run_manifest.json is included with Standard version info
- evidence_pack_manifest.json follows Standard schema
- validator_runner executes and result file is included
- checksums.json contains all generated files
"""

import pytest
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duckdb_client import DuckDBClient


@dataclass
class MockRunContext:
    """Mock run context for testing."""
    run_id: str = "test_run_001"
    run_key: str = "test_run_key_hash"
    started_at: datetime = None
    status: str = "running"
    input_manifest_hash: str = "abc123"
    signature_version: str = "1.0"
    rule_version: str = "1"
    prompt_version: str = "1"
    taxonomy_version: str = "0.1.1"
    evidence_pack_version: str = "1.0"
    engine_spec_version: str = "1.5"
    code_version: str = "test"
    work_dir: Path = None
    standard_info: object = None
    
    def __post_init__(self):
        if self.started_at is None:
            self.started_at = datetime.utcnow()


@pytest.fixture
def temp_db(tmp_path):
    """Create temporary DuckDB database with test data."""
    db_path = tmp_path / "test.duckdb"
    client = DuckDBClient(str(db_path))
    yield client
    client.close()


@pytest.fixture
def sample_run_context(tmp_path):
    """Create a sample run context."""
    work_dir = tmp_path / "work" / "test_run_001"
    work_dir.mkdir(parents=True, exist_ok=True)
    
    ctx = MockRunContext(work_dir=work_dir)
    return ctx


@pytest.fixture
def db_with_test_data(temp_db, sample_run_context):
    """Create DB with test analysis data."""
    run_id = sample_run_context.run_id
    
    # Insert run record
    temp_db.upsert("runs", {
        "run_id": run_id,
        "run_key": sample_run_context.run_key,
        "started_at": datetime.utcnow().isoformat(),
        "status": "running",
        "input_manifest_hash": sample_run_context.input_manifest_hash,
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1"
    }, conflict_key="run_id")
    
    # Insert signature_stats
    temp_db.upsert("signature_stats", {
        "run_id": run_id,
        "url_signature": "test_sig_001",
        "norm_host": "chat.openai.com",
        "norm_path_template": "/api/chat",
        "dest_domain": "openai.com",
        "bytes_sent_sum": 10000,
        "access_count": 100,
        "unique_users": 5,
        "candidate_flags": "A|genai"
    }, conflict_key="run_id, url_signature")
    
    temp_db.upsert("signature_stats", {
        "run_id": run_id,
        "url_signature": "test_sig_002",
        "norm_host": "www.google.com",
        "norm_path_template": "/search",
        "dest_domain": "google.com",
        "bytes_sent_sum": 5000,
        "access_count": 50,
        "unique_users": 3,
        "candidate_flags": "B"
    }, conflict_key="run_id, url_signature")
    
    # Insert analysis_cache
    temp_db.upsert("analysis_cache", {
        "url_signature": "test_sig_001",
        "service_name": "ChatGPT",
        "category": "GenAI",
        "usage_type": "genai",
        "risk_level": "high",
        "confidence": 0.95,
        "classification_source": "LLM",
        "rationale_short": "OpenAI ChatGPT service",
        "fs_code": "FS-001",
        "im_code": "IM-001",
        "uc_codes_json": '["UC-001"]',
        "dt_codes_json": '["DT-001"]',
        "ch_codes_json": '["CH-001"]',
        "rs_codes_json": '["RS-001"]',
        "ev_codes_json": '["LG-001"]',
        "ob_codes_json": '[]',
        "taxonomy_schema_version": "0.1.1",
        "status": "active"
    }, conflict_key="url_signature")
    
    temp_db.upsert("analysis_cache", {
        "url_signature": "test_sig_002",
        "service_name": "Google Search",
        "category": "Search",
        "usage_type": "business",
        "risk_level": "low",
        "confidence": 0.98,
        "classification_source": "RULE",
        "rationale_short": "Google search engine",
        "fs_code": "FS-002",
        "im_code": "IM-002",
        "uc_codes_json": '["UC-002"]',
        "dt_codes_json": '["DT-002"]',
        "ch_codes_json": '["CH-001"]',
        "rs_codes_json": '["RS-002"]',
        "ev_codes_json": '["LG-001"]',
        "ob_codes_json": '[]',
        "taxonomy_schema_version": "0.1.1",
        "status": "active"
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    return temp_db


class TestBundleGeneration:
    """Tests for Evidence Bundle generation."""
    
    def test_generator_imports(self):
        """Test that the generator can be imported."""
        from reporting.standard_evidence_bundle_generator import (
            StandardEvidenceBundleGenerator,
            BundleGenerationResult
        )
        
        assert StandardEvidenceBundleGenerator is not None
        assert BundleGenerationResult is not None
    
    def test_generator_initialization(self):
        """Test generator initialization."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        assert generator.aimo_standard_version == "0.1.1"
    
    def test_bundle_generation(self, db_with_test_data, sample_run_context, tmp_path):
        """Test that bundle can be generated from sample data."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=True
        )
        
        # Check result structure
        assert result.bundle_path is not None
        assert result.bundle_path.exists()
        assert result.run_manifest_path.exists()
        assert result.evidence_pack_manifest_path.exists()
        assert result.checksums_path.exists()
        assert result.validation_result_path.exists()
    
    def test_run_manifest_contains_standard_version(self, db_with_test_data, sample_run_context):
        """Test that run_manifest.json contains Standard version info."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Read run_manifest.json
        with open(result.run_manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        # Check Standard version info
        assert "aimo_standard" in manifest
        assert manifest["aimo_standard"]["version"] == "0.1.1"
        
        # Check run metadata
        assert "run_id" in manifest
        assert "run_key" in manifest
        assert "input_manifest_hash" in manifest
        assert "versions" in manifest
    
    def test_evidence_pack_manifest_structure(self, db_with_test_data, sample_run_context):
        """Test that evidence_pack_manifest.json follows Standard schema."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Read evidence_pack_manifest.json
        with open(result.evidence_pack_manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        # Check required fields per Standard schema
        assert "pack_id" in manifest
        assert "pack_version" in manifest
        assert "created_date" in manifest
        assert "last_updated" in manifest
        assert "taxonomy_version" in manifest
        assert "codes" in manifest
        assert "evidence_files" in manifest
        
        # Check codes structure (8 dimensions)
        codes = manifest["codes"]
        assert "FS" in codes
        assert "UC" in codes
        assert "DT" in codes
        assert "CH" in codes
        assert "IM" in codes
        assert "RS" in codes
        assert "LG" in codes
    
    def test_validation_result_included(self, db_with_test_data, sample_run_context):
        """Test that validation_result.json is included in bundle."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Check validation_result.json exists
        assert result.validation_result_path.exists()
        
        # Read validation result
        with open(result.validation_result_path, 'r', encoding='utf-8') as f:
            validation = json.load(f)
        
        assert "passed" in validation
        assert "status" in validation
        assert "aimo_standard_version" in validation
        assert "errors" in validation
        assert "error_count" in validation
    
    def test_checksums_contains_all_files(self, db_with_test_data, sample_run_context):
        """Test that checksums.json contains all generated files."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Read checksums.json
        with open(result.checksums_path, 'r', encoding='utf-8') as f:
            checksums = json.load(f)
        
        assert "algorithm" in checksums
        assert checksums["algorithm"] == "SHA-256"
        assert "files" in checksums
        
        # Check that key files have checksums
        files = checksums["files"]
        assert len(files) > 0
        
        # Verify checksums are SHA-256 format (64 hex chars)
        for file_path, checksum in files.items():
            assert len(checksum) == 64
            assert all(c in "0123456789abcdef" for c in checksum)
    
    def test_shadow_ai_discovery_log_generated(self, db_with_test_data, sample_run_context):
        """Test that shadow_ai_discovery.jsonl is generated."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Check logs directory exists (v0.1: under payloads/)
        logs_dir = result.bundle_path / "payloads" / "logs"
        assert logs_dir.exists()
        
        # Check shadow_ai_discovery.jsonl exists (file should be created even if empty)
        shadow_ai_log = logs_dir / "shadow_ai_discovery.jsonl"
        assert shadow_ai_log.exists()
        
        # Read log entries (may be empty if no GenAI data in test DB)
        with open(shadow_ai_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # If there are entries, validate structure
        if len(lines) > 0:
            entry = json.loads(lines[0])
            assert "event_time" in entry
            assert "actor_id" in entry
            assert "ai_service" in entry
            assert "decision" in entry
            assert "record_id" in entry

    def test_v01_root_structure(self, db_with_test_data, sample_run_context):
        """Test that bundle has v0.1 root structure: manifest.json, objects/, payloads/, signatures/, hashes/."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator

        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False,
        )
        root = result.bundle_path
        assert (root / "manifest.json").exists()
        assert (root / "objects").is_dir()
        assert (root / "objects" / "index.json").exists()
        assert (root / "payloads").is_dir()
        assert (root / "signatures").is_dir()
        assert (root / "hashes").is_dir()
        with open(root / "manifest.json", "r", encoding="utf-8") as f:
            manifest = json.load(f)
        assert "bundle_id" in manifest
        assert "object_index" in manifest
        assert "payload_index" in manifest
        assert "hash_chain" in manifest
        assert "signing" in manifest
        assert manifest["hash_chain"]["covers"] == ["manifest.json", "objects/index.json"]
        assert len(manifest["signing"]["signatures"]) >= 1
        assert any("manifest.json" in s.get("targets", []) for s in manifest["signing"]["signatures"])

    def test_v01_payloads_include_dictionary_summary_changelog(self, db_with_test_data, sample_run_context):
        """Test that payloads include dictionary.json, summary.json, change_log.json (Standard 0.1.1 Phase 3)."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator

        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False,
        )
        payloads = result.bundle_path / "payloads"
        assert (payloads / "summary.json").exists(), "payloads/summary.json required (Phase 3)"
        assert (payloads / "change_log.json").exists(), "payloads/change_log.json required (Phase 3)"
        # dictionary.json is optional when Standard artifacts are unavailable
        if (payloads / "dictionary.json").exists():
            with open(payloads / "summary.json", "r", encoding="utf-8") as f:
                summary = json.load(f)
            assert "run_id" in summary and "total_signatures" in summary


class TestBundleValidation:
    """Tests for bundle validation."""
    
    def test_validation_runs(self, db_with_test_data, sample_run_context):
        """Test that validation runs and produces result."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Validation should have run
        assert isinstance(result.validation_passed, bool)
        assert isinstance(result.validation_errors, list)
    
    def test_validation_errors_logged(self, db_with_test_data, sample_run_context):
        """Test that validation errors are properly logged."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Check validation result file contains error info
        with open(result.validation_result_path, 'r', encoding='utf-8') as f:
            validation = json.load(f)
        
        assert validation["error_count"] == len(validation["errors"])


class TestDerivedOutputs:
    """Tests for derived (legacy) outputs."""
    
    def test_derived_outputs_generated(self, db_with_test_data, sample_run_context):
        """Test that derived outputs are generated when requested."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=True
        )
        
        # Check derived directory exists
        # v0.1: derived outputs under payloads/
        derived_dir = result.bundle_path / "payloads" / "derived"
        assert derived_dir.exists()
    
    def test_derived_outputs_skipped_when_disabled(self, db_with_test_data, sample_run_context):
        """Test that derived outputs are skipped when disabled."""
        from reporting.standard_evidence_bundle_generator import StandardEvidenceBundleGenerator
        
        generator = StandardEvidenceBundleGenerator(aimo_standard_version="0.1.1")
        
        result = generator.generate(
            run_context=sample_run_context,
            output_dir=sample_run_context.work_dir,
            db_reader=db_with_test_data.get_reader(),
            include_derived=False
        )
        
        # Derived directory should not be in files list
        derived_files = [f for f in result.files_generated if "derived" in f]
        assert len(derived_files) == 0


class TestOrchestratorIntegration:
    """Tests for Orchestrator integration."""
    
    def test_orchestrator_has_generate_evidence_bundle(self):
        """Test that Orchestrator has generate_evidence_bundle method."""
        from orchestrator import Orchestrator
        
        assert hasattr(Orchestrator, 'generate_evidence_bundle')
