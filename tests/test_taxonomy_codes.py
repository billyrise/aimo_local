"""
Tests for Taxonomyセット (7 codes) support.

Tests:
- 7 codes are required in LLM JSON Schema
- 7 codes are saved to analysis_cache and signature_stats
- Evidence Pack includes 7 codes (列欠落禁止)
- Versioning consistency (taxonomy_version/evidence_pack_version/engine_spec_version)
"""

import pytest
import json
from pathlib import Path
from datetime import datetime
import duckdb

from src.db.duckdb_client import DuckDBClient
from src.orchestrator import Orchestrator
from src.classifiers.rule_classifier import RuleClassifier
from src.reporting.evidence_pack_generator import EvidencePackGenerator


@pytest.fixture
def temp_db(tmp_path):
    """Create temporary DuckDB database."""
    db_path = tmp_path / "test.duckdb"
    client = DuckDBClient(str(db_path))
    yield client
    client.close()


@pytest.fixture
def sample_run_context(temp_db, tmp_path):
    """Create sample run context with Taxonomy versions."""
    work_dir = tmp_path / "work"
    orchestrator = Orchestrator(
        db_client=temp_db,
        work_base_dir=work_dir,
        taxonomy_version="1.0",
        evidence_pack_version="1.0",
        engine_spec_version="1.4"
    )
    
    # Create a dummy run
    input_files = [tmp_path / "test_input.csv"]
    input_files[0].touch()
    
    run_context = orchestrator.get_or_create_run(input_files)
    return orchestrator, run_context


def test_llm_schema_includes_taxonomy_codes():
    """Test that LLM JSON Schema includes all 7 taxonomy codes as required."""
    schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    
    # Check required fields
    required = schema.get("required", [])
    assert "fs_uc_code" in required, "fs_uc_code must be required"
    assert "dt_code" in required, "dt_code must be required"
    assert "ch_code" in required, "ch_code must be required"
    assert "im_code" in required, "im_code must be required"
    assert "rs_code" in required, "rs_code must be required"
    assert "ob_code" in required, "ob_code must be required"
    assert "ev_code" in required, "ev_code must be required"
    assert "taxonomy_version" in required, "taxonomy_version must be required"
    
    # Check properties exist
    properties = schema.get("properties", {})
    assert "fs_uc_code" in properties
    assert "dt_code" in properties
    assert "ch_code" in properties
    assert "im_code" in properties
    assert "rs_code" in properties
    assert "ob_code" in properties
    assert "ev_code" in properties
    assert "taxonomy_version" in properties


def test_rule_classifier_returns_taxonomy_codes():
    """Test that RuleClassifier returns taxonomy codes (even if empty)."""
    classifier = RuleClassifier()
    
    # Test classification (should include taxonomy codes)
    classification = classifier.classify(
        url_signature="test_sig",
        norm_host="example.com",
        norm_path_template="/test"
    )
    
    if classification:
        # Check that taxonomy codes are present (列欠落禁止)
        assert "fs_uc_code" in classification
        assert "dt_code" in classification
        assert "ch_code" in classification
        assert "im_code" in classification
        assert "rs_code" in classification
        assert "ob_code" in classification
        assert "ev_code" in classification


def test_analysis_cache_saves_taxonomy_codes(temp_db, sample_run_context):
    """Test that analysis_cache saves taxonomy codes."""
    orchestrator, run_context = sample_run_context
    
    # Insert test data with taxonomy codes
    test_signature = "test_signature_123"
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Test Service",
        "usage_type": "business",
        "risk_level": "low",
        "category": "Test",
        "confidence": 0.9,
        "rationale_short": "Test classification",
        "classification_source": "RULE",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "FS-UC-001",
        "dt_code": "DT-001",
        "ch_code": "CH-001",
        "im_code": "IM-001",
        "rs_code": "RS-001",
        "ob_code": "OB-001",
        "ev_code": "EV-001",
        "status": "active",
        "is_human_verified": False,
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Verify taxonomy codes are saved
    reader = temp_db.get_reader()
    result = reader.execute(
        "SELECT fs_uc_code, dt_code, ch_code, im_code, rs_code, ob_code, ev_code, taxonomy_version FROM analysis_cache WHERE url_signature = ?",
        [test_signature]
    ).fetchone()
    
    assert result is not None
    assert result[0] == "FS-UC-001"
    assert result[1] == "DT-001"
    assert result[2] == "CH-001"
    assert result[3] == "IM-001"
    assert result[4] == "RS-001"
    assert result[5] == "OB-001"
    assert result[6] == "EV-001"
    assert result[7] == "1.0"


def test_evidence_pack_includes_taxonomy_codes(temp_db, sample_run_context, tmp_path):
    """Test that Evidence Pack includes all 7 taxonomy codes (列欠落禁止)."""
    orchestrator, run_context = sample_run_context
    
    # Insert test data
    test_signature = "test_signature_123"
    temp_db.upsert("signature_stats", {
        "run_id": run_context.run_id,
        "url_signature": test_signature,
        "norm_host": "example.com",
        "norm_path_template": "/test",
        "bytes_sent_sum": 1000,
        "access_count": 10,
        "unique_users": 1,
        "fs_uc_code": "FS-UC-001",
        "dt_code": "DT-001",
        "ch_code": "CH-001",
        "im_code": "IM-001",
        "rs_code": "RS-001",
        "ob_code": "OB-001",
        "ev_code": "EV-001",
        "taxonomy_version": "1.0"
    }, conflict_key="run_id, url_signature")
    
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Test Service",
        "usage_type": "business",
        "risk_level": "low",
        "category": "Test",
        "confidence": 0.9,
        "rationale_short": "Test",
        "classification_source": "RULE",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "FS-UC-001",
        "dt_code": "DT-001",
        "ch_code": "CH-001",
        "im_code": "IM-001",
        "rs_code": "RS-001",
        "ob_code": "OB-001",
        "ev_code": "EV-001",
        "status": "active",
        "is_human_verified": False,
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Generate Evidence Pack
    output_dir = tmp_path / "output" / run_context.run_id
    evidence_pack_dir = output_dir / "evidence_pack"
    generator = EvidencePackGenerator(evidence_pack_dir)
    
    paths = generator.generate_evidence_pack(
        run_id=run_context.run_id,
        db_reader=temp_db.get_reader(),
        taxonomy_version="1.0",
        evidence_pack_version="1.0",
        engine_spec_version="1.4"
    )
    
    # Verify JSON includes taxonomy codes
    with open(paths["json_path"], 'r', encoding='utf-8') as f:
        evidence_pack = json.load(f)
    
    assert "signatures" in evidence_pack
    assert len(evidence_pack["signatures"]) > 0
    
    sig = evidence_pack["signatures"][0]
    # Check that all 7 codes are present (列欠落禁止)
    assert "fs_uc_code" in sig
    assert "dt_code" in sig
    assert "ch_code" in sig
    assert "im_code" in sig
    assert "rs_code" in sig
    assert "ob_code" in sig
    assert "ev_code" in sig
    assert "taxonomy_version" in sig


def test_run_manifest_includes_all_versions(temp_db, sample_run_context, tmp_path):
    """Test that run_manifest.json includes all version information."""
    orchestrator, run_context = sample_run_context
    
    output_dir = tmp_path / "output" / run_context.run_id
    evidence_pack_dir = output_dir / "evidence_pack"
    generator = EvidencePackGenerator(evidence_pack_dir)
    
    manifest_path = generator.generate_run_manifest(
        run_id=run_context.run_id,
        run_key=run_context.run_key,
        started_at=run_context.started_at,
        finished_at=datetime.utcnow(),
        signature_version="1.0",
        rule_version="1",
        prompt_version="1",
        taxonomy_version="1.0",
        evidence_pack_version="1.0",
        engine_spec_version="1.4"
    )
    
    # Verify manifest includes all versions
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    
    assert "versions" in manifest
    versions = manifest["versions"]
    assert "engine_spec_version" in versions
    assert "taxonomy_version" in versions
    assert "evidence_pack_version" in versions
    assert "signature_version" in versions
    assert "rule_version" in versions
    assert "prompt_version" in versions
    
    assert versions["engine_spec_version"] == "1.4"
    assert versions["taxonomy_version"] == "1.0"
    assert versions["evidence_pack_version"] == "1.0"


def test_run_key_includes_taxonomy_versions():
    """Test that run_key calculation includes taxonomy versions."""
    from src.orchestrator import Orchestrator
    from src.db.duckdb_client import DuckDBClient
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        db_client = DuckDBClient(str(db_path))
        
        orchestrator1 = Orchestrator(
            db_client=db_client,
            work_base_dir=Path(tmpdir) / "work",
            taxonomy_version="1.0",
            evidence_pack_version="1.0",
            engine_spec_version="1.4"
        )
        
        orchestrator2 = Orchestrator(
            db_client=db_client,
            work_base_dir=Path(tmpdir) / "work",
            taxonomy_version="2.0",  # Different version
            evidence_pack_version="1.0",
            engine_spec_version="1.4"
        )
        
        # Same input manifest hash
        input_manifest_hash = "test_hash"
        
        run_key1 = orchestrator1.compute_run_key(input_manifest_hash)
        run_key2 = orchestrator2.compute_run_key(input_manifest_hash)
        
        # Different taxonomy versions should produce different run_keys
        assert run_key1 != run_key2, "Different taxonomy versions should produce different run_keys"
        
        db_client.close()
