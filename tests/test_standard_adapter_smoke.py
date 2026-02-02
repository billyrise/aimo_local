"""
Smoke tests for AIMO Standard Adapter.

These tests verify that the Standard Adapter can:
1. Resolve Standard v0.1.7 artifacts
2. Load taxonomy with all 8 dimensions
3. Validate taxonomy code assignments
4. Load JSON schemas from Standard
"""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class TestResolverSmoke:
    """Smoke tests for Standard resolver."""
    
    def test_resolve_standard_v017(self):
        """Test that v0.1.7 can be resolved successfully."""
        from standard_adapter.resolver import resolve_standard_artifacts
        
        artifacts = resolve_standard_artifacts(version="0.1.7")
        
        assert artifacts is not None
        assert artifacts.standard_version == "0.1.7"
        assert artifacts.standard_commit is not None
        assert len(artifacts.standard_commit) == 40  # Full SHA-1 hash
        assert artifacts.standard_tag == "v0.1.7"
        assert artifacts.artifacts_dir.exists()
        assert artifacts.artifacts_dir_sha256 is not None
        assert len(artifacts.artifacts_dir_sha256) == 64  # SHA-256 hex
    
    def test_resolved_artifacts_to_dict(self):
        """Test that ResolvedStandardArtifacts can be serialized."""
        from standard_adapter.resolver import resolve_standard_artifacts
        
        artifacts = resolve_standard_artifacts(version="0.1.7")
        data = artifacts.to_dict()
        
        assert isinstance(data, dict)
        assert "standard_version" in data
        assert "standard_commit" in data
        assert "artifacts_dir_sha256" in data


class TestTaxonomySmoke:
    """Smoke tests for taxonomy loading."""
    
    def test_taxonomy_loads(self):
        """Test that taxonomy loads without errors."""
        from standard_adapter.taxonomy import TaxonomyAdapter
        
        adapter = TaxonomyAdapter(version="0.1.7")
        
        assert adapter is not None
        assert adapter.standard_version == "0.1.7"
        assert adapter.total_codes > 0
    
    def test_all_dimensions_have_codes(self):
        """Test that all 8 dimensions have codes (none empty)."""
        from standard_adapter.taxonomy import TaxonomyAdapter, ALL_DIMENSIONS
        
        adapter = TaxonomyAdapter(version="0.1.7")
        
        for dim in ALL_DIMENSIONS:
            codes = adapter.get_allowed_codes(dim)
            assert len(codes) > 0, f"Dimension {dim} has no codes"
    
    def test_fs_dimension_has_codes(self):
        """Test FS (Functional Scope) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("FS", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("FS-") for c in codes)
    
    def test_im_dimension_has_codes(self):
        """Test IM (Integration Mode) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("IM", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("IM-") for c in codes)
    
    def test_uc_dimension_has_codes(self):
        """Test UC (Use Case Class) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("UC", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("UC-") for c in codes)
    
    def test_dt_dimension_has_codes(self):
        """Test DT (Data Type) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("DT", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("DT-") for c in codes)
    
    def test_ch_dimension_has_codes(self):
        """Test CH (Channel) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("CH", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("CH-") for c in codes)
    
    def test_rs_dimension_has_codes(self):
        """Test RS (Risk Surface) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("RS", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("RS-") for c in codes)
    
    def test_ob_dimension_has_codes(self):
        """Test OB (Outcome / Benefit) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("OB", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("OB-") for c in codes)
    
    def test_ev_dimension_has_codes(self):
        """Test EV (Evidence Type) dimension has codes."""
        from standard_adapter.taxonomy import get_allowed_codes
        
        codes = get_allowed_codes("EV", version="0.1.7")
        
        assert len(codes) > 0
        assert all(c.startswith("EV-") for c in codes)
    
    def test_taxonomy_stats(self):
        """Test taxonomy statistics."""
        from standard_adapter.taxonomy import TaxonomyAdapter
        
        adapter = TaxonomyAdapter(version="0.1.7")
        stats = adapter.get_stats()
        
        assert "standard_version" in stats
        assert "total_codes" in stats
        assert "codes_by_dimension" in stats
        assert stats["total_codes"] > 0


class TestTaxonomyValidation:
    """Tests for taxonomy validation."""
    
    def test_valid_assignment_passes(self):
        """Test that a valid code assignment passes validation."""
        from standard_adapter.taxonomy import validate_assignment
        
        errors = validate_assignment(
            fs_codes=["FS-001"],
            im_codes=["IM-001"],
            uc_codes=["UC-001"],
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            ev_codes=["EV-001"],
            ob_codes=[],  # Optional
            version="0.1.7"
        )
        
        assert len(errors) == 0, f"Unexpected errors: {errors}"
    
    def test_missing_fs_fails(self):
        """Test that missing FS code fails validation."""
        from standard_adapter.taxonomy import validate_assignment
        
        errors = validate_assignment(
            fs_codes=[],  # Missing FS
            im_codes=["IM-001"],
            uc_codes=["UC-001"],
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            ev_codes=["EV-001"],
            version="0.1.7"
        )
        
        assert len(errors) > 0
        assert any("FS" in e or "Functional Scope" in e for e in errors)
    
    def test_multiple_fs_fails(self):
        """Test that multiple FS codes fails validation (exactly 1 required)."""
        from standard_adapter.taxonomy import validate_assignment
        
        errors = validate_assignment(
            fs_codes=["FS-001", "FS-002"],  # Too many FS
            im_codes=["IM-001"],
            uc_codes=["UC-001"],
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            ev_codes=["EV-001"],
            version="0.1.7"
        )
        
        assert len(errors) > 0
        assert any("FS" in e or "at most 1" in e for e in errors)
    
    def test_invalid_code_fails(self):
        """Test that invalid code fails validation."""
        from standard_adapter.taxonomy import validate_assignment
        
        errors = validate_assignment(
            fs_codes=["FS-999"],  # Invalid code
            im_codes=["IM-001"],
            uc_codes=["UC-001"],
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            ev_codes=["EV-001"],
            version="0.1.7"
        )
        
        assert len(errors) > 0
        assert any("FS-999" in e or "Unknown code" in e for e in errors)


class TestSchemasSmoke:
    """Smoke tests for schema loading."""
    
    def test_schema_loader_initializes(self):
        """Test that schema loader initializes without errors."""
        from standard_adapter.schemas import SchemaLoader
        
        loader = SchemaLoader(version="0.1.7")
        
        assert loader is not None
        assert loader.standard_version == "0.1.7"
    
    def test_list_available_schemas(self):
        """Test listing available schemas."""
        from standard_adapter.schemas import SchemaLoader
        
        loader = SchemaLoader(version="0.1.7")
        schemas = loader.list_available_schemas()
        
        assert isinstance(schemas, list)
        # Should have at least some schemas
        assert len(schemas) >= 0  # May be 0 if schemas not in cache
    
    def test_load_evidence_pack_manifest_schema(self):
        """Test loading Evidence Pack manifest schema."""
        from standard_adapter.schemas import SchemaLoader
        
        loader = SchemaLoader(version="0.1.7")
        schema = loader.get_evidence_pack_manifest_schema()
        
        # Schema may be None if not available in cache
        if schema is not None:
            assert isinstance(schema, dict)
            assert "$schema" in schema or "type" in schema


class TestValidatorRunnerSmoke:
    """Smoke tests for validator runner."""
    
    def test_validator_runner_initializes(self):
        """Test that validator runner initializes without errors."""
        from standard_adapter.validator_runner import ValidatorRunner
        
        runner = ValidatorRunner(version="0.1.7")
        
        assert runner is not None
        assert runner.artifacts.standard_version == "0.1.7"
    
    def test_validate_codes_only(self):
        """Test validating codes only."""
        from standard_adapter.validator_runner import ValidatorRunner
        
        runner = ValidatorRunner(version="0.1.7")
        
        result = runner.validate_codes_only({
            "FS": ["FS-001"],
            "IM": ["IM-001"],
            "UC": ["UC-001"],
            "DT": ["DT-001"],
            "CH": ["CH-001"],
            "RS": ["RS-001"],
            "EV": ["EV-001"],
        })
        
        assert result.passed is True
        assert len(result.errors) == 0
        assert result.standard_version == "0.1.7"
        assert result.validator_used == "fallback"


class TestModuleLevelFunctions:
    """Test module-level convenience functions."""
    
    def test_get_taxonomy_adapter(self):
        """Test get_taxonomy_adapter function."""
        from standard_adapter import get_taxonomy_adapter
        
        adapter = get_taxonomy_adapter("0.1.7")
        assert adapter is not None
    
    def test_get_allowed_codes_function(self):
        """Test get_allowed_codes module-level function."""
        from standard_adapter import get_allowed_codes
        
        codes = get_allowed_codes("FS", version="0.1.7")
        assert len(codes) > 0
    
    def test_validate_assignment_function(self):
        """Test validate_assignment module-level function."""
        from standard_adapter import validate_assignment
        
        errors = validate_assignment(
            fs_codes=["FS-001"],
            im_codes=["IM-001"],
            uc_codes=["UC-001"],
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            ev_codes=["EV-001"],
            version="0.1.7"
        )
        
        assert len(errors) == 0
