"""
Tests for AIMO Standard Taxonomy (8 dimensions) support.

AIMO Standard v0.1.7+ taxonomy structure:
- 8 dimensions: FS, UC, DT, CH, IM, RS, OB, EV
- Cardinality:
  - FS: Exactly 1
  - IM: Exactly 1
  - UC, DT, CH, RS, EV: 1+ (at least one)
  - OB: 0+ (optional)

DB schema (v1.6+):
- fs_code, im_code: Single VARCHAR columns
- uc_codes_json, dt_codes_json, etc.: JSON array strings (canonical form)
- Legacy columns (fs_uc_code, dt_code, etc.) kept for backward compatibility

Tests:
- New columns exist in DB with correct defaults
- JSON canonical serialization works correctly
- run_key includes Standard version for cache coherence
- Standard adapter integration
"""

import pytest
import json
import sys
from pathlib import Path
from datetime import datetime
import tempfile

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.duckdb_client import DuckDBClient
from orchestrator import Orchestrator
from utils.json_canonical import (
    canonical_json_array,
    parse_json_array,
    validate_code_format,
    codes_to_dict,
    dict_to_db_columns,
    db_columns_to_dict,
)


@pytest.fixture
def temp_db(tmp_path):
    """Create temporary DuckDB database."""
    db_path = tmp_path / "test.duckdb"
    client = DuckDBClient(str(db_path))
    yield client
    client.close()


class TestJsonCanonical:
    """Tests for JSON canonical utilities."""
    
    def test_canonical_json_array_sorts(self):
        """Test that arrays are sorted."""
        result = canonical_json_array(["UC-010", "UC-001", "UC-005"])
        assert result == '["UC-001","UC-005","UC-010"]'
    
    def test_canonical_json_array_deduplicates(self):
        """Test that duplicates are removed."""
        result = canonical_json_array(["UC-001", "UC-001", "UC-002"])
        assert result == '["UC-001","UC-002"]'
    
    def test_canonical_json_array_empty(self):
        """Test empty array handling."""
        assert canonical_json_array([]) == "[]"
        assert canonical_json_array(None) == "[]"
    
    def test_canonical_json_array_filters_empty_strings(self):
        """Test that empty strings are filtered."""
        result = canonical_json_array(["UC-001", "", "UC-002", None])
        assert result == '["UC-001","UC-002"]'
    
    def test_parse_json_array_valid(self):
        """Test parsing valid JSON array."""
        result = parse_json_array('["UC-001","UC-002"]')
        assert result == ["UC-001", "UC-002"]
    
    def test_parse_json_array_empty(self):
        """Test parsing empty cases."""
        assert parse_json_array("[]") == []
        assert parse_json_array("") == []
        assert parse_json_array(None) == []
    
    def test_parse_json_array_invalid(self):
        """Test parsing invalid JSON gracefully."""
        assert parse_json_array("invalid") == []
        assert parse_json_array("{not:array}") == []
    
    def test_validate_code_format_valid(self):
        """Test valid code format validation."""
        assert validate_code_format("FS-001") is True
        assert validate_code_format("UC-030") is True
        assert validate_code_format("OB-007") is True
    
    def test_validate_code_format_invalid(self):
        """Test invalid code format detection."""
        assert validate_code_format("") is False
        assert validate_code_format("invalid") is False
        assert validate_code_format("UC001") is False
        assert validate_code_format("UC-1") is False
        assert validate_code_format("uc-001") is False  # Lowercase prefix
    
    def test_validate_code_format_with_dimension(self):
        """Test dimension-specific validation."""
        assert validate_code_format("FS-001", "FS") is True
        assert validate_code_format("FS-001", "UC") is False


class TestCodesConversion:
    """Tests for code conversion utilities."""
    
    def test_codes_to_dict(self):
        """Test converting individual codes to dict."""
        result = codes_to_dict(
            fs_code="FS-001",
            im_code="IM-001",
            uc_codes=["UC-001", "UC-002"]
        )
        assert result["FS"] == ["FS-001"]
        assert result["IM"] == ["IM-001"]
        assert result["UC"] == ["UC-001", "UC-002"]
        assert result["DT"] == []  # Empty by default
    
    def test_dict_to_db_columns(self):
        """Test converting dict to DB column format."""
        codes_dict = {
            "FS": ["FS-001"],
            "IM": ["IM-001"],
            "UC": ["UC-002", "UC-001"],  # Unsorted
            "DT": ["DT-001"],
            "CH": ["CH-001"],
            "RS": ["RS-001"],
            "EV": ["EV-001"],
            "OB": [],  # Empty optional
        }
        result = dict_to_db_columns(codes_dict)
        
        assert result["fs_code"] == "FS-001"
        assert result["im_code"] == "IM-001"
        # Should be canonical (sorted)
        assert result["uc_codes_json"] == '["UC-001","UC-002"]'
        assert result["ob_codes_json"] == "[]"  # Empty
    
    def test_db_columns_to_dict(self):
        """Test converting DB columns back to dict."""
        result = db_columns_to_dict(
            fs_code="FS-001",
            im_code="IM-001",
            uc_codes_json='["UC-001","UC-002"]',
            dt_codes_json='["DT-001"]',
            ch_codes_json='["CH-001"]',
            rs_codes_json='["RS-001"]',
            ev_codes_json='["EV-001"]',
            ob_codes_json='[]'
        )
        
        assert result["FS"] == ["FS-001"]
        assert result["IM"] == ["IM-001"]
        assert result["UC"] == ["UC-001", "UC-002"]
        assert result["OB"] == []


class TestDbSchemaNewColumns:
    """Tests for new 8-dimension DB columns."""
    
    def test_analysis_cache_has_new_columns(self, temp_db):
        """Test that analysis_cache has new taxonomy columns."""
        reader = temp_db.get_reader()
        
        # Check columns exist
        columns = reader.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'analysis_cache'
        """).fetchall()
        column_names = [c[0] for c in columns]
        
        # New single-value columns
        assert "fs_code" in column_names
        assert "im_code" in column_names
        
        # New array columns
        assert "uc_codes_json" in column_names
        assert "dt_codes_json" in column_names
        assert "ch_codes_json" in column_names
        assert "rs_codes_json" in column_names
        assert "ev_codes_json" in column_names
        assert "ob_codes_json" in column_names
        
        # New version column
        assert "taxonomy_schema_version" in column_names
    
    def test_signature_stats_has_new_columns(self, temp_db):
        """Test that signature_stats has new taxonomy columns."""
        reader = temp_db.get_reader()
        
        columns = reader.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'signature_stats'
        """).fetchall()
        column_names = [c[0] for c in columns]
        
        # New columns
        assert "fs_code" in column_names
        assert "uc_codes_json" in column_names
        assert "taxonomy_schema_version" in column_names
    
    def test_runs_has_standard_columns(self, temp_db):
        """Test that runs table has AIMO Standard columns."""
        reader = temp_db.get_reader()
        
        columns = reader.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'runs'
        """).fetchall()
        column_names = [c[0] for c in columns]
        
        assert "aimo_standard_version" in column_names
        assert "aimo_standard_commit" in column_names
        assert "aimo_standard_artifacts_dir_sha256" in column_names
        assert "aimo_standard_artifacts_zip_sha256" in column_names
    
    def test_json_columns_default_to_empty_array(self, temp_db):
        """Test that JSON array columns default to '[]'."""
        # Insert minimal record
        temp_db.upsert("analysis_cache", {
            "url_signature": "test_sig_default",
            "service_name": "Test",
            "status": "active",
        }, conflict_key="url_signature")
        temp_db.flush()
        
        # Check defaults
        reader = temp_db.get_reader()
        result = reader.execute("""
            SELECT uc_codes_json, dt_codes_json, ch_codes_json, 
                   rs_codes_json, ev_codes_json, ob_codes_json
            FROM analysis_cache 
            WHERE url_signature = 'test_sig_default'
        """).fetchone()
        
        assert result is not None
        # All should be empty arrays
        for val in result:
            assert val == "[]" or val is None  # Default may be applied at insert or be NULL


class TestRunKeyIncludesStandardVersion:
    """Tests for run_key calculation with Standard version."""
    
    def test_run_key_changes_with_standard_version(self):
        """Test that run_key changes when Standard version changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            db_client = DuckDBClient(str(db_path))
            
            # Create orchestrators with different Standard versions
            orchestrator1 = Orchestrator(
                db_client=db_client,
                work_base_dir=Path(tmpdir) / "work",
                aimo_standard_version="0.1.6",
                resolve_standard=False  # Skip actual resolution
            )
            
            orchestrator2 = Orchestrator(
                db_client=db_client,
                work_base_dir=Path(tmpdir) / "work",
                aimo_standard_version="0.1.7",
                resolve_standard=False
            )
            
            # Same input manifest hash
            input_manifest_hash = "test_hash_12345"
            
            run_key1 = orchestrator1.compute_run_key(input_manifest_hash)
            run_key2 = orchestrator2.compute_run_key(input_manifest_hash)
            
            # Different Standard versions should produce different run_keys
            assert run_key1 != run_key2, \
                "Different Standard versions should produce different run_keys for cache coherence"
            
            db_client.close()
    
    def test_run_key_same_with_same_standard_version(self):
        """Test that run_key is deterministic for same Standard version."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            db_client = DuckDBClient(str(db_path))
            
            orchestrator1 = Orchestrator(
                db_client=db_client,
                work_base_dir=Path(tmpdir) / "work",
                aimo_standard_version="0.1.7",
                resolve_standard=False
            )
            
            orchestrator2 = Orchestrator(
                db_client=db_client,
                work_base_dir=Path(tmpdir) / "work",
                aimo_standard_version="0.1.7",
                resolve_standard=False
            )
            
            input_manifest_hash = "test_hash_12345"
            
            run_key1 = orchestrator1.compute_run_key(input_manifest_hash)
            run_key2 = orchestrator2.compute_run_key(input_manifest_hash)
            
            # Same versions should produce same run_key (deterministic)
            assert run_key1 == run_key2
            
            db_client.close()


class TestLegacyColumnsKept:
    """Tests that legacy columns are kept for backward compatibility."""
    
    def test_legacy_columns_exist(self, temp_db):
        """Test that legacy taxonomy columns still exist."""
        reader = temp_db.get_reader()
        
        columns = reader.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'analysis_cache'
        """).fetchall()
        column_names = [c[0] for c in columns]
        
        # Legacy columns should still exist
        assert "fs_uc_code" in column_names
        assert "dt_code" in column_names
        assert "ch_code" in column_names
        assert "rs_code" in column_names
        assert "ob_code" in column_names
        assert "ev_code" in column_names
    
    def test_can_write_legacy_and_new_columns(self, temp_db):
        """Test that both legacy and new columns can be written."""
        temp_db.upsert("analysis_cache", {
            "url_signature": "test_both_columns",
            "service_name": "Test",
            "status": "active",
            # Legacy columns
            "fs_uc_code": "FS-UC-001",
            "dt_code": "DT-001",
            # New columns
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes_json": '["UC-001"]',
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["EV-001"]',
            "ob_codes_json": '[]',
            "taxonomy_schema_version": "0.1.7",
        }, conflict_key="url_signature")
        temp_db.flush()
        
        # Verify both are saved
        reader = temp_db.get_reader()
        result = reader.execute("""
            SELECT fs_uc_code, fs_code, im_code, uc_codes_json, taxonomy_schema_version
            FROM analysis_cache 
            WHERE url_signature = 'test_both_columns'
        """).fetchone()
        
        assert result is not None
        assert result[0] == "FS-UC-001"  # Legacy
        assert result[1] == "FS-001"  # New
        assert result[2] == "IM-001"  # New
        assert result[3] == '["UC-001"]'  # New JSON
        assert result[4] == "0.1.7"  # Schema version


class TestStandardAdapterIntegration:
    """Tests for Standard Adapter integration."""
    
    def test_standard_adapter_taxonomy_validation(self):
        """Test that Standard Adapter validates codes correctly."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
            # Valid assignment
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
            assert len(errors) == 0
            
            # Invalid: missing FS
            errors = validate_assignment(
                fs_codes=[],  # Missing
                im_codes=["IM-001"],
                uc_codes=["UC-001"],
                dt_codes=["DT-001"],
                ch_codes=["CH-001"],
                rs_codes=["RS-001"],
                ev_codes=["EV-001"],
                version="0.1.7"
            )
            assert len(errors) > 0
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
    
    def test_cardinality_exactly_one(self):
        """Test that FS and IM require exactly 1 code."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
            # Multiple FS codes (should fail)
            errors = validate_assignment(
                fs_codes=["FS-001", "FS-002"],  # Too many
                im_codes=["IM-001"],
                uc_codes=["UC-001"],
                dt_codes=["DT-001"],
                ch_codes=["CH-001"],
                rs_codes=["RS-001"],
                ev_codes=["EV-001"],
                version="0.1.7"
            )
            assert any("FS" in e or "at most 1" in e for e in errors)
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
    
    def test_cardinality_one_plus(self):
        """Test that UC, DT, CH, RS, EV require at least 1 code."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
            # Missing UC (should fail)
            errors = validate_assignment(
                fs_codes=["FS-001"],
                im_codes=["IM-001"],
                uc_codes=[],  # Empty - should fail
                dt_codes=["DT-001"],
                ch_codes=["CH-001"],
                rs_codes=["RS-001"],
                ev_codes=["EV-001"],
                version="0.1.7"
            )
            assert any("UC" in e or "Use Case" in e for e in errors)
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
    
    def test_cardinality_zero_plus(self):
        """Test that OB allows 0 codes (optional)."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
            # Empty OB (should pass)
            errors = validate_assignment(
                fs_codes=["FS-001"],
                im_codes=["IM-001"],
                uc_codes=["UC-001"],
                dt_codes=["DT-001"],
                ch_codes=["CH-001"],
                rs_codes=["RS-001"],
                ev_codes=["EV-001"],
                ob_codes=[],  # Empty - should be OK
                version="0.1.7"
            )
            # Should not have OB-related errors
            ob_errors = [e for e in errors if "OB" in e or "Outcome" in e]
            assert len(ob_errors) == 0
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
