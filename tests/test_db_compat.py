"""
Tests for DB compatibility layer (src/db/compat.py).

Tests backward-compatible reading of legacy and new format records.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from db.compat import (
    TaxonomyRecord,
    normalize_taxonomy_record,
    normalize_db_rows,
    record_to_bundle_format,
    export_legacy_format,
    get_migration_status,
)


class TestTaxonomyRecord:
    """Tests for TaxonomyRecord dataclass."""
    
    def test_default_values(self):
        """Test default values are set correctly."""
        record = TaxonomyRecord()
        
        assert record.fs_code == ""
        assert record.im_code == ""
        assert record.uc_codes == []
        assert record.dt_codes == []
        assert record.ch_codes == []
        assert record.rs_codes == []
        assert record.lg_codes == []
        assert record.ob_codes == []
        assert record.taxonomy_version == "0.1.1"
        assert record.needs_review == False
        assert record.source_format == "new"
    
    def test_to_dict(self):
        """Test conversion to dimension dict."""
        record = TaxonomyRecord(
            fs_code="FS-001",
            im_code="IM-001",
            uc_codes=["UC-001", "UC-002"],
            dt_codes=["DT-001"],
            ch_codes=[],
            rs_codes=[],
            lg_codes=[],
        )
        
        d = record.to_dict()
        
        assert d["FS"] == ["FS-001"]
        assert d["IM"] == ["IM-001"]
        assert d["UC"] == ["UC-001", "UC-002"]
        assert d["DT"] == ["DT-001"]
    
    def test_to_flat_dict(self):
        """Test conversion to flat dict."""
        record = TaxonomyRecord(
            fs_code="FS-001",
            uc_codes=["UC-001"],
            needs_review=True
        )
        
        d = record.to_flat_dict()
        
        assert d["fs_code"] == "FS-001"
        assert d["uc_codes"] == ["UC-001"]
        assert d["needs_review"] == True
    
    def test_is_complete_true(self):
        """Test completeness check for complete record (Standard 0.1.1: lg_codes)."""
        record = TaxonomyRecord(
            fs_code="FS-001",
            im_code="IM-001",
            uc_codes=["UC-001"],
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            lg_codes=["LG-001"],
            ob_codes=[]  # Optional
        )
        
        assert record.is_complete() == True
    
    def test_is_complete_false_missing_uc(self):
        """Test completeness check for incomplete record."""
        record = TaxonomyRecord(
            fs_code="FS-001",
            im_code="IM-001",
            uc_codes=[],  # Missing
            dt_codes=["DT-001"],
            ch_codes=["CH-001"],
            rs_codes=["RS-001"],
            lg_codes=["LG-001"],
        )
        
        assert record.is_complete() == False


class TestNormalizeTaxonomyRecord:
    """Tests for normalize_taxonomy_record function."""
    
    def test_new_format_only(self):
        """Test normalization of new format record."""
        row = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes_json": '["UC-001","UC-002"]',
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["LG-001"]',
            "ob_codes_json": '[]',
            "taxonomy_schema_version": "0.1.1"
        }
        
        record = normalize_taxonomy_record(row)
        
        assert record.fs_code == "FS-001"
        assert record.im_code == "IM-001"
        assert record.uc_codes == ["UC-001", "UC-002"]
        assert record.dt_codes == ["DT-001"]
        assert record.needs_review == False
        assert record.source_format == "new"
    
    def test_legacy_format_only(self):
        """Test normalization of legacy format record."""
        row = {
            "fs_code": "",
            "im_code": "IM-001",
            "uc_codes_json": "[]",
            "dt_codes_json": "[]",
            "ch_codes_json": "[]",
            "rs_codes_json": "[]",
            "ev_codes_json": "[]",
            "ob_codes_json": "[]",
            # Legacy columns
            "fs_uc_code": "FS-001",
            "dt_code": "DT-002",
            "ch_code": "CH-002",
            "rs_code": "RS-002",
            "ev_code": "LG-002",  # Legacy column stores LG code (Standard 0.1.1)
            "ob_code": "OB-001",
        }
        
        record = normalize_taxonomy_record(row)
        
        assert record.fs_code == "FS-001"
        assert record.im_code == "IM-001"
        assert record.dt_codes == ["DT-002"]
        assert record.ch_codes == ["CH-002"]
        assert record.lg_codes == ["LG-002"]
        assert record.needs_review == True
        assert record.source_format == "legacy"
    
    def test_mixed_format_new_wins(self):
        """Test that new format takes priority over legacy."""
        row = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["LG-001"]',
            "ob_codes_json": '[]',
            "uc_codes_json": '["UC-001"]',
            # Legacy (should be ignored)
            "dt_code": "DT-LEGACY",
            "ch_code": "CH-LEGACY",
        }
        
        record = normalize_taxonomy_record(row)
        
        # New format wins
        assert record.dt_codes == ["DT-001"]
        assert record.ch_codes == ["CH-001"]
        assert record.needs_review == False
    
    def test_empty_new_falls_back_to_legacy(self):
        """Test fallback to legacy when new column is empty."""
        row = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes_json": '["UC-001"]',
            "dt_codes_json": "[]",  # Empty
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["LG-001"]',
            "ob_codes_json": '[]',
            # Legacy fallback
            "dt_code": "DT-LEGACY",
        }
        
        record = normalize_taxonomy_record(row)
        
        # Falls back to legacy
        assert record.dt_codes == ["DT-LEGACY"]
        assert record.needs_review == True
        assert record.source_format == "legacy"
    
    def test_deprecated_fs_uc_code_ignored(self):
        """Test that DEPRECATED fs_uc_code is ignored."""
        row = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes_json": '["UC-001"]',
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["LG-001"]',
            "ob_codes_json": '[]',
            "fs_uc_code": "DEPRECATED",
        }
        
        record = normalize_taxonomy_record(row)
        
        assert record.fs_code == "FS-001"
        assert record.needs_review == False
    
    def test_incomplete_record_needs_review(self):
        """Test that incomplete records are marked needs_review."""
        row = {
            "fs_code": "FS-001",
            "im_code": "",  # Missing
            "uc_codes_json": "[]",  # Empty
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["LG-001"]',
            "ob_codes_json": '[]',
        }
        
        record = normalize_taxonomy_record(row)
        
        assert record.needs_review == True


class TestNormalizeDbRows:
    """Tests for normalize_db_rows function."""
    
    def test_multiple_rows(self):
        """Test normalization of multiple rows."""
        rows = [
            {"fs_code": "FS-001", "im_code": "IM-001", "uc_codes_json": '["UC-001"]',
             "dt_codes_json": '["DT-001"]', "ch_codes_json": '["CH-001"]',
             "rs_codes_json": '["RS-001"]', "ev_codes_json": '["EV-001"]',
             "ob_codes_json": '[]'},
            {"fs_code": "FS-002", "im_code": "IM-002", "uc_codes_json": '["UC-002"]',
             "dt_codes_json": '["DT-002"]', "ch_codes_json": '["CH-002"]',
             "rs_codes_json": '["RS-002"]', "ev_codes_json": '["LG-002"]',
             "ob_codes_json": '[]'},
        ]
        
        records = normalize_db_rows(rows)
        
        assert len(records) == 2
        assert records[0].fs_code == "FS-001"
        assert records[1].fs_code == "FS-002"


class TestRecordToBundleFormat:
    """Tests for record_to_bundle_format function."""
    
    def test_conversion(self):
        """Test conversion to bundle format."""
        record = TaxonomyRecord(
            fs_code="FS-001",
            im_code="IM-001",
            uc_codes=["UC-001"],
            needs_review=True,
            source_format="legacy"
        )
        
        bundle = record_to_bundle_format(record)
        
        assert bundle["fs_code"] == "FS-001"
        assert bundle["uc_codes"] == ["UC-001"]
        assert bundle["_needs_review"] == True
        assert bundle["_source_format"] == "legacy"


class TestExportLegacyFormat:
    """Tests for export_legacy_format function."""
    
    def test_export_to_legacy(self):
        """Test export to legacy format."""
        record = TaxonomyRecord(
            fs_code="FS-001",
            im_code="IM-001",
            dt_codes=["DT-001", "DT-002"],  # Multiple
            ch_codes=["CH-001"],
        )
        
        legacy = export_legacy_format(record)
        
        assert legacy["fs_uc_code"] == "FS-001"
        assert legacy["im_code"] == "IM-001"
        assert legacy["dt_code"] == "DT-001"  # First element
        assert legacy["ch_code"] == "CH-001"
    
    def test_empty_fs_becomes_deprecated(self):
        """Test that empty FS becomes DEPRECATED."""
        record = TaxonomyRecord(fs_code="")
        
        legacy = export_legacy_format(record)
        
        assert legacy["fs_uc_code"] == "DEPRECATED"


class TestGetMigrationStatus:
    """Tests for get_migration_status function."""
    
    def test_new_format_complete(self):
        """Test status for complete new format record."""
        row = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes_json": '["UC-001"]',
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": '["CH-001"]',
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": '["LG-001"]',
            "ob_codes_json": '[]',
        }
        
        status = get_migration_status(row)
        
        assert status["has_new_format"] == True
        assert status["has_legacy_only"] == False
        assert status["needs_migration"] == False
        assert status["missing_dimensions"] == []
    
    def test_legacy_only(self):
        """Test status for legacy-only record."""
        row = {
            "fs_code": "",
            "im_code": "",  # Empty - no new format
            "uc_codes_json": "[]",
            "dt_codes_json": "[]",
            "ch_codes_json": "[]",
            "rs_codes_json": "[]",
            "ev_codes_json": "[]",
            "fs_uc_code": "FS-001",
            "dt_code": "DT-001",
        }
        
        status = get_migration_status(row)
        
        assert status["has_legacy_only"] == True
        assert status["needs_migration"] == True
    
    def test_missing_dimensions(self):
        """Test detection of missing dimensions."""
        row = {
            "fs_code": "FS-001",
            "im_code": "",
            "uc_codes_json": "[]",
            "dt_codes_json": '["DT-001"]',
            "ch_codes_json": "[]",
            "rs_codes_json": '["RS-001"]',
            "ev_codes_json": "[]",
        }
        
        status = get_migration_status(row)
        
        assert "IM" in status["missing_dimensions"]
        assert "UC" in status["missing_dimensions"]
        assert "CH" in status["missing_dimensions"]
        assert "LG" in status["missing_dimensions"]
        assert status["needs_migration"] == True


class TestBundleGenerationWithLegacyData:
    """Integration tests for bundle generation with legacy data."""
    
    def test_legacy_record_normalized_in_bundle(self):
        """Test that legacy records are normalized when generating bundle."""
        # Simulate legacy record
        row = {
            "fs_code": "",
            "im_code": "IM-001",
            "uc_codes_json": "[]",
            "dt_codes_json": "[]",
            "ch_codes_json": "[]",
            "rs_codes_json": "[]",
            "ev_codes_json": "[]",
            "ob_codes_json": "[]",
            "fs_uc_code": "FS-001",
            "dt_code": "DT-002",
            "ch_code": "CH-002",
            "rs_code": "RS-002",
            "ev_code": "EV-002",
        }
        
        record = normalize_taxonomy_record(row)
        bundle = record_to_bundle_format(record)
        
        # Arrays should be populated from legacy
        assert bundle["fs_code"] == "FS-001"
        assert bundle["dt_codes"] == ["DT-002"]
        assert bundle["ch_codes"] == ["CH-002"]
        assert bundle["_needs_review"] == True
