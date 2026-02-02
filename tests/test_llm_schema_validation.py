"""
Tests for LLM output schema validation (8-dimension taxonomy).

AIMO Standard v0.1.7+ taxonomy:
- 8 dimensions: FS, UC, DT, CH, IM, RS, OB, EV
- Cardinality:
  - FS: Exactly 1 (string)
  - IM: Exactly 1 (string)
  - UC, DT, CH, RS, EV: 1+ (arrays)
  - OB: 0+ (array, optional)

Tests:
- Schema validation for valid outputs
- Schema validation rejects invalid outputs
- Cardinality enforcement
- Pattern validation (XX-NNN format)
"""

import pytest
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import jsonschema


@pytest.fixture
def schema():
    """Load the LLM output schema."""
    schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


@pytest.fixture
def validator(schema):
    """Create a JSON Schema validator."""
    return jsonschema.Draft202012Validator(schema)


class TestSchemaStructure:
    """Tests for schema structure."""
    
    def test_schema_has_required_fields(self, schema):
        """Test that schema defines all required fields."""
        required = schema.get("required", [])
        
        # Core classification fields
        assert "service_name" in required
        assert "usage_type" in required
        assert "risk_level" in required
        assert "category" in required
        assert "confidence" in required
        assert "rationale_short" in required
        
        # 8-dimension taxonomy fields
        assert "fs_code" in required
        assert "im_code" in required
        assert "uc_codes" in required
        assert "dt_codes" in required
        assert "ch_codes" in required
        assert "rs_codes" in required
        assert "ev_codes" in required
        assert "ob_codes" in required
        assert "aimo_standard_version" in required
    
    def test_schema_forbids_additional_properties(self, schema):
        """Test that schema forbids additional properties."""
        assert schema.get("additionalProperties") is False
    
    def test_schema_has_no_legacy_fields(self, schema):
        """Test that schema does not include legacy fields."""
        required = schema.get("required", [])
        
        # Legacy fields should NOT be required
        assert "fs_uc_code" not in required
        assert "dt_code" not in required
        assert "ch_code" not in required
        assert "rs_code" not in required
        assert "ob_code" not in required
        assert "ev_code" not in required
        assert "taxonomy_version" not in required


class TestValidOutputs:
    """Tests for valid classification outputs."""
    
    def test_valid_genai_classification(self, validator):
        """Test valid GenAI service classification."""
        output = {
            "service_name": "ChatGPT / OpenAI",
            "usage_type": "genai",
            "risk_level": "high",
            "category": "GenAI",
            "confidence": 0.95,
            "rationale_short": "OpenAI's ChatGPT service",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001", "DT-002"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001", "RS-002"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        # Should not raise
        validator.validate(output)
    
    def test_valid_business_classification(self, validator):
        """Test valid business service classification."""
        output = {
            "service_name": "Microsoft 365",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Productivity",
            "confidence": 0.98,
            "rationale_short": "Microsoft productivity suite",
            "fs_code": "FS-002",
            "im_code": "IM-002",
            "uc_codes": ["UC-002", "UC-003"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-002"],
            "rs_codes": ["RS-003"],
            "ev_codes": ["EV-002"],
            "ob_codes": ["OB-001", "OB-002"],
            "aimo_standard_version": "0.1.7"
        }
        
        validator.validate(output)
    
    def test_valid_unknown_classification(self, validator):
        """Test valid unknown service classification."""
        output = {
            "service_name": "Unknown",
            "usage_type": "unknown",
            "risk_level": "medium",
            "category": "Unknown",
            "confidence": 0.3,
            "rationale_short": "Unable to identify service",
            "fs_code": "FS-099",
            "im_code": "IM-099",
            "uc_codes": ["UC-099"],
            "dt_codes": ["DT-099"],
            "ch_codes": ["CH-099"],
            "rs_codes": ["RS-099"],
            "ev_codes": ["EV-099"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        validator.validate(output)
    
    def test_valid_with_optional_fields(self, validator):
        """Test valid output with optional fields."""
        output = {
            "service_name": "Custom Service",
            "usage_type": "devtools",
            "risk_level": "medium",
            "category": "DevTools",
            "confidence": 0.85,
            "rationale_short": "Developer tool service",
            "fs_code": "FS-003",
            "im_code": "IM-003",
            "uc_codes": ["UC-005"],
            "dt_codes": ["DT-002"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-002"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7",
            "suggested_domains": ["example.com", "api.example.com"],
            "is_ai_service": False
        }
        
        validator.validate(output)


class TestInvalidOutputs:
    """Tests for invalid classification outputs."""
    
    def test_missing_required_field(self, validator):
        """Test that missing required fields are rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            # Missing: category, confidence, rationale_short, taxonomy codes
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_invalid_usage_type(self, validator):
        """Test that invalid usage_type is rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "invalid_type",  # Not in enum
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_invalid_risk_level(self, validator):
        """Test that invalid risk_level is rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "critical",  # Not in enum
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_confidence_out_of_range(self, validator):
        """Test that confidence outside 0-1 range is rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 1.5,  # Out of range
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_additional_properties_rejected(self, validator):
        """Test that additional properties are rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7",
            "extra_field": "should be rejected"  # Not allowed
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_legacy_fields_rejected(self, validator):
        """Test that legacy taxonomy fields are rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7",
            "fs_uc_code": "FS-UC-001"  # Legacy field - should be rejected
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)


class TestCardinalityValidation:
    """Tests for cardinality enforcement."""
    
    def test_empty_uc_codes_rejected(self, validator):
        """Test that empty uc_codes array is rejected (minItems=1)."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": [],  # Should have at least 1
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_empty_dt_codes_rejected(self, validator):
        """Test that empty dt_codes array is rejected (minItems=1)."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": [],  # Should have at least 1
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_empty_ob_codes_allowed(self, validator):
        """Test that empty ob_codes array is allowed (minItems=0)."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],  # Should be allowed (optional)
            "aimo_standard_version": "0.1.7"
        }
        
        # Should not raise
        validator.validate(output)
    
    def test_multiple_uc_codes_allowed(self, validator):
        """Test that multiple uc_codes are allowed."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001", "UC-002", "UC-003"],  # Multiple allowed
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        validator.validate(output)


class TestPatternValidation:
    """Tests for code pattern validation (XX-NNN format)."""
    
    def test_valid_fs_code_pattern(self, validator):
        """Test valid FS code pattern."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        validator.validate(output)
    
    def test_invalid_fs_code_pattern(self, validator):
        """Test that invalid FS code pattern is rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS001",  # Missing dash
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)
    
    def test_invalid_uc_code_pattern(self, validator):
        """Test that invalid UC code pattern in array is rejected."""
        output = {
            "service_name": "Test",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.5,
            "rationale_short": "Test",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-1"],  # Should be UC-NNN
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(output)


class TestTaxonomyAdapterValidation:
    """Tests for taxonomy adapter validation (if available)."""
    
    def test_adapter_validates_valid_codes(self):
        """Test that taxonomy adapter accepts valid codes."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
            errors = validate_assignment(
                fs_codes=["FS-001"],
                im_codes=["IM-001"],
                uc_codes=["UC-001"],
                dt_codes=["DT-001"],
                ch_codes=["CH-001"],
                rs_codes=["RS-001"],
                ev_codes=["EV-001"],
                ob_codes=[],
                version="0.1.7"
            )
            
            assert len(errors) == 0
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
    
    def test_adapter_rejects_empty_required_dimension(self):
        """Test that taxonomy adapter rejects empty required dimensions."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
            # Missing UC codes (required 1+)
            errors = validate_assignment(
                fs_codes=["FS-001"],
                im_codes=["IM-001"],
                uc_codes=[],  # Empty - should fail
                dt_codes=["DT-001"],
                ch_codes=["CH-001"],
                rs_codes=["RS-001"],
                ev_codes=["EV-001"],
                ob_codes=[],
                version="0.1.7"
            )
            
            assert len(errors) > 0
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
    
    def test_adapter_allows_empty_ob_codes(self):
        """Test that taxonomy adapter allows empty OB codes (optional)."""
        try:
            from standard_adapter.taxonomy import validate_assignment
            
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
            
            # Filter out OB-related errors
            ob_errors = [e for e in errors if "OB" in e or "Outcome" in e]
            assert len(ob_errors) == 0
            
        except ImportError:
            pytest.skip("Standard Adapter not available")
