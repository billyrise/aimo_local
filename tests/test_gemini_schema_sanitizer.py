"""
Test Gemini Schema Sanitizer (Phase 6: 仕様固定)

Tests that JSON Schema is properly sanitized for Gemini API compatibility.
Gemini's _responseJsonSchema has specific requirements:
- Removes $schema, $id (JSON Schema metadata)
- Keeps only allowlisted fields (type, properties, required, etc.)
- Recursively cleans nested objects and arrays
- remove_title_desc=True removes title/description by default
"""

import pytest
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm.client import clean_schema_for_gemini


class TestGeminiSchemaSanitizer:
    """Test Gemini schema sanitization logic (仕様固定)."""
    
    def test_removes_schema_metadata(self):
        """$schema and $id should be removed."""
        test_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://example.local/schemas/test.json",
            "type": "object",
            "properties": {
                "test": {"type": "string"}
            }
        }
        
        cleaned = clean_schema_for_gemini(test_schema)
        
        # Verify metadata removed
        assert "$schema" not in cleaned
        assert "$id" not in cleaned
        
        # Verify allowed fields kept
        assert "type" in cleaned
        assert "properties" in cleaned
    
    def test_keeps_allowlisted_fields(self):
        """Only allowlisted fields should remain."""
        test_schema = {
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 100,
                    "title": "Service Name",  # Should be removed (not in allowlist by default)
                    "description": "Service name"  # Should be removed (not in allowlist by default)
                }
            },
            "required": ["service_name"],
            "additionalProperties": False
        }
        
        cleaned = clean_schema_for_gemini(test_schema, remove_title_desc=True)
        
        # Verify allowlisted fields kept
        assert "type" in cleaned
        assert "properties" in cleaned
        assert "required" in cleaned
        assert "additionalProperties" in cleaned
        
        # Verify non-allowlisted fields removed from nested properties
        service_name_prop = cleaned["properties"]["service_name"]
        assert "type" in service_name_prop
        assert "minLength" in service_name_prop
        assert "maxLength" in service_name_prop
        assert "title" not in service_name_prop
        assert "description" not in service_name_prop
    
    def test_remove_title_desc_true(self):
        """remove_title_desc=True should remove title and description."""
        test_schema = {
            "type": "object",
            "title": "Test Schema",
            "description": "Test description",
            "properties": {
                "field": {
                    "type": "string",
                    "title": "Field Title",
                    "description": "Field description"
                }
            }
        }
        
        cleaned = clean_schema_for_gemini(test_schema, remove_title_desc=True)
        
        # Verify title/description removed
        assert "title" not in cleaned
        assert "description" not in cleaned
        assert "title" not in cleaned["properties"]["field"]
        assert "description" not in cleaned["properties"]["field"]
    
    def test_remove_title_desc_false(self):
        """remove_title_desc=False should keep title and description."""
        test_schema = {
            "type": "object",
            "title": "Test Schema",
            "description": "Test description",
            "properties": {
                "field": {
                    "type": "string",
                    "title": "Field Title",
                    "description": "Field description"
                }
            }
        }
        
        cleaned = clean_schema_for_gemini(test_schema, remove_title_desc=False)
        
        # Verify title/description kept
        assert "title" in cleaned
        assert "description" in cleaned
        assert "title" in cleaned["properties"]["field"]
        assert "description" in cleaned["properties"]["field"]
    
    def test_recursive_cleaning(self):
        """Nested objects and arrays should be recursively cleaned."""
        test_schema = {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "$schema": "should-be-removed",
                    "properties": {
                        "deep": {
                            "type": "string",
                            "$id": "should-be-removed"
                        }
                    }
                },
                "array_prop": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "$schema": "should-be-removed",
                        "properties": {
                            "item": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        cleaned = clean_schema_for_gemini(test_schema)
        
        # Verify nested $schema/$id removed
        nested = cleaned["properties"]["nested"]
        assert "$schema" not in nested
        assert "type" in nested
        
        deep = nested["properties"]["deep"]
        assert "$id" not in deep
        assert "type" in deep
        
        # Verify array items cleaned
        array_items = cleaned["properties"]["array_prop"]["items"]
        assert "$schema" not in array_items
        assert "type" in array_items
    
    def test_oneof_anyof_cleaning(self):
        """oneOf/anyOf should be recursively cleaned."""
        test_schema = {
            "type": "object",
            "oneOf": [
                {
                    "type": "object",
                    "$schema": "should-be-removed",
                    "properties": {
                        "field1": {"type": "string"}
                    }
                },
                {
                    "type": "object",
                    "$id": "should-be-removed",
                    "properties": {
                        "field2": {"type": "number"}
                    }
                }
            ]
        }
        
        cleaned = clean_schema_for_gemini(test_schema)
        
        # Verify oneOf exists
        assert "oneOf" in cleaned
        assert len(cleaned["oneOf"]) == 2
        
        # Verify nested metadata removed
        assert "$schema" not in cleaned["oneOf"][0]
        assert "$id" not in cleaned["oneOf"][1]
    
    def test_snapshot_consistency(self):
        """Schema sanitization should be deterministic (snapshot test)."""
        # Load actual schema
        schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
        with open(schema_path, 'r') as f:
            original_schema = json.load(f)
        
        cleaned = clean_schema_for_gemini(original_schema)
        
        # Verify no metadata
        assert "$schema" not in cleaned
        assert "$id" not in cleaned
        
        # Verify structure preserved
        assert "type" in cleaned
        assert "properties" in cleaned
        assert "required" in cleaned
        
        # Verify all properties are cleaned
        for prop_name, prop_schema in cleaned["properties"].items():
            assert "$schema" not in prop_schema
            assert "$id" not in prop_schema
    
    def test_additional_properties_handling(self):
        """additionalProperties should be preserved."""
        test_schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "field": {
                    "type": "string",
                    "additionalProperties": True  # Nested (should be preserved if in allowlist)
                }
            }
        }
        
        cleaned = clean_schema_for_gemini(test_schema)
        
        # Verify additionalProperties preserved
        assert "additionalProperties" in cleaned
        assert cleaned["additionalProperties"] is False
