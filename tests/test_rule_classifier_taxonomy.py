"""
Tests for RuleClassifier 8-dimension taxonomy output.

AIMO Standard v0.1.7+ taxonomy:
- 8 dimensions: FS, UC, DT, CH, IM, RS, OB, EV
- Cardinality:
  - FS: Exactly 1 (string)
  - IM: Exactly 1 (string)
  - UC, DT, CH, RS, EV: 1+ (arrays)
  - OB: 0+ (array, optional)

Tests:
- Rules with complete taxonomy produce valid 8-dimension output
- Rules with incomplete taxonomy return None (pass to LLM)
- Output format is correct (strings vs arrays)
- Legacy format conversion
"""

import pytest
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from classifiers.rule_classifier import RuleClassifier


@pytest.fixture
def classifier():
    """Create a RuleClassifier instance."""
    return RuleClassifier()


class TestRuleClassifierOutput:
    """Tests for RuleClassifier output format."""
    
    def test_classifier_loads(self, classifier):
        """Test that classifier loads successfully."""
        assert classifier is not None
        assert len(classifier.enabled_rules) >= 0
    
    def test_classification_returns_none_for_unknown(self, classifier):
        """Test that unknown hosts return None."""
        result = classifier.classify(
            url_signature="unknown_sig",
            norm_host="completely-unknown-domain-xyz123.invalid",
            norm_path_template="/random/path"
        )
        
        # Should return None (no match) - pass to LLM
        assert result is None
    
    def test_output_has_8dim_structure(self, classifier):
        """Test that matched output has 8-dimension structure if complete."""
        # This test depends on having a rule with complete taxonomy
        # For now, we test the structure if we get a match
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        # Find a host from a complete rule
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test_sig",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            # Check 8-dimension structure
            assert "fs_code" in result
            assert "im_code" in result
            assert "uc_codes" in result
            assert "dt_codes" in result
            assert "ch_codes" in result
            assert "rs_codes" in result
            assert "ev_codes" in result
            assert "ob_codes" in result
            assert "aimo_standard_version" in result
            
            # Check types
            assert isinstance(result["fs_code"], str)
            assert isinstance(result["im_code"], str)
            assert isinstance(result["uc_codes"], list)
            assert isinstance(result["dt_codes"], list)
            assert isinstance(result["ch_codes"], list)
            assert isinstance(result["rs_codes"], list)
            assert isinstance(result["ev_codes"], list)
            assert isinstance(result["ob_codes"], list)


class TestRuleClassifierCardinality:
    """Tests for cardinality enforcement in rule output."""
    
    def test_fs_code_is_single_string(self, classifier):
        """Test that fs_code is a single string, not array."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            assert isinstance(result["fs_code"], str)
            assert not isinstance(result["fs_code"], list)
    
    def test_im_code_is_single_string(self, classifier):
        """Test that im_code is a single string, not array."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            assert isinstance(result["im_code"], str)
            assert not isinstance(result["im_code"], list)
    
    def test_uc_codes_is_array(self, classifier):
        """Test that uc_codes is an array."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            assert isinstance(result["uc_codes"], list)
            assert len(result["uc_codes"]) >= 1  # Must have at least 1


class TestIncompleteRules:
    """Tests for handling of incomplete rules."""
    
    def test_incomplete_rules_identified(self, classifier):
        """Test that incomplete rules can be identified."""
        incomplete = classifier.get_rules_needing_migration()
        # This should run without error
        assert isinstance(incomplete, list)
    
    def test_incomplete_rule_returns_none(self, classifier):
        """Test that a rule without complete taxonomy returns None."""
        # Create a mock rule without complete taxonomy
        incomplete_rule = {
            "rule_id": "test_incomplete",
            "rule_version": "1.0",
            "service_name": "Test Service",
            "category": "Test",
            "usage_type": "business",
            "default_risk": "low",
            "enabled": True,
            "priority": 1,
            "match": {
                "domain_exact": ["test-incomplete-rule.example.com"]
            },
            "taxonomy_codes": {
                # Only partial taxonomy - missing uc_codes, etc.
                "fs_code": "FS-001",
                "im_code": "IM-001"
                # Missing: uc_codes, dt_codes, ch_codes, rs_codes, ev_codes
            }
        }
        
        # Build classification manually
        result = classifier._build_classification(incomplete_rule, "test")
        
        # Should return None because taxonomy is incomplete
        assert result is None


class TestLegacyFormatConversion:
    """Tests for legacy format conversion."""
    
    def test_legacy_format_with_fs_uc_code(self, classifier):
        """Test that legacy format with fs_uc_code is handled."""
        legacy_rule = {
            "rule_id": "test_legacy",
            "rule_version": "1.0",
            "service_name": "Legacy Service",
            "category": "Test",
            "usage_type": "business",
            "default_risk": "low",
            "enabled": True,
            "priority": 1,
            "match": {
                "domain_exact": ["legacy.example.com"]
            },
            "taxonomy_codes": {
                # Legacy format
                "fs_uc_code": "FS-001",
                "im_code": "IM-001",
                "dt_code": "DT-001",
                "ch_code": "CH-001",
                "rs_code": "RS-001",
                "ev_code": "EV-001",
                "ob_code": ""
                # Note: UC is not available in legacy
            }
        }
        
        # Build classification
        result = classifier._build_classification(legacy_rule, "test")
        
        # Should return None because UC is not available in legacy format
        assert result is None


class TestClassificationSource:
    """Tests for classification source tracking."""
    
    def test_classification_source_is_rule(self, classifier):
        """Test that classification_source is set to RULE."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            assert result.get("classification_source") == "RULE"
    
    def test_confidence_is_one(self, classifier):
        """Test that rule-based confidence is always 1.0."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            assert result.get("confidence") == 1.0


class TestLegacyFieldsForCompatibility:
    """Tests for legacy field compatibility."""
    
    def test_legacy_fs_uc_code_is_deprecated(self, classifier):
        """Test that fs_uc_code is set to DEPRECATED."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result:
            # fs_uc_code should be DEPRECATED for new format
            assert result.get("fs_uc_code") == "DEPRECATED"
    
    def test_legacy_dt_code_has_first_element(self, classifier):
        """Test that legacy dt_code has first element of dt_codes."""
        rules_with_taxonomy = classifier.get_rules_with_complete_taxonomy()
        
        if not rules_with_taxonomy:
            pytest.skip("No rules with complete 8-dimension taxonomy")
        
        rule = rules_with_taxonomy[0]
        match_config = rule.get("match", {})
        domain_exact = match_config.get("domain_exact", [])
        
        if not domain_exact:
            pytest.skip("Rule has no domain_exact to test with")
        
        result = classifier.classify(
            url_signature="test",
            norm_host=domain_exact[0],
            norm_path_template="/"
        )
        
        if result and result.get("dt_codes"):
            # dt_code should be first element of dt_codes
            assert result.get("dt_code") == result["dt_codes"][0]


class TestBatchClassification:
    """Tests for batch classification."""
    
    def test_batch_classification(self, classifier):
        """Test batch classification returns dict."""
        signatures = [
            {"url_signature": "sig1", "norm_host": "unknown1.invalid", "norm_path_template": "/"},
            {"url_signature": "sig2", "norm_host": "unknown2.invalid", "norm_path_template": "/"},
        ]
        
        results = classifier.classify_batch(signatures)
        
        assert isinstance(results, dict)
        assert "sig1" in results
        assert "sig2" in results
        # Both should be None (no match)
        assert results["sig1"] is None
        assert results["sig2"] is None


class TestDbRecordConversion:
    """Tests for converting classification to DB record."""
    
    def test_classification_to_db_record(self):
        """Test converting 8-dimension classification to DB record."""
        from utils.json_canonical import classification_to_db_record
        
        classification = {
            "service_name": "Test Service",
            "usage_type": "business",
            "risk_level": "low",
            "category": "Test",
            "confidence": 0.95,
            "rationale_short": "Test classification",
            "classification_source": "RULE",
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001", "UC-002"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": [],
            "aimo_standard_version": "0.1.7"
        }
        
        db_record = classification_to_db_record(classification)
        
        # Check new columns
        assert db_record["fs_code"] == "FS-001"
        assert db_record["im_code"] == "IM-001"
        assert db_record["uc_codes_json"] == '["UC-001","UC-002"]'
        assert db_record["dt_codes_json"] == '["DT-001"]'
        assert db_record["ob_codes_json"] == '[]'
        
        # Check legacy columns
        assert db_record["fs_uc_code"] == "DEPRECATED"
        assert db_record["dt_code"] == "DT-001"
        
        # Check non-taxonomy fields
        assert db_record["service_name"] == "Test Service"
        assert db_record["confidence"] == 0.95
    
    def test_needs_taxonomy_review_complete(self):
        """Test needs_taxonomy_review returns False for complete classification."""
        from utils.json_canonical import needs_taxonomy_review
        
        classification = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": ["UC-001"],
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": []
        }
        
        assert needs_taxonomy_review(classification) is False
    
    def test_needs_taxonomy_review_incomplete(self):
        """Test needs_taxonomy_review returns True for incomplete classification."""
        from utils.json_canonical import needs_taxonomy_review
        
        # Missing uc_codes
        classification = {
            "fs_code": "FS-001",
            "im_code": "IM-001",
            "uc_codes": [],  # Empty
            "dt_codes": ["DT-001"],
            "ch_codes": ["CH-001"],
            "rs_codes": ["RS-001"],
            "ev_codes": ["EV-001"],
            "ob_codes": []
        }
        
        assert needs_taxonomy_review(classification) is True
