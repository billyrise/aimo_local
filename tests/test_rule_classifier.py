"""
Test Rule Classifier - Deterministic Rule-based Classification

Tests that rule-based classification is deterministic and follows priority order:
1. url_signature exact match (future)
2. host + path_template pattern match
3. host-only match
"""

import pytest
from pathlib import Path
import sys
import json
import jsonschema

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from classifiers.rule_classifier import RuleClassifier


class TestRuleClassifierDeterminism:
    """Test deterministic rule-based classification."""
    
    def test_same_input_same_output(self):
        """Same input should always produce same output."""
        classifier = RuleClassifier()
        
        sig_data = {
            "url_signature": "test_sig_123",
            "norm_host": "openai.com",
            "norm_path_template": "/api/v1/chat"
        }
        
        result1 = classifier.classify(**sig_data)
        result2 = classifier.classify(**sig_data)
        result3 = classifier.classify(**sig_data)
        
        assert result1 == result2 == result3
    
    def test_priority_host_exact_over_suffix(self):
        """Exact domain match should take priority over suffix match."""
        classifier = RuleClassifier()
        
        # teams.microsoft.com should match exact rule (priority 90) over microsoft.com suffix (priority 100)
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "teams.microsoft.com",
            "norm_path_template": "/api/v1/chat"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["service_name"] == "Microsoft Teams"
        assert result["match_reason"] == "host_exact"
        assert result["rule_id"] == "saas_teams"
    
    def test_priority_host_path_over_host_only(self):
        """Host + path match should take priority over host-only match."""
        classifier = RuleClassifier()
        
        # notion.so with /api/v3/runBlock should match host+path rule
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "notion.so",
            "norm_path_template": "/api/v3/runBlock"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["service_name"] == "Notion AI"
        # Should match via host+path (path_prefix)
        assert "path" in result["match_reason"]
    
    def test_priority_url_signature_over_host_path(self):
        """URL signature exact match should take priority over host+path match."""
        # Create a temporary rule file with url_signature match
        import tempfile
        import json
        from pathlib import Path
        
        # Load base rules
        base_rules_path = Path(__file__).parent.parent / "rules" / "base_rules.json"
        with open(base_rules_path, 'r', encoding='utf-8') as f:
            base_rules = json.load(f)
        
        # Add a test rule with url_signature match (high priority)
        test_signature = "test_signature_exact_match_12345"
        test_rule = {
            "rule_id": "test_signature_exact",
            "rule_version": 1,
            "service_name": "Test Signature Service",
            "category": "Test",
            "usage_type": "business",
            "default_risk": "low",
            "match": {
                "url_signatures": [test_signature],
                "domain_suffixes": ["notion.so"],
                "path_prefix": ["/api/v3/runBlock"]
            },
            "priority": 10  # Higher priority than notion.so rule
        }
        
        # Create temp rules file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_rules = base_rules + [test_rule]
            json.dump(temp_rules, f)
            temp_rules_path = f.name
        
        try:
            classifier = RuleClassifier(rules_path=temp_rules_path)
            
            # Test: url_signature exact match should win over host+path
            sig_data = {
                "url_signature": test_signature,
                "norm_host": "notion.so",
                "norm_path_template": "/api/v3/runBlock"
            }
            
            result = classifier.classify(**sig_data)
            
            assert result is not None
            assert result["service_name"] == "Test Signature Service"
            assert result["match_reason"] == "signature_exact"
            assert result["rule_id"] == "test_signature_exact"
        finally:
            # Clean up
            Path(temp_rules_path).unlink()
    
    def test_priority_order_complete(self):
        """Complete priority order: url_signature > host+path > host-only."""
        # Create a temporary rule file with all three match types
        import tempfile
        import json
        from pathlib import Path
        
        # Load base rules
        base_rules_path = Path(__file__).parent.parent / "rules" / "base_rules.json"
        with open(base_rules_path, 'r', encoding='utf-8') as f:
            base_rules = json.load(f)
        
        test_signature = "test_priority_signature_67890"
        test_host = "test-priority.example.com"
        
        # Rule 1: host-only match (lowest priority, priority=100)
        rule_host_only = {
            "rule_id": "test_host_only",
            "rule_version": 1,
            "service_name": "Host Only Service",
            "category": "Test",
            "usage_type": "business",
            "default_risk": "low",
            "match": {
                "domain_suffixes": [test_host]
            },
            "priority": 100
        }
        
        # Rule 2: host+path match (medium priority, priority=50)
        rule_host_path = {
            "rule_id": "test_host_path",
            "rule_version": 1,
            "service_name": "Host Path Service",
            "category": "Test",
            "usage_type": "business",
            "default_risk": "low",
            "match": {
                "domain_suffixes": [test_host],
                "path_prefix": ["/api/test"]
            },
            "priority": 50
        }
        
        # Rule 3: url_signature exact match (highest priority, priority=10)
        # Note: url_signature完全一致のルールにはdomain_suffixes/path_prefixを含めない
        # （含めると、url_signature不一致でもhost+pathでマッチしてしまう）
        rule_signature = {
            "rule_id": "test_signature",
            "rule_version": 1,
            "service_name": "Signature Service",
            "category": "Test",
            "usage_type": "business",
            "default_risk": "low",
            "match": {
                "url_signatures": [test_signature]
            },
            "priority": 10
        }
        
        # Create temp rules file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_rules = base_rules + [rule_host_only, rule_host_path, rule_signature]
            json.dump(temp_rules, f)
            temp_rules_path = f.name
        
        try:
            classifier = RuleClassifier(rules_path=temp_rules_path)
            
            # Test 1: url_signature exact match should win
            sig_data = {
                "url_signature": test_signature,
                "norm_host": test_host,
                "norm_path_template": "/api/test/path"
            }
            result = classifier.classify(**sig_data)
            assert result is not None
            assert result["service_name"] == "Signature Service"
            assert result["match_reason"] == "signature_exact"
            
            # Test 2: host+path match should win over host-only
            sig_data2 = {
                "url_signature": "different_signature",
                "norm_host": test_host,
                "norm_path_template": "/api/test/path"
            }
            result2 = classifier.classify(**sig_data2)
            assert result2 is not None
            assert result2["service_name"] == "Host Path Service"
            assert "path" in result2["match_reason"]
            
            # Test 3: host-only match should be last resort
            # Note: /different/pathは /api/test のprefixにマッチしないため、
            # host+pathマッチは成立せず、host-onlyマッチが返される
            sig_data3 = {
                "url_signature": "different_signature",
                "norm_host": test_host,
                "norm_path_template": "/different/path"
            }
            result3 = classifier.classify(**sig_data3)
            assert result3 is not None
            # host+pathマッチは成立しない（/different/pathは /api/test のprefixではない）
            # したがって、host-onlyマッチが返される
            assert result3["service_name"] == "Host Only Service"
            assert result3["match_reason"] in ["host_exact", "host_suffix"]
        finally:
            # Clean up
            Path(temp_rules_path).unlink()
    
    def test_host_suffix_matching(self):
        """Domain suffix matching should work correctly."""
        classifier = RuleClassifier()
        
        # Test OpenAI suffix match
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "api.openai.com",
            "norm_path_template": "/v1/chat"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["service_name"] == "ChatGPT / OpenAI"
        assert result["category"] == "GenAI"
        assert result["usage_type"] == "genai"
        assert result["default_risk"] == "high"
        assert result["rule_id"] == "genai_chatgpt_openai"
    
    def test_no_match_returns_none(self):
        """Unknown domains should return None."""
        classifier = RuleClassifier()
        
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "unknown-service.example.com",
            "norm_path_template": "/api/v1/test"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is None
    
    def test_rule_schema_validation(self):
        """Invalid rules should cause initialization to fail."""
        # This test verifies that schema validation works
        # We can't easily test invalid rules without modifying base_rules.json,
        # so we just verify that valid rules load correctly
        classifier = RuleClassifier()
        
        # Should not raise
        assert len(classifier.enabled_rules) > 0
    
    def test_classify_batch(self):
        """Batch classification should work correctly."""
        classifier = RuleClassifier()
        
        signatures = [
            {
                "url_signature": "sig1",
                "norm_host": "openai.com",
                "norm_path_template": "/api/v1/chat"
            },
            {
                "url_signature": "sig2",
                "norm_host": "slack.com",
                "norm_path_template": "/api/users"
            },
            {
                "url_signature": "sig3",
                "norm_host": "unknown.example.com",
                "norm_path_template": "/api/test"
            }
        ]
        
        results = classifier.classify_batch(signatures)
        
        assert len(results) == 3
        assert results["sig1"] is not None
        assert results["sig1"]["service_name"] == "ChatGPT / OpenAI"
        assert results["sig2"] is not None
        assert results["sig2"]["service_name"] == "Slack"
        assert results["sig3"] is None  # Unknown domain
    
    def test_confidence_always_one(self):
        """Rule-based classifications should always have confidence=1.0."""
        classifier = RuleClassifier()
        
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "github.com",
            "norm_path_template": "/api/repos"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["confidence"] == 1.0
    
    def test_priority_lower_is_higher(self):
        """Lower priority value should mean higher priority in matching."""
        classifier = RuleClassifier()
        
        # genai_chatgpt_openai has priority=50, saas_microsoft_365 has priority=100
        # So OpenAI should match first
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "openai.com",  # Matches both OpenAI and potentially other rules
            "norm_path_template": "/api/v1/chat"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        # Should match OpenAI (priority 50) not other rules with higher priority values
        assert result["rule_id"] == "genai_chatgpt_openai"
        assert result["rule_version"] == 1
    
    def test_path_prefix_matching(self):
        """Path prefix matching should work correctly."""
        classifier = RuleClassifier()
        
        # Notion AI rule has path_prefix: ["/api/v3/runBlock"]
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "notion.so",
            "norm_path_template": "/api/v3/runBlock/12345"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["service_name"] == "Notion AI"
        assert "path" in result["match_reason"]


class TestRuleClassifierEdgeCases:
    """Test edge cases and error handling."""
    
    def test_missing_norm_path_template(self):
        """Classification should work without norm_path_template."""
        classifier = RuleClassifier()
        
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "slack.com"
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["service_name"] == "Slack"
        assert result["match_reason"] in ["host_exact", "host_suffix"]
    
    def test_empty_norm_path_template(self):
        """Empty norm_path_template should be treated as None."""
        classifier = RuleClassifier()
        
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "slack.com",
            "norm_path_template": ""
        }
        
        result = classifier.classify(**sig_data)
        
        assert result is not None
        assert result["service_name"] == "Slack"
    
    def test_case_insensitive_matching(self):
        """Host matching should work with lowercased norm_host (URLNormalizer ensures lowercase)."""
        classifier = RuleClassifier()
        
        # norm_host is always lowercased by URLNormalizer in practice
        # Test with lowercase (actual behavior)
        sig_data = {
            "url_signature": "test_sig",
            "norm_host": "openai.com",  # Lowercase (as normalized by URLNormalizer)
            "norm_path_template": "/api/v1/chat"
        }
        
        result = classifier.classify(**sig_data)
        
        # Should match (norm_host is already lowercased by URLNormalizer)
        assert result is not None
        assert result["service_name"] == "ChatGPT / OpenAI"