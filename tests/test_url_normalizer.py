"""
Test URL Normalizer - Deterministic Normalization

Tests that URL normalization is deterministic and follows config/url_normalization.yml.
"""

import pytest
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from normalize.url_normalizer import URLNormalizer


class TestURLNormalizerDeterminism:
    """Test deterministic URL normalization."""
    
    def test_same_input_same_output(self):
        """Same input should always produce same output."""
        normalizer = URLNormalizer()
        
        url = "https://www.example.com/path/to/resource?utm_source=test&id=12345"
        
        result1 = normalizer.normalize(url)
        result2 = normalizer.normalize(url)
        result3 = normalizer.normalize(url)
        
        assert result1 == result2 == result3
    
    def test_scheme_removal(self):
        """HTTP/HTTPS schemes should be removed."""
        normalizer = URLNormalizer()
        
        url1 = "https://example.com/path"
        url2 = "http://example.com/path"
        url3 = "example.com/path"
        
        result1 = normalizer.normalize(url1)
        result2 = normalizer.normalize(url2)
        result3 = normalizer.normalize(url3)
        
        # All should normalize to same host and path
        assert result1["norm_host"] == result2["norm_host"] == result3["norm_host"]
        assert result1["norm_path"] == result2["norm_path"] == result3["norm_path"]
    
    def test_host_lowercase(self):
        """Host should be lowercased."""
        normalizer = URLNormalizer()
        
        url = "HTTPS://WWW.EXAMPLE.COM/path"
        result = normalizer.normalize(url)
        
        assert result["norm_host"] == "www.example.com"
    
    def test_default_port_removal(self):
        """Default ports (:80, :443) should be removed."""
        normalizer = URLNormalizer()
        
        url1 = "example.com:80/path"
        url2 = "example.com:443/path"
        url3 = "example.com/path"
        
        result1 = normalizer.normalize(url1)
        result2 = normalizer.normalize(url2)
        result3 = normalizer.normalize(url3)
        
        assert result1["norm_host"] == result2["norm_host"] == result3["norm_host"]
    
    def test_slash_collapse(self):
        """Multiple slashes should be collapsed."""
        normalizer = URLNormalizer()
        
        url = "example.com/path//to///resource"
        result = normalizer.normalize(url)
        
        assert result["norm_path"] == "/path/to/resource"
    
    def test_trailing_slash_removal(self):
        """Trailing slash should be removed (except root)."""
        normalizer = URLNormalizer()
        
        url1 = "example.com/path/"
        url2 = "example.com/path"
        url3 = "example.com/"
        
        result1 = normalizer.normalize(url1)
        result2 = normalizer.normalize(url2)
        result3 = normalizer.normalize(url3)
        
        assert result1["norm_path"] == result2["norm_path"] == "/path"
        assert result3["norm_path"] == "/"  # Root should remain
    
    def test_tracking_params_removed(self):
        """Tracking parameters should be removed."""
        normalizer = URLNormalizer()
        
        url = "example.com/path?utm_source=test&utm_medium=email&gclid=abc123&id=123"
        result = normalizer.normalize(url)
        
        # utm_* and gclid should be removed
        assert "utm_source" not in result["norm_query"]
        assert "utm_medium" not in result["norm_query"]
        assert "gclid" not in result["norm_query"]
        # id should remain (not in drop list by default)
        assert "id=123" in result["norm_query"]
    
    def test_query_sorting(self):
        """Query parameters should be sorted deterministically."""
        normalizer = URLNormalizer()
        
        url1 = "example.com/path?z=1&a=2&m=3"
        url2 = "example.com/path?m=3&a=2&z=1"
        url3 = "example.com/path?a=2&z=1&m=3"
        
        result1 = normalizer.normalize(url1)
        result2 = normalizer.normalize(url2)
        result3 = normalizer.normalize(url3)
        
        # All should produce same sorted query
        assert result1["norm_query"] == result2["norm_query"] == result3["norm_query"]
        assert result1["norm_query"] == "a=2&m=3&z=1"
    
    def test_uuid_redaction(self):
        """UUIDs should be redacted."""
        normalizer = URLNormalizer()
        
        url = "example.com/users/550e8400-e29b-41d4-a716-446655440000"
        result = normalizer.normalize(url)
        
        assert ":uuid" in result["norm_path"]
        assert "550e8400" not in result["norm_path"]
    
    def test_email_redaction(self):
        """Email addresses should be redacted."""
        normalizer = URLNormalizer()
        
        url = "example.com/user?email=test@example.com"
        result = normalizer.normalize(url)
        
        assert ":email" in result["norm_query"]
        assert "test@example.com" not in result["norm_query"]
    
    def test_ipv4_redaction(self):
        """IPv4 addresses should be redacted."""
        normalizer = URLNormalizer()
        
        url = "example.com/api?ip=192.168.1.1"
        result = normalizer.normalize(url)
        
        assert ":ip" in result["norm_query"]
        assert "192.168.1.1" not in result["norm_query"]
    
    def test_numeric_id_redaction(self):
        """Long numeric IDs should be redacted."""
        normalizer = URLNormalizer()
        
        url = "example.com/user/123456789"
        result = normalizer.normalize(url)
        
        assert ":id" in result["norm_path"]
        assert "123456789" not in result["norm_path"]
    
    def test_pii_detection_callback(self):
        """PII detection callback should be called."""
        normalizer = URLNormalizer()
        
        detected_pii = []
        
        def pii_callback(pii_type, field_source, original_hash):
            detected_pii.append((pii_type, field_source, original_hash))
        
        url = "example.com/user?email=test@example.com&token=abc123"
        result = normalizer.normalize(url, pii_audit_callback=pii_callback)
        
        assert len(detected_pii) > 0
        assert any("email" in pii[0] for pii in detected_pii)
    
    def test_complex_url_normalization(self):
        """Test complex URL with all normalization steps."""
        normalizer = URLNormalizer()
        
        url = "HTTPS://WWW.EXAMPLE.COM:443/path//to//resource/?utm_source=test&id=12345&email=user@example.com"
        result = normalizer.normalize(url)
        
        assert result["norm_host"] == "www.example.com"
        assert result["norm_path"] == "/path/to/resource"
        assert "utm_source" not in result["norm_query"]
        assert ":email" in result["norm_query"] or ":email" in result["norm_path"]
    
    def test_empty_query(self):
        """Empty query should be handled."""
        normalizer = URLNormalizer()
        
        url = "example.com/path"
        result = normalizer.normalize(url)
        
        assert result["norm_query"] == ""
    
    def test_root_path(self):
        """Root path should be preserved."""
        normalizer = URLNormalizer()
        
        url = "example.com/"
        result = normalizer.normalize(url)
        
        assert result["norm_path"] == "/"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
