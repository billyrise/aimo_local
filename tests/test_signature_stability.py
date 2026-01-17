"""
Test Signature Stability

Tests that URL signatures are stable (same input → same signature).
"""

import pytest
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from normalize.url_normalizer import URLNormalizer
from signatures.signature_builder import SignatureBuilder


class TestSignatureStability:
    """Test signature stability and determinism."""
    
    def test_same_input_same_signature(self):
        """Same input should always produce same signature."""
        normalizer = URLNormalizer()
        builder = SignatureBuilder()
        
        url = "https://www.example.com/path/to/resource?utm_source=test&id=12345"
        http_method = "GET"
        bytes_sent = 1024
        
        # Normalize
        norm_result = normalizer.normalize(url)
        
        # Build signature multiple times
        sig1 = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method=http_method,
            bytes_sent=bytes_sent
        )
        
        sig2 = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method=http_method,
            bytes_sent=bytes_sent
        )
        
        sig3 = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method=http_method,
            bytes_sent=bytes_sent
        )
        
        # All signatures should be identical
        assert sig1["url_signature"] == sig2["url_signature"] == sig3["url_signature"]
        assert sig1["norm_host"] == sig2["norm_host"] == sig3["norm_host"]
        assert sig1["norm_path_template"] == sig2["norm_path_template"] == sig3["norm_path_template"]
    
    def test_signature_includes_version(self):
        """Signature should include signature_version."""
        builder = SignatureBuilder()
        
        sig = builder.build_signature(
            norm_host="example.com",
            norm_path="/path",
            norm_query="",
            http_method="GET",
            bytes_sent=1024
        )
        
        assert "signature_version" in sig
        assert sig["signature_version"] == "1.0"  # Default from config
    
    def test_method_group_mapping(self):
        """HTTP methods should map to correct method groups."""
        builder = SignatureBuilder()
        
        # GET methods
        assert builder.get_method_group("GET") == "GET"
        assert builder.get_method_group("HEAD") == "GET"
        assert builder.get_method_group("OPTIONS") == "GET"
        
        # WRITE methods
        assert builder.get_method_group("POST") == "WRITE"
        assert builder.get_method_group("PUT") == "WRITE"
        assert builder.get_method_group("PATCH") == "WRITE"
        assert builder.get_method_group("DELETE") == "WRITE"
        
        # Unknown methods
        assert builder.get_method_group("UNKNOWN") == "OTHER"
        assert builder.get_method_group(None) == "OTHER"
    
    def test_bytes_bucket_mapping(self):
        """Bytes should map to correct buckets."""
        builder = SignatureBuilder()
        
        # T (tiny): 0-1023
        assert builder.get_bytes_bucket(0) == "T"
        assert builder.get_bytes_bucket(512) == "T"
        assert builder.get_bytes_bucket(1023) == "T"
        
        # L (low): 1024-1048575
        assert builder.get_bytes_bucket(1024) == "L"
        assert builder.get_bytes_bucket(1048575) == "L"
        
        # M (medium): 1048576-10485759
        assert builder.get_bytes_bucket(1048576) == "M"
        assert builder.get_bytes_bucket(10485759) == "M"
        
        # H (high): 10485760-1073741823
        assert builder.get_bytes_bucket(10485760) == "H"
        assert builder.get_bytes_bucket(1073741823) == "H"
        
        # X (extreme): >= 1073741824
        assert builder.get_bytes_bucket(1073741824) == "X"
        assert builder.get_bytes_bucket(10000000000) == "X"
    
    def test_path_template_construction(self):
        """Path template should be constructed correctly."""
        builder = SignatureBuilder()
        
        # Path without query
        template1 = builder.build_path_template("/path/to/resource", "")
        assert template1 == "/path/to/resource"
        
        # Path with query (param count)
        template2 = builder.build_path_template("/path/to/resource", "a=1&b=2&c=3")
        assert template2 == "/path/to/resource?p=3"
        
        # Root path
        template3 = builder.build_path_template("/", "")
        assert template3 == "/"
    
    def test_path_depth_calculation(self):
        """Path depth should be calculated correctly."""
        builder = SignatureBuilder()
        
        sig1 = builder.build_signature("example.com", "/", "", "GET", 1024)
        assert sig1["path_depth"] == 0
        
        sig2 = builder.build_signature("example.com", "/path", "", "GET", 1024)
        assert sig2["path_depth"] == 1
        
        sig3 = builder.build_signature("example.com", "/path/to/resource", "", "GET", 1024)
        assert sig3["path_depth"] == 3
    
    def test_param_count_calculation(self):
        """Query parameter count should be calculated correctly."""
        builder = SignatureBuilder()
        
        sig1 = builder.build_signature("example.com", "/path", "", "GET", 1024)
        assert sig1["param_count"] == 0
        
        sig2 = builder.build_signature("example.com", "/path", "a=1", "GET", 1024)
        assert sig2["param_count"] == 1
        
        sig3 = builder.build_signature("example.com", "/path", "a=1&b=2&c=3", "GET", 1024)
        assert sig3["param_count"] == 3
    
    def test_different_methods_different_signatures(self):
        """Different HTTP methods should produce different signatures."""
        normalizer = URLNormalizer()
        builder = SignatureBuilder()
        
        url = "example.com/path"
        norm_result = normalizer.normalize(url)
        
        sig_get = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method="GET",
            bytes_sent=1024
        )
        
        sig_post = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method="POST",
            bytes_sent=1024
        )
        
        assert sig_get["url_signature"] != sig_post["url_signature"]
        assert sig_get["method_group"] == "GET"
        assert sig_post["method_group"] == "WRITE"
    
    def test_different_bytes_buckets_different_signatures(self):
        """Different bytes buckets should produce different signatures."""
        normalizer = URLNormalizer()
        builder = SignatureBuilder()
        
        url = "example.com/path"
        norm_result = normalizer.normalize(url)
        
        sig_small = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method="GET",
            bytes_sent=512  # T bucket
        )
        
        sig_large = builder.build_signature(
            norm_host=norm_result["norm_host"],
            norm_path=norm_result["norm_path"],
            norm_query=norm_result["norm_query"],
            http_method="GET",
            bytes_sent=1048576  # M bucket
        )
        
        assert sig_small["url_signature"] != sig_large["url_signature"]
        assert sig_small["bytes_bucket"] == "T"
        assert sig_large["bytes_bucket"] == "M"
    
    def test_signature_hex_format(self):
        """Signature should be 64-character hex string (sha256)."""
        builder = SignatureBuilder()
        
        sig = builder.build_signature(
            norm_host="example.com",
            norm_path="/path",
            norm_query="",
            http_method="GET",
            bytes_sent=1024
        )
        
        assert len(sig["url_signature"]) == 64
        assert all(c in "0123456789abcdef" for c in sig["url_signature"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
