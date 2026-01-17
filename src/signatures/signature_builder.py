"""
Signature Builder for AIMO Analysis Engine

Generates deterministic URL signatures (sha256) for cache deduplication.
Signatures include signature_version to ensure stability across version changes.
"""

import hashlib
from typing import Dict, Optional
import yaml
from pathlib import Path


class SignatureBuilder:
    """
    Builds deterministic URL signatures.
    
    Signature formula (from spec 6.3):
    url_signature = sha256(
        norm_host + "|" +
        norm_path_template + "|" +
        key_param_subset + "|" +
        method_group + "|" +
        bytes_bucket + "|" +
        signature_version
    )
    """
    
    def __init__(self, 
                 signature_version: Optional[str] = None,
                 thresholds_path: Optional[str] = None,
                 bytes_buckets_path: Optional[str] = None):
        """
        Initialize signature builder.
        
        Args:
            signature_version: Signature version string (default: from url_normalization.yml)
            thresholds_path: Path to thresholds.yaml (default: config/thresholds.yaml)
            bytes_buckets_path: Path to bytes_buckets.yml (default: config/bytes_buckets.yml)
        """
        # Load signature version
        if signature_version is None:
            norm_config_path = Path(__file__).parent.parent.parent / "config" / "url_normalization.yml"
            with open(norm_config_path, 'r', encoding='utf-8') as f:
                norm_config = yaml.safe_load(f)
            signature_version = norm_config.get("signature_version", "1.0")
        
        self.signature_version = signature_version
        
        # Load method groups
        if thresholds_path is None:
            thresholds_path = Path(__file__).parent.parent.parent / "config" / "thresholds.yaml"
        
        with open(thresholds_path, 'r', encoding='utf-8') as f:
            thresholds_config = yaml.safe_load(f)
        
        self.method_groups = thresholds_config.get("method_groups", {})
        self.default_method_group = thresholds_config.get("default_method_group", "OTHER")
        
        # Load bytes buckets
        if bytes_buckets_path is None:
            bytes_buckets_path = Path(__file__).parent.parent.parent / "config" / "bytes_buckets.yml"
        
        with open(bytes_buckets_path, 'r', encoding='utf-8') as f:
            bytes_config = yaml.safe_load(f)
        
        self.bytes_buckets = bytes_config.get("bytes_buckets", [])
        # Sort by min value for lookup
        self.bytes_buckets.sort(key=lambda x: x["min"])
    
    def get_method_group(self, http_method: Optional[str]) -> str:
        """
        Map HTTP method to method group.
        
        Args:
            http_method: HTTP method string (e.g., "GET", "POST")
        
        Returns:
            Method group: "GET", "WRITE", or "OTHER"
        """
        if not http_method:
            return self.default_method_group
        
        http_method = http_method.upper()
        
        for group, methods in self.method_groups.items():
            if http_method in methods:
                return group
        
        return self.default_method_group
    
    def get_bytes_bucket(self, bytes_sent: int) -> str:
        """
        Map bytes_sent to bucket label.
        
        Args:
            bytes_sent: Number of bytes sent (upload equivalent)
        
        Returns:
            Bucket label: "T", "L", "M", "H", or "X"
        """
        for bucket in self.bytes_buckets:
            if bucket["min"] <= bytes_sent <= bucket["max"]:
                return bucket["name"]
        
        # Default to highest bucket if out of range
        return self.bytes_buckets[-1]["name"] if self.bytes_buckets else "X"
    
    def build_path_template(self, norm_path: str, norm_query: str) -> str:
        """
        Build path template from normalized path and query.
        
        Path template includes:
        - Normalized path
        - Query parameter count (not values, for abstraction)
        
        Args:
            norm_path: Normalized path
            norm_query: Normalized query string (may be empty)
        
        Returns:
            Path template string
        """
        # Count query parameters
        param_count = 0
        if norm_query:
            param_count = len(norm_query.split('&'))
        
        # Build template: path + param count indicator
        if param_count > 0:
            return f"{norm_path}?p={param_count}"
        return norm_path
    
    def build_signature(self,
                       norm_host: str,
                       norm_path: str,
                       norm_query: str,
                       http_method: Optional[str],
                       bytes_sent: int,
                       key_param_subset: Optional[str] = None) -> Dict[str, any]:
        """
        Build complete signature record.
        
        Args:
            norm_host: Normalized hostname
            norm_path: Normalized path
            norm_query: Normalized query string
            http_method: HTTP method
            bytes_sent: Bytes sent (upload equivalent)
            key_param_subset: Key parameters to include in signature (from whitelist)
        
        Returns:
            Dictionary with signature fields:
                - url_signature: sha256 hex digest
                - signature_version: Version string
                - norm_host: Normalized hostname
                - norm_path_template: Path template
                - path_depth: Path depth (number of segments)
                - param_count: Query parameter count
                - method_group: Method group (GET/WRITE/OTHER)
                - bytes_bucket: Bytes bucket (T/L/M/H/X)
        """
        # Build path template
        norm_path_template = self.build_path_template(norm_path, norm_query)
        
        # Calculate path depth
        path_depth = len([s for s in norm_path.split('/') if s]) if norm_path else 0
        
        # Count query parameters
        param_count = len(norm_query.split('&')) if norm_query else 0
        
        # Get method group
        method_group = self.get_method_group(http_method)
        
        # Get bytes bucket
        bytes_bucket = self.get_bytes_bucket(bytes_sent)
        
        # Build key_param_subset (from whitelist, sorted)
        if key_param_subset is None:
            key_param_subset = ""
        else:
            # Ensure deterministic order
            if key_param_subset:
                params = sorted(key_param_subset.split('&'))
                key_param_subset = '&'.join(params)
        
        # Build signature input (deterministic order)
        signature_input = "|".join([
            norm_host,
            norm_path_template,
            key_param_subset or "",
            method_group,
            bytes_bucket,
            self.signature_version
        ])
        
        # Compute sha256
        url_signature = hashlib.sha256(signature_input.encode('utf-8')).hexdigest()
        
        # Detect auth token-like patterns (heuristic)
        has_auth_token_like = False
        if norm_query:
            auth_keywords = ['token', 'auth', 'key', 'secret', 'session', 'jwt']
            query_lower = norm_query.lower()
            has_auth_token_like = any(keyword in query_lower for keyword in auth_keywords)
        
        return {
            "url_signature": url_signature,
            "signature_version": self.signature_version,
            "norm_host": norm_host,
            "norm_path_template": norm_path_template,
            "path_depth": path_depth,
            "param_count": param_count,
            "method_group": method_group,
            "bytes_bucket": bytes_bucket,
            "has_auth_token_like": has_auth_token_like
        }


# Example usage:
# if __name__ == "__main__":
#     builder = SignatureBuilder()
#     
#     # Example signature
#     sig = builder.build_signature(
#         norm_host="api.example.com",
#         norm_path="/v1/users",
#         norm_query="limit=10",
#         http_method="GET",
#         bytes_sent=1024
#     )
#     
#     print(f"Signature: {sig['url_signature']}")
#     print(f"Path template: {sig['norm_path_template']}")
#     print(f"Method group: {sig['method_group']}")
#     print(f"Bytes bucket: {sig['bytes_bucket']}")
