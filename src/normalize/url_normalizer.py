"""
URL Normalizer for AIMO Analysis Engine

Implements deterministic URL normalization according to config/url_normalization.yml.
All normalization steps are strictly ordered and deterministic to ensure signature stability.
"""

import re
import hashlib
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yaml
import idna
import tldextract


class URLNormalizer:
    """
    Deterministic URL normalizer.
    
    Normalization steps (strict order):
    1. Input preprocessing (trim, scheme removal, lowercase host, punycode, port removal)
    2. Query normalization (drop tracking params, sort keys, keep whitelist)
    3. ID/token abstraction (UUID, hex, base64, email, IPv4, numeric ID)
    4. PII detection and audit logging
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize URL normalizer.
        
        Args:
            config_path: Path to url_normalization.yml (default: config/url_normalization.yml)
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "url_normalization.yml"
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Compile regex patterns for redaction
        self.redaction_patterns = []
        for rule in self.config.get("redaction", {}).get("rules", []):
            pattern = re.compile(rule["pattern"])
            self.redaction_patterns.append((pattern, rule["replace_with"]))
        
        # Compile PII detection patterns
        self.pii_patterns = []
        for pattern_str in self.config.get("pii_detection", {}).get("patterns", []):
            self.pii_patterns.append(re.compile(pattern_str, re.IGNORECASE))
        
        # Tracking parameter lists
        self.drop_keys_exact = set(self.config.get("query", {}).get("drop_keys_exact", []))
        self.drop_keys_prefix = self.config.get("query", {}).get("drop_keys_prefix", [])
        self.keep_keys_whitelist = set(self.config.get("query", {}).get("keep_keys_whitelist", []))
        
        # Initialize domain extractor (Public Suffix List)
        psl_path = Path(__file__).parent.parent.parent / "data" / "psl" / "public_suffix_list.dat"
        if psl_path.exists():
            # Use local file with file:// protocol
            suffix_list_urls = [f"file://{psl_path.absolute()}"]
        else:
            # Use default URLs if local file doesn't exist
            suffix_list_urls = None
        
        self.domain_extractor = tldextract.TLDExtract(
            suffix_list_urls=suffix_list_urls,
            fallback_to_snapshot=True  # Fallback to snapshot if download fails
        )
    
    def normalize(self, url: str, pii_audit_callback: Optional[callable] = None) -> Dict[str, str]:
        """
        Normalize a URL deterministically.
        
        Args:
            url: Input URL (can be full URL or host+path+query)
            pii_audit_callback: Optional callback for PII detection logging
                Signature: callback(pii_type: str, field_source: str, original_hash: str)
        
        Returns:
            Dictionary with keys:
                - norm_host: Normalized hostname
                - norm_path: Normalized path
                - norm_query: Normalized query string (or empty)
                - norm_url: Full normalized URL
                - pii_detected: List of detected PII types
        """
        # Step 1: Input preprocessing
        url = url.strip()
        
        # Remove scheme (http:// or https://)
        url = re.sub(r'^https?://', '', url, flags=re.IGNORECASE)
        
        # Split into host, path, query, fragment
        parts = urllib.parse.urlparse(f"//{url}")
        host = parts.netloc or parts.path.split('/', 1)[0] if parts.path else ""
        path = parts.path if parts.netloc else (parts.path if parts.path.startswith('/') else f"/{parts.path}")
        query = parts.query
        fragment = parts.fragment  # Note: fragment is typically not used in signatures
        
        # Normalize host
        if host:
            # Lowercase
            if self.config.get("host", {}).get("lowercase", True):
                host = host.lower()
            
            # Remove default ports
            if self.config.get("host", {}).get("remove_default_ports", True):
                host = re.sub(r':80$', '', host)
                host = re.sub(r':443$', '', host)
            
            # Punycode normalization (IDN)
            if self.config.get("host", {}).get("normalize_punycode", True):
                try:
                    # Encode to punycode if needed
                    host = idna.encode(host).decode('ascii')
                except Exception:
                    # If encoding fails, keep as-is
                    pass
        
        # Normalize path
        if path:
            # Collapse slashes
            if self.config.get("path", {}).get("collapse_slashes", True):
                path = re.sub(r'/+', '/', path)
            
            # Remove trailing slash (except for root "/")
            if self.config.get("path", {}).get("remove_trailing_slash", True):
                if path != "/" and path.endswith("/"):
                    path = path[:-1]
            
            # Percent encoding normalization
            if self.config.get("path", {}).get("normalize_percent_encoding", True):
                try:
                    # Decode and re-encode consistently
                    path = urllib.parse.unquote(path)
                    path = urllib.parse.quote(path, safe='/')
                except Exception:
                    pass
        
        # Step 2: Query normalization
        norm_query = ""
        if query:
            # Parse query string
            query_params = urllib.parse.parse_qs(query, keep_blank_values=False)
            
            # Filter parameters
            filtered_params = {}
            for key, values in query_params.items():
                # Drop exact matches
                if key in self.drop_keys_exact:
                    continue
                
                # Drop prefix matches
                if any(key.startswith(prefix) for prefix in self.drop_keys_prefix):
                    continue
                
                # Keep whitelist or all if whitelist is empty
                if self.keep_keys_whitelist and key not in self.keep_keys_whitelist:
                    continue
                
                # Drop empty values if configured
                if self.config.get("query", {}).get("drop_empty_values", True):
                    values = [v for v in values if v]
                
                if values:
                    filtered_params[key] = values
            
            # Sort keys (deterministic order)
            if self.config.get("query", {}).get("sort_keys", True):
                sorted_keys = sorted(filtered_params.keys())
            else:
                sorted_keys = list(filtered_params.keys())
            
            # Rebuild query string
            query_parts = []
            for key in sorted_keys:
                for value in filtered_params[key]:
                    query_parts.append(f"{key}={value}")
            
            norm_query = "&".join(query_parts)
        
        # Step 3: ID/token abstraction (apply to path and query)
        pii_detected = []
        original_path = path
        original_query = norm_query
        
        # Apply redaction to path
        for pattern, replacement in self.redaction_patterns:
            matches = pattern.findall(path)
            if matches:
                path = pattern.sub(replacement, path)
                # Detect PII type from pattern name
                pii_type = self._get_pii_type_from_replacement(replacement)
                if pii_type:
                    pii_detected.append(("path", pii_type))
        
        # Apply redaction to query
        for pattern, replacement in self.redaction_patterns:
            matches = pattern.findall(norm_query)
            if matches:
                norm_query = pattern.sub(replacement, norm_query)
                pii_type = self._get_pii_type_from_replacement(replacement)
                if pii_type:
                    pii_detected.append(("query", pii_type))
        
        # Step 4: PII detection (additional patterns)
        for pattern in self.pii_patterns:
            if pattern.search(path) or pattern.search(norm_query):
                pii_detected.append(("path_or_query", "sensitive_param"))
        
        # Call PII audit callback if provided
        if pii_audit_callback and pii_detected:
            for field_source, pii_type in pii_detected:
                # Hash original value for audit
                original_value = original_path if field_source == "path" else original_query
                original_hash = hashlib.sha256(original_value.encode('utf-8')).hexdigest()
                pii_audit_callback(pii_type, field_source, original_hash)
        
        # Build normalized URL
        norm_url = host
        if path:
            norm_url += path
        if norm_query:
            norm_url += f"?{norm_query}"
        
        return {
            "norm_host": host,
            "norm_path": path,
            "norm_query": norm_query,
            "norm_url": norm_url,
            "pii_detected": [pii_type for _, pii_type in pii_detected]
        }
    
    def _get_pii_type_from_replacement(self, replacement: str) -> Optional[str]:
        """Map replacement token to PII type."""
        mapping = {
            ":uuid": "uuid",
            ":hex": "hex_token",
            ":tok": "base64_token",
            ":email": "email",
            ":ip": "ipv4",
            ":id": "numeric_id"
        }
        return mapping.get(replacement)
    
    def extract_domain(self, host: str) -> str:
        """
        Extract eTLD+1 domain from hostname using Public Suffix List.
        
        Uses tldextract library to accurately extract eTLD+1, handling
        complex cases like .co.jp, .com.au, etc.
        
        Args:
            host: Normalized hostname
        
        Returns:
            eTLD+1 domain (e.g., "example.com" from "www.example.com",
            "example.co.jp" from "www.example.co.jp")
        """
        if not host:
            return host
        
        try:
            extracted = self.domain_extractor(host)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return host
        except Exception:
            # Fallback to original host if extraction fails
            return host


# Example usage:
# if __name__ == "__main__":
#     normalizer = URLNormalizer()
#     
#     test_urls = [
#         "https://www.example.com/path/to/resource?utm_source=test&id=12345",
#         "https://api.example.com/v1/users/550e8400-e29b-41d4-a716-446655440000",
#         "http://example.com:80/path//to//resource/?query=value"
#     ]
#     
#     for url in test_urls:
#         result = normalizer.normalize(url)
#         print(f"Input: {url}")
#         print(f"Normalized: {result['norm_url']}")
#         print(f"Host: {result['norm_host']}, Path: {result['norm_path']}, Query: {result['norm_query']}")
#         print()
