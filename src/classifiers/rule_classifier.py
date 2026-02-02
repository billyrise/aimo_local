"""
Rule-based Classifier for AIMO Analysis Engine

Classifies signatures using rules/base_rules.json with deterministic matching.
Priority order:
1. url_signature exact match (if rules support it)
2. host + path_template pattern match
3. host-only match (domain_suffixes, domain_exact)

All rules are validated against rules/rule.schema.json at load time.

Taxonomy Output (AIMO Standard v0.1.7+):
- 8 dimensions: FS, IM, UC, DT, CH, RS, EV, OB
- Cardinality: FS=1, IM=1, UC/DT/CH/RS/EV=1+, OB=0+
- Rules that cannot fill all required dimensions return None (pass to LLM)
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
import jsonschema

# Default AIMO Standard version
DEFAULT_AIMO_STANDARD_VERSION = "0.1.7"


class RuleClassifier:
    """
    Rule-based classifier for service identification.
    
    Matches signatures against rules/base_rules.json using deterministic priority:
    1. url_signature exact match (future: if rules support signature matching)
    2. host + path_template pattern match (regex, wildcard)
    3. host-only match (domain_suffixes, domain_exact)
    
    All classifications are deterministic: same input → same output.
    
    Output format (8-dimension, AIMO Standard v0.1.7+):
    - fs_code: Single string (exactly 1)
    - im_code: Single string (exactly 1)
    - uc_codes: List of strings (1+)
    - dt_codes: List of strings (1+)
    - ch_codes: List of strings (1+)
    - rs_codes: List of strings (1+)
    - ev_codes: List of strings (1+)
    - ob_codes: List of strings (0+)
    """
    
    def __init__(self, 
                 rules_path: Optional[str] = None, 
                 schema_path: Optional[str] = None,
                 aimo_standard_version: str = DEFAULT_AIMO_STANDARD_VERSION):
        """
        Initialize rule classifier.
        
        Args:
            rules_path: Path to rules/base_rules.json (default: config/rules/base_rules.json)
            schema_path: Path to rules/rule.schema.json (default: config/rules/rule.schema.json)
            aimo_standard_version: AIMO Standard version for taxonomy
        
        Raises:
            FileNotFoundError: If rules or schema file not found
            jsonschema.ValidationError: If rules don't match schema
        """
        if rules_path is None:
            rules_path = Path(__file__).parent.parent.parent / "rules" / "base_rules.json"
        if schema_path is None:
            schema_path = Path(__file__).parent.parent.parent / "rules" / "rule.schema.json"
        
        self.rules_path = Path(rules_path)
        self.schema_path = Path(schema_path)
        self.aimo_standard_version = aimo_standard_version
        
        if not self.rules_path.exists():
            raise FileNotFoundError(f"Rules file not found: {self.rules_path}")
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
        
        # Load and validate rules
        with open(self.rules_path, 'r', encoding='utf-8') as f:
            self.rules = json.load(f)
        
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
        
        # Validate all rules against schema
        validator = jsonschema.Draft202012Validator(self.schema)
        for i, rule in enumerate(self.rules):
            try:
                validator.validate(rule)
            except jsonschema.ValidationError as e:
                raise jsonschema.ValidationError(
                    f"Rule {i} (rule_id={rule.get('rule_id', 'unknown')}) validation failed: {e.message}"
                ) from e
        
        # Filter enabled rules only (default: enabled=True)
        self.enabled_rules = [r for r in self.rules if r.get("enabled", True)]
        
        # Sort by priority (lower = higher priority)
        self.enabled_rules.sort(key=lambda r: (r.get("priority", 100), r.get("rule_id", "")))
        
        # Pre-compile regex patterns for performance
        self._compile_patterns()
        
        # Initialize taxonomy adapter (optional, for validation)
        self._taxonomy_adapter = None
        try:
            from standard_adapter.taxonomy import get_taxonomy_adapter
            self._taxonomy_adapter = get_taxonomy_adapter(version=aimo_standard_version)
        except ImportError:
            pass  # Standard Adapter not available
        except Exception:
            pass  # Any error, continue without adapter
    
    def _compile_patterns(self):
        """Pre-compile regex patterns for all rules."""
        for rule in self.enabled_rules:
            match_config = rule.get("match", {})
            
            # Compile url_regex patterns
            url_regexes = match_config.get("url_regex", [])
            rule["_compiled_url_regex"] = [
                re.compile(pattern, re.IGNORECASE) for pattern in url_regexes
            ]
            
            # Compile path_prefix patterns (convert to regex for matching)
            path_prefixes = match_config.get("path_prefix", [])
            rule["_compiled_path_prefix"] = [
                re.compile(re.escape(prefix) + r".*", re.IGNORECASE) if not prefix.endswith("*") 
                else re.compile(re.escape(prefix[:-1]) + r".*", re.IGNORECASE)
                for prefix in path_prefixes
            ]
    
    def classify(self, 
                 url_signature: str,
                 norm_host: str,
                 norm_path_template: Optional[str] = None,
                 **kwargs) -> Optional[Dict[str, Any]]:
        """
        Classify a signature using rule-based matching.
        
        Args:
            url_signature: URL signature (for future exact matching)
            norm_host: Normalized host (e.g., "example.com")
            norm_path_template: Normalized path template (e.g., "/api/v1/users/:id")
            **kwargs: Additional context (not used in current implementation)
        
        Returns:
            Classification dict with 8-dimension taxonomy:
            - service_name: Human-readable service name
            - category: Service category
            - usage_type: business/genai/devtools/storage/social/unknown
            - risk_level: low/medium/high
            - rule_id: Matched rule ID
            - rule_version: Matched rule version
            - match_reason: How the match was made
            - confidence: 1.0 for rule-based (always deterministic)
            - fs_code: FS code (single string)
            - im_code: IM code (single string)
            - uc_codes: UC codes (array)
            - dt_codes: DT codes (array)
            - ch_codes: CH codes (array)
            - rs_codes: RS codes (array)
            - ev_codes: EV codes (array)
            - ob_codes: OB codes (array, may be empty)
            - aimo_standard_version: Version used
            - classification_source: "RULE"
            
            None if no rule matches OR if rule cannot fill all required dimensions.
        """
        # Priority 1: url_signature exact match (highest priority)
        match_result = self._match_url_signature(url_signature)
        if match_result:
            return match_result
        
        # Priority 2: host + path_template pattern match
        if norm_path_template:
            match_result = self._match_host_and_path(norm_host, norm_path_template)
            if match_result:
                return match_result
        
        # Priority 3: host-only match
        match_result = self._match_host_only(norm_host)
        if match_result:
            return match_result
        
        # No match
        return None
    
    def _match_url_signature(self, url_signature: str) -> Optional[Dict[str, Any]]:
        """
        Match url_signature exactly against rules (highest priority).
        
        Returns:
            Classification dict if exact match found, None otherwise
        """
        best_match = None
        best_priority = float('inf')
        
        for rule in self.enabled_rules:
            match_config = rule.get("match", {})
            priority = rule.get("priority", 100)
            
            # Skip if priority is worse than current best
            if priority >= best_priority:
                continue
            
            # Check url_signatures (exact match)
            url_signatures = match_config.get("url_signatures", [])
            if url_signatures:
                if url_signature in url_signatures:
                    if priority < best_priority:
                        classification = self._build_classification(rule, "signature_exact")
                        if classification:  # Only accept if all dimensions filled
                            best_match = classification
                            best_priority = priority
        
        return best_match
    
    def _match_host_and_path(self, norm_host: str, norm_path_template: str) -> Optional[Dict[str, Any]]:
        """
        Match host + path_template against rules.
        
        Priority: exact domain + path_prefix > exact domain + url_regex > suffix + path_prefix > suffix + url_regex
        
        Note: This method ONLY returns matches when BOTH host AND path match.
        If path_prefix is specified but doesn't match, this method returns None (no fallback to host-only).
        """
        best_match = None
        best_priority = float('inf')
        
        for rule in self.enabled_rules:
            match_config = rule.get("match", {})
            priority = rule.get("priority", 100)
            
            # Skip if priority is worse than current best
            if priority >= best_priority:
                continue
            
            # Check domain_exact first (more specific)
            domain_exact = match_config.get("domain_exact", [])
            if domain_exact:
                if norm_host in domain_exact:
                    # Check path_prefix (if specified, must match)
                    path_prefixes = match_config.get("path_prefix", [])
                    if path_prefixes:
                        path_matched = False
                        for prefix_pattern in rule.get("_compiled_path_prefix", []):
                            if prefix_pattern.match(norm_path_template):
                                path_matched = True
                                classification = self._build_classification(rule, "host+path_exact")
                                if classification:
                                    best_match = classification
                                    best_priority = priority
                                break
                        # If path_prefix is specified but doesn't match, skip this rule
                        if not path_matched:
                            continue
                    else:
                        # No path_prefix specified, check url_regex
                        url_regexes = rule.get("_compiled_url_regex", [])
                        if url_regexes:
                            full_url_pattern = f"{norm_host}{norm_path_template}"
                            for regex in url_regexes:
                                if regex.search(full_url_pattern):
                                    classification = self._build_classification(rule, "host+path_regex")
                                    if classification:
                                        best_match = classification
                                        best_priority = priority
                                    break
                        # If no path_prefix and no url_regex, this rule doesn't match host+path
                        if not url_regexes:
                            continue
            
            # Check domain_suffixes (less specific)
            domain_suffixes = match_config.get("domain_suffixes", [])
            if domain_suffixes:
                host_matched = False
                for suffix in domain_suffixes:
                    if norm_host == suffix or norm_host.endswith("." + suffix):
                        host_matched = True
                        break
                
                if host_matched:
                    # Check path_prefix (if specified, must match)
                    path_prefixes = match_config.get("path_prefix", [])
                    if path_prefixes:
                        path_matched = False
                        for prefix_pattern in rule.get("_compiled_path_prefix", []):
                            if prefix_pattern.match(norm_path_template):
                                path_matched = True
                                if priority < best_priority:
                                    classification = self._build_classification(rule, "host+path_suffix")
                                    if classification:
                                        best_match = classification
                                        best_priority = priority
                                break
                        # If path_prefix is specified but doesn't match, skip this rule
                        if not path_matched:
                            continue
                    else:
                        # No path_prefix specified, check url_regex
                        url_regexes = rule.get("_compiled_url_regex", [])
                        if url_regexes:
                            full_url_pattern = f"{norm_host}{norm_path_template}"
                            for regex in url_regexes:
                                if regex.search(full_url_pattern):
                                    if priority < best_priority:
                                        classification = self._build_classification(rule, "host+path_regex")
                                        if classification:
                                            best_match = classification
                                            best_priority = priority
                                    break
                        # If no path_prefix and no url_regex, this rule doesn't match host+path
                        if not url_regexes:
                            continue
        
        return best_match
    
    def _match_host_only(self, norm_host: str) -> Optional[Dict[str, Any]]:
        """
        Match host-only against rules.
        
        Priority: exact domain > domain suffix (lower priority value = higher priority)
        
        Note: This method ONLY matches rules that have NO path_prefix or url_regex.
        Rules with path_prefix or url_regex require path matching and should be handled by _match_host_and_path.
        """
        best_match = None
        best_priority = float('inf')
        
        for rule in self.enabled_rules:
            match_config = rule.get("match", {})
            priority = rule.get("priority", 100)
            
            # Skip if priority is worse than current best
            if priority >= best_priority:
                continue
            
            # Skip rules that require path matching (path_prefix or url_regex)
            # These should only match via _match_host_and_path
            path_prefixes = match_config.get("path_prefix", [])
            url_regexes = match_config.get("url_regex", [])
            if path_prefixes or url_regexes:
                continue
            
            # Check domain_exact first (more specific)
            domain_exact = match_config.get("domain_exact", [])
            if domain_exact:
                if norm_host in domain_exact:
                    if priority < best_priority:
                        classification = self._build_classification(rule, "host_exact")
                        if classification:
                            best_match = classification
                            best_priority = priority
                    continue
            
            # Check domain_suffixes (less specific)
            domain_suffixes = match_config.get("domain_suffixes", [])
            if domain_suffixes:
                for suffix in domain_suffixes:
                    if norm_host == suffix or norm_host.endswith("." + suffix):
                        if priority < best_priority:
                            classification = self._build_classification(rule, "host_suffix")
                            if classification:
                                best_match = classification
                                best_priority = priority
                        break
        
        return best_match
    
    def _build_classification(self, rule: Dict[str, Any], match_reason: str) -> Optional[Dict[str, Any]]:
        """
        Build classification dict from matched rule.
        
        Returns None if rule cannot fill all required taxonomy dimensions.
        Rules must provide complete 8-dimension taxonomy to be used.
        
        Args:
            rule: Matched rule dict
            match_reason: How the match was made
        
        Returns:
            Classification dict (8-dimension format) or None if incomplete
        """
        # Get taxonomy codes from rule
        taxonomy_codes = rule.get("taxonomy_codes", {})
        
        # Check if rule has new 8-dimension format
        has_new_format = "fs_code" in taxonomy_codes or "uc_codes" in taxonomy_codes
        
        if has_new_format:
            # New 8-dimension format
            fs_code = taxonomy_codes.get("fs_code", "")
            im_code = taxonomy_codes.get("im_code", "")
            uc_codes = taxonomy_codes.get("uc_codes", [])
            dt_codes = taxonomy_codes.get("dt_codes", [])
            ch_codes = taxonomy_codes.get("ch_codes", [])
            rs_codes = taxonomy_codes.get("rs_codes", [])
            ev_codes = taxonomy_codes.get("ev_codes", [])
            ob_codes = taxonomy_codes.get("ob_codes", [])
        else:
            # Legacy format - try to convert
            fs_uc = taxonomy_codes.get("fs_uc_code", "")
            
            # Extract FS code if available
            if fs_uc and fs_uc.startswith("FS-"):
                fs_code = fs_uc
            else:
                fs_code = ""
            
            im_code = taxonomy_codes.get("im_code", "")
            
            # Convert single codes to arrays
            dt_codes = [taxonomy_codes.get("dt_code", "")] if taxonomy_codes.get("dt_code") else []
            ch_codes = [taxonomy_codes.get("ch_code", "")] if taxonomy_codes.get("ch_code") else []
            rs_codes = [taxonomy_codes.get("rs_code", "")] if taxonomy_codes.get("rs_code") else []
            ob_codes = [taxonomy_codes.get("ob_code", "")] if taxonomy_codes.get("ob_code") else []
            ev_codes = [taxonomy_codes.get("ev_code", "")] if taxonomy_codes.get("ev_code") else []
            
            # UC is not available in legacy format
            uc_codes = []
        
        # Check cardinality requirements
        # FS and IM must be exactly 1
        if not fs_code or not im_code:
            # Cannot satisfy cardinality - pass to LLM
            return None
        
        # UC, DT, CH, RS, EV must have at least 1
        if not uc_codes or not dt_codes or not ch_codes or not rs_codes or not ev_codes:
            # Cannot satisfy cardinality - pass to LLM
            return None
        
        # Build classification
        classification = {
            "service_name": rule["service_name"],
            "category": rule["category"],
            "usage_type": rule["usage_type"],
            "risk_level": rule["default_risk"],
            "rule_id": rule["rule_id"],
            "rule_version": rule["rule_version"],
            "match_reason": match_reason,
            "confidence": 1.0,  # Rule-based classifications are always 1.0
            "rationale_short": rule.get("notes", "")[:400],  # Max 400 chars
            "classification_source": "RULE",
            
            # 8-dimension taxonomy
            "fs_code": fs_code,
            "im_code": im_code,
            "uc_codes": uc_codes,
            "dt_codes": dt_codes,
            "ch_codes": ch_codes,
            "rs_codes": rs_codes,
            "ev_codes": ev_codes,
            "ob_codes": ob_codes,
            
            # Version
            "aimo_standard_version": self.aimo_standard_version,
            
            # Legacy fields for backward compatibility (deprecated)
            "fs_uc_code": "DEPRECATED",
            "dt_code": dt_codes[0] if dt_codes else "",
            "ch_code": ch_codes[0] if ch_codes else "",
            "rs_code": rs_codes[0] if rs_codes else "",
            "ob_code": ob_codes[0] if ob_codes else "",
            "ev_code": ev_codes[0] if ev_codes else "",
        }
        
        return classification
    
    def classify_batch(self, signatures: List[Dict[str, Any]]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Classify multiple signatures in batch.
        
        Args:
            signatures: List of dicts with keys: url_signature, norm_host, norm_path_template (optional)
        
        Returns:
            Dict mapping url_signature -> classification (or None if no match)
        """
        results = {}
        for sig_data in signatures:
            url_sig = sig_data["url_signature"]
            classification = self.classify(
                url_signature=url_sig,
                norm_host=sig_data["norm_host"],
                norm_path_template=sig_data.get("norm_path_template")
            )
            results[url_sig] = classification
        
        return results
    
    def get_rules_with_complete_taxonomy(self) -> List[Dict[str, Any]]:
        """
        Get list of rules that have complete 8-dimension taxonomy.
        
        Returns:
            List of rules with all required taxonomy dimensions filled
        """
        complete_rules = []
        for rule in self.enabled_rules:
            taxonomy = rule.get("taxonomy_codes", {})
            
            # Check if rule has complete new format
            has_fs = bool(taxonomy.get("fs_code"))
            has_im = bool(taxonomy.get("im_code"))
            has_uc = bool(taxonomy.get("uc_codes"))
            has_dt = bool(taxonomy.get("dt_codes"))
            has_ch = bool(taxonomy.get("ch_codes"))
            has_rs = bool(taxonomy.get("rs_codes"))
            has_ev = bool(taxonomy.get("ev_codes"))
            
            if has_fs and has_im and has_uc and has_dt and has_ch and has_rs and has_ev:
                complete_rules.append(rule)
        
        return complete_rules
    
    def get_rules_needing_migration(self) -> List[Dict[str, Any]]:
        """
        Get list of rules that need migration to 8-dimension format.
        
        Returns:
            List of rules using legacy format or missing dimensions
        """
        incomplete_rules = []
        for rule in self.enabled_rules:
            taxonomy = rule.get("taxonomy_codes", {})
            
            # Check if rule has legacy format or is incomplete
            has_legacy = "fs_uc_code" in taxonomy
            has_new = "fs_code" in taxonomy or "uc_codes" in taxonomy
            
            if has_legacy and not has_new:
                incomplete_rules.append(rule)
            elif not has_new:
                incomplete_rules.append(rule)
            else:
                # Check completeness of new format
                has_all = (
                    taxonomy.get("fs_code") and
                    taxonomy.get("im_code") and
                    taxonomy.get("uc_codes") and
                    taxonomy.get("dt_codes") and
                    taxonomy.get("ch_codes") and
                    taxonomy.get("rs_codes") and
                    taxonomy.get("ev_codes")
                )
                if not has_all:
                    incomplete_rules.append(rule)
        
        return incomplete_rules
