"""
Stub Classifier for Contract Testing

A deterministic classifier that generates AIMO Standard v0.1.7+ compliant
8-dimension classifications WITHOUT calling LLM.

This classifier is used for:
1. Contract E2E tests (Evidence Bundle generation → Validator → PASS)
2. CI testing without API keys
3. Testing pipeline integrity without LLM costs

IMPORTANT:
- All codes are fetched from Standard Adapter (no hardcoding)
- Cardinality rules are strictly followed:
  - FS, IM: Exactly 1
  - UC, DT, CH, RS, EV: 1+
  - OB: 0+

Environment Variable:
- AIMO_CLASSIFIER=stub: Use this classifier instead of rule/LLM pipeline
"""

import os
from typing import Dict, List, Optional, Any

from standard_adapter.taxonomy import get_taxonomy_adapter, DIMENSION_CARDINALITY
from standard_adapter.constants import AIMO_STANDARD_VERSION_DEFAULT


class StubClassifier:
    """
    Stub classifier for testing without LLM.
    
    Generates valid 8-dimension classifications using codes from
    Standard Adapter. The codes are deterministic (always selects
    the first code from each dimension's allowed list).
    """
    
    def __init__(self, version: str = AIMO_STANDARD_VERSION_DEFAULT):
        """
        Initialize stub classifier.
        
        Args:
            version: AIMO Standard version to use for taxonomy
        """
        self.version = version
        self._adapter = get_taxonomy_adapter(version=version)
        
        # Pre-fetch codes for each dimension
        self._default_codes = self._build_default_codes()
    
    def _build_default_codes(self) -> Dict[str, List[str]]:
        """
        Build default code selections for each dimension.
        
        Returns:
            Dict mapping dimension -> list of selected codes
        """
        codes = {}
        
        for dim, cardinality in DIMENSION_CARDINALITY.items():
            allowed = self._adapter.get_allowed_codes(dim)
            
            if not allowed:
                # Should not happen if Standard is properly synced
                raise ValueError(f"No codes found for dimension {dim} in Standard v{self.version}")
            
            min_count = cardinality["min"]
            
            if dim == "OB":
                # OB is optional (0+), don't include any by default
                codes[dim] = []
            elif min_count == 1 and cardinality["max"] == 1:
                # Exactly 1 (FS, IM)
                codes[dim] = [allowed[0]]
            else:
                # 1+ (UC, DT, CH, RS, EV) - select first code
                codes[dim] = [allowed[0]]
        
        return codes
    
    def classify(self,
                 url_signature: str,
                 norm_host: str,
                 norm_path_template: Optional[str] = None,
                 **kwargs) -> Dict[str, Any]:
        """
        Generate a stub classification for a single signature.
        
        Args:
            url_signature: URL signature
            norm_host: Normalized host
            norm_path_template: Normalized path template (optional)
            **kwargs: Additional fields (ignored)
        
        Returns:
            Classification dict compliant with AIMO Standard 8-dimension format
        """
        return {
            "service_name": f"Stub Service ({norm_host})",
            "usage_type": "unknown",
            "risk_level": "medium",
            "category": "Stub Classification",
            "confidence": 1.0,
            "rationale_short": "Stub classification for contract testing (no LLM)",
            
            # 8-dimension codes from Standard Adapter
            "fs_code": self._default_codes["FS"][0],
            "im_code": self._default_codes["IM"][0],
            "uc_codes": self._default_codes["UC"].copy(),
            "dt_codes": self._default_codes["DT"].copy(),
            "ch_codes": self._default_codes["CH"].copy(),
            "rs_codes": self._default_codes["RS"].copy(),
            "ev_codes": self._default_codes["EV"].copy(),
            "ob_codes": self._default_codes["OB"].copy(),
            
            # Metadata
            "aimo_standard_version": self.version,
            "classification_source": "STUB",
            "match_reason": "stub_classifier",
            "_validation_errors": [],
            "_needs_review": False,
        }
    
    def classify_batch(self, signatures: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Classify a batch of signatures.
        
        Args:
            signatures: List of signature dicts
        
        Returns:
            Dict mapping url_signature -> classification result
        """
        results = {}
        for sig in signatures:
            url_signature = sig.get("url_signature", "")
            norm_host = sig.get("norm_host", "unknown")
            norm_path_template = sig.get("norm_path_template")
            
            classification = self.classify(
                url_signature=url_signature,
                norm_host=norm_host,
                norm_path_template=norm_path_template
            )
            results[url_signature] = classification
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get classifier statistics."""
        return {
            "classifier_type": "stub",
            "aimo_standard_version": self.version,
            "default_codes": {
                dim: codes for dim, codes in self._default_codes.items()
            },
            "taxonomy_stats": self._adapter.get_stats()
        }


def is_stub_classifier_enabled() -> bool:
    """
    Check if stub classifier is enabled via environment variable.
    
    Returns:
        True if AIMO_CLASSIFIER=stub
    """
    return os.getenv("AIMO_CLASSIFIER", "").lower() == "stub"


def get_stub_classifier(version: str = AIMO_STANDARD_VERSION_DEFAULT) -> StubClassifier:
    """
    Get a stub classifier instance.
    
    Args:
        version: AIMO Standard version
    
    Returns:
        StubClassifier instance
    """
    return StubClassifier(version=version)
