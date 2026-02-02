"""
Classifiers for AIMO Analysis Engine

Rule-based, LLM-based, and stub service classification.

Classifiers:
- RuleClassifier: Deterministic rule-based classification (highest priority)
- StubClassifier: Testing classifier (no LLM, Standard-compliant codes)
- LLM classification: Via llm.client.LLMClient (lowest priority)

Environment Variables:
- AIMO_CLASSIFIER=stub: Use StubClassifier instead of rule/LLM
- AIMO_DISABLE_LLM=1: Completely disable LLM calls (raises error if called)
"""

from .rule_classifier import RuleClassifier
from .stub_classifier import StubClassifier, is_stub_classifier_enabled, get_stub_classifier

__all__ = [
    "RuleClassifier",
    "StubClassifier",
    "is_stub_classifier_enabled",
    "get_stub_classifier",
]
