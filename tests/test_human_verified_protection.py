"""
Tests for is_human_verified protection (上書き禁止).

Tests:
- is_human_verified=true rows are not overwritten by UPSERT
- RULE/LLM/自動処理がis_human_verified=true行を上書きしない
"""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile

from src.db.duckdb_client import DuckDBClient


@pytest.fixture
def temp_db(tmp_path):
    """Create temporary DuckDB database."""
    db_path = tmp_path / "test.duckdb"
    client = DuckDBClient(str(db_path))
    yield client
    client.close()


def test_human_verified_not_overwritten_by_rule(temp_db):
    """Test that is_human_verified=true rows are not overwritten by RULE classification."""
    test_signature = "test_sig_human_verified"
    
    # Insert human-verified classification
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Human Verified Service",
        "usage_type": "business",
        "risk_level": "low",
        "category": "Human Verified",
        "confidence": 1.0,
        "rationale_short": "Human verified classification",
        "classification_source": "HUMAN",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "",
        "dt_code": "",
        "ch_code": "",
        "im_code": "",
        "rs_code": "",
        "ob_code": "",
        "ev_code": "",
        "status": "active",
        "is_human_verified": True,  # Human verified
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Try to overwrite with RULE classification
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Rule Classified Service",  # Different service
        "usage_type": "genai",  # Different usage type
        "risk_level": "high",  # Different risk level
        "category": "Rule Classified",
        "confidence": 0.9,
        "rationale_short": "Rule classification",
        "classification_source": "RULE",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "",
        "dt_code": "",
        "ch_code": "",
        "im_code": "",
        "rs_code": "",
        "ob_code": "",
        "ev_code": "",
        "status": "active",
        "is_human_verified": False,  # Try to overwrite
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Verify human-verified classification is preserved
    reader = temp_db.get_reader()
    result = reader.execute(
        "SELECT service_name, usage_type, risk_level, classification_source, is_human_verified FROM analysis_cache WHERE url_signature = ?",
        [test_signature]
    ).fetchone()
    
    assert result is not None
    # Human-verified values should be preserved
    assert result[0] == "Human Verified Service", "Human-verified service_name should be preserved"
    assert result[1] == "business", "Human-verified usage_type should be preserved"
    assert result[2] == "low", "Human-verified risk_level should be preserved"
    assert result[3] == "HUMAN", "Human-verified classification_source should be preserved"
    assert result[4] is True, "is_human_verified should remain True"


def test_human_verified_not_overwritten_by_llm(temp_db):
    """Test that is_human_verified=true rows are not overwritten by LLM classification."""
    test_signature = "test_sig_human_verified_llm"
    
    # Insert human-verified classification
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Human Verified Service",
        "usage_type": "business",
        "risk_level": "low",
        "category": "Human Verified",
        "confidence": 1.0,
        "rationale_short": "Human verified classification",
        "classification_source": "HUMAN",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "",
        "dt_code": "",
        "ch_code": "",
        "im_code": "",
        "rs_code": "",
        "ob_code": "",
        "ev_code": "",
        "status": "active",
        "is_human_verified": True,  # Human verified
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Try to overwrite with LLM classification
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "LLM Classified Service",  # Different service
        "usage_type": "genai",  # Different usage type
        "risk_level": "high",  # Different risk level
        "category": "LLM Classified",
        "confidence": 0.8,
        "rationale_short": "LLM classification",
        "classification_source": "LLM",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "",
        "dt_code": "",
        "ch_code": "",
        "im_code": "",
        "rs_code": "",
        "ob_code": "",
        "ev_code": "",
        "model": "gpt-4",
        "status": "active",
        "is_human_verified": False,  # Try to overwrite
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Verify human-verified classification is preserved
    reader = temp_db.get_reader()
    result = reader.execute(
        "SELECT service_name, usage_type, risk_level, classification_source, is_human_verified FROM analysis_cache WHERE url_signature = ?",
        [test_signature]
    ).fetchone()
    
    assert result is not None
    # Human-verified values should be preserved
    assert result[0] == "Human Verified Service", "Human-verified service_name should be preserved"
    assert result[1] == "business", "Human-verified usage_type should be preserved"
    assert result[2] == "low", "Human-verified risk_level should be preserved"
    assert result[3] == "HUMAN", "Human-verified classification_source should be preserved"
    assert result[4] is True, "is_human_verified should remain True"


def test_non_human_verified_can_be_overwritten(temp_db):
    """Test that is_human_verified=false rows can be overwritten."""
    test_signature = "test_sig_not_human_verified"
    
    # Insert non-human-verified classification
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Original Service",
        "usage_type": "business",
        "risk_level": "low",
        "category": "Original",
        "confidence": 0.9,
        "rationale_short": "Original classification",
        "classification_source": "RULE",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "",
        "dt_code": "",
        "ch_code": "",
        "im_code": "",
        "rs_code": "",
        "ob_code": "",
        "ev_code": "",
        "status": "active",
        "is_human_verified": False,  # Not human verified
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Overwrite with new classification
    temp_db.upsert("analysis_cache", {
        "url_signature": test_signature,
        "service_name": "Updated Service",  # Different service
        "usage_type": "genai",  # Different usage type
        "risk_level": "high",  # Different risk level
        "category": "Updated",
        "confidence": 0.95,
        "rationale_short": "Updated classification",
        "classification_source": "LLM",
        "signature_version": "1.0",
        "rule_version": "1",
        "prompt_version": "1",
        "taxonomy_version": "1.0",
        "fs_uc_code": "",
        "dt_code": "",
        "ch_code": "",
        "im_code": "",
        "rs_code": "",
        "ob_code": "",
        "ev_code": "",
        "model": "gpt-4",
        "status": "active",
        "is_human_verified": False,
        "analysis_date": datetime.utcnow().isoformat()
    }, conflict_key="url_signature")
    
    temp_db.flush()
    
    # Verify classification was updated
    reader = temp_db.get_reader()
    result = reader.execute(
        "SELECT service_name, usage_type, risk_level, classification_source FROM analysis_cache WHERE url_signature = ?",
        [test_signature]
    ).fetchone()
    
    assert result is not None
    # Values should be updated
    assert result[0] == "Updated Service", "Non-human-verified service_name should be updated"
    assert result[1] == "genai", "Non-human-verified usage_type should be updated"
    assert result[2] == "high", "Non-human-verified risk_level should be updated"
    assert result[3] == "LLM", "Non-human-verified classification_source should be updated"
