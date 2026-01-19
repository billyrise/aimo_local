"""
Test that default provider is Gemini.

Tests that:
1. When no provider is explicitly specified, Gemini is selected
2. Configuration file default_provider: gemini is respected
"""

import pytest
import yaml
from pathlib import Path
import sys
import tempfile

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm.client import LLMClient


class TestProviderDefaultIsGemini:
    """Test that Gemini is the default provider."""
    
    def test_default_provider_from_config(self, tmp_path):
        """Test that default_provider: gemini in config is respected."""
        config_path = tmp_path / "llm_providers.yaml"
        schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
        
        # Create config with gemini as default
        config_content = """
default_provider: gemini

providers:
  gemini:
    model: gemini-2.5-flash
    auth_env: GEMINI_API_KEY
    structured_output: true
    timeout_seconds: 30
    max_retries: 2
    pricing:
      input_per_1m_tokens_usd: 0.30
      output_per_1m_tokens_usd: 2.50

  openai:
    base_url: "https://api.openai.com/v1"
    auth_env: "OPENAI_API_KEY"
    model: "gpt-4o-mini"
    structured_output: true
    timeout_seconds: 30
    max_retries: 2
    pricing:
      input_per_1m_tokens: 0.15
      output_per_1m_tokens: 0.60

budget:
  daily_limit_usd: 10.0

batching:
  max_signatures_per_request: 20
"""
        config_path.write_text(config_content)
        
        # Create client
        client = LLMClient(config_path=str(config_path), schema_path=str(schema_path))
        
        # Verify default provider is gemini
        assert client.default_provider == "gemini"
        assert client.provider_name == "gemini"
        assert "gemini" in client.providers
        assert client.providers["gemini"]["model"] == "gemini-2.5-flash"
    
    def test_gemini_provider_config_loaded(self, tmp_path):
        """Test that Gemini provider configuration is properly loaded."""
        config_path = tmp_path / "llm_providers.yaml"
        schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
        
        config_content = """
default_provider: gemini

providers:
  gemini:
    model: gemini-2.5-flash
    auth_env: GEMINI_API_KEY
    structured_output: true
    timeout_seconds: 30
    max_retries: 2
    pricing:
      input_per_1m_tokens_usd: 0.30
      output_per_1m_tokens_usd: 2.50

budget:
  daily_limit_usd: 10.0

batching:
  max_signatures_per_request: 20
"""
        config_path.write_text(config_content)
        
        client = LLMClient(config_path=str(config_path), schema_path=str(schema_path))
        
        # Verify provider config
        assert client.provider_config["model"] == "gemini-2.5-flash"
        assert client.provider_config["auth_env"] == "GEMINI_API_KEY"
        assert client.provider_config["structured_output"] is True
        assert client.provider_config["pricing"]["input_per_1m_tokens_usd"] == 0.30
        assert client.provider_config["pricing"]["output_per_1m_tokens_usd"] == 2.50
    
    def test_default_provider_fallback(self, tmp_path):
        """Test that if default_provider is not set, it falls back to openai (legacy behavior)."""
        config_path = tmp_path / "llm_providers.yaml"
        schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
        
        # Config without default_provider
        config_content = """
providers:
  openai:
    base_url: "https://api.openai.com/v1"
    auth_env: "OPENAI_API_KEY"
    model: "gpt-4o-mini"

budget:
  daily_limit_usd: 10.0
"""
        config_path.write_text(config_content)
        
        client = LLMClient(config_path=str(config_path), schema_path=str(schema_path))
        
        # Should fall back to openai
        assert client.default_provider == "openai"
        assert client.provider_name == "openai"
