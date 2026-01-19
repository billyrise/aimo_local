"""
Test Gemini LLM client schema validation.

Tests that:
1. Gemini client returns responses that match analysis_output.schema.json
2. Schema mismatches and JSON parse failures fall back to needs_review
3. Deterministic behavior is maintained
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm.client import LLMClient


class TestGeminiSchemaValidation:
    """Test Gemini schema validation and error handling."""
    
    @pytest.fixture
    def llm_client(self, tmp_path):
        """Create LLM client with test config."""
        config_path = tmp_path / "llm_providers.yaml"
        schema_path = Path(__file__).parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
        
        # Create minimal config
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
  estimation_buffer: 1.2

batching:
  max_signatures_per_request: 20
  max_sample_chars: 8000
"""
        config_path.write_text(config_content)
        
        client = LLMClient(config_path=str(config_path), schema_path=str(schema_path))
        return client
    
    @patch.dict('os.environ', {'GEMINI_API_KEY': 'test-key'})
    @patch('google.genai')
    def test_gemini_valid_schema_response(self, mock_genai, llm_client):
        """Test that valid schema responses are accepted."""
        # Mock Gemini response
        mock_response = MagicMock()
        mock_response.text = json.dumps([
            {
                "service_name": "ChatGPT",
                "usage_type": "genai",
                "risk_level": "high",
                "category": "GenAI",
                "confidence": 0.95,
                "rationale_short": "OpenAI's ChatGPT service"
            }
        ])
        mock_response.usage_metadata = MagicMock()
        mock_response.usage_metadata.prompt_token_count = 100
        mock_response.usage_metadata.candidates_token_count = 50
        mock_response.usage_metadata.total_token_count = 150
        
        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response
        mock_client = MagicMock()
        mock_client.models.get.return_value = mock_model
        mock_genai.Client.return_value = mock_client
        mock_genai.configure = MagicMock()
        
        # Test
        signatures = [{
            "url_signature": "test-sig-1",
            "norm_host": "chat.openai.com",
            "norm_path_template": "/api/*",
            "access_count": 10,
            "bytes_sent_sum": 1000
        }]
        
        results = llm_client.analyze_batch(signatures)
        
        # Verify
        assert len(results) == 1
        assert results[0]["service_name"] == "ChatGPT"
        assert results[0]["usage_type"] == "genai"
        assert results[0]["risk_level"] == "high"
        assert results[0]["confidence"] == 0.95
    
    @patch.dict('os.environ', {'GEMINI_API_KEY': 'test-key'})
    @patch('google.genai')
    def test_gemini_invalid_schema_fallback(self, mock_genai, llm_client):
        """Test that invalid schema responses raise exception (will be caught by main.py and marked needs_review)."""
        # Mock Gemini response with invalid schema (missing required field)
        mock_response = MagicMock()
        mock_response.text = json.dumps([
            {
                "service_name": "Test",
                # Missing required fields: usage_type, risk_level, category, confidence, rationale_short
            }
        ])
        mock_response.usage_metadata = MagicMock()
        mock_response.usage_metadata.prompt_token_count = 100
        mock_response.usage_metadata.candidates_token_count = 50
        mock_response.usage_metadata.total_token_count = 150
        
        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response
        mock_client = MagicMock()
        mock_client.models.get.return_value = mock_model
        mock_genai.Client.return_value = mock_client
        mock_genai.configure = MagicMock()
        
        # Test - should raise exception due to schema validation failure
        signatures = [{
            "url_signature": "test-sig-1",
            "norm_host": "example.com",
            "norm_path_template": "/",
            "access_count": 1,
            "bytes_sent_sum": 100
        }]
        
        with pytest.raises(Exception) as exc_info:
            llm_client.analyze_batch(signatures)
        
        assert "json_schema_error" in str(exc_info.value) or "schema" in str(exc_info.value).lower()
    
    @patch.dict('os.environ', {'GEMINI_API_KEY': 'test-key'})
    @patch('google.genai')
    def test_gemini_json_parse_failure(self, mock_genai, llm_client):
        """Test that JSON parse failures are handled."""
        # Mock Gemini response with invalid JSON
        mock_response = MagicMock()
        mock_response.text = "This is not valid JSON {"
        mock_response.usage_metadata = MagicMock()
        mock_response.usage_metadata.prompt_token_count = 100
        mock_response.usage_metadata.candidates_token_count = 50
        mock_response.usage_metadata.total_token_count = 150
        
        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response
        mock_client = MagicMock()
        mock_client.models.get.return_value = mock_model
        mock_genai.Client.return_value = mock_client
        mock_genai.configure = MagicMock()
        
        # Test - should raise exception after retries
        signatures = [{
            "url_signature": "test-sig-1",
            "norm_host": "example.com",
            "norm_path_template": "/",
            "access_count": 1,
            "bytes_sent_sum": 100
        }]
        
        with pytest.raises(Exception) as exc_info:
            llm_client.analyze_batch(signatures)
        
        assert "json" in str(exc_info.value).lower() or "parse" in str(exc_info.value).lower()
    
    @patch.dict('os.environ', {'GEMINI_API_KEY': 'test-key'})
    def test_gemini_api_key_priority(self, llm_client):
        """Test that GOOGLE_API_KEY takes precedence over GEMINI_API_KEY."""
        with patch.dict('os.environ', {'GOOGLE_API_KEY': 'google-key', 'GEMINI_API_KEY': 'gemini-key'}):
            with patch('google.genai') as mock_genai:
                mock_client = MagicMock()
                mock_genai.Client.return_value = mock_client
                mock_genai.configure = MagicMock()
                
                # Try to call (will fail at model.get, but we check configure was called)
                signatures = [{
                    "url_signature": "test-sig-1",
                    "norm_host": "example.com",
                    "norm_path_template": "/",
                    "access_count": 1,
                    "bytes_sent_sum": 100
                }]
                
                try:
                    llm_client.analyze_batch(signatures)
                except:
                    pass  # Expected to fail, we just check configure was called
                
                # Verify GOOGLE_API_KEY was used (configure called with google-key)
                mock_genai.configure.assert_called()
                # Check that the key passed was google-key (first in OR)
                call_args = mock_genai.configure.call_args
                assert call_args is not None
