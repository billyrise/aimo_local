"""
Test Fallback Code Resolution Logic

Tests that _get_fallback_code() correctly resolves fallback codes
from the Standard Adapter with proper priority:
1. Code with "Unknown" in label
2. Code with "Other" in label
3. Code ending in -099
4. Last code in allowed codes
5. Static fallback {DIM}-099 (only if adapter unavailable)
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class TestFallbackCodeResolution:
    """Test _get_fallback_code() priority logic."""
    
    @pytest.fixture
    def mock_taxonomy_adapter(self):
        """Create a mock taxonomy adapter."""
        adapter = Mock()
        return adapter
    
    @pytest.fixture
    def llm_client_with_mock_adapter(self, mock_taxonomy_adapter):
        """Create LLMClient with mocked taxonomy adapter."""
        with patch('llm.client.yaml.safe_load', return_value={
            'default_provider': 'gemini',
            'providers': {'gemini': {'model': 'gemini-2.0-flash'}},
            'budget': {'daily_limit_usd': 10.0},
            'batching': {}
        }):
            with patch('llm.client.json.load', return_value={}):
                with patch('builtins.open', create=True):
                    with patch.object(Path, 'exists', return_value=True):
                        from llm.client import LLMClient
                        client = LLMClient.__new__(LLMClient)
                        # Minimal initialization
                        client._taxonomy_adapter = mock_taxonomy_adapter
                        client._fallback_code_cache = {}
                        client.aimo_standard_version = "0.1.7"
                        return client
    
    def test_priority_1_unknown_in_label(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Code with 'Unknown' in label should be selected first."""
        client = llm_client_with_mock_adapter
        
        # Setup mock: FS-001 = "Known Service", FS-002 = "Unknown Function", FS-099 = "Other"
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002", "FS-099"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Known Service",
            "FS-002": "Unknown Function",
            "FS-099": "Other Service"
        }.get(code, "")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-002", "Should select code with 'Unknown' in label"
    
    def test_priority_2_other_in_label(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Code with 'Other' in label should be selected if no 'Unknown'."""
        client = llm_client_with_mock_adapter
        
        # Setup mock: No "Unknown", but "Other" exists
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002", "FS-003"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Known Service",
            "FS-002": "Business Function",
            "FS-003": "Other Service"
        }.get(code, "")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-003", "Should select code with 'Other' in label"
    
    def test_priority_3_code_ending_099(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Code ending in -099 should be selected if no label match."""
        client = llm_client_with_mock_adapter
        
        # Setup mock: No "Unknown" or "Other" labels, but -099 exists
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002", "FS-099"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Service A",
            "FS-002": "Service B",
            "FS-099": "Unclassified"  # No "Unknown" or "Other"
        }.get(code, "")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-099", "Should select code ending in -099"
    
    def test_priority_4_last_code(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Last code should be selected if no other match."""
        client = llm_client_with_mock_adapter
        
        # Setup mock: No "Unknown", "Other", or -099
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002", "FS-003"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Service A",
            "FS-002": "Service B",
            "FS-003": "Service C"
        }.get(code, "")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-003", "Should select last code in allowed codes"
    
    def test_priority_5_static_fallback_no_adapter(self, llm_client_with_mock_adapter):
        """Static fallback should be used if adapter unavailable."""
        client = llm_client_with_mock_adapter
        client._taxonomy_adapter = None  # Simulate adapter unavailable
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-099", "Should use static fallback when adapter unavailable"
    
    def test_cache_is_used(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Cache should be used for subsequent calls."""
        client = llm_client_with_mock_adapter
        
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Service A",
            "FS-002": "Unknown Function"
        }.get(code, "")
        
        # First call
        result1 = client._get_fallback_code("FS")
        # Second call
        result2 = client._get_fallback_code("FS")
        
        assert result1 == result2 == "FS-002"
        # get_allowed_codes should only be called once (cached)
        assert mock_taxonomy_adapter.get_allowed_codes.call_count == 1
    
    def test_different_dimensions(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Different dimensions should resolve independently."""
        client = llm_client_with_mock_adapter
        
        def mock_get_allowed_codes(dim):
            return {
                "FS": ["FS-001", "FS-002"],
                "IM": ["IM-001", "IM-099"],
                "UC": ["UC-001", "UC-002", "UC-003"]
            }.get(dim, [])
        
        def mock_get_code_label(code):
            return {
                "FS-001": "Service A",
                "FS-002": "Unknown Service",
                "IM-001": "Model A",
                "IM-099": "Other Model",
                "UC-001": "Use Case A",
                "UC-002": "Use Case B",
                "UC-003": "Use Case C"
            }.get(code, "")
        
        mock_taxonomy_adapter.get_allowed_codes.side_effect = mock_get_allowed_codes
        mock_taxonomy_adapter.get_code_label.side_effect = mock_get_code_label
        
        fs_result = client._get_fallback_code("FS")
        im_result = client._get_fallback_code("IM")
        uc_result = client._get_fallback_code("UC")
        
        assert fs_result == "FS-002", "FS should get 'Unknown' labeled code"
        assert im_result == "IM-099", "IM should get -099 code (Other in label)"
        assert uc_result == "UC-003", "UC should get last code (no Unknown/Other/099)"
    
    def test_empty_allowed_codes_uses_static(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Empty allowed codes should use static fallback."""
        client = llm_client_with_mock_adapter
        
        mock_taxonomy_adapter.get_allowed_codes.return_value = []
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-099", "Should use static fallback for empty allowed codes"
    
    def test_adapter_exception_uses_static(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """Adapter exception should fall back to static."""
        client = llm_client_with_mock_adapter
        
        mock_taxonomy_adapter.get_allowed_codes.side_effect = Exception("Adapter error")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-099", "Should use static fallback on adapter exception"
    
    def test_case_insensitive_unknown_match(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """'Unknown' match should be case-insensitive."""
        client = llm_client_with_mock_adapter
        
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Service A",
            "FS-002": "UNKNOWN Function"  # Uppercase
        }.get(code, "")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-002", "Should match 'UNKNOWN' case-insensitively"
    
    def test_case_insensitive_other_match(self, llm_client_with_mock_adapter, mock_taxonomy_adapter):
        """'Other' match should be case-insensitive."""
        client = llm_client_with_mock_adapter
        
        mock_taxonomy_adapter.get_allowed_codes.return_value = ["FS-001", "FS-002"]
        mock_taxonomy_adapter.get_code_label.side_effect = lambda code: {
            "FS-001": "Service A",
            "FS-002": "OTHER Service"  # Uppercase
        }.get(code, "")
        
        result = client._get_fallback_code("FS")
        
        assert result == "FS-002", "Should match 'OTHER' case-insensitively"


class TestUnknownClassificationUsesFallback:
    """Test that _get_unknown_classification uses _get_fallback_code."""
    
    def test_unknown_classification_uses_dynamic_codes(self):
        """_get_unknown_classification should use _get_fallback_code for all dimensions."""
        with patch('llm.client.yaml.safe_load', return_value={
            'default_provider': 'gemini',
            'providers': {'gemini': {'model': 'gemini-2.0-flash'}},
            'budget': {'daily_limit_usd': 10.0},
            'batching': {}
        }):
            with patch('llm.client.json.load', return_value={}):
                with patch('builtins.open', create=True):
                    with patch.object(Path, 'exists', return_value=True):
                        from llm.client import LLMClient
                        client = LLMClient.__new__(LLMClient)
                        
                        # Setup mock adapter that returns specific codes
                        mock_adapter = Mock()
                        mock_adapter.get_allowed_codes.return_value = ["XX-001", "XX-UNKNOWN"]
                        mock_adapter.get_code_label.side_effect = lambda code: {
                            "XX-001": "Normal",
                            "XX-UNKNOWN": "Unknown Category"
                        }.get(code, "")
                        
                        client._taxonomy_adapter = mock_adapter
                        client._fallback_code_cache = {}
                        client.aimo_standard_version = "0.1.7"
                        
                        # Mock _get_fallback_code to track calls
                        fallback_calls = []
                        original_get_fallback = client._get_fallback_code
                        
                        def mock_get_fallback(dim):
                            fallback_calls.append(dim)
                            # Return predictable codes for testing
                            return f"{dim}-FALLBACK"
                        
                        client._get_fallback_code = mock_get_fallback
                        
                        result = client._get_unknown_classification()
                        
                        # Verify _get_fallback_code was called for each dimension
                        assert "FS" in fallback_calls
                        assert "IM" in fallback_calls
                        assert "UC" in fallback_calls
                        assert "DT" in fallback_calls
                        assert "CH" in fallback_calls
                        assert "RS" in fallback_calls
                        assert "EV" in fallback_calls
                        
                        # Verify result uses fallback codes
                        assert result["fs_code"] == "FS-FALLBACK"
                        assert result["im_code"] == "IM-FALLBACK"
                        assert result["uc_codes"] == ["UC-FALLBACK"]
                        assert result["ob_codes"] == []  # OB should be empty
