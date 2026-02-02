"""
LLM Client for AIMO Analysis Engine

Provides LLM-based service classification using Structured Outputs (JSON Schema).
Supports multiple providers (OpenAI, Azure OpenAI, Anthropic) with retry logic and error handling.

All LLM requests are deterministic: same input → same output (via caching).

Environment Variables:
- AIMO_DISABLE_LLM=1: Completely disable LLM calls. Any call raises LLMDisabledError.
  Used in CI/testing to ensure LLM is not invoked accidentally.
"""

import os
import json
import time
import random
import hashlib
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import yaml
import jsonschema
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class LLMDisabledError(Exception):
    """
    Raised when LLM calls are disabled via AIMO_DISABLE_LLM=1.
    
    This error is raised immediately when any LLM API call is attempted
    while LLM is disabled. This ensures tests fail fast if they
    accidentally try to use LLM.
    """
    
    def __init__(self, method_name: str):
        super().__init__(
            f"LLM call attempted ({method_name}) but AIMO_DISABLE_LLM=1 is set. "
            "LLM calls are completely disabled. Use stub_classifier for testing."
        )


def _check_llm_disabled():
    """
    Check if LLM is disabled via environment variable.
    
    Returns:
        True if AIMO_DISABLE_LLM=1 is set
    """
    return os.getenv("AIMO_DISABLE_LLM", "").lower() in ("1", "true", "yes")

# Suppress urllib3 SSL warnings for LibreSSL compatibility
# urllib3 1.x works with LibreSSL, but may show warnings in some environments
warnings.filterwarnings('ignore', category=UserWarning, module='urllib3')

# Google GenAI SDK (optional import)
try:
    import google.genai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


def clean_schema_for_gemini(schema_obj: Dict[str, Any], remove_title_desc: bool = True) -> Dict[str, Any]:
    """
    Recursively clean JSON Schema to Gemini _responseJsonSchema compatible format.
    
    Uses allowlist approach: only keeps fields explicitly supported by Gemini API.
    This is more audit-friendly and prevents future unknown-field errors.
    
    Supported fields per Gemini API documentation:
    - type, properties, required, additionalProperties
    - anyOf, oneOf, items, enum
    - string: minLength, maxLength
    - number: minimum, maximum
    - array: items
    - object: properties, additionalProperties
    
    NOT supported:
    - $schema, $id (JSON Schema metadata)
    - title, description (optional, removed by default for safety)
    
    Args:
        schema_obj: JSON Schema object to clean
        remove_title_desc: If True, remove title and description fields (default: True)
    
    Returns:
        Cleaned schema object compatible with Gemini _responseJsonSchema
    """
    if not isinstance(schema_obj, dict):
        return schema_obj
    
    # Allowlist of supported fields for Gemini _responseJsonSchema
    ALLOWED_FIELDS = {
        # Core schema fields
        "type", "properties", "required", "additionalProperties",
        # Union types
        "anyOf", "oneOf", "allOf",
        # Array/object structure
        "items",
        # String constraints
        "minLength", "maxLength", "pattern",
        # Number constraints
        "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
        # Enum
        "enum",
    }
    
    # Add title/description to allowed fields if not removing them
    if not remove_title_desc:
        ALLOWED_FIELDS.add("title")
        ALLOWED_FIELDS.add("description")
    
    cleaned = {}
    for key, value in schema_obj.items():
        # Skip unsupported metadata fields ($schema, $id)
        if key in ["$schema", "$id"]:
            continue
        
        # Special handling for 'properties': recursively clean each property definition
        # Properties themselves are not in ALLOWED_FIELDS, but their contents need cleaning
        if key == "properties":
            if isinstance(value, dict):
                cleaned_properties = {}
                for prop_name, prop_schema in value.items():
                    if isinstance(prop_schema, dict):
                        cleaned_properties[prop_name] = clean_schema_for_gemini(prop_schema, remove_title_desc)
                    else:
                        cleaned_properties[prop_name] = prop_schema
                cleaned[key] = cleaned_properties
            else:
                cleaned[key] = value
            continue
        
        # Only keep allowlisted fields
        if key not in ALLOWED_FIELDS:
            continue
        
        # Recursively clean nested objects
        if isinstance(value, dict):
            cleaned[key] = clean_schema_for_gemini(value, remove_title_desc)
        elif isinstance(value, list):
            # Handle arrays (e.g., required, anyOf, oneOf, allOf, items)
            if key in ["required", "enum"]:
                # Keep as-is for required/enum arrays
                cleaned[key] = value
            elif key in ["anyOf", "oneOf", "allOf"]:
                # Recursively clean each option in union
                cleaned[key] = [clean_schema_for_gemini(item, remove_title_desc) if isinstance(item, dict) else item for item in value]
            elif key == "items":
                # items can be a dict (schema) or array of schemas
                if isinstance(value[0], dict) if value else False:
                    cleaned[key] = clean_schema_for_gemini(value[0], remove_title_desc) if len(value) == 1 else [clean_schema_for_gemini(item, remove_title_desc) if isinstance(item, dict) else item for item in value]
                else:
                    cleaned[key] = value
            else:
                # Other arrays (shouldn't occur in standard JSON Schema, but handle gracefully)
                cleaned[key] = [clean_schema_for_gemini(item, remove_title_desc) if isinstance(item, dict) else item for item in value]
        else:
            # Primitive values (strings, numbers, booleans)
            cleaned[key] = value
    
    return cleaned


class LLMClient:
    """
    LLM client for service classification.
    
    Features:
    - Structured Outputs (JSON Schema) for reliable parsing
    - Automatic retry with exponential backoff
    - Error classification (permanent vs transient)
    - Budget tracking and enforcement
    - Deterministic caching (same input → same output)
    """
    
    # Permanent errors (do not retry)
    PERMANENT_ERRORS = {
        "invalid_request_error",
        "context_length_exceeded",
        "invalid_api_key",
        "authentication_error"
    }
    
    # Transient errors (retry with backoff)
    TRANSIENT_ERRORS = {
        "rate_limit_error",
        "timeout",
        "network_error",
        "server_error"
    }
    
    def __init__(self, 
                 config_path: Optional[str] = None,
                 schema_path: Optional[str] = None):
        """
        Initialize LLM client.
        
        Args:
            config_path: Path to config/llm_providers.yaml (default: config/llm_providers.yaml)
            schema_path: Path to llm/schemas/analysis_output.schema.json (default: llm/schemas/analysis_output.schema.json)
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "llm_providers.yaml"
        if schema_path is None:
            schema_path = Path(__file__).parent.parent.parent / "llm" / "schemas" / "analysis_output.schema.json"
        
        self.config_path = Path(config_path)
        self.schema_path = Path(schema_path)
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"LLM config not found: {self.config_path}")
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
        
        # Load config
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Load schema
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
        
        # Get provider config
        self.default_provider = self.config.get("default_provider", "openai")
        self.providers = self.config.get("providers", {})
        self.budget_config = self.config.get("budget", {})
        self.batching_config = self.config.get("batching", {})
        
        # Initialize provider
        self.provider_name = self.default_provider
        self.provider_config = self.providers.get(self.provider_name, {})
        
        # Initialize Gemini client (lazy initialization)
        self._gemini_client = None
        
        # Budget tracking
        self.daily_budget_usd = self.budget_config.get("daily_limit_usd", 10.0)
        self.daily_spent_usd = 0.0
        self.daily_reset_date = datetime.utcnow().date()
        
        # Retry config
        self.max_retries = self.provider_config.get("max_retries", 2)
        self.initial_delay_seconds = 1.0
        self.exponential_base = 2.0
        self.jitter_max_ms = 300
        
        # Initialize HTTP session with retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.initial_delay_seconds,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
                # Initialize budget controller with priority support
        from llm.budget import BudgetController
        priority_order = self.budget_config.get("priority_order", ["A", "B", "C"])
        self.budget_controller = BudgetController(
            daily_limit_usd=self.daily_budget_usd,
            priority_order=priority_order,
            estimation_buffer=self.budget_config.get("estimation_buffer", 1.2)
        )
        
        # Load prompt templates
        from llm.prompt_templates import (
            SERVICE_ANALYSIS_SYSTEM,
            JSON_RETRY_PROMPT,
            format_samples_for_prompt,
            get_json_schema_for_prompt,
            build_user_prompt,
            DEFAULT_AIMO_STANDARD_VERSION
        )
        self.system_prompt = SERVICE_ANALYSIS_SYSTEM
        self.json_retry_prompt = JSON_RETRY_PROMPT
        self.format_samples = format_samples_for_prompt
        self.get_json_schema = get_json_schema_for_prompt
        self.build_user_prompt = build_user_prompt
        self.aimo_standard_version = DEFAULT_AIMO_STANDARD_VERSION
        
        # Initialize taxonomy validator (optional, for validation)
        self._taxonomy_adapter = None
        try:
            from standard_adapter.taxonomy import get_taxonomy_adapter
            self._taxonomy_adapter = get_taxonomy_adapter(version=self.aimo_standard_version)
        except ImportError:
            pass  # Standard Adapter not available
        except Exception as e:
            print(f"Warning: Could not initialize taxonomy adapter: {e}", flush=True)
        
        # Cache for fallback codes (populated lazily)
        self._fallback_code_cache: Dict[str, str] = {}
    
    def _get_fallback_code(self, dim: str) -> str:
        """
        Get a fallback code for a dimension dynamically from Standard Adapter.
        
        Priority order:
        1. Code with "Unknown" in label (e.g., "Unknown Function")
        2. Code with "Other" in label (e.g., "Other Service")
        3. Code ending in -099 (legacy convention)
        4. Last code in allowed codes (fallback)
        5. Static fallback {DIM}-099 (only if adapter unavailable)
        
        Args:
            dim: Dimension code (e.g., "FS", "IM", "UC", "DT", "CH", "RS", "EV", "OB")
        
        Returns:
            A valid fallback code for the dimension
        """
        dim = dim.upper()
        
        # Check cache first
        if dim in self._fallback_code_cache:
            return self._fallback_code_cache[dim]
        
        # Static fallback (used only if adapter unavailable)
        static_fallback = f"{dim}-099"
        
        # Try to get from taxonomy adapter
        if self._taxonomy_adapter is None:
            return static_fallback
        
        try:
            # Get allowed codes for dimension
            allowed_codes = self._taxonomy_adapter.get_allowed_codes(dim)
            
            if not allowed_codes:
                return static_fallback
            
            # Priority 1: Look for "Unknown" in label
            for code in allowed_codes:
                try:
                    label = self._taxonomy_adapter.get_code_label(code)
                    if label and "unknown" in label.lower():
                        self._fallback_code_cache[dim] = code
                        return code
                except Exception:
                    continue
            
            # Priority 2: Look for "Other" in label
            for code in allowed_codes:
                try:
                    label = self._taxonomy_adapter.get_code_label(code)
                    if label and "other" in label.lower():
                        self._fallback_code_cache[dim] = code
                        return code
                except Exception:
                    continue
            
            # Priority 3: Look for -099 suffix (legacy convention)
            for code in allowed_codes:
                if code.endswith("-099"):
                    self._fallback_code_cache[dim] = code
                    return code
            
            # Priority 4: Use last code in allowed codes (assumed to be "other" by convention)
            fallback = allowed_codes[-1]
            self._fallback_code_cache[dim] = fallback
            return fallback
            
        except Exception as e:
            # Log warning and use static fallback
            print(f"Warning: Could not get fallback code for {dim}: {e}", flush=True)
            return static_fallback
    
    def _check_budget(self, estimated_cost_usd: float, candidate_flags: Optional[str] = None) -> Tuple[bool, str]:
        """
        Check if request is within daily budget with priority-based control.
        
        Args:
            estimated_cost_usd: Estimated cost for this request
            candidate_flags: Pipe-separated flags (e.g., "A|B|burst") for priority determination
        
        Returns:
            Tuple of (is_allowed: bool, reason: str)
        """
        # Use budget controller for priority-based checking
        should_analyze, reason = self.budget_controller.should_analyze(estimated_cost_usd, candidate_flags)
        return should_analyze, reason
    
    def _estimate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """
        Estimate cost for a request.
        
        Args:
            input_tokens: Estimated input tokens
            output_tokens: Estimated output tokens
        
        Returns:
            Estimated cost in USD
        """
        pricing = self.provider_config.get("pricing", {})
        # Support both naming conventions
        input_per_1m = pricing.get("input_per_1m_tokens_usd") or pricing.get("input_per_1m_tokens", 0.0)
        output_per_1m = pricing.get("output_per_1m_tokens_usd") or pricing.get("output_per_1m_tokens", 0.0)
        
        cost = (input_tokens / 1_000_000) * input_per_1m + (output_tokens / 1_000_000) * output_per_1m
        
        # Apply buffer
        buffer = self.budget_config.get("estimation_buffer", 1.2)
        return cost * buffer
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for exponential backoff with jitter."""
        base_delay = self.initial_delay_seconds * (self.exponential_base ** (attempt - 1))
        jitter = random.uniform(0, self.jitter_max_ms / 1000)
        return base_delay + jitter
    
    def _classify_error(self, error: Exception) -> Tuple[str, bool]:
        """
        Classify error as permanent or transient.
        
        Args:
            error: Exception object
        
        Returns:
            Tuple of (error_type, is_permanent)
        """
        error_str = str(error).lower()
        
        # Check for permanent errors
        for perm_err in self.PERMANENT_ERRORS:
            if perm_err in error_str:
                return perm_err, True
        
        # Check for transient errors
        for trans_err in self.TRANSIENT_ERRORS:
            if trans_err in error_str:
                return trans_err, False
        
        # Default: treat as transient
        return "unknown_error", False
    
    def _call_openai_api(self, messages: List[Dict[str, str]], 
                         response_format: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Call OpenAI API (or Azure OpenAI).
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            response_format: Optional response format (for structured outputs)
        
        Returns:
            API response dict
        """
        base_url = self.provider_config.get("base_url", "https://api.openai.com/v1")
        model = self.provider_config.get("model", "gpt-4o-mini")
        api_key_env = self.provider_config.get("auth_env", "OPENAI_API_KEY")
        api_key = os.getenv(api_key_env)
        
        if not api_key:
            raise ValueError(f"API key not found in environment variable: {api_key_env}")
        
        # Build request
        url = f"{base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Azure OpenAI specific headers
        if "azure" in self.provider_name.lower():
            api_version = self.provider_config.get("api_version", "2024-10-01-preview")
            headers["api-key"] = api_key
            url = f"{base_url}/openai/deployments/{model}/chat/completions?api-version={api_version}"
        
        payload = {
            "model": model,
            "messages": messages,
            "temperature": 0.0,  # Deterministic
            "max_tokens": 2000
        }
        
        # Add structured output if supported
        if self.provider_config.get("structured_output", False) and response_format:
            payload["response_format"] = response_format
        
        # Make request
        timeout = self.provider_config.get("timeout_seconds", 30)
        response = self.session.post(url, json=payload, headers=headers, timeout=timeout)
        
        # Check for errors
        if response.status_code == 429:
            raise Exception("rate_limit_error: Rate limit exceeded")
        elif response.status_code >= 500:
            raise Exception(f"server_error: Server error {response.status_code}")
        elif not response.ok:
            error_data = response.json() if response.content else {}
            error_type = error_data.get("error", {}).get("type", "unknown_error")
            raise Exception(f"{error_type}: {error_data.get('error', {}).get('message', 'Unknown error')}")
        
        return response.json()
    
    def _call_gemini_api(self, messages: List[Dict[str, str]], 
                         response_json_schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Call Google Gemini API using REST API directly.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            response_json_schema: JSON schema for structured outputs
            
        Returns:
            API response dict with 'text' and 'usage' keys (OpenAI-compatible format)
        """
        model_name = self.provider_config.get("model", "gemini-2.0-flash")
        auth_env = self.provider_config.get("auth_env", "GEMINI_API_KEY")
        
        # Try GOOGLE_API_KEY first (takes precedence), then GEMINI_API_KEY
        api_key = os.getenv("GOOGLE_API_KEY") or os.getenv(auth_env)
        
        if not api_key:
            raise ValueError(f"API key not found in environment variables: GOOGLE_API_KEY or {auth_env}")
        
        # Build API endpoint URL
        # Format: https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
        
        # Convert messages to Gemini format
        # Gemini uses 'parts' with 'text' content
        contents = []
        system_prompt = None
        
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            
            if role == "system":
                # Store system prompt to prepend to first user message
                system_prompt = content
            elif role == "user":
                # Prepend system prompt if available
                if system_prompt:
                    content = f"{system_prompt}\n\n{content}"
                    system_prompt = None  # Only prepend once
                
                contents.append({
                    "parts": [{"text": content}]
                })
            elif role == "assistant":
                contents.append({
                    "role": "model",
                    "parts": [{"text": content}]
                })
        
        # Build request payload
        payload = {
            "contents": contents,
            "generationConfig": {
                "temperature": 0.0,  # Deterministic
                "maxOutputTokens": 2000,
            }
        }
        
        # Add structured output if schema provided
        # Gemini API: Use _responseJsonSchema for JSON Schema (not responseSchema which is for OpenAPI Schema)
        # Reference: Gemini API supports _responseJsonSchema with specific allowed fields
        # Allowed fields: type, properties, required, additionalProperties, anyOf, oneOf, items, enum, etc.
        # NOT allowed: $schema, $id (JSON Schema metadata)
        if response_json_schema:
            # Clean schema using allowlist approach (more audit-friendly)
            # Only keep fields explicitly supported by Gemini's _responseJsonSchema
            schema_copy = clean_schema_for_gemini(response_json_schema, remove_title_desc=True)
            
            payload["generationConfig"]["responseMimeType"] = "application/json"
            payload["generationConfig"]["_responseJsonSchema"] = schema_copy  # Use _responseJsonSchema, not responseSchema
        
        # Make HTTP request
        timeout = self.provider_config.get("timeout_seconds", 30)
        headers = {
            "Content-Type": "application/json",
            "X-goog-api-key": api_key
        }
        
        try:
            response = requests.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=timeout
            )
            # Log response status for debugging
            if response.status_code != 200:
                print(f"  DEBUG: Gemini API response status: {response.status_code}", flush=True)
                try:
                    error_body = response.json()
                    print(f"  DEBUG: Gemini API error response: {error_body.get('error', {}).get('message', 'Unknown error')[:200]}", flush=True)
                except:
                    print(f"  DEBUG: Gemini API error response (raw): {response.text[:200]}", flush=True)
            
            response.raise_for_status()
            response_data = response.json()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and e.response else None
            error_str = str(e).lower()
            retry_after = None
            
            # Log error response for debugging
            if hasattr(e, 'response') and e.response:
                try:
                    error_body = e.response.json()
                    print(f"  DEBUG: Gemini API error (status {status_code}): {error_body.get('error', {}).get('message', 'Unknown error')[:200]}", flush=True)
                except:
                    print(f"  DEBUG: Gemini API error (status {status_code}): {e.response.text[:200]}", flush=True)
                
                # Extract Retry-After header if present
                retry_after = e.response.headers.get("Retry-After")
                if retry_after:
                    try:
                        retry_after = int(retry_after)
                    except:
                        retry_after = None
            
            if status_code == 429 or "quota" in error_str or "rate" in error_str:
                # Include Retry-After in error message for downstream handling
                if retry_after:
                    raise Exception(f"rate_limit_error: Rate limit exceeded (Retry-After: {retry_after}s)")
                else:
                    raise Exception("rate_limit_error: Rate limit exceeded")
            elif status_code == 401 or status_code == 403 or "invalid" in error_str or "auth" in error_str or "key" in error_str:
                raise Exception("invalid_api_key: Invalid API key or authentication error")
            elif "timeout" in error_str:
                raise Exception("timeout: Request timeout")
            else:
                raise Exception(f"server_error: {e}")
        except requests.exceptions.Timeout:
            raise Exception("timeout: Request timeout")
        except Exception as e:
            error_str = str(e).lower()
            if "quota" in error_str or "rate" in error_str:
                raise Exception("rate_limit_error: Rate limit exceeded")
            elif "invalid" in error_str or "auth" in error_str or "key" in error_str:
                raise Exception("invalid_api_key: Invalid API key or authentication error")
            else:
                raise Exception(f"server_error: {e}")
        
        # Extract text from response
        text = ""
        if "candidates" in response_data and len(response_data["candidates"]) > 0:
            candidate = response_data["candidates"][0]
            if "content" in candidate and "parts" in candidate["content"]:
                text_parts = []
                for part in candidate["content"]["parts"]:
                    if "text" in part:
                        text_parts.append(part["text"])
                text = "".join(text_parts)
        
        # Extract usage (if available)
        usage = {}
        if "usageMetadata" in response_data:
            usage_meta = response_data["usageMetadata"]
            usage = {
                "prompt_tokens": usage_meta.get("promptTokenCount", 0),
                "completion_tokens": usage_meta.get("candidatesTokenCount", 0),
                "total_tokens": usage_meta.get("totalTokenCount", 0)
            }
        
        # Return in OpenAI-compatible format
        return {
            "choices": [{
                "message": {
                    "content": text
                }
            }],
            "usage": usage
        }
    
    def _validate_schema(self, result: Any) -> bool:
        """
        Validate result against JSON schema.
        
        Args:
            result: Parsed JSON result
        
        Returns:
            True if valid, False otherwise
        """
        try:
            validator = jsonschema.Draft202012Validator(self.schema)
            validator.validate(result)
            return True
        except jsonschema.ValidationError:
            return False
    
    def _get_unknown_classification(self) -> Dict[str, Any]:
        """
        Get a default "Unknown" classification with 8-dimension format.
        
        Uses _get_fallback_code() to dynamically resolve valid codes from
        the Standard Adapter, ensuring compatibility with Standard updates.
        
        Returns:
            Classification dict with fallback codes
        """
        return {
            "service_name": "Unknown",
            "usage_type": "unknown",
            "risk_level": "medium",
            "category": "Unknown",
            "confidence": 0.3,
            "rationale_short": "Unable to identify service",
            "fs_code": self._get_fallback_code("FS"),
            "im_code": self._get_fallback_code("IM"),
            "uc_codes": [self._get_fallback_code("UC")],
            "dt_codes": [self._get_fallback_code("DT")],
            "ch_codes": [self._get_fallback_code("CH")],
            "rs_codes": [self._get_fallback_code("RS")],
            "ev_codes": [self._get_fallback_code("EV")],
            "ob_codes": [],  # OB is 0+ cardinality, empty is valid
            "aimo_standard_version": self.aimo_standard_version
        }
    
    def _normalize_and_validate_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize and validate a single classification result.
        
        Ensures:
        - All required fields are present
        - Array fields are arrays
        - Single-value fields are strings
        - Taxonomy validation if adapter available
        
        Args:
            result: Raw classification result
        
        Returns:
            Normalized result dict with validation status
        """
        normalized = dict(result)
        
        # Ensure aimo_standard_version
        if "aimo_standard_version" not in normalized:
            normalized["aimo_standard_version"] = self.aimo_standard_version
        
        # Handle legacy format conversion (if old format detected)
        if "fs_uc_code" in normalized and "fs_code" not in normalized:
            # Legacy 7-code format - convert to 8-dimension
            normalized = self._convert_legacy_to_8dim(normalized)
        
        # Ensure fs_code is string (use dynamic fallback)
        if "fs_code" not in normalized or not isinstance(normalized.get("fs_code"), str):
            normalized["fs_code"] = self._get_fallback_code("FS")
        
        # Ensure im_code is string (use dynamic fallback)
        if "im_code" not in normalized or not isinstance(normalized.get("im_code"), str):
            normalized["im_code"] = self._get_fallback_code("IM")
        
        # Ensure array fields (use dynamic fallback codes)
        for field in ["uc_codes", "dt_codes", "ch_codes", "rs_codes", "ev_codes", "ob_codes"]:
            if field not in normalized or not isinstance(normalized.get(field), list):
                # Use dynamic fallback from Standard Adapter
                dim = field.replace("_codes", "").upper()
                normalized[field] = [self._get_fallback_code(dim)] if field != "ob_codes" else []
        
        # Validate cardinality
        validation_errors = []
        
        # FS and IM must be exactly 1 (string)
        if not normalized.get("fs_code"):
            validation_errors.append("fs_code is required")
        if not normalized.get("im_code"):
            validation_errors.append("im_code is required")
        
        # UC, DT, CH, RS, EV must have at least 1
        for field in ["uc_codes", "dt_codes", "ch_codes", "rs_codes", "ev_codes"]:
            if not normalized.get(field) or len(normalized[field]) < 1:
                validation_errors.append(f"{field} requires at least 1 element")
                # Add dynamic fallback from Standard Adapter
                dim = field.replace("_codes", "").upper()
                normalized[field] = [self._get_fallback_code(dim)]
        
        # Validate against taxonomy adapter if available
        if self._taxonomy_adapter and not validation_errors:
            try:
                from standard_adapter.taxonomy import validate_assignment
                
                adapter_errors = validate_assignment(
                    fs_codes=[normalized["fs_code"]],
                    im_codes=[normalized["im_code"]],
                    uc_codes=normalized["uc_codes"],
                    dt_codes=normalized["dt_codes"],
                    ch_codes=normalized["ch_codes"],
                    rs_codes=normalized["rs_codes"],
                    ev_codes=normalized["ev_codes"],
                    ob_codes=normalized.get("ob_codes", []),
                    version=self.aimo_standard_version
                )
                
                if adapter_errors:
                    validation_errors.extend(adapter_errors)
                    
            except Exception as e:
                # Log but don't fail
                print(f"Warning: Taxonomy validation error: {e}", flush=True)
        
        # Add validation status
        normalized["_validation_errors"] = validation_errors
        normalized["_needs_review"] = len(validation_errors) > 0
        
        return normalized
    
    def _convert_legacy_to_8dim(self, legacy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert legacy 7-code format to 8-dimension format.
        
        Uses _get_fallback_code() for dynamic fallback resolution,
        ensuring compatibility with Standard updates.
        
        Args:
            legacy: Legacy classification with fs_uc_code, dt_code, etc.
        
        Returns:
            New format classification
        """
        result = dict(legacy)
        
        # fs_uc_code -> fs_code (best effort, may need review)
        fs_uc = legacy.get("fs_uc_code", "")
        if fs_uc and fs_uc.startswith("FS-"):
            result["fs_code"] = fs_uc
        else:
            result["fs_code"] = self._get_fallback_code("FS")
        
        # im_code stays as-is (single value)
        if "im_code" not in result or not result["im_code"]:
            result["im_code"] = self._get_fallback_code("IM")
        
        # Convert single codes to arrays
        for old_field, new_field in [
            ("dt_code", "dt_codes"),
            ("ch_code", "ch_codes"),
            ("rs_code", "rs_codes"),
            ("ob_code", "ob_codes"),
            ("ev_code", "ev_codes")
        ]:
            old_val = legacy.get(old_field, "")
            if old_val:
                result[new_field] = [old_val]
            else:
                dim = old_field.replace("_code", "").upper()
                # OB has 0+ cardinality, use empty array; others use fallback
                result[new_field] = [self._get_fallback_code(dim)] if new_field != "ob_codes" else []
        
        # UC from fs_uc_code is not directly extractable - use dynamic fallback
        result["uc_codes"] = [self._get_fallback_code("UC")]
        
        # Version
        result["aimo_standard_version"] = self.aimo_standard_version
        
        # Remove legacy fields
        for field in ["fs_uc_code", "dt_code", "ch_code", "rs_code", "ob_code", "ev_code", "taxonomy_version"]:
            result.pop(field, None)
        
        return result
    
    def analyze_batch(self, signatures: List[Dict[str, Any]], 
                     initial_batch_size: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Analyze a batch of signatures using LLM.
        
        Args:
            signatures: List of signature dicts with keys:
                - url_signature
                - norm_host
                - norm_path_template
                - access_count (optional)
                - bytes_sent_sum (optional)
            initial_batch_size: Initial batch size (for dynamic reduction on 429 errors)
        
        Returns:
            Tuple of (classifications, retry_summary):
            - classifications: List of classification dicts (one per signature)
            - retry_summary: Dict with retry metadata (attempts, backoff_ms_total, last_error_code, rate_limit_events)
        
        Raises:
            LLMDisabledError: If AIMO_DISABLE_LLM=1 is set
        """
        # Check if LLM is disabled
        if _check_llm_disabled():
            raise LLMDisabledError("analyze_batch")
        
        if not signatures:
            return [], {
                "attempts": 0,
                "backoff_ms_total": 0,
                "last_error_code": None,
                "rate_limit_events": 0
            }
        
        # Estimate cost
        # Rough estimate: ~100 tokens per signature, ~200 tokens output per signature
        input_tokens = len(signatures) * 100
        output_tokens = len(signatures) * 200
        estimated_cost = self._estimate_cost(input_tokens, output_tokens)
        
        # Check budget with priority-based control
        # For batch requests, check if any signature has priority flags
        # If batch contains mixed priorities, use highest priority
        highest_priority_flags = None
        for sig in signatures:
            flags = sig.get("candidate_flags")
            if flags:
                # Extract priority from flags (A > B > C)
                if "A" in flags:
                    highest_priority_flags = flags
                    break  # A is highest priority
                elif "B" in flags and highest_priority_flags is None:
                    highest_priority_flags = flags
                elif "C" in flags and highest_priority_flags is None:
                    highest_priority_flags = flags
        
        is_allowed, reason = self._check_budget(estimated_cost, highest_priority_flags)
        if not is_allowed:
            raise Exception(f"budget_exceeded: {reason}")
        
        # Build prompt (using new 8-dimension format)
        user_prompt = self.build_user_prompt(signatures, self.aimo_standard_version)
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        # Build response format for structured outputs
        response_format = None
        response_json_schema = None
        if self.provider_config.get("structured_output", False):
            if self.provider_name == "gemini":
                # Gemini uses response_json_schema directly
                response_json_schema = self.schema
            else:
                # OpenAI uses response_format
                response_format = {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "service_classification",
                        "strict": True,
                        "schema": self.schema
                    }
                }
        
        # Retry loop with tracking
        last_error = None
        retry_summary = {
            "attempts": 0,
            "backoff_ms_total": 0,
            "last_error_code": None,
            "rate_limit_events": 0
        }
        current_batch_size = initial_batch_size or len(signatures)
        
        for attempt in range(1, self.max_retries + 2):  # +2 for initial + retries
            retry_summary["attempts"] = attempt
            try:
                # Call API based on provider
                if self.provider_name == "gemini":
                    response = self._call_gemini_api(messages, response_json_schema)
                else:
                    response = self._call_openai_api(messages, response_format)
                
                # Parse response
                choices = response.get("choices", [])
                if not choices:
                    raise Exception("invalid_response: No choices in response")
                
                content = choices[0].get("message", {}).get("content", "")
                if not content:
                    raise Exception("invalid_response: Empty content")
                
                # Parse JSON
                try:
                    # Remove markdown code blocks if present
                    content = content.strip()
                    if content.startswith("```"):
                        # Extract JSON from code block
                        lines = content.split("\n")
                        json_lines = []
                        in_json = False
                        for line in lines:
                            if line.strip().startswith("```"):
                                in_json = not in_json
                                continue
                            if in_json:
                                json_lines.append(line)
                        content = "\n".join(json_lines)
                    
                    results = json.loads(content)
                except json.JSONDecodeError as e:
                    # JSON parse error - retry with retry prompt
                    if attempt < self.max_retries + 1:
                        messages = [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": self.json_retry_prompt.format(
                                error_message=str(e),
                                json_schema=str(self.schema) if self.schema else "{}",
                                original_samples=user_prompt
                            )}
                        ]
                        time.sleep(self._calculate_delay(attempt))
                        continue
                    else:
                        raise Exception(f"json_schema_error: Failed to parse JSON after {self.max_retries} retries: {e}")
                
                # Validate schema
                if isinstance(results, list):
                    # Batch response
                    for result in results:
                        if not self._validate_schema(result):
                            raise Exception("json_schema_error: Result does not match schema")
                else:
                    # Single result
                    if not self._validate_schema(results):
                        raise Exception("json_schema_error: Result does not match schema")
                    results = [results]
                
                # Update budget
                usage = response.get("usage", {})
                actual_input_tokens = usage.get("prompt_tokens", input_tokens)
                actual_output_tokens = usage.get("completion_tokens", output_tokens)
                actual_cost = self._estimate_cost(actual_input_tokens, actual_output_tokens)
                # Use budget controller to record spending
                self.budget_controller.record_spending(actual_cost)
                # Also update legacy tracking for backward compatibility
                self.daily_spent_usd = self.budget_controller.daily_spent_usd
                
                # Ensure we have one result per signature
                if len(results) != len(signatures):
                    # Pad or truncate to match
                    if len(results) < len(signatures):
                        # Pad with "Unknown" (using 8-dimension format)
                        for i in range(len(results), len(signatures)):
                            results.append(self._get_unknown_classification())
                    else:
                        # Truncate
                        results = results[:len(signatures)]
                
                # Validate and normalize each result (8-dimension format)
                validated_results = []
                for result in results:
                    normalized = self._normalize_and_validate_result(result)
                    validated_results.append(normalized)
                results = validated_results
                
                # Success - return results with retry summary
                return results, retry_summary
                
            except Exception as e:
                last_error = e
                error_type, is_permanent = self._classify_error(e)
                
                # Track error code
                error_str = str(e).lower()
                if "rate_limit" in error_str or "429" in error_str:
                    retry_summary["rate_limit_events"] += 1
                    retry_summary["last_error_code"] = "429"
                    # Dynamic batch size reduction on rate limit
                    if current_batch_size > 1:
                        current_batch_size = max(1, current_batch_size // 2)
                        print(f"  DEBUG: Rate limit hit, reducing batch size to {current_batch_size}", flush=True)
                elif "invalid_api_key" in error_str or "401" in error_str or "403" in error_str:
                    retry_summary["last_error_code"] = "401/403"
                elif "timeout" in error_str:
                    retry_summary["last_error_code"] = "timeout"
                else:
                    retry_summary["last_error_code"] = "server_error"
                
                if is_permanent:
                    # Don't retry permanent errors
                    raise
                
                # Retry transient errors
                if attempt < self.max_retries + 1:
                    # Calculate and track backoff delay
                    # For 429 errors, check for Retry-After header
                    delay_seconds = self._calculate_delay(attempt)
                    if "rate_limit" in error_str or "429" in error_str:
                        # Try to extract Retry-After from error message
                        retry_after_match = None
                        if "Retry-After:" in str(e):
                            try:
                                import re
                                match = re.search(r"Retry-After:\s*(\d+)", str(e))
                                if match:
                                    retry_after_match = int(match.group(1))
                            except:
                                pass
                        if retry_after_match:
                            delay_seconds = max(delay_seconds, retry_after_match)
                    
                    retry_summary["backoff_ms_total"] += int(delay_seconds * 1000)
                    time.sleep(delay_seconds)
                    continue
                else:
                    # Max retries exceeded - return error with retry summary
                    retry_summary["last_error_code"] = error_type
                    raise Exception(f"{error_type}: Max retries exceeded: {e}") from e
        
        # Should not reach here, but handle it
        raise Exception(f"unknown_error: Failed after {self.max_retries} retries: {last_error}")
