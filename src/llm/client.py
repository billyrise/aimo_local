"""
LLM Client for AIMO Analysis Engine

Provides LLM-based service classification using Structured Outputs (JSON Schema).
Supports multiple providers (OpenAI, Azure OpenAI, Anthropic) with retry logic and error handling.

All LLM requests are deterministic: same input → same output (via caching).
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

# Suppress urllib3 SSL warnings for LibreSSL compatibility
# urllib3 1.x works with LibreSSL, but may show warnings in some environments
warnings.filterwarnings('ignore', category=UserWarning, module='urllib3')

# Google GenAI SDK (optional import)
try:
    import google.genai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


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
            SERVICE_ANALYSIS_USER,
            JSON_RETRY_PROMPT,
            format_samples_for_prompt,
            get_json_schema_for_prompt
        )
        self.system_prompt = SERVICE_ANALYSIS_SYSTEM
        self.user_prompt_template = SERVICE_ANALYSIS_USER
        self.json_retry_prompt = JSON_RETRY_PROMPT
        self.format_samples = format_samples_for_prompt
        self.get_json_schema = get_json_schema_for_prompt
    
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
            def clean_schema_for_gemini(schema_obj):
                """Recursively clean JSON Schema to Gemini _responseJsonSchema compatible format.
                
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
                - title, description (optional, but not in allowlist for safety)
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
                    # Optional: title/description (Gemini may accept but not in official allowlist)
                    # Uncomment if needed: "title", "description"
                }
                
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
                                    cleaned_properties[prop_name] = clean_schema_for_gemini(prop_schema)
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
                        cleaned[key] = clean_schema_for_gemini(value)
                    elif isinstance(value, list):
                        # Handle arrays (e.g., required, anyOf, oneOf, allOf, items)
                        if key in ["required", "enum"]:
                            # Keep as-is for required/enum arrays
                            cleaned[key] = value
                        elif key in ["anyOf", "oneOf", "allOf"]:
                            # Recursively clean each option in union
                            cleaned[key] = [clean_schema_for_gemini(item) if isinstance(item, dict) else item for item in value]
                        elif key == "items":
                            # items can be a dict (schema) or array of schemas
                            if isinstance(value[0], dict) if value else False:
                                cleaned[key] = clean_schema_for_gemini(value[0]) if len(value) == 1 else [clean_schema_for_gemini(item) if isinstance(item, dict) else item for item in value]
                            else:
                                cleaned[key] = value
                        else:
                            # Other arrays (shouldn't occur in standard JSON Schema, but handle gracefully)
                            cleaned[key] = [clean_schema_for_gemini(item) if isinstance(item, dict) else item for item in value]
                    else:
                        # Primitive values (strings, numbers, booleans)
                        cleaned[key] = value
                
                return cleaned
            
            schema_copy = clean_schema_for_gemini(response_json_schema)
            
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
        """
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
        
        # Build prompt
        samples_text = self.format_samples(signatures)
        json_schema_text = self.get_json_schema()
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.user_prompt_template.format(
                json_schema=json_schema_text,
                samples=samples_text
            )}
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
                                json_schema=json_schema_text,
                                original_samples=samples_text
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
                        # Pad with "Unknown" (including required taxonomy codes)
                        for i in range(len(results), len(signatures)):
                            results.append({
                                "service_name": "Unknown",
                                "usage_type": "unknown",
                                "risk_level": "medium",
                                "category": "Unknown",
                                "confidence": 0.3,
                                "rationale_short": "LLM returned fewer results than expected",
                                "fs_uc_code": "",
                                "dt_code": "",
                                "ch_code": "",
                                "im_code": "",
                                "rs_code": "",
                                "ob_code": "",
                                "ev_code": "",
                                "taxonomy_version": "1.0"
                            })
                    else:
                        # Truncate
                        results = results[:len(signatures)]
                
                # Ensure all results have required taxonomy codes (列欠落禁止)
                for result in results:
                    # Set default values for missing taxonomy codes
                    if "fs_uc_code" not in result:
                        result["fs_uc_code"] = ""
                    if "dt_code" not in result:
                        result["dt_code"] = ""
                    if "ch_code" not in result:
                        result["ch_code"] = ""
                    if "im_code" not in result:
                        result["im_code"] = ""
                    if "rs_code" not in result:
                        result["rs_code"] = ""
                    if "ob_code" not in result:
                        result["ob_code"] = ""
                    if "ev_code" not in result:
                        result["ev_code"] = ""
                    if "taxonomy_version" not in result:
                        result["taxonomy_version"] = "1.0"
                
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
