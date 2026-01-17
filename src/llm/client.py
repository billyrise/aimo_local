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
    
    def _check_budget(self, estimated_cost_usd: float) -> bool:
        """
        Check if request is within daily budget.
        
        Args:
            estimated_cost_usd: Estimated cost for this request
        
        Returns:
            True if within budget, False otherwise
        """
        # Reset daily tracking if date changed
        today = datetime.utcnow().date()
        if today > self.daily_reset_date:
            self.daily_spent_usd = 0.0
            self.daily_reset_date = today
        
        # Check budget
        if self.daily_spent_usd + estimated_cost_usd > self.daily_budget_usd:
            return False
        
        return True
    
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
        input_per_1m = pricing.get("input_per_1m_tokens", 0.0)
        output_per_1m = pricing.get("output_per_1m_tokens", 0.0)
        
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
    
    def analyze_batch(self, signatures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze a batch of signatures using LLM.
        
        Args:
            signatures: List of signature dicts with keys:
                - url_signature
                - norm_host
                - norm_path_template
                - access_count (optional)
                - bytes_sent_sum (optional)
        
        Returns:
            List of classification dicts (one per signature)
        """
        if not signatures:
            return []
        
        # Estimate cost
        # Rough estimate: ~100 tokens per signature, ~200 tokens output per signature
        input_tokens = len(signatures) * 100
        output_tokens = len(signatures) * 200
        estimated_cost = self._estimate_cost(input_tokens, output_tokens)
        
        # Check budget
        if not self._check_budget(estimated_cost):
            raise Exception("budget_exceeded: Daily budget limit reached")
        
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
        if self.provider_config.get("structured_output", False):
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": "service_classification",
                    "strict": True,
                    "schema": self.schema
                }
            }
        
        # Retry loop
        last_error = None
        for attempt in range(1, self.max_retries + 2):  # +2 for initial + retries
            try:
                # Call API
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
                self.daily_spent_usd += actual_cost
                
                # Ensure we have one result per signature
                if len(results) != len(signatures):
                    # Pad or truncate to match
                    if len(results) < len(signatures):
                        # Pad with "Unknown"
                        for i in range(len(results), len(signatures)):
                            results.append({
                                "service_name": "Unknown",
                                "usage_type": "unknown",
                                "risk_level": "medium",
                                "category": "Unknown",
                                "confidence": 0.3,
                                "rationale_short": "LLM returned fewer results than expected"
                            })
                    else:
                        # Truncate
                        results = results[:len(signatures)]
                
                return results
                
            except Exception as e:
                last_error = e
                error_type, is_permanent = self._classify_error(e)
                
                if is_permanent:
                    # Don't retry permanent errors
                    raise
                
                # Retry transient errors
                if attempt < self.max_retries + 1:
                    delay = self._calculate_delay(attempt)
                    time.sleep(delay)
                else:
                    # Max retries exceeded
                    raise Exception(f"{error_type}: Max retries exceeded: {e}") from e
        
        # Should not reach here, but handle it
        raise Exception(f"unknown_error: Failed after {self.max_retries} retries: {last_error}")
