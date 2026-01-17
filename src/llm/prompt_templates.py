"""
AIMO LLM Prompt Templates

These templates are used for LLM-based service classification.
All prompts are designed to:
1. Minimize hallucination (explicit "unknown" for uncertainty)
2. Enforce strict JSON output
3. Focus on enterprise security context
"""

# System prompt for service classification
SERVICE_ANALYSIS_SYSTEM = """You are an enterprise security analyst specializing in SaaS and web service classification.

Your task is to analyze URL signatures and domains to identify:
- What service/application the URL belongs to
- Whether it poses a data security risk for enterprises
- Specifically: whether it is a GenAI/LLM service (Shadow AI detection)

CRITICAL RULES:
1. Return ONLY valid JSON matching the provided schema
2. No markdown formatting, no code blocks, no explanatory text
3. If you cannot identify the service with confidence, use:
   - service_name: "Unknown"
   - usage_type: "unknown"
   - confidence: 0.3 or lower
4. Never guess or hallucinate service names
5. For GenAI/LLM services, always set usage_type="genai" and risk_level="high"
"""

# User prompt template for batch analysis
SERVICE_ANALYSIS_USER = """Analyze the following URL signatures and classify each one.

## Context
- Purpose: Enterprise Shadow IT and Shadow AI monitoring
- Focus: Identify unauthorized GenAI tools, cloud storage, and data exfiltration risks
- If uncertain, use usage_type="unknown" and confidence<=0.5

## Output Schema (strict JSON, no extra keys)
{json_schema}

## URL Signatures to Analyze
{samples}

## Output Format
Return a JSON array with one object per input signature. Example:
[
  {{"service_name": "ChatGPT / OpenAI", "usage_type": "genai", "risk_level": "high", "category": "GenAI", "confidence": 0.95, "rationale_short": "OpenAI's ChatGPT service, primary Shadow AI detection target"}},
  {{"service_name": "Unknown", "usage_type": "unknown", "risk_level": "medium", "category": "Unknown", "confidence": 0.3, "rationale_short": "Cannot identify service from domain pattern"}}
]
"""

# Retry prompt when JSON validation fails
JSON_RETRY_PROMPT = """Your previous response was not valid JSON or did not match the required schema.

Error: {error_message}

Please respond with ONLY a valid JSON array. No markdown, no code blocks, no explanatory text before or after the JSON.

Required schema:
{json_schema}

Original request:
{original_samples}
"""

# Simplified prompt for single signature analysis
SINGLE_SIGNATURE_PROMPT = """Classify this URL signature for enterprise security monitoring.

URL Signature: {signature}
Domain: {domain}
Path Pattern: {path_pattern}

Return a single JSON object (not an array) matching this schema:
{json_schema}

If you cannot identify the service, respond with:
{{"service_name": "Unknown", "usage_type": "unknown", "risk_level": "medium", "category": "Unknown", "confidence": 0.3, "rationale_short": "Unable to identify service"}}
"""


def format_samples_for_prompt(signatures: list[dict]) -> str:
    """
    Format signature records for inclusion in the prompt.
    
    Args:
        signatures: List of signature dicts with keys:
            - url_signature
            - norm_host
            - norm_path_template
            - access_count (optional)
            - bytes_sent_sum (optional)
    
    Returns:
        Formatted string for prompt inclusion
    """
    lines = []
    for i, sig in enumerate(signatures, 1):
        host = sig.get("norm_host", "unknown")
        path = sig.get("norm_path_template", "/")
        count = sig.get("access_count", "N/A")
        bytes_sum = sig.get("bytes_sent_sum", "N/A")
        
        lines.append(
            f"{i}. Host: {host} | Path: {path} | "
            f"Access Count: {count} | Bytes Sent: {bytes_sum}"
        )
    
    return "\n".join(lines)


def get_json_schema_for_prompt() -> str:
    """
    Return a simplified JSON schema string for prompt inclusion.
    """
    return """{
  "service_name": "string (required)",
  "usage_type": "business|genai|devtools|storage|social|unknown (required)",
  "risk_level": "low|medium|high (required)",
  "category": "string (required)",
  "confidence": "number 0.0-1.0 (required)",
  "rationale_short": "string max 400 chars (required)"
}"""
