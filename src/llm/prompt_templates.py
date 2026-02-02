"""
AIMO LLM Prompt Templates

These templates are used for LLM-based service classification.
All prompts are designed to:
1. Minimize hallucination (explicit "unknown" handling)
2. Enforce strict JSON output with 8-dimension taxonomy
3. Focus on enterprise security context
4. Use AIMO Standard v0.1.7+ taxonomy codes

Taxonomy Dimensions (AIMO Standard v0.1.7):
- FS: Functional Scope (exactly 1)
- IM: Integration Mode (exactly 1)
- UC: Use Case Class (1+)
- DT: Data Type (1+)
- CH: Channel (1+)
- RS: Risk Surface (1+)
- EV: Evidence Type (1+)
- OB: Outcome/Benefit (0+, optional)
"""

# Default AIMO Standard version
DEFAULT_AIMO_STANDARD_VERSION = "0.1.7"

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
   - Use default fallback codes for unknown services
4. Never guess or hallucinate service names
5. For GenAI/LLM services, always set usage_type="genai" and risk_level="high"
6. TAXONOMY CODES MUST be selected from the ALLOWED CODES list only
7. All taxonomy codes must match the pattern XX-NNN (e.g., FS-001, UC-010)
"""

# User prompt template for batch analysis
SERVICE_ANALYSIS_USER = """Analyze the following URL signatures and classify each one.

## Context
- Purpose: Enterprise Shadow IT and Shadow AI monitoring
- Focus: Identify unauthorized GenAI tools, cloud storage, and data exfiltration risks
- If uncertain, use usage_type="unknown" and confidence<=0.5
- AIMO Standard Version: {aimo_standard_version}

## ALLOWED TAXONOMY CODES (you MUST only use codes from this list)

{taxonomy_codes_section}

## Cardinality Rules (CRITICAL)
- fs_code: EXACTLY 1 code required (single string, not array)
- im_code: EXACTLY 1 code required (single string, not array)
- uc_codes: AT LEAST 1 code required (array)
- dt_codes: AT LEAST 1 code required (array)
- ch_codes: AT LEAST 1 code required (array)
- rs_codes: AT LEAST 1 code required (array)
- ev_codes: AT LEAST 1 code required (array)
- ob_codes: 0 or more codes allowed (array, can be empty)

## Output Schema (strict JSON, no extra keys)
{json_schema}

## URL Signatures to Analyze
{samples}

## Output Format
Return a JSON array with one object per input signature. Example:
[
  {{"service_name": "ChatGPT / OpenAI", "usage_type": "genai", "risk_level": "high", "category": "GenAI", "confidence": 0.95, "rationale_short": "OpenAI's ChatGPT service, primary Shadow AI detection target", "fs_code": "FS-001", "im_code": "IM-001", "uc_codes": ["UC-001"], "dt_codes": ["DT-001"], "ch_codes": ["CH-001"], "rs_codes": ["RS-001"], "ev_codes": ["EV-001"], "ob_codes": [], "aimo_standard_version": "{aimo_standard_version}"}},
  {{"service_name": "Unknown", "usage_type": "unknown", "risk_level": "medium", "category": "Unknown", "confidence": 0.3, "rationale_short": "Cannot identify service from domain pattern", "fs_code": "FS-099", "im_code": "IM-099", "uc_codes": ["UC-099"], "dt_codes": ["DT-099"], "ch_codes": ["CH-099"], "rs_codes": ["RS-099"], "ev_codes": ["EV-099"], "ob_codes": [], "aimo_standard_version": "{aimo_standard_version}"}}
]

## CRITICAL REMINDERS
1. Use ONLY codes from the ALLOWED TAXONOMY CODES list above
2. fs_code and im_code are single strings, NOT arrays
3. uc_codes, dt_codes, ch_codes, rs_codes, ev_codes, ob_codes are arrays
4. uc_codes, dt_codes, ch_codes, rs_codes, ev_codes must have at least 1 element
5. ob_codes can be empty (0 or more allowed)
6. aimo_standard_version must be "{aimo_standard_version}"
"""

# Retry prompt when JSON validation fails
JSON_RETRY_PROMPT = """Your previous response was not valid JSON or did not match the required schema.

Error: {error_message}

Please respond with ONLY a valid JSON array. No markdown, no code blocks, no explanatory text before or after the JSON.

CRITICAL REMINDERS:
1. fs_code and im_code are single STRINGS, not arrays
2. uc_codes, dt_codes, ch_codes, rs_codes, ev_codes, ob_codes are ARRAYS
3. uc_codes, dt_codes, ch_codes, rs_codes, ev_codes must have AT LEAST 1 element
4. ob_codes can be empty []
5. Use ONLY codes from the allowed list
6. All codes must match pattern XX-NNN (e.g., FS-001)

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

AIMO Standard Version: {aimo_standard_version}

ALLOWED TAXONOMY CODES:
{taxonomy_codes_section}

Return a single JSON object (not an array) matching this schema:
{json_schema}

If you cannot identify the service, respond with:
{{"service_name": "Unknown", "usage_type": "unknown", "risk_level": "medium", "category": "Unknown", "confidence": 0.3, "rationale_short": "Unable to identify service", "fs_code": "FS-099", "im_code": "IM-099", "uc_codes": ["UC-099"], "dt_codes": ["DT-099"], "ch_codes": ["CH-099"], "rs_codes": ["RS-099"], "ev_codes": ["EV-099"], "ob_codes": [], "aimo_standard_version": "{aimo_standard_version}"}}

CRITICAL:
- fs_code and im_code are single strings
- uc_codes, dt_codes, ch_codes, rs_codes, ev_codes, ob_codes are arrays
- All codes must be from the ALLOWED list above
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


def get_json_schema_for_prompt(aimo_standard_version: str = DEFAULT_AIMO_STANDARD_VERSION) -> str:
    """
    Return a simplified JSON schema string for prompt inclusion.
    
    Args:
        aimo_standard_version: AIMO Standard version to embed
    
    Returns:
        JSON schema description string
    """
    return f"""{{
  "service_name": "string (required)",
  "usage_type": "business|genai|devtools|storage|social|unknown (required)",
  "risk_level": "low|medium|high (required)",
  "category": "string (required)",
  "confidence": "number 0.0-1.0 (required)",
  "rationale_short": "string max 400 chars (required)",
  "fs_code": "string XX-NNN format, exactly 1 required (e.g., FS-001)",
  "im_code": "string XX-NNN format, exactly 1 required (e.g., IM-001)",
  "uc_codes": "array of strings, at least 1 required (e.g., [\"UC-001\", \"UC-002\"])",
  "dt_codes": "array of strings, at least 1 required",
  "ch_codes": "array of strings, at least 1 required",
  "rs_codes": "array of strings, at least 1 required",
  "ev_codes": "array of strings, at least 1 required",
  "ob_codes": "array of strings, 0 or more allowed (can be empty [])",
  "aimo_standard_version": "string, must be '{aimo_standard_version}'"
}}"""


def get_taxonomy_codes_section(version: str = DEFAULT_AIMO_STANDARD_VERSION) -> str:
    """
    Get taxonomy codes section for prompt from Standard Adapter.
    
    If Standard Adapter is not available, returns a fallback minimal set.
    
    Args:
        version: AIMO Standard version
    
    Returns:
        Formatted taxonomy codes section for prompt
    """
    try:
        from standard_adapter.taxonomy import get_taxonomy_adapter
        
        adapter = get_taxonomy_adapter(version=version)
        
        sections = []
        for dim in ["FS", "IM", "UC", "DT", "CH", "RS", "EV", "OB"]:
            codes = adapter.get_allowed_codes(dim)
            cardinality = adapter.get_cardinality(dim)
            
            # Format cardinality for display
            if cardinality == "exactly_one":
                card_str = "exactly 1"
            elif cardinality == "one_or_more":
                card_str = "1+"
            else:
                card_str = "0+"
            
            # Get dimension label
            dim_labels = {
                "FS": "Functional Scope",
                "IM": "Integration Mode",
                "UC": "Use Case Class",
                "DT": "Data Type",
                "CH": "Channel",
                "RS": "Risk Surface",
                "EV": "Evidence Type",
                "OB": "Outcome/Benefit"
            }
            
            # Format codes with labels
            code_lines = []
            for code in sorted(codes):
                label = adapter.get_code_label(code)
                if label:
                    code_lines.append(f"  - {code}: {label}")
                else:
                    code_lines.append(f"  - {code}")
            
            section = f"### {dim} ({dim_labels.get(dim, dim)}) [{card_str}]\n" + "\n".join(code_lines)
            sections.append(section)
        
        return "\n\n".join(sections)
        
    except ImportError:
        # Standard Adapter not available, use fallback
        return _get_fallback_taxonomy_codes_section()
    except Exception as e:
        # Any error, use fallback
        print(f"Warning: Could not load taxonomy from Standard Adapter: {e}", flush=True)
        return _get_fallback_taxonomy_codes_section()


def _get_fallback_taxonomy_codes_section() -> str:
    """
    Fallback taxonomy codes section when Standard Adapter is not available.
    
    Returns minimal set for basic operation.
    """
    return """### FS (Functional Scope) [exactly 1]
  - FS-001: Core Business System
  - FS-002: Productivity Tool
  - FS-003: Communication Platform
  - FS-099: Unknown/Other

### IM (Integration Mode) [exactly 1]
  - IM-001: Direct Web Access
  - IM-002: API Integration
  - IM-003: Browser Extension
  - IM-099: Unknown/Other

### UC (Use Case Class) [1+]
  - UC-001: Content Generation
  - UC-002: Data Analysis
  - UC-003: File Sharing
  - UC-099: Unknown/Other

### DT (Data Type) [1+]
  - DT-001: Text/Document
  - DT-002: Code/Script
  - DT-003: Image/Media
  - DT-099: Unknown/Other

### CH (Channel) [1+]
  - CH-001: Web Browser
  - CH-002: API Call
  - CH-003: Mobile App
  - CH-099: Unknown/Other

### RS (Risk Surface) [1+]
  - RS-001: Data Exfiltration
  - RS-002: Shadow IT
  - RS-003: Compliance
  - RS-099: Unknown/Other

### EV (Evidence Type) [1+]
  - EV-001: URL Pattern
  - EV-002: Traffic Volume
  - EV-003: User Agent
  - EV-099: Unknown/Other

### OB (Outcome/Benefit) [0+]
  - OB-001: Productivity Gain
  - OB-002: Cost Reduction
  - OB-099: Unknown/Other"""


def build_user_prompt(
    signatures: list[dict],
    aimo_standard_version: str = DEFAULT_AIMO_STANDARD_VERSION
) -> str:
    """
    Build complete user prompt for batch analysis.
    
    Args:
        signatures: List of signature dicts
        aimo_standard_version: AIMO Standard version
    
    Returns:
        Complete user prompt string
    """
    samples_text = format_samples_for_prompt(signatures)
    json_schema_text = get_json_schema_for_prompt(aimo_standard_version)
    taxonomy_section = get_taxonomy_codes_section(aimo_standard_version)
    
    return SERVICE_ANALYSIS_USER.format(
        aimo_standard_version=aimo_standard_version,
        taxonomy_codes_section=taxonomy_section,
        json_schema=json_schema_text,
        samples=samples_text
    )


def build_single_prompt(
    signature: str,
    domain: str,
    path_pattern: str,
    aimo_standard_version: str = DEFAULT_AIMO_STANDARD_VERSION
) -> str:
    """
    Build prompt for single signature analysis.
    
    Args:
        signature: URL signature
        domain: Domain name
        path_pattern: Path pattern
        aimo_standard_version: AIMO Standard version
    
    Returns:
        Complete prompt string
    """
    json_schema_text = get_json_schema_for_prompt(aimo_standard_version)
    taxonomy_section = get_taxonomy_codes_section(aimo_standard_version)
    
    return SINGLE_SIGNATURE_PROMPT.format(
        signature=signature,
        domain=domain,
        path_pattern=path_pattern,
        aimo_standard_version=aimo_standard_version,
        taxonomy_codes_section=taxonomy_section,
        json_schema=json_schema_text
    )
