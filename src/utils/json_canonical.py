"""
JSON Canonical Utilities

Provides canonical JSON serialization for taxonomy code arrays.
Canonical form ensures deterministic storage and comparison:
- Sorted alphabetically
- Duplicates removed
- Consistent JSON formatting (no extra whitespace)

Usage:
    from utils.json_canonical import canonical_json_array, parse_json_array
    
    # Serialize to canonical JSON
    json_str = canonical_json_array(["UC-010", "UC-001", "UC-001"])
    # Result: '["UC-001","UC-010"]'
    
    # Parse back to list
    codes = parse_json_array(json_str)
    # Result: ["UC-001", "UC-010"]
"""

import json
from typing import Optional


def canonical_json_array(codes: Optional[list[str]]) -> str:
    """
    Convert a list of codes to canonical JSON string.
    
    Canonical form:
    - Sorted alphabetically (case-sensitive)
    - Duplicates removed
    - No extra whitespace
    - Empty list becomes '[]'
    
    Args:
        codes: List of code strings (e.g., ["UC-010", "UC-001"])
    
    Returns:
        Canonical JSON string (e.g., '["UC-001","UC-010"]')
    
    Examples:
        >>> canonical_json_array(["UC-010", "UC-001"])
        '["UC-001","UC-010"]'
        >>> canonical_json_array(["UC-001", "UC-001"])
        '["UC-001"]'
        >>> canonical_json_array([])
        '[]'
        >>> canonical_json_array(None)
        '[]'
    """
    if codes is None:
        return "[]"
    
    # Filter out None/empty strings, sort, deduplicate
    cleaned = sorted(set(c for c in codes if c and isinstance(c, str)))
    
    # Serialize with no extra whitespace
    return json.dumps(cleaned, ensure_ascii=False, separators=(",", ":"))


def parse_json_array(json_str: Optional[str]) -> list[str]:
    """
    Parse a JSON array string to a list of codes.
    
    Safe parsing:
    - Returns empty list for None, empty string, or invalid JSON
    - Filters out non-string values
    
    Args:
        json_str: JSON array string (e.g., '["UC-001","UC-010"]')
    
    Returns:
        List of code strings (e.g., ["UC-001", "UC-010"])
    
    Examples:
        >>> parse_json_array('["UC-001","UC-010"]')
        ['UC-001', 'UC-010']
        >>> parse_json_array('[]')
        []
        >>> parse_json_array(None)
        []
        >>> parse_json_array('')
        []
        >>> parse_json_array('invalid')
        []
    """
    if not json_str or not isinstance(json_str, str):
        return []
    
    json_str = json_str.strip()
    if not json_str or json_str == "[]":
        return []
    
    try:
        parsed = json.loads(json_str)
        if not isinstance(parsed, list):
            return []
        # Filter to strings only
        return [item for item in parsed if isinstance(item, str)]
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def merge_code_arrays(*arrays: Optional[list[str]]) -> list[str]:
    """
    Merge multiple code arrays into a single canonical list.
    
    Args:
        *arrays: Variable number of code lists
    
    Returns:
        Merged, sorted, deduplicated list
    
    Examples:
        >>> merge_code_arrays(["UC-001"], ["UC-002", "UC-001"])
        ['UC-001', 'UC-002']
    """
    all_codes = set()
    for arr in arrays:
        if arr:
            for code in arr:
                if code and isinstance(code, str):
                    all_codes.add(code)
    return sorted(all_codes)


def validate_code_format(code: str, dimension: Optional[str] = None) -> bool:
    """
    Validate that a code matches the expected format (XX-NNN).
    
    Args:
        code: Code string to validate
        dimension: Optional dimension prefix to check (e.g., "FS", "UC")
    
    Returns:
        True if valid format, False otherwise
    
    Examples:
        >>> validate_code_format("UC-001")
        True
        >>> validate_code_format("UC-001", "UC")
        True
        >>> validate_code_format("UC-001", "FS")
        False
        >>> validate_code_format("invalid")
        False
    """
    if not code or not isinstance(code, str):
        return False
    
    parts = code.split("-")
    if len(parts) != 2:
        return False
    
    prefix, num = parts
    
    # Check prefix is 2 uppercase letters
    if len(prefix) != 2 or not prefix.isupper():
        return False
    
    # Check number is 3 digits
    if len(num) != 3 or not num.isdigit():
        return False
    
    # Check dimension if specified
    if dimension and prefix != dimension:
        return False
    
    return True


def codes_to_dict(
    fs_code: Optional[str] = None,
    im_code: Optional[str] = None,
    uc_codes: Optional[list[str]] = None,
    dt_codes: Optional[list[str]] = None,
    ch_codes: Optional[list[str]] = None,
    rs_codes: Optional[list[str]] = None,
    ev_codes: Optional[list[str]] = None,
    ob_codes: Optional[list[str]] = None
) -> dict[str, list[str]]:
    """
    Convert individual code arguments to a dimension dict.
    
    Converts single-value dimensions (FS, IM) to single-element lists
    for consistent handling.
    
    Args:
        fs_code: FS code (single value)
        im_code: IM code (single value)
        uc_codes: UC codes (list)
        dt_codes: DT codes (list)
        ch_codes: CH codes (list)
        rs_codes: RS codes (list)
        ev_codes: EV codes (list)
        ob_codes: OB codes (list)
    
    Returns:
        Dict mapping dimension -> list of codes
    
    Examples:
        >>> codes_to_dict(fs_code="FS-001", im_code="IM-001", uc_codes=["UC-001"])
        {'FS': ['FS-001'], 'IM': ['IM-001'], 'UC': ['UC-001'], ...}
    """
    return {
        "FS": [fs_code] if fs_code else [],
        "IM": [im_code] if im_code else [],
        "UC": uc_codes or [],
        "DT": dt_codes or [],
        "CH": ch_codes or [],
        "RS": rs_codes or [],
        "EV": ev_codes or [],
        "OB": ob_codes or [],
    }


def dict_to_db_columns(codes_dict: dict[str, list[str]]) -> dict[str, str]:
    """
    Convert a dimension dict to DB column values.
    
    Args:
        codes_dict: Dict mapping dimension -> list of codes
    
    Returns:
        Dict with DB column names and values:
        - fs_code, im_code: Single string or None
        - *_codes_json: Canonical JSON arrays
    
    Examples:
        >>> dict_to_db_columns({'FS': ['FS-001'], 'UC': ['UC-001', 'UC-002']})
        {'fs_code': 'FS-001', 'uc_codes_json': '["UC-001","UC-002"]', ...}
    """
    result = {}
    
    # Single-value dimensions
    fs = codes_dict.get("FS", [])
    result["fs_code"] = fs[0] if fs else None
    
    im = codes_dict.get("IM", [])
    result["im_code"] = im[0] if im else None
    
    # Array dimensions
    result["uc_codes_json"] = canonical_json_array(codes_dict.get("UC", []))
    result["dt_codes_json"] = canonical_json_array(codes_dict.get("DT", []))
    result["ch_codes_json"] = canonical_json_array(codes_dict.get("CH", []))
    result["rs_codes_json"] = canonical_json_array(codes_dict.get("RS", []))
    result["ev_codes_json"] = canonical_json_array(codes_dict.get("EV", []))
    result["ob_codes_json"] = canonical_json_array(codes_dict.get("OB", []))
    
    return result


def db_columns_to_dict(
    fs_code: Optional[str] = None,
    im_code: Optional[str] = None,
    uc_codes_json: Optional[str] = None,
    dt_codes_json: Optional[str] = None,
    ch_codes_json: Optional[str] = None,
    rs_codes_json: Optional[str] = None,
    ev_codes_json: Optional[str] = None,
    ob_codes_json: Optional[str] = None
) -> dict[str, list[str]]:
    """
    Convert DB column values to a dimension dict.
    
    Args:
        fs_code, im_code: Single value columns
        *_codes_json: JSON array columns
    
    Returns:
        Dict mapping dimension -> list of codes
    """
    return {
        "FS": [fs_code] if fs_code else [],
        "IM": [im_code] if im_code else [],
        "UC": parse_json_array(uc_codes_json),
        "DT": parse_json_array(dt_codes_json),
        "CH": parse_json_array(ch_codes_json),
        "RS": parse_json_array(rs_codes_json),
        "EV": parse_json_array(ev_codes_json),
        "OB": parse_json_array(ob_codes_json),
    }
