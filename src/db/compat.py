"""
Database Compatibility Layer for AIMO Standard v0.1.7 Migration

Provides backward-compatible reading of DB records that may contain:
- New 8-dimension format (uc_codes_json, dt_codes_json, etc.)
- Legacy 7-code format (fs_uc_code, dt_code, etc.)
- Mixed format during migration

Priority for reading:
1. New columns (uc_codes_json, etc.) if non-empty
2. Fall back to legacy columns (dt_code, etc.) as single-element array
3. Mark records with legacy-only data as needs_review=True

Usage:
    from db.compat import normalize_taxonomy_record, TaxonomyRecord
    
    # Normalize a DB row dict
    record = normalize_taxonomy_record(row_dict)
    print(record.fs_code)       # Single value
    print(record.uc_codes)      # List
    print(record.needs_review)  # True if legacy fallback used
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple

from utils.json_canonical import parse_json_array


@dataclass
class TaxonomyRecord:
    """
    Normalized taxonomy record with 8-dimension codes.
    
    All dimensions are represented as lists for consistency.
    Single-value dimensions (FS, IM) have exactly one element.
    
    Attributes:
        fs_code: Functional Scope code (single value)
        im_code: Integration Mode code (single value)
        uc_codes: Use Case Class codes (1+)
        dt_codes: Data Type codes (1+)
        ch_codes: Channel codes (1+)
        rs_codes: Risk Surface codes (1+)
        ev_codes: Evidence Type codes (1+)
        ob_codes: Outcome/Benefit codes (0+, optional)
        taxonomy_version: AIMO Standard version
        needs_review: True if record used legacy fallback or is incomplete
        source_format: "new" or "legacy" indicating original format
    """
    fs_code: str = ""
    im_code: str = ""
    uc_codes: List[str] = field(default_factory=list)
    dt_codes: List[str] = field(default_factory=list)
    ch_codes: List[str] = field(default_factory=list)
    rs_codes: List[str] = field(default_factory=list)
    ev_codes: List[str] = field(default_factory=list)
    ob_codes: List[str] = field(default_factory=list)
    taxonomy_version: str = "0.1.7"
    needs_review: bool = False
    source_format: str = "new"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for bundle generation."""
        return {
            "FS": [self.fs_code] if self.fs_code else [],
            "IM": [self.im_code] if self.im_code else [],
            "UC": self.uc_codes,
            "DT": self.dt_codes,
            "CH": self.ch_codes,
            "RS": self.rs_codes,
            "EV": self.ev_codes,
            "OB": self.ob_codes,
        }
    
    def to_flat_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary (for JSON export)."""
        return {
            "fs_code": self.fs_code,
            "im_code": self.im_code,
            "uc_codes": self.uc_codes,
            "dt_codes": self.dt_codes,
            "ch_codes": self.ch_codes,
            "rs_codes": self.rs_codes,
            "ev_codes": self.ev_codes,
            "ob_codes": self.ob_codes,
            "taxonomy_version": self.taxonomy_version,
            "needs_review": self.needs_review,
        }
    
    def is_complete(self) -> bool:
        """Check if all required dimensions have at least one code."""
        return (
            bool(self.fs_code) and
            bool(self.im_code) and
            len(self.uc_codes) >= 1 and
            len(self.dt_codes) >= 1 and
            len(self.ch_codes) >= 1 and
            len(self.rs_codes) >= 1 and
            len(self.ev_codes) >= 1
            # OB is optional (0+)
        )


def normalize_taxonomy_record(
    row: Dict[str, Any],
    default_version: str = "0.1.7"
) -> TaxonomyRecord:
    """
    Normalize a DB row to TaxonomyRecord with consistent 8-dimension format.
    
    Priority for reading each dimension:
    1. New column (*_codes_json or fs_code/im_code) if non-empty
    2. Legacy column (*_code) as single-element list
    3. Empty list if neither available
    
    Records that fall back to legacy format are marked needs_review=True.
    
    Args:
        row: Dictionary from DB query result
        default_version: Default taxonomy version if not in record
    
    Returns:
        TaxonomyRecord with normalized values
    
    Example:
        >>> row = {
        ...     "fs_code": "",
        ...     "im_code": "IM-001",
        ...     "uc_codes_json": "[]",
        ...     "dt_codes_json": '["DT-001"]',
        ...     "dt_code": "DT-002",  # Legacy
        ...     ...
        ... }
        >>> record = normalize_taxonomy_record(row)
        >>> record.dt_codes
        ['DT-001']  # New column wins
    """
    used_legacy = False
    source_format = "new"
    
    # FS code (single value)
    fs_code = row.get("fs_code", "") or ""
    if not fs_code:
        # Try legacy fs_uc_code
        legacy_fs_uc = row.get("fs_uc_code", "") or ""
        if legacy_fs_uc and legacy_fs_uc != "DEPRECATED" and legacy_fs_uc.startswith("FS-"):
            fs_code = legacy_fs_uc
            used_legacy = True
            source_format = "legacy"
    
    # IM code (single value)
    im_code = row.get("im_code", "") or ""
    if not im_code:
        # Check legacy im_code (same name, might be present)
        legacy_im = row.get("im_code", "") or ""
        if legacy_im:
            im_code = legacy_im
    
    # UC codes (1+)
    uc_codes = _normalize_array_column(row, "uc_codes_json", None)
    if not uc_codes:
        # No legacy column for UC in old schema
        # Try to infer from fs_uc_code if it looks like UC-xxx
        legacy_fs_uc = row.get("fs_uc_code", "") or ""
        if legacy_fs_uc and legacy_fs_uc.startswith("UC-"):
            uc_codes = [legacy_fs_uc]
            used_legacy = True
            source_format = "legacy"
    
    # DT codes (1+)
    dt_codes, dt_from_legacy = _normalize_array_column_with_source(row, "dt_codes_json", "dt_code")
    if dt_from_legacy:
        used_legacy = True
        source_format = "legacy"
    
    # CH codes (1+)
    ch_codes, ch_from_legacy = _normalize_array_column_with_source(row, "ch_codes_json", "ch_code")
    if ch_from_legacy:
        used_legacy = True
        source_format = "legacy"
    
    # RS codes (1+)
    rs_codes, rs_from_legacy = _normalize_array_column_with_source(row, "rs_codes_json", "rs_code")
    if rs_from_legacy:
        used_legacy = True
        source_format = "legacy"
    
    # EV codes (1+)
    ev_codes, ev_from_legacy = _normalize_array_column_with_source(row, "ev_codes_json", "ev_code")
    if ev_from_legacy:
        used_legacy = True
        source_format = "legacy"
    
    # OB codes (0+, optional)
    ob_codes, ob_from_legacy = _normalize_array_column_with_source(row, "ob_codes_json", "ob_code")
    if ob_from_legacy:
        used_legacy = True
        source_format = "legacy"
    
    # Taxonomy version
    version = (
        row.get("taxonomy_schema_version") or 
        row.get("taxonomy_version") or 
        default_version
    )
    
    record = TaxonomyRecord(
        fs_code=fs_code,
        im_code=im_code,
        uc_codes=uc_codes,
        dt_codes=dt_codes,
        ch_codes=ch_codes,
        rs_codes=rs_codes,
        ev_codes=ev_codes,
        ob_codes=ob_codes,
        taxonomy_version=version,
        needs_review=used_legacy or not _check_completeness(
            fs_code, im_code, uc_codes, dt_codes, ch_codes, rs_codes, ev_codes
        ),
        source_format=source_format
    )
    
    return record


def _normalize_array_column(
    row: Dict[str, Any],
    new_col: str,
    legacy_col: Optional[str]
) -> List[str]:
    """
    Normalize an array column with fallback to legacy single-value column.
    
    Args:
        row: DB row dict
        new_col: Name of new JSON array column (e.g., "uc_codes_json")
        legacy_col: Name of legacy single-value column (e.g., "uc_code")
    
    Returns:
        List of codes (may be empty)
    """
    codes, _ = _normalize_array_column_with_source(row, new_col, legacy_col)
    return codes


def _normalize_array_column_with_source(
    row: Dict[str, Any],
    new_col: str,
    legacy_col: Optional[str]
) -> Tuple[List[str], bool]:
    """
    Normalize an array column with fallback to legacy, returning source info.
    
    Args:
        row: DB row dict
        new_col: Name of new JSON array column (e.g., "uc_codes_json")
        legacy_col: Name of legacy single-value column (e.g., "uc_code")
    
    Returns:
        Tuple of (codes list, used_legacy_fallback)
    """
    # Try new column first
    new_val = row.get(new_col)
    if new_val and new_val != "[]":
        codes = parse_json_array(new_val)
        if codes:
            return codes, False
    
    # Fall back to legacy column
    if legacy_col:
        legacy_val = row.get(legacy_col)
        if legacy_val and isinstance(legacy_val, str) and legacy_val.strip():
            # Return as single-element list, mark as legacy
            return [legacy_val.strip()], True
    
    return [], False


def _check_completeness(
    fs_code: str,
    im_code: str,
    uc_codes: List[str],
    dt_codes: List[str],
    ch_codes: List[str],
    rs_codes: List[str],
    ev_codes: List[str]
) -> bool:
    """Check if all required dimensions have at least one code."""
    return (
        bool(fs_code) and
        bool(im_code) and
        len(uc_codes) >= 1 and
        len(dt_codes) >= 1 and
        len(ch_codes) >= 1 and
        len(rs_codes) >= 1 and
        len(ev_codes) >= 1
    )


def normalize_db_rows(
    rows: List[Dict[str, Any]],
    default_version: str = "0.1.7"
) -> List[TaxonomyRecord]:
    """
    Normalize a list of DB rows.
    
    Args:
        rows: List of DB row dicts
        default_version: Default taxonomy version
    
    Returns:
        List of TaxonomyRecord objects
    """
    return [normalize_taxonomy_record(row, default_version) for row in rows]


def record_to_bundle_format(record: TaxonomyRecord) -> Dict[str, Any]:
    """
    Convert a TaxonomyRecord to Evidence Bundle format.
    
    This is the format expected by StandardEvidenceBundleGenerator.
    
    Args:
        record: TaxonomyRecord to convert
    
    Returns:
        Dict with codes in bundle format
    """
    return {
        "fs_code": record.fs_code,
        "im_code": record.im_code,
        "uc_codes": record.uc_codes,
        "dt_codes": record.dt_codes,
        "ch_codes": record.ch_codes,
        "rs_codes": record.rs_codes,
        "ev_codes": record.ev_codes,
        "ob_codes": record.ob_codes,
        "taxonomy_version": record.taxonomy_version,
        "_needs_review": record.needs_review,
        "_source_format": record.source_format,
    }


def export_legacy_format(record: TaxonomyRecord) -> Dict[str, str]:
    """
    Export a TaxonomyRecord back to legacy 7-code format.
    
    For use in derived/legacy_*.csv exports.
    Takes first element of each array dimension.
    
    Args:
        record: TaxonomyRecord to convert
    
    Returns:
        Dict with legacy column names and values
    """
    return {
        "fs_uc_code": record.fs_code or "DEPRECATED",
        "im_code": record.im_code,
        "dt_code": record.dt_codes[0] if record.dt_codes else "",
        "ch_code": record.ch_codes[0] if record.ch_codes else "",
        "rs_code": record.rs_codes[0] if record.rs_codes else "",
        "ob_code": record.ob_codes[0] if record.ob_codes else "",
        "ev_code": record.ev_codes[0] if record.ev_codes else "",
        "taxonomy_version": record.taxonomy_version,
    }


def get_migration_status(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze a DB row and return migration status.
    
    Args:
        row: DB row dict
    
    Returns:
        Dict with migration status info:
        - has_new_format: True if new columns are populated
        - has_legacy_only: True if only legacy columns are populated
        - needs_migration: True if should be re-analyzed
        - missing_dimensions: List of dimensions with no data
    """
    # Check new format columns
    has_new_fs = bool(row.get("fs_code"))
    has_new_im = bool(row.get("im_code"))
    has_new_uc = bool(parse_json_array(row.get("uc_codes_json", "")))
    has_new_dt = bool(parse_json_array(row.get("dt_codes_json", "")))
    has_new_ch = bool(parse_json_array(row.get("ch_codes_json", "")))
    has_new_rs = bool(parse_json_array(row.get("rs_codes_json", "")))
    has_new_ev = bool(parse_json_array(row.get("ev_codes_json", "")))
    
    has_new_format = any([
        has_new_fs, has_new_im, has_new_uc, has_new_dt,
        has_new_ch, has_new_rs, has_new_ev
    ])
    
    # Check legacy columns
    has_legacy_fs_uc = bool(row.get("fs_uc_code") and row.get("fs_uc_code") != "DEPRECATED")
    has_legacy_dt = bool(row.get("dt_code"))
    has_legacy_ch = bool(row.get("ch_code"))
    has_legacy_rs = bool(row.get("rs_code"))
    has_legacy_ev = bool(row.get("ev_code"))
    
    has_legacy = any([
        has_legacy_fs_uc, has_legacy_dt, has_legacy_ch, has_legacy_rs, has_legacy_ev
    ])
    
    # Legacy only = has legacy columns but NO new format columns at all
    has_legacy_only = has_legacy and not has_new_format
    
    # Determine missing dimensions
    missing = []
    if not has_new_fs and not has_legacy_fs_uc:
        missing.append("FS")
    if not has_new_im and not row.get("im_code"):
        missing.append("IM")
    if not has_new_uc:
        missing.append("UC")
    if not has_new_dt and not has_legacy_dt:
        missing.append("DT")
    if not has_new_ch and not has_legacy_ch:
        missing.append("CH")
    if not has_new_rs and not has_legacy_rs:
        missing.append("RS")
    if not has_new_ev and not has_legacy_ev:
        missing.append("EV")
    
    needs_migration = has_legacy_only or len(missing) > 0
    
    return {
        "has_new_format": has_new_format,
        "has_legacy_only": has_legacy_only,
        "needs_migration": needs_migration,
        "missing_dimensions": missing,
    }
