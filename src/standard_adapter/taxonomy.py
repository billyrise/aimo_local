"""
AIMO Standard Taxonomy Adapter

Loads and validates taxonomy codes from AIMO Standard artifacts.
The English version is the authoritative source for code validation.

Cardinality rules (per AIMO Standard):
- FS (Functional Scope): Exactly 1
- IM (Integration Mode): Exactly 1
- UC (Use Case Class): 1+ (at least one)
- DT (Data Type): 1+
- CH (Channel): 1+
- RS (Risk Surface): 1+
- EV (Evidence Type): 1+
- OB (Outcome / Benefit): 0+ (optional)
"""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .resolver import resolve_standard_artifacts, ResolvedStandardArtifacts
from .constants import AIMO_STANDARD_VERSION_DEFAULT


# Dimension cardinality definitions
DIMENSION_CARDINALITY = {
    "FS": {"min": 1, "max": 1, "name": "Functional Scope"},
    "IM": {"min": 1, "max": 1, "name": "Integration Mode"},
    "UC": {"min": 1, "max": None, "name": "Use Case Class"},
    "DT": {"min": 1, "max": None, "name": "Data Type"},
    "CH": {"min": 1, "max": None, "name": "Channel"},
    "RS": {"min": 1, "max": None, "name": "Risk Surface"},
    "EV": {"min": 1, "max": None, "name": "Evidence Type"},
    "OB": {"min": 0, "max": None, "name": "Outcome / Benefit"},
}

# All dimensions in canonical order
ALL_DIMENSIONS = ["FS", "UC", "DT", "CH", "IM", "RS", "OB", "EV"]


@dataclass
class TaxonomyCode:
    """A single taxonomy code entry."""
    code: str
    dimension: str
    dimension_name: str
    label: str
    definition: str
    status: str
    introduced_in: str
    scope_notes: str
    examples: list[str]


class TaxonomyAdapter:
    """
    Adapter for loading and validating AIMO Standard taxonomy.
    
    The English version (en) is the authoritative source for code validation.
    Labels and definitions are loaded from the English taxonomy dictionary.
    """
    
    def __init__(
        self,
        artifacts: Optional[ResolvedStandardArtifacts] = None,
        version: str = AIMO_STANDARD_VERSION_DEFAULT
    ):
        """
        Initialize taxonomy adapter.
        
        Args:
            artifacts: Pre-resolved artifacts (optional)
            version: Standard version to use if artifacts not provided
        """
        if artifacts is None:
            artifacts = resolve_standard_artifacts(version=version)
        
        self.artifacts = artifacts
        self._codes_by_dimension: dict[str, list[TaxonomyCode]] = {}
        self._all_codes: dict[str, TaxonomyCode] = {}
        
        self._load_taxonomy()
    
    def _find_taxonomy_csv(self) -> Path:
        """Find the English taxonomy dictionary CSV."""
        # Priority 1: artifacts/taxonomy/current/en/taxonomy_dictionary.csv
        csv_path = self.artifacts.artifacts_dir / "artifacts" / "taxonomy" / "current" / "en" / "taxonomy_dictionary.csv"
        if csv_path.exists():
            return csv_path
        
        # Priority 2: dir/taxonomy/current/en/taxonomy_dictionary.csv (cache structure)
        csv_path = self.artifacts.artifacts_dir / "dir" / "taxonomy" / "current" / "en" / "taxonomy_dictionary.csv"
        if csv_path.exists():
            return csv_path
        
        # Priority 3: Search recursively for taxonomy_dictionary.csv with "en" in path
        for path in self.artifacts.artifacts_dir.rglob("taxonomy_dictionary.csv"):
            if "/en/" in str(path) or "\\en\\" in str(path):
                return path
        
        raise FileNotFoundError(
            f"Taxonomy dictionary (English) not found in {self.artifacts.artifacts_dir}"
        )
    
    def _load_taxonomy(self):
        """Load taxonomy from CSV."""
        csv_path = self._find_taxonomy_csv()
        
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = TaxonomyCode(
                    code=row["code"],
                    dimension=row["dimension"],
                    dimension_name=row["dimension_name"],
                    label=row["label"],
                    definition=row["definition"],
                    status=row["status"],
                    introduced_in=row["introduced_in"],
                    scope_notes=row.get("scope_notes", ""),
                    examples=[e.strip() for e in row.get("examples", "").split("|") if e.strip()]
                )
                
                # Only load active codes
                if code.status == "active":
                    self._all_codes[code.code] = code
                    
                    if code.dimension not in self._codes_by_dimension:
                        self._codes_by_dimension[code.dimension] = []
                    self._codes_by_dimension[code.dimension].append(code)
    
    def get_dimensions(self) -> list[str]:
        """Get list of all dimensions."""
        return ALL_DIMENSIONS.copy()
    
    def get_allowed_codes(self, dimension: str) -> list[str]:
        """
        Get list of allowed codes for a dimension.
        
        Args:
            dimension: Dimension ID (FS, UC, DT, CH, IM, RS, OB, EV)
        
        Returns:
            List of valid code strings (e.g., ["FS-001", "FS-002", ...])
        
        Raises:
            ValueError: If dimension is not recognized
        """
        if dimension not in ALL_DIMENSIONS:
            raise ValueError(f"Unknown dimension: {dimension}. Valid dimensions: {ALL_DIMENSIONS}")
        
        codes = self._codes_by_dimension.get(dimension, [])
        return [c.code for c in codes]
    
    def get_code_info(self, code: str) -> Optional[TaxonomyCode]:
        """Get detailed info for a specific code."""
        return self._all_codes.get(code)
    
    def get_code_label(self, code: str) -> str:
        """Get the English label for a code."""
        info = self._all_codes.get(code)
        return info.label if info else code
    
    def get_cardinality(self, dimension: str) -> dict:
        """
        Get cardinality constraints for a dimension.
        
        Returns:
            dict with 'min', 'max' (None = unlimited), 'name'
        """
        if dimension not in DIMENSION_CARDINALITY:
            raise ValueError(f"Unknown dimension: {dimension}")
        return DIMENSION_CARDINALITY[dimension].copy()
    
    def validate_code(self, code: str) -> list[str]:
        """
        Validate a single code.
        
        Returns:
            List of error messages (empty if valid)
        """
        errors = []
        
        if not code:
            errors.append("Code is empty")
            return errors
        
        # Check format: XX-NNN
        parts = code.split("-")
        if len(parts) != 2:
            errors.append(f"Invalid code format: {code} (expected XX-NNN)")
            return errors
        
        dimension = parts[0]
        if dimension not in ALL_DIMENSIONS:
            errors.append(f"Unknown dimension in code: {code}")
            return errors
        
        if code not in self._all_codes:
            errors.append(f"Unknown code: {code} (not in taxonomy dictionary)")
        
        return errors
    
    def validate_assignment(
        self,
        fs_codes: list[str],
        im_codes: list[str],
        uc_codes: Optional[list[str]] = None,
        dt_codes: Optional[list[str]] = None,
        ch_codes: Optional[list[str]] = None,
        rs_codes: Optional[list[str]] = None,
        ev_codes: Optional[list[str]] = None,
        ob_codes: Optional[list[str]] = None
    ) -> list[str]:
        """
        Validate a complete 8-dimension code assignment.
        
        Args:
            fs_codes: Functional Scope codes (exactly 1)
            im_codes: Integration Mode codes (exactly 1)
            uc_codes: Use Case Class codes (1+)
            dt_codes: Data Type codes (1+)
            ch_codes: Channel codes (1+)
            rs_codes: Risk Surface codes (1+)
            ev_codes: Evidence Type codes (1+)
            ob_codes: Outcome / Benefit codes (0+, optional)
        
        Returns:
            List of error messages (empty if all valid)
        """
        errors = []
        
        # Default empty lists
        uc_codes = uc_codes or []
        dt_codes = dt_codes or []
        ch_codes = ch_codes or []
        rs_codes = rs_codes or []
        ev_codes = ev_codes or []
        ob_codes = ob_codes or []
        
        # Validate each dimension
        dimension_assignments = {
            "FS": fs_codes,
            "IM": im_codes,
            "UC": uc_codes,
            "DT": dt_codes,
            "CH": ch_codes,
            "RS": rs_codes,
            "EV": ev_codes,
            "OB": ob_codes,
        }
        
        for dim, codes in dimension_assignments.items():
            cardinality = DIMENSION_CARDINALITY[dim]
            dim_name = cardinality["name"]
            min_count = cardinality["min"]
            max_count = cardinality["max"]
            
            # Check cardinality
            if len(codes) < min_count:
                if min_count == 1:
                    errors.append(f"{dim_name} ({dim}): at least 1 code required, got {len(codes)}")
                else:
                    errors.append(f"{dim_name} ({dim}): at least {min_count} codes required, got {len(codes)}")
            
            if max_count is not None and len(codes) > max_count:
                errors.append(f"{dim_name} ({dim}): at most {max_count} code(s) allowed, got {len(codes)}")
            
            # Validate each code
            for code in codes:
                code_errors = self.validate_code(code)
                errors.extend(code_errors)
                
                # Check dimension prefix matches
                if code and not code.startswith(f"{dim}-"):
                    errors.append(f"Code {code} does not belong to dimension {dim}")
        
        return errors
    
    def validate_codes_dict(self, codes: dict[str, list[str]]) -> list[str]:
        """
        Validate codes provided as a dictionary.
        
        Args:
            codes: Dict mapping dimension -> list of codes
                   e.g., {"FS": ["FS-001"], "UC": ["UC-001", "UC-002"], ...}
        
        Returns:
            List of error messages
        """
        return self.validate_assignment(
            fs_codes=codes.get("FS", []),
            im_codes=codes.get("IM", []),
            uc_codes=codes.get("UC", []),
            dt_codes=codes.get("DT", []),
            ch_codes=codes.get("CH", []),
            rs_codes=codes.get("RS", []),
            ev_codes=codes.get("EV", []),
            ob_codes=codes.get("OB", [])
        )
    
    @property
    def standard_version(self) -> str:
        """Get the Standard version being used."""
        return self.artifacts.standard_version
    
    @property
    def total_codes(self) -> int:
        """Get total number of codes loaded."""
        return len(self._all_codes)
    
    def get_stats(self) -> dict:
        """Get statistics about loaded taxonomy."""
        return {
            "standard_version": self.standard_version,
            "total_codes": self.total_codes,
            "codes_by_dimension": {
                dim: len(self._codes_by_dimension.get(dim, []))
                for dim in ALL_DIMENSIONS
            }
        }


# Module-level convenience functions

_default_adapter: Optional[TaxonomyAdapter] = None


def get_taxonomy_adapter(version: str = AIMO_STANDARD_VERSION_DEFAULT) -> TaxonomyAdapter:
    """Get or create the default taxonomy adapter."""
    global _default_adapter
    if _default_adapter is None or _default_adapter.standard_version != version:
        _default_adapter = TaxonomyAdapter(version=version)
    return _default_adapter


def get_allowed_codes(dimension: str, version: str = AIMO_STANDARD_VERSION_DEFAULT) -> list[str]:
    """Convenience function to get allowed codes for a dimension."""
    return get_taxonomy_adapter(version).get_allowed_codes(dimension)


def validate_assignment(
    fs_codes: list[str],
    im_codes: list[str],
    uc_codes: Optional[list[str]] = None,
    dt_codes: Optional[list[str]] = None,
    ch_codes: Optional[list[str]] = None,
    rs_codes: Optional[list[str]] = None,
    ev_codes: Optional[list[str]] = None,
    ob_codes: Optional[list[str]] = None,
    version: str = AIMO_STANDARD_VERSION_DEFAULT
) -> list[str]:
    """Convenience function to validate code assignment."""
    return get_taxonomy_adapter(version).validate_assignment(
        fs_codes=fs_codes,
        im_codes=im_codes,
        uc_codes=uc_codes,
        dt_codes=dt_codes,
        ch_codes=ch_codes,
        rs_codes=rs_codes,
        ev_codes=ev_codes,
        ob_codes=ob_codes
    )
