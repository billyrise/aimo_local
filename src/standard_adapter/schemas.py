"""
AIMO Standard Schema Loader

Loads JSON Schemas from AIMO Standard artifacts.
These schemas are used for validating Evidence Packs, manifests,
and other Standard-compliant documents.
"""

import json
from pathlib import Path
from typing import Optional

from .resolver import resolve_standard_artifacts, ResolvedStandardArtifacts
from .constants import AIMO_STANDARD_VERSION_DEFAULT


# Known schema names and their expected filenames
KNOWN_SCHEMAS = {
    "evidence_pack_manifest": "evidence_pack_manifest.schema.json",
    "aimo_ev": "aimo-ev.schema.json",
    "aimo_dictionary": "aimo-dictionary.schema.json",
    "aimo_standard": "aimo-standard.schema.json",
    "shadow_ai_discovery": "shadow-ai-discovery.schema.json",
    "agent_activity": "agent-activity.schema.json",
}


class SchemaLoader:
    """
    Loader for AIMO Standard JSON Schemas.
    
    Schemas are loaded from the Standard artifacts and cached
    for repeated use during validation.
    """
    
    def __init__(
        self,
        artifacts: Optional[ResolvedStandardArtifacts] = None,
        version: str = AIMO_STANDARD_VERSION_DEFAULT
    ):
        """
        Initialize schema loader.
        
        Args:
            artifacts: Pre-resolved artifacts (optional)
            version: Standard version to use if artifacts not provided
        """
        if artifacts is None:
            artifacts = resolve_standard_artifacts(version=version)
        
        self.artifacts = artifacts
        self._schema_cache: dict[str, dict] = {}
        self._schema_dir: Optional[Path] = None
        
        self._find_schema_dir()
    
    def _find_schema_dir(self):
        """Find the JSON Schema directory in artifacts."""
        # Priority 1: schemas/jsonschema/
        schema_dir = self.artifacts.artifacts_dir / "schemas" / "jsonschema"
        if schema_dir.exists():
            self._schema_dir = schema_dir
            return
        
        # Priority 2: Search for jsonschema directory
        for path in self.artifacts.artifacts_dir.rglob("jsonschema"):
            if path.is_dir():
                self._schema_dir = path
                return
        
        # Priority 3: Search for *.schema.json files
        for path in self.artifacts.artifacts_dir.rglob("*.schema.json"):
            self._schema_dir = path.parent
            return
        
        # No schema directory found - this is okay, we'll return None for loads
        self._schema_dir = None
    
    def list_available_schemas(self) -> list[str]:
        """
        List all available schema names.
        
        Returns:
            List of schema identifiers (without .schema.json suffix)
        """
        if self._schema_dir is None:
            return []
        
        schemas = []
        for path in self._schema_dir.glob("*.schema.json"):
            # Convert filename to schema name
            name = path.stem.replace(".schema", "").replace("-", "_")
            schemas.append(name)
        
        return sorted(schemas)
    
    def _resolve_schema_path(self, name_or_path: str) -> Optional[Path]:
        """
        Resolve a schema name or path to an actual file path.
        
        Args:
            name_or_path: Either a known schema name (e.g., "evidence_pack_manifest"),
                         a filename (e.g., "evidence_pack_manifest.schema.json"),
                         or a relative path within the schema directory
        
        Returns:
            Path to the schema file, or None if not found
        """
        if self._schema_dir is None:
            return None
        
        # Check if it's a known schema name
        if name_or_path in KNOWN_SCHEMAS:
            filename = KNOWN_SCHEMAS[name_or_path]
            path = self._schema_dir / filename
            if path.exists():
                return path
        
        # Check if it's a direct filename
        path = self._schema_dir / name_or_path
        if path.exists():
            return path
        
        # Check with .schema.json suffix
        path = self._schema_dir / f"{name_or_path}.schema.json"
        if path.exists():
            return path
        
        # Check with hyphens instead of underscores
        hyphen_name = name_or_path.replace("_", "-")
        path = self._schema_dir / f"{hyphen_name}.schema.json"
        if path.exists():
            return path
        
        # Try to find in subdirectories
        for p in self._schema_dir.rglob(f"*{name_or_path}*.json"):
            if p.is_file():
                return p
        
        return None
    
    def load_json_schema(self, name_or_path: str) -> Optional[dict]:
        """
        Load a JSON Schema by name or path.
        
        Args:
            name_or_path: Schema identifier (see _resolve_schema_path for options)
        
        Returns:
            Schema dict, or None if not found
        
        Raises:
            json.JSONDecodeError: If schema file is invalid JSON
        """
        # Check cache
        if name_or_path in self._schema_cache:
            return self._schema_cache[name_or_path]
        
        # Resolve path
        path = self._resolve_schema_path(name_or_path)
        if path is None:
            return None
        
        # Load and cache
        with open(path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        
        self._schema_cache[name_or_path] = schema
        return schema
    
    def get_evidence_pack_manifest_schema(self) -> Optional[dict]:
        """Get the Evidence Pack manifest schema."""
        return self.load_json_schema("evidence_pack_manifest")
    
    def get_aimo_ev_schema(self) -> Optional[dict]:
        """Get the AIMO EV (Evidence) schema."""
        return self.load_json_schema("aimo_ev")
    
    def get_shadow_ai_discovery_schema(self) -> Optional[dict]:
        """Get the Shadow AI Discovery log schema."""
        return self.load_json_schema("shadow_ai_discovery")
    
    @property
    def standard_version(self) -> str:
        """Get the Standard version being used."""
        return self.artifacts.standard_version
    
    @property
    def schema_dir(self) -> Optional[Path]:
        """Get the schema directory path."""
        return self._schema_dir


# Module-level convenience functions

_default_loader: Optional[SchemaLoader] = None


def get_schema_loader(version: str = AIMO_STANDARD_VERSION_DEFAULT) -> SchemaLoader:
    """Get or create the default schema loader."""
    global _default_loader
    if _default_loader is None or _default_loader.standard_version != version:
        _default_loader = SchemaLoader(version=version)
    return _default_loader


def load_json_schema(
    name_or_path: str,
    version: str = AIMO_STANDARD_VERSION_DEFAULT
) -> Optional[dict]:
    """Convenience function to load a schema."""
    return get_schema_loader(version).load_json_schema(name_or_path)
