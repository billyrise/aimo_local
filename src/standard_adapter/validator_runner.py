"""
AIMO Standard Validator Runner

Runs validation against AIMO Standard specifications.
Prioritizes the official validator CLI from the Standard if available,
with fallback to local validation logic.
"""

import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import jsonschema
except ImportError:
    jsonschema = None  # type: ignore

from .resolver import resolve_standard_artifacts, ResolvedStandardArtifacts
from .taxonomy import TaxonomyAdapter
from .schemas import SchemaLoader
from .constants import AIMO_STANDARD_VERSION_DEFAULT


@dataclass
class ValidationResult:
    """Result of a validation run."""
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    
    # Audit trail
    standard_version: str = ""
    standard_commit: str = ""
    standard_sha256: str = ""
    
    # Validation details
    validator_used: str = ""  # "standard_cli" or "fallback"
    checks_performed: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dict for serialization."""
        return {
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "standard_version": self.standard_version,
            "standard_commit": self.standard_commit,
            "standard_sha256": self.standard_sha256,
            "validator_used": self.validator_used,
            "checks_performed": self.checks_performed,
        }


class ValidatorRunner:
    """
    Runs validation against AIMO Standard.
    
    Validation priority:
    1. Official validator CLI from Standard (if available)
    2. Fallback: jsonschema validation + taxonomy validation
    """
    
    def __init__(
        self,
        artifacts: Optional[ResolvedStandardArtifacts] = None,
        version: str = AIMO_STANDARD_VERSION_DEFAULT
    ):
        """
        Initialize validator runner.
        
        Args:
            artifacts: Pre-resolved artifacts (optional)
            version: Standard version to use if artifacts not provided
        """
        if artifacts is None:
            artifacts = resolve_standard_artifacts(version=version)
        
        self.artifacts = artifacts
        self._taxonomy: Optional[TaxonomyAdapter] = None
        self._schemas: Optional[SchemaLoader] = None
        self._validator_cli_path: Optional[Path] = None
        
        self._find_validator_cli()
    
    def _find_validator_cli(self):
        """Find the official validator CLI if available."""
        # Look for validator/src/validate.py
        cli_path = self.artifacts.artifacts_dir / "validator" / "src" / "validate.py"
        if cli_path.exists():
            self._validator_cli_path = cli_path
            return
        
        # Also check submodule directly
        cli_path = self.artifacts.submodule_dir / "validator" / "src" / "validate.py"
        if cli_path.exists():
            self._validator_cli_path = cli_path
    
    @property
    def taxonomy(self) -> TaxonomyAdapter:
        """Get taxonomy adapter (lazy load)."""
        if self._taxonomy is None:
            self._taxonomy = TaxonomyAdapter(artifacts=self.artifacts)
        return self._taxonomy
    
    @property
    def schemas(self) -> SchemaLoader:
        """Get schema loader (lazy load)."""
        if self._schemas is None:
            self._schemas = SchemaLoader(artifacts=self.artifacts)
        return self._schemas
    
    def _run_standard_cli(self, target_path: Path) -> Optional[ValidationResult]:
        """
        Run the official Standard validator CLI.
        
        Returns:
            ValidationResult if CLI ran successfully, None if CLI not available
        """
        if self._validator_cli_path is None:
            return None
        
        try:
            result = subprocess.run(
                [sys.executable, str(self._validator_cli_path), str(target_path)],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            # Parse output
            errors = []
            warnings = []
            
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.startswith("ERROR:"):
                    errors.append(line[6:].strip())
                elif line.startswith("WARNING:"):
                    warnings.append(line[8:].strip())
            
            for line in result.stderr.splitlines():
                line = line.strip()
                if line and not line.startswith("INFO:"):
                    errors.append(line)
            
            return ValidationResult(
                passed=result.returncode == 0 and len(errors) == 0,
                errors=errors,
                warnings=warnings,
                standard_version=self.artifacts.standard_version,
                standard_commit=self.artifacts.standard_commit,
                standard_sha256=self.artifacts.artifacts_dir_sha256,
                validator_used="standard_cli",
                checks_performed=["standard_cli_validation"]
            )
        
        except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError) as e:
            # CLI failed, return None to trigger fallback
            return None
    
    def _validate_manifest_schema(self, manifest: dict) -> list[str]:
        """Validate manifest against JSON Schema."""
        errors = []
        
        if jsonschema is None:
            errors.append("jsonschema library not available for schema validation")
            return errors
        
        schema = self.schemas.get_evidence_pack_manifest_schema()
        if schema is None:
            errors.append("Evidence Pack manifest schema not found")
            return errors
        
        try:
            jsonschema.validate(instance=manifest, schema=schema)
        except jsonschema.ValidationError as e:
            errors.append(f"Schema validation error: {e.message}")
        except jsonschema.SchemaError as e:
            errors.append(f"Invalid schema: {e.message}")
        
        return errors
    
    def _validate_taxonomy_codes(self, codes: dict) -> list[str]:
        """Validate taxonomy code assignment."""
        return self.taxonomy.validate_codes_dict(codes)
    
    def run_validation(
        self,
        evidence_bundle_dir: Optional[Path] = None,
        manifest: Optional[dict] = None,
        manifest_path: Optional[Path] = None
    ) -> ValidationResult:
        """
        Run validation on an Evidence Bundle or manifest.
        
        Args:
            evidence_bundle_dir: Path to Evidence Bundle directory
            manifest: Manifest dict (if already loaded)
            manifest_path: Path to manifest.json file
        
        Returns:
            ValidationResult with pass/fail, errors, and audit info
        """
        errors = []
        warnings = []
        checks_performed = []
        
        # Step 1: Try official CLI if bundle directory provided
        if evidence_bundle_dir and evidence_bundle_dir.exists():
            cli_result = self._run_standard_cli(evidence_bundle_dir)
            if cli_result is not None:
                return cli_result
        
        # Step 2: Fallback validation
        
        # Load manifest if needed
        if manifest is None:
            if manifest_path and manifest_path.exists():
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
            elif evidence_bundle_dir:
                for name in ["manifest.json", "evidence_pack_manifest.json"]:
                    path = evidence_bundle_dir / name
                    if path.exists():
                        with open(path, "r", encoding="utf-8") as f:
                            manifest = json.load(f)
                        break
        
        if manifest is None:
            errors.append("No manifest found or provided")
            return ValidationResult(
                passed=False,
                errors=errors,
                warnings=warnings,
                standard_version=self.artifacts.standard_version,
                standard_commit=self.artifacts.standard_commit,
                standard_sha256=self.artifacts.artifacts_dir_sha256,
                validator_used="fallback",
                checks_performed=checks_performed
            )
        
        # Validate manifest against schema
        checks_performed.append("manifest_schema_validation")
        schema_errors = self._validate_manifest_schema(manifest)
        errors.extend(schema_errors)
        
        # Validate taxonomy codes
        if "codes" in manifest:
            checks_performed.append("taxonomy_validation")
            taxonomy_errors = self._validate_taxonomy_codes(manifest["codes"])
            errors.extend(taxonomy_errors)
        else:
            warnings.append("No 'codes' field in manifest, skipping taxonomy validation")
        
        # Check evidence files exist (if bundle directory provided)
        if evidence_bundle_dir and "evidence_files" in manifest:
            checks_performed.append("evidence_files_existence")
            for ev_file in manifest["evidence_files"]:
                filename = ev_file.get("filename", "")
                if filename:
                    file_path = evidence_bundle_dir / filename
                    if not file_path.exists():
                        errors.append(f"Evidence file not found: {filename}")
        
        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            standard_version=self.artifacts.standard_version,
            standard_commit=self.artifacts.standard_commit,
            standard_sha256=self.artifacts.artifacts_dir_sha256,
            validator_used="fallback",
            checks_performed=checks_performed
        )
    
    def validate_codes_only(self, codes: dict) -> ValidationResult:
        """
        Validate only the taxonomy code assignment.
        
        Args:
            codes: Dict mapping dimension -> list of codes
        
        Returns:
            ValidationResult
        """
        errors = self._validate_taxonomy_codes(codes)
        
        return ValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            standard_version=self.artifacts.standard_version,
            standard_commit=self.artifacts.standard_commit,
            standard_sha256=self.artifacts.artifacts_dir_sha256,
            validator_used="fallback",
            checks_performed=["taxonomy_validation"]
        )


# Module-level convenience functions

_default_runner: Optional[ValidatorRunner] = None


def get_validator_runner(version: str = AIMO_STANDARD_VERSION_DEFAULT) -> ValidatorRunner:
    """Get or create the default validator runner."""
    global _default_runner
    if _default_runner is None or _default_runner.artifacts.standard_version != version:
        _default_runner = ValidatorRunner(version=version)
    return _default_runner


def run_validation(
    evidence_bundle_dir: Optional[Path] = None,
    manifest: Optional[dict] = None,
    manifest_path: Optional[Path] = None,
    version: str = AIMO_STANDARD_VERSION_DEFAULT
) -> ValidationResult:
    """Convenience function to run validation."""
    return get_validator_runner(version).run_validation(
        evidence_bundle_dir=evidence_bundle_dir,
        manifest=manifest,
        manifest_path=manifest_path
    )
