"""
AIMO Standard Adapter

This module provides integration with the AIMO Standard specification.
The Standard is managed as a git submodule and versioned per-run for
audit reproducibility.

Main components:
- resolver: Resolves and syncs Standard artifacts
- taxonomy: Loads and validates 8-dimension taxonomy codes
- schemas: Loads JSON Schemas from Standard
- validator_runner: Runs validation against Standard specifications
"""

from .constants import (
    AIMO_STANDARD_VERSION_DEFAULT,
    AIMO_STANDARD_SUBMODULE_PATH,
    AIMO_STANDARD_CACHE_DIR_DEFAULT,
)
from .resolver import (
    ResolvedStandardArtifacts,
    resolve_standard_artifacts,
    get_cached_artifacts,
)
from .taxonomy import (
    TaxonomyAdapter,
    TaxonomyCode,
    DIMENSION_CARDINALITY,
    ALL_DIMENSIONS,
    get_taxonomy_adapter,
    get_allowed_codes,
    validate_assignment,
)
from .schemas import (
    SchemaLoader,
    get_schema_loader,
    load_json_schema,
)
from .validator_runner import (
    ValidatorRunner,
    ValidationResult,
    get_validator_runner,
    run_validation,
)

__all__ = [
    # Constants
    "AIMO_STANDARD_VERSION_DEFAULT",
    "AIMO_STANDARD_SUBMODULE_PATH",
    "AIMO_STANDARD_CACHE_DIR_DEFAULT",
    # Resolver
    "ResolvedStandardArtifacts",
    "resolve_standard_artifacts",
    "get_cached_artifacts",
    # Taxonomy
    "TaxonomyAdapter",
    "TaxonomyCode",
    "DIMENSION_CARDINALITY",
    "ALL_DIMENSIONS",
    "get_taxonomy_adapter",
    "get_allowed_codes",
    "validate_assignment",
    # Schemas
    "SchemaLoader",
    "get_schema_loader",
    "load_json_schema",
    # Validator
    "ValidatorRunner",
    "ValidationResult",
    "get_validator_runner",
    "run_validation",
]
