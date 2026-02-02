"""
AIMO Standard Constants

This module defines constants for AIMO Standard integration.
These values are used by the sync script and Engine runtime
to ensure consistent version referencing.

The AIMO Standard is managed as a git submodule to enable:
1. Version pinning per Engine release
2. Audit trail (commit hash + SHA256 checksums)
3. Reproducible analysis across runs

Each Engine run records:
- aimo_standard_version: The semantic version (e.g., "0.1.7")
- aimo_standard_commit: The git commit hash
- aimo_standard_sha256: SHA256 of the artifacts directory

This ensures that any analysis can be reproduced exactly,
even if the Standard is updated later.
"""

# Default version of AIMO Standard to use
# This should match a tag in the aimo-standard repository
AIMO_STANDARD_VERSION_DEFAULT = "0.1.7"

# Path to the AIMO Standard submodule (relative to project root)
AIMO_STANDARD_SUBMODULE_PATH = "third_party/aimo-standard"

# Default cache directory for AIMO Standard artifacts
# Artifacts are extracted/copied here for Engine use
# Format: {cache_dir}/v{version}/
AIMO_STANDARD_CACHE_DIR_DEFAULT = "~/.cache/aimo/standard"

# Supported artifact types in the submodule
AIMO_STANDARD_ARTIFACT_TYPES = [
    "zip",        # dist/aimo-standard-artifacts.zip
    "dir",        # artifacts/
    "source_pack",  # source_pack/
    "schemas",    # schemas/
]

# Key directories to copy from submodule to cache
AIMO_STANDARD_KEY_DIRECTORIES = [
    "schemas",      # JSON Schema definitions
    "data",         # Taxonomy data
    "artifacts",    # Generated artifacts
    "source_pack",  # Source definitions
    "templates",    # Evidence templates
    "examples",     # Example files
]

# Fields to record in run manifest for audit trail
AIMO_STANDARD_RUN_MANIFEST_FIELDS = [
    "aimo_standard_version",     # e.g., "0.1.7"
    "aimo_standard_tag",         # e.g., "v0.1.7"
    "aimo_standard_commit",      # e.g., "88ab75d286a252..."
    "aimo_standard_sha256",      # Directory hash
    "aimo_standard_cache_dir",   # Local cache path
]
