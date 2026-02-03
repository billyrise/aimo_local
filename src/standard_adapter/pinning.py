"""
AIMO Standard Version Pinning Guard

This module enforces strict version pinning for AIMO Standard artifacts.
It prevents accidental version drift, tag mutation, and artifact tampering.

CRITICAL: These values must be updated explicitly through the upgrade procedure.
See: docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md

DO NOT:
- Change these values without following the upgrade playbook
- Implement "latest" or auto-follow modes
- Ignore verification failures
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .resolver import ResolvedStandardArtifacts

# =============================================================================
# PINNED VALUES - Updated via upgrade procedure only
# =============================================================================

# Current pinned AIMO Standard version
PINNED_STANDARD_VERSION = "0.1.1"

# Git commit hash that v0.1.1 tag points to
# If this changes, the tag was mutated (a serious issue in Standard repo)
PINNED_STANDARD_COMMIT = "556fa4ddb1bcce3c5169e3d36b7697a94a80a2ff"

# SHA256 of the artifacts directory (cache after sync)
# If this changes, artifact contents were modified
PINNED_ARTIFACTS_DIR_SHA256 = "02c8f54602904174b68ba5e17b0b7c36123225a5f1c42baa197f64e866981db0"

# Optional: SHA256 of artifacts zip (if distributed as zip)
# Set to None if zip is not used
PINNED_ARTIFACTS_ZIP_SHA256 = None


# =============================================================================
# Verification
# =============================================================================

@dataclass
class PinningVerificationResult:
    """Result of pinning verification."""
    passed: bool
    version_match: bool
    commit_match: bool
    artifacts_sha_match: bool
    errors: list[str]
    
    def raise_if_failed(self):
        """Raise exception if verification failed."""
        if not self.passed:
            raise StandardPinningError(self.errors)


class StandardPinningError(Exception):
    """
    Raised when AIMO Standard pinning verification fails.
    
    This indicates one of:
    1. Tag was mutated in Standard repo (commit mismatch)
    2. Artifacts were modified (SHA mismatch)
    3. Wrong version resolved (version mismatch)
    
    See: docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md for resolution steps.
    """
    
    def __init__(self, errors: list[str]):
        self.errors = errors
        message = self._build_message(errors)
        super().__init__(message)
    
    def _build_message(self, errors: list[str]) -> str:
        lines = [
            "",
            "=" * 70,
            "AIMO STANDARD PINNING VERIFICATION FAILED",
            "=" * 70,
            "",
            "The resolved AIMO Standard artifacts do not match the pinned values.",
            "This is a critical error that blocks execution.",
            "",
            "Errors:",
        ]
        for error in errors:
            lines.append(f"  - {error}")
        lines.extend([
            "",
            "Possible causes:",
            "  1. The Standard tag was mutated (commit hash changed)",
            "  2. Artifacts were modified after the pin was set",
            "  3. Submodule is at wrong version",
            "",
            "NEXT STEPS:",
            "  1. Review: docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md",
            "  2. If this is an intentional upgrade, run:",
            "     ./scripts/upgrade_standard_version.sh --version X.Y.Z",
            "  3. If the tag was mutated, report to Standard repo maintainers",
            "",
            "DO NOT ignore this error or modify pinning values without review.",
            "=" * 70,
        ])
        return "\n".join(lines)


def verify_pinning(
    resolved: "ResolvedStandardArtifacts",
    expected_version: str = PINNED_STANDARD_VERSION,
    expected_commit: str = PINNED_STANDARD_COMMIT,
    expected_artifacts_sha: str = PINNED_ARTIFACTS_DIR_SHA256,
) -> PinningVerificationResult:
    """
    Verify that resolved artifacts match pinned values.
    
    This function MUST be called after resolving artifacts to ensure
    we're using exactly the expected Standard version.
    
    Args:
        resolved: ResolvedStandardArtifacts from resolver
        expected_version: Expected version string
        expected_commit: Expected git commit hash
        expected_artifacts_sha: Expected artifacts directory SHA256
    
    Returns:
        PinningVerificationResult with match details
    
    Raises:
        Nothing - call result.raise_if_failed() to raise on failure
    """
    errors = []
    
    # Check version
    version_match = resolved.standard_version == expected_version
    if not version_match:
        errors.append(
            f"Version mismatch: expected '{expected_version}', "
            f"got '{resolved.standard_version}'"
        )
    
    # Check commit (full hash comparison)
    commit_match = resolved.standard_commit.startswith(expected_commit[:12])
    if not commit_match:
        errors.append(
            f"Commit mismatch: expected '{expected_commit[:12]}...', "
            f"got '{resolved.standard_commit[:12]}...'. "
            f"Tag may have been mutated!"
        )
    
    # Check artifacts SHA
    artifacts_sha_match = resolved.artifacts_dir_sha256 == expected_artifacts_sha
    if not artifacts_sha_match:
        errors.append(
            f"Artifacts SHA mismatch: expected '{expected_artifacts_sha[:16]}...', "
            f"got '{resolved.artifacts_dir_sha256[:16]}...'. "
            f"Artifacts may have been modified!"
        )
    
    passed = version_match and commit_match and artifacts_sha_match
    
    return PinningVerificationResult(
        passed=passed,
        version_match=version_match,
        commit_match=commit_match,
        artifacts_sha_match=artifacts_sha_match,
        errors=errors,
    )


def enforce_pinning(resolved: "ResolvedStandardArtifacts") -> None:
    """
    Verify pinning and raise exception if failed.
    
    This is the main entry point for pinning verification.
    Call this after resolve_standard_artifacts() to ensure correctness.
    
    Args:
        resolved: ResolvedStandardArtifacts from resolver
    
    Raises:
        StandardPinningError: If any pinning check fails
    """
    result = verify_pinning(resolved)
    result.raise_if_failed()


def get_pinned_info() -> dict:
    """
    Get the currently pinned Standard information.
    
    Returns:
        Dict with pinned version, commit, and SHA values
    """
    return {
        "version": PINNED_STANDARD_VERSION,
        "commit": PINNED_STANDARD_COMMIT,
        "artifacts_dir_sha256": PINNED_ARTIFACTS_DIR_SHA256,
        "artifacts_zip_sha256": PINNED_ARTIFACTS_ZIP_SHA256,
    }
