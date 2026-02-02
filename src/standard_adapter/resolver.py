"""
AIMO Standard Resolver

Resolves and loads AIMO Standard artifacts for a specific version.
This module is the entry point for accessing Standard resources
(taxonomy, schemas, validator) in the Engine.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .constants import (
    AIMO_STANDARD_VERSION_DEFAULT,
    AIMO_STANDARD_SUBMODULE_PATH,
    AIMO_STANDARD_CACHE_DIR_DEFAULT,
)


@dataclass
class ResolvedStandardArtifacts:
    """
    Resolved AIMO Standard artifacts with version info and checksums.
    
    This class contains all metadata needed for audit reproducibility.
    """
    # Version info
    standard_version: str
    standard_commit: str
    standard_tag: str
    
    # Paths
    artifacts_dir: Path
    submodule_dir: Path
    
    # Checksums for audit trail
    artifacts_dir_sha256: str
    
    # Optional: if a zip exists
    artifacts_zip_path: Optional[Path] = None
    artifacts_zip_sha256: Optional[str] = None
    
    # Manifest data
    manifest: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert to dict for serialization."""
        return {
            "standard_version": self.standard_version,
            "standard_commit": self.standard_commit,
            "standard_tag": self.standard_tag,
            "artifacts_dir": str(self.artifacts_dir),
            "submodule_dir": str(self.submodule_dir),
            "artifacts_dir_sha256": self.artifacts_dir_sha256,
            "artifacts_zip_path": str(self.artifacts_zip_path) if self.artifacts_zip_path else None,
            "artifacts_zip_sha256": self.artifacts_zip_sha256,
        }


def get_project_root() -> Path:
    """Get the project root directory."""
    # Navigate from src/standard_adapter to project root
    return Path(__file__).resolve().parent.parent.parent


def calculate_file_sha256(file_path: Path) -> str:
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def calculate_directory_sha256(directory: Path) -> str:
    """
    Calculate SHA256 hash of a directory by hashing all files in sorted order.
    """
    sha256_hash = hashlib.sha256()
    all_files = sorted(directory.rglob("*"))
    
    for file_path in all_files:
        if file_path.is_file():
            rel_path = file_path.relative_to(directory)
            sha256_hash.update(str(rel_path).encode("utf-8"))
            file_hash = calculate_file_sha256(file_path)
            sha256_hash.update(file_hash.encode("utf-8"))
    
    return sha256_hash.hexdigest()


def run_git_command(args: list[str], cwd: Optional[Path] = None) -> tuple[int, str, str]:
    """Run a git command and return (returncode, stdout, stderr)."""
    result = subprocess.run(
        ["git"] + args,
        cwd=cwd,
        capture_output=True,
        text=True
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def ensure_submodule_initialized(project_root: Path, submodule_path: str) -> bool:
    """Ensure the submodule is initialized."""
    full_path = project_root / submodule_path
    
    if not full_path.exists() or not (full_path / ".git").exists():
        returncode, stdout, stderr = run_git_command(
            ["submodule", "update", "--init", submodule_path],
            cwd=project_root
        )
        if returncode != 0:
            raise RuntimeError(f"Failed to initialize submodule: {stderr}")
    
    return True


def checkout_version(submodule_path: Path, version: str) -> tuple[str, str]:
    """
    Checkout the specified version in the submodule.
    
    Returns:
        (commit_hash, tag_name)
    """
    # Fetch tags
    run_git_command(["fetch", "--all", "--tags"], cwd=submodule_path)
    
    # Try to checkout as tag (with v prefix)
    tag_name = f"v{version}" if not version.startswith("v") else version
    returncode, stdout, stderr = run_git_command(["checkout", tag_name], cwd=submodule_path)
    
    if returncode != 0:
        # Try without v prefix
        returncode, stdout, stderr = run_git_command(["checkout", version], cwd=submodule_path)
        if returncode != 0:
            raise RuntimeError(f"Failed to checkout version {version}: {stderr}")
        tag_name = version
    
    # Get commit hash
    returncode, commit_hash, stderr = run_git_command(["rev-parse", "HEAD"], cwd=submodule_path)
    if returncode != 0:
        raise RuntimeError(f"Failed to get commit hash: {stderr}")
    
    return commit_hash, tag_name


def sync_to_cache(
    submodule_path: Path,
    cache_dir: Path,
    version: str
) -> tuple[Path, dict]:
    """
    Sync artifacts from submodule to local cache.
    
    Returns:
        (cache_path, manifest_dict)
    """
    version_cache_dir = cache_dir / (f"v{version}" if not version.startswith("v") else version)
    version_cache_dir = Path(os.path.expanduser(str(version_cache_dir)))
    
    # Check if cache already exists with valid manifest
    manifest_path = version_cache_dir / "manifest.json"
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        # Verify cache is valid
        if "directory_sha256" in manifest and version_cache_dir.exists():
            return version_cache_dir, manifest
    
    # Clear and recreate cache
    if version_cache_dir.exists():
        shutil.rmtree(version_cache_dir)
    version_cache_dir.mkdir(parents=True, exist_ok=True)
    
    manifest = {
        "version": version,
        "cache_dir": str(version_cache_dir),
        "files": []
    }
    
    # Copy key directories
    for dir_name in ["schemas", "data", "artifacts", "templates", "examples", "validator"]:
        src_dir = submodule_path / dir_name
        if src_dir.exists():
            dest_dir = version_cache_dir / dir_name
            shutil.copytree(src_dir, dest_dir)
    
    # Calculate directory hash
    manifest["directory_sha256"] = calculate_directory_sha256(version_cache_dir)
    
    # List files
    for file_path in sorted(version_cache_dir.rglob("*")):
        if file_path.is_file():
            rel_path = file_path.relative_to(version_cache_dir)
            manifest["files"].append(str(rel_path))
    
    manifest["file_count"] = len(manifest["files"])
    
    # Save manifest
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    return version_cache_dir, manifest


def resolve_standard_artifacts(
    version: str = AIMO_STANDARD_VERSION_DEFAULT,
    submodule_path: str = AIMO_STANDARD_SUBMODULE_PATH,
    cache_dir: str = AIMO_STANDARD_CACHE_DIR_DEFAULT,
    force_sync: bool = False,
    skip_pinning_check: bool = False
) -> ResolvedStandardArtifacts:
    """
    Resolve AIMO Standard artifacts for a specific version.
    
    This is the main entry point for accessing Standard resources.
    It ensures the submodule is at the correct version and syncs
    artifacts to the local cache.
    
    IMPORTANT: By default, this function enforces pinning verification.
    If the resolved artifacts don't match the pinned values, a
    StandardPinningError is raised to prevent accidental version drift.
    
    Args:
        version: Target version (e.g., "0.1.7")
        submodule_path: Relative path to submodule from project root
        cache_dir: Base cache directory
        force_sync: If True, force resync even if cache exists
        skip_pinning_check: If True, skip pinning verification
            (ONLY use in upgrade scripts, never in production)
    
    Returns:
        ResolvedStandardArtifacts with paths and checksums
    
    Raises:
        RuntimeError: If submodule initialization or checkout fails
        StandardPinningError: If pinning verification fails
    """
    from .pinning import (
        enforce_pinning,
        PINNED_STANDARD_VERSION,
        StandardPinningError,
    )
    
    project_root = get_project_root()
    full_submodule_path = project_root / submodule_path
    cache_base = Path(os.path.expanduser(cache_dir))
    
    # Step 1: Ensure submodule is initialized
    ensure_submodule_initialized(project_root, submodule_path)
    
    # Step 2: Checkout target version
    commit_hash, tag_name = checkout_version(full_submodule_path, version)
    
    # Step 3: Sync to cache
    cache_path, manifest = sync_to_cache(full_submodule_path, cache_base, version)
    
    # Step 4: Check for zip artifacts
    zip_path = full_submodule_path / "dist" / "aimo-standard-artifacts.zip"
    zip_sha256 = None
    if zip_path.exists():
        zip_sha256 = calculate_file_sha256(zip_path)
    
    resolved = ResolvedStandardArtifacts(
        standard_version=version,
        standard_commit=commit_hash,
        standard_tag=tag_name,
        artifacts_dir=cache_path,
        submodule_dir=full_submodule_path,
        artifacts_dir_sha256=manifest.get("directory_sha256", ""),
        artifacts_zip_path=zip_path if zip_path.exists() else None,
        artifacts_zip_sha256=zip_sha256,
        manifest=manifest
    )
    
    # Step 5: Enforce pinning verification (critical for audit reproducibility)
    if not skip_pinning_check:
        # Only verify if resolving the pinned version
        if version == PINNED_STANDARD_VERSION:
            enforce_pinning(resolved)
    
    return resolved


def get_cached_artifacts(
    version: str = AIMO_STANDARD_VERSION_DEFAULT,
    cache_dir: str = AIMO_STANDARD_CACHE_DIR_DEFAULT
) -> Optional[ResolvedStandardArtifacts]:
    """
    Get cached artifacts without syncing from submodule.
    
    Returns None if cache doesn't exist or is invalid.
    """
    cache_base = Path(os.path.expanduser(cache_dir))
    version_cache_dir = cache_base / (f"v{version}" if not version.startswith("v") else version)
    
    manifest_path = version_cache_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        
        return ResolvedStandardArtifacts(
            standard_version=version,
            standard_commit=manifest.get("commit", "unknown"),
            standard_tag=manifest.get("tag", f"v{version}"),
            artifacts_dir=version_cache_dir,
            submodule_dir=Path(get_project_root() / AIMO_STANDARD_SUBMODULE_PATH),
            artifacts_dir_sha256=manifest.get("directory_sha256", ""),
            manifest=manifest
        )
    except (json.JSONDecodeError, KeyError):
        return None
