#!/usr/bin/env python3
"""
AIMO Standard Sync Script

Synchronizes the AIMO Standard submodule to a specific version and prepares
the artifacts for Engine use. Records version, commit hash, and SHA256
checksums for audit reproducibility.

Usage:
    python scripts/sync_aimo_standard.py --version 0.1.7
    python scripts/sync_aimo_standard.py --version 0.1.7 --cache-dir ~/.cache/aimo/standard
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Default constants (can be overridden by src/standard_adapter/constants.py)
AIMO_STANDARD_VERSION_DEFAULT = "0.1.7"
AIMO_STANDARD_SUBMODULE_PATH = "third_party/aimo-standard"
AIMO_STANDARD_CACHE_DIR_DEFAULT = "~/.cache/aimo/standard"

# Global flag to control log output destination
# When True, all logging goes to stderr to keep stdout clean for JSON
_LOG_TO_STDERR = False


def _log(msg: str):
    """Log message to appropriate output stream."""
    if _LOG_TO_STDERR:
        print(msg, file=sys.stderr)
    else:
        print(msg)


def get_project_root() -> Path:
    """Get the project root directory (where .git is located)."""
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent


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
    """
    Ensure the submodule is initialized.
    
    Returns:
        True if submodule is ready, False otherwise
    """
    full_path = project_root / submodule_path
    
    if not full_path.exists():
        _log(f"Initializing submodule at {submodule_path}...")
        returncode, stdout, stderr = run_git_command(
            ["submodule", "update", "--init", submodule_path],
            cwd=project_root
        )
        if returncode != 0:
            print(f"ERROR: Failed to initialize submodule: {stderr}", file=sys.stderr)
            return False
        _log("Submodule initialized successfully.")
    
    # Check if .git exists in submodule
    git_dir = full_path / ".git"
    if not git_dir.exists():
        print(f"ERROR: Submodule at {submodule_path} is not a valid git repository", file=sys.stderr)
        return False
    
    return True


def checkout_version(submodule_path: Path, version: str) -> tuple[bool, str]:
    """
    Checkout the specified version (tag or commit) in the submodule.
    
    Args:
        submodule_path: Path to the submodule directory
        version: Version string (e.g., "0.1.7" or full commit hash)
    
    Returns:
        (success, commit_hash)
    """
    # Fetch all tags
    _log("Fetching tags from remote...")
    returncode, _, stderr = run_git_command(["fetch", "--all", "--tags"], cwd=submodule_path)
    if returncode != 0:
        print(f"WARNING: Failed to fetch tags: {stderr}", file=sys.stderr)
    
    # Try to checkout as tag first (with v prefix)
    tag_name = f"v{version}" if not version.startswith("v") else version
    _log(f"Attempting to checkout tag: {tag_name}")
    
    returncode, stdout, stderr = run_git_command(["checkout", tag_name], cwd=submodule_path)
    
    if returncode != 0:
        # Try without v prefix
        _log(f"Tag {tag_name} not found, trying {version}...")
        returncode, stdout, stderr = run_git_command(["checkout", version], cwd=submodule_path)
        
        if returncode != 0:
            print(f"ERROR: Failed to checkout version {version}: {stderr}", file=sys.stderr)
            return False, ""
    
    # Get the current commit hash
    returncode, commit_hash, stderr = run_git_command(["rev-parse", "HEAD"], cwd=submodule_path)
    
    if returncode != 0:
        print(f"ERROR: Failed to get commit hash: {stderr}", file=sys.stderr)
        return False, ""
    
    _log(f"Checked out version {version} at commit: {commit_hash[:12]}")
    return True, commit_hash


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
    
    The hash includes relative paths and file contents to detect any changes.
    """
    sha256_hash = hashlib.sha256()
    
    # Get all files recursively, sorted by relative path
    all_files = sorted(directory.rglob("*"))
    
    for file_path in all_files:
        if file_path.is_file():
            # Include relative path in hash (for reproducibility)
            rel_path = file_path.relative_to(directory)
            sha256_hash.update(str(rel_path).encode("utf-8"))
            
            # Include file content
            file_hash = calculate_file_sha256(file_path)
            sha256_hash.update(file_hash.encode("utf-8"))
    
    return sha256_hash.hexdigest()


def find_artifacts_source(submodule_path: Path) -> tuple[Optional[Path], str]:
    """
    Find the artifacts source in the submodule.
    
    Returns:
        (source_path, artifact_type) where artifact_type is 'zip', 'dir', or 'source_pack'
    """
    # Priority 1: dist/aimo-standard-artifacts.zip
    dist_zip = submodule_path / "dist" / "aimo-standard-artifacts.zip"
    if dist_zip.exists():
        return dist_zip, "zip"
    
    # Priority 2: artifacts/ directory
    artifacts_dir = submodule_path / "artifacts"
    if artifacts_dir.exists() and artifacts_dir.is_dir():
        return artifacts_dir, "dir"
    
    # Priority 3: source_pack/ directory
    source_pack_dir = submodule_path / "source_pack"
    if source_pack_dir.exists() and source_pack_dir.is_dir():
        return source_pack_dir, "source_pack"
    
    # Priority 4: schemas/ directory (minimal artifacts)
    schemas_dir = submodule_path / "schemas"
    if schemas_dir.exists() and schemas_dir.is_dir():
        return schemas_dir, "schemas"
    
    return None, ""


def sync_artifacts_to_cache(
    submodule_path: Path,
    cache_dir: Path,
    version: str
) -> tuple[bool, dict]:
    """
    Sync artifacts from submodule to local cache.
    
    Args:
        submodule_path: Path to the submodule
        cache_dir: Base cache directory (e.g., ~/.cache/aimo/standard)
        version: Version string
    
    Returns:
        (success, manifest_dict)
    """
    # Find artifacts source
    source_path, artifact_type = find_artifacts_source(submodule_path)
    
    if source_path is None:
        print(f"WARNING: No artifacts found in submodule. Will copy key directories.", file=sys.stderr)
        # Fallback: copy schemas and data directories
        artifact_type = "fallback"
    
    # Prepare cache directory
    version_cache_dir = cache_dir / f"v{version}" if not version.startswith("v") else cache_dir / version
    version_cache_dir = Path(os.path.expanduser(str(version_cache_dir)))
    
    # Clear existing cache for this version
    if version_cache_dir.exists():
        _log(f"Clearing existing cache at {version_cache_dir}")
        shutil.rmtree(version_cache_dir)
    
    version_cache_dir.mkdir(parents=True, exist_ok=True)
    
    manifest = {
        "version": version,
        "artifact_type": artifact_type,
        "cache_dir": str(version_cache_dir),
        "files": []
    }
    
    if artifact_type == "zip":
        # Extract zip to cache
        import zipfile
        _log(f"Extracting {source_path} to {version_cache_dir}")
        with zipfile.ZipFile(source_path, 'r') as zip_ref:
            zip_ref.extractall(version_cache_dir)
        manifest["zip_sha256"] = calculate_file_sha256(source_path)
    
    elif artifact_type in ("dir", "source_pack", "schemas"):
        # Copy directory to cache
        _log(f"Copying {artifact_type} from {source_path} to {version_cache_dir}")
        dest_dir = version_cache_dir / artifact_type
        shutil.copytree(source_path, dest_dir)
        manifest["source_dir"] = str(source_path.relative_to(submodule_path))
    
    else:  # fallback
        # Copy key directories
        for dir_name in ["schemas", "data", "artifacts", "source_pack", "templates", "examples"]:
            src_dir = submodule_path / dir_name
            if src_dir.exists():
                _log(f"Copying {dir_name}/ to cache")
                dest_dir = version_cache_dir / dir_name
                shutil.copytree(src_dir, dest_dir)
    
    # Calculate directory hash
    manifest["directory_sha256"] = calculate_directory_sha256(version_cache_dir)
    # Also provide as artifacts_dir_sha256 for API consistency
    manifest["artifacts_dir_sha256"] = manifest["directory_sha256"]
    
    # List all files in cache
    for file_path in sorted(version_cache_dir.rglob("*")):
        if file_path.is_file():
            rel_path = file_path.relative_to(version_cache_dir)
            manifest["files"].append(str(rel_path))
    
    manifest["file_count"] = len(manifest["files"])
    
    return True, manifest


def sync_aimo_standard(
    version: str = AIMO_STANDARD_VERSION_DEFAULT,
    submodule_path: str = AIMO_STANDARD_SUBMODULE_PATH,
    cache_dir: str = AIMO_STANDARD_CACHE_DIR_DEFAULT,
    quiet: bool = False
) -> dict:
    """
    Main sync function.
    
    Args:
        version: Target version (e.g., "0.1.7")
        submodule_path: Relative path to submodule
        cache_dir: Base cache directory
    
    Returns:
        Manifest dict with version info and SHA256 checksums
    """
    global _LOG_TO_STDERR
    _LOG_TO_STDERR = quiet
    
    project_root = get_project_root()
    full_submodule_path = project_root / submodule_path
    cache_path = Path(os.path.expanduser(cache_dir))
    
    _log("=" * 60)
    _log("AIMO Standard Sync")
    _log("=" * 60)
    _log(f"Version: {version}")
    _log(f"Submodule: {submodule_path}")
    _log(f"Cache: {cache_dir}")
    _log("=" * 60)
    
    # Step 1: Ensure submodule is initialized
    if not ensure_submodule_initialized(project_root, submodule_path):
        return {"error": "Failed to initialize submodule"}
    
    # Step 2: Checkout target version
    success, commit_hash = checkout_version(full_submodule_path, version)
    if not success:
        return {"error": f"Failed to checkout version {version}"}
    
    # Step 3: Sync artifacts to cache
    success, manifest = sync_artifacts_to_cache(full_submodule_path, cache_path, version)
    if not success:
        return {"error": "Failed to sync artifacts"}
    
    # Add git info to manifest
    manifest["commit"] = commit_hash
    manifest["tag"] = f"v{version}" if not version.startswith("v") else version
    
    # Save manifest to cache
    manifest_path = Path(manifest["cache_dir"]) / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    _log("=" * 60)
    _log("Sync completed successfully!")
    _log("=" * 60)
    _log(f"Version:       {manifest['version']}")
    _log(f"Commit:        {manifest['commit'][:12]}")
    _log(f"Tag:           {manifest['tag']}")
    _log(f"Cache Dir:     {manifest['cache_dir']}")
    _log(f"File Count:    {manifest['file_count']}")
    _log(f"Directory SHA256: {manifest['directory_sha256'][:16]}...")
    _log("=" * 60)
    
    return manifest


def verify_against_pin(manifest: dict, version: str) -> tuple[bool, list[str]]:
    """
    Verify synced manifest against pinned values.
    
    This ensures we're syncing exactly what's expected.
    If pinning verification fails, the sync should be rejected.
    
    Returns:
        (passed, error_messages)
    """
    # Import pinning from src
    project_root = get_project_root()
    sys.path.insert(0, str(project_root / "src"))
    
    try:
        from standard_adapter.pinning import (
            PINNED_STANDARD_VERSION,
            PINNED_STANDARD_COMMIT,
            PINNED_ARTIFACTS_DIR_SHA256,
        )
    except ImportError:
        # Pinning module not available, skip verification
        return True, []
    
    errors = []
    
    # Only verify for the pinned version
    if version != PINNED_STANDARD_VERSION:
        # Not the pinned version, skip verification
        return True, []
    
    # Check commit
    commit = manifest.get("commit", "")
    if commit and not commit.startswith(PINNED_STANDARD_COMMIT[:12]):
        errors.append(
            f"Commit mismatch: expected '{PINNED_STANDARD_COMMIT[:12]}...', "
            f"got '{commit[:12]}...'. Tag may have been mutated!"
        )
    
    # Check directory SHA
    dir_sha = manifest.get("directory_sha256", "")
    if dir_sha and dir_sha != PINNED_ARTIFACTS_DIR_SHA256:
        errors.append(
            f"Artifacts SHA mismatch: expected '{PINNED_ARTIFACTS_DIR_SHA256[:16]}...', "
            f"got '{dir_sha[:16]}...'. Artifacts may have been modified!"
        )
    
    return len(errors) == 0, errors


def _is_skip_pinning_allowed() -> bool:
    """
    Check if skip pinning check is allowed via environment variable.
    
    SECURITY: Skipping pinning check is ONLY allowed when:
    1. AIMO_ALLOW_SKIP_PINNING=1 is explicitly set
    2. This should NEVER be set in CI/production
    
    Returns:
        True if skip is allowed (environment variable is set)
    """
    return os.getenv("AIMO_ALLOW_SKIP_PINNING", "").lower() in ("1", "true", "yes")


def main():
    parser = argparse.ArgumentParser(
        description="Sync AIMO Standard submodule to a specific version"
    )
    parser.add_argument(
        "--version", "-v",
        default=AIMO_STANDARD_VERSION_DEFAULT,
        help=f"Target version (default: {AIMO_STANDARD_VERSION_DEFAULT})"
    )
    parser.add_argument(
        "--submodule-path",
        default=AIMO_STANDARD_SUBMODULE_PATH,
        help=f"Path to submodule (default: {AIMO_STANDARD_SUBMODULE_PATH})"
    )
    parser.add_argument(
        "--cache-dir",
        default=AIMO_STANDARD_CACHE_DIR_DEFAULT,
        help=f"Cache directory (default: {AIMO_STANDARD_CACHE_DIR_DEFAULT})"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output manifest as JSON"
    )
    parser.add_argument(
        "--skip-pin-check",
        action="store_true",
        help="Skip pinning verification (REQUIRES AIMO_ALLOW_SKIP_PINNING=1)"
    )
    
    args = parser.parse_args()
    
    manifest = sync_aimo_standard(
        version=args.version,
        submodule_path=args.submodule_path,
        cache_dir=args.cache_dir,
        quiet=args.json  # Suppress stdout logs when JSON output is requested
    )
    
    if args.json:
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
    
    if "error" in manifest:
        sys.exit(1)
    
    # Verify against pinned values (unless skipped with environment variable guard)
    should_skip_pin_check = False
    if args.skip_pin_check:
        if _is_skip_pinning_allowed():
            should_skip_pin_check = True
            print(
                "WARNING: Pinning check is SKIPPED (AIMO_ALLOW_SKIP_PINNING=1). "
                "This is for upgrade testing ONLY.",
                file=sys.stderr
            )
        else:
            print("", file=sys.stderr)
            print("=" * 70, file=sys.stderr)
            print("ERROR: --skip-pin-check requires AIMO_ALLOW_SKIP_PINNING=1", file=sys.stderr)
            print("=" * 70, file=sys.stderr)
            print("", file=sys.stderr)
            print("Pinning check cannot be skipped without explicit environment", file=sys.stderr)
            print("variable. This prevents accidental version drift.", file=sys.stderr)
            print("", file=sys.stderr)
            print("For upgrade testing ONLY, run:", file=sys.stderr)
            print("  AIMO_ALLOW_SKIP_PINNING=1 python scripts/sync_aimo_standard.py --skip-pin-check", file=sys.stderr)
            print("", file=sys.stderr)
            print("NEVER set AIMO_ALLOW_SKIP_PINNING in CI or production.", file=sys.stderr)
            print("=" * 70, file=sys.stderr)
            sys.exit(3)
    
    if not should_skip_pin_check:
        passed, errors = verify_against_pin(manifest, args.version)
        if not passed:
            print("", file=sys.stderr)
            print("=" * 70, file=sys.stderr)
            print("PINNING VERIFICATION FAILED", file=sys.stderr)
            print("=" * 70, file=sys.stderr)
            for error in errors:
                print(f"  - {error}", file=sys.stderr)
            print("", file=sys.stderr)
            print("See: docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md", file=sys.stderr)
            print("=" * 70, file=sys.stderr)
            sys.exit(2)
    
    return manifest


if __name__ == "__main__":
    main()
