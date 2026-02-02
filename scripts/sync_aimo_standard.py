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
        print(f"Initializing submodule at {submodule_path}...")
        returncode, stdout, stderr = run_git_command(
            ["submodule", "update", "--init", submodule_path],
            cwd=project_root
        )
        if returncode != 0:
            print(f"ERROR: Failed to initialize submodule: {stderr}", file=sys.stderr)
            return False
        print(f"Submodule initialized successfully.")
    
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
    print(f"Fetching tags from remote...")
    returncode, _, stderr = run_git_command(["fetch", "--all", "--tags"], cwd=submodule_path)
    if returncode != 0:
        print(f"WARNING: Failed to fetch tags: {stderr}", file=sys.stderr)
    
    # Try to checkout as tag first (with v prefix)
    tag_name = f"v{version}" if not version.startswith("v") else version
    print(f"Attempting to checkout tag: {tag_name}")
    
    returncode, stdout, stderr = run_git_command(["checkout", tag_name], cwd=submodule_path)
    
    if returncode != 0:
        # Try without v prefix
        print(f"Tag {tag_name} not found, trying {version}...")
        returncode, stdout, stderr = run_git_command(["checkout", version], cwd=submodule_path)
        
        if returncode != 0:
            print(f"ERROR: Failed to checkout version {version}: {stderr}", file=sys.stderr)
            return False, ""
    
    # Get the current commit hash
    returncode, commit_hash, stderr = run_git_command(["rev-parse", "HEAD"], cwd=submodule_path)
    
    if returncode != 0:
        print(f"ERROR: Failed to get commit hash: {stderr}", file=sys.stderr)
        return False, ""
    
    print(f"Checked out version {version} at commit: {commit_hash[:12]}")
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
        print(f"Clearing existing cache at {version_cache_dir}")
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
        print(f"Extracting {source_path} to {version_cache_dir}")
        with zipfile.ZipFile(source_path, 'r') as zip_ref:
            zip_ref.extractall(version_cache_dir)
        manifest["zip_sha256"] = calculate_file_sha256(source_path)
    
    elif artifact_type in ("dir", "source_pack", "schemas"):
        # Copy directory to cache
        print(f"Copying {artifact_type} from {source_path} to {version_cache_dir}")
        dest_dir = version_cache_dir / artifact_type
        shutil.copytree(source_path, dest_dir)
        manifest["source_dir"] = str(source_path.relative_to(submodule_path))
    
    else:  # fallback
        # Copy key directories
        for dir_name in ["schemas", "data", "artifacts", "source_pack", "templates", "examples"]:
            src_dir = submodule_path / dir_name
            if src_dir.exists():
                print(f"Copying {dir_name}/ to cache")
                dest_dir = version_cache_dir / dir_name
                shutil.copytree(src_dir, dest_dir)
    
    # Calculate directory hash
    manifest["directory_sha256"] = calculate_directory_sha256(version_cache_dir)
    
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
    project_root = get_project_root()
    full_submodule_path = project_root / submodule_path
    cache_path = Path(os.path.expanduser(cache_dir))
    
    # Helper for conditional output (stderr if quiet, stdout otherwise)
    def log(msg: str):
        if not quiet:
            print(msg)
        else:
            print(msg, file=sys.stderr)
    
    log(f"=" * 60)
    log(f"AIMO Standard Sync")
    log(f"=" * 60)
    log(f"Version: {version}")
    log(f"Submodule: {submodule_path}")
    log(f"Cache: {cache_dir}")
    log(f"=" * 60)
    
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
    
    log(f"=" * 60)
    log(f"Sync completed successfully!")
    log(f"=" * 60)
    log(f"Version:       {manifest['version']}")
    log(f"Commit:        {manifest['commit'][:12]}")
    log(f"Tag:           {manifest['tag']}")
    log(f"Cache Dir:     {manifest['cache_dir']}")
    log(f"File Count:    {manifest['file_count']}")
    log(f"Directory SHA256: {manifest['directory_sha256'][:16]}...")
    log(f"=" * 60)
    
    return manifest


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
    
    return manifest


if __name__ == "__main__":
    main()
