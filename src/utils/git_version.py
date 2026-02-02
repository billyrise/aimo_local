"""
Git version utility for AIMO Analysis Engine.

Gets git commit hash for code_version tracking.
"""

import subprocess
import os
from pathlib import Path
from typing import Optional


def get_code_version(repo_root: Optional[Path] = None) -> str:
    """
    Get git commit hash for code version tracking.
    
    Args:
        repo_root: Repository root path (default: current working directory)
        
    Returns:
        Git commit hash (short, 7 chars) or "unknown" if git is not available
    """
    if repo_root is None:
        repo_root = Path.cwd()
    
    repo_root = Path(repo_root).resolve()
    
    # Check if .git directory exists
    git_dir = repo_root / ".git"
    if not git_dir.exists():
        return "unknown"
    
    try:
        # Try to get git commit hash
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            # Return short hash (first 7 chars)
            return result.stdout.strip()[:7]
        else:
            return "unknown"
    except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
        # Git not available or command failed
        return "unknown"
