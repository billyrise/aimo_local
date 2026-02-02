"""
AIMO Standard Adapter

This module provides integration with the AIMO Standard specification.
The Standard is managed as a git submodule and versioned per-run for
audit reproducibility.
"""

from .constants import (
    AIMO_STANDARD_VERSION_DEFAULT,
    AIMO_STANDARD_SUBMODULE_PATH,
    AIMO_STANDARD_CACHE_DIR_DEFAULT,
)

__all__ = [
    "AIMO_STANDARD_VERSION_DEFAULT",
    "AIMO_STANDARD_SUBMODULE_PATH",
    "AIMO_STANDARD_CACHE_DIR_DEFAULT",
]
