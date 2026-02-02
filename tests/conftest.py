"""
AIMO Test Configuration and Fixtures

テスト規約（Phase 6 固定）:
============================================================

1. DB分離（必須）:
   - すべてのDBテストは tmp_path を使用してテストごとに分離
   - temp_directory も tmp_path 配下に作成
   - 例: db_path = tmp_path / "test.duckdb"
         temp_dir = tmp_path / "duckdb_tmp"

2. ユニークキー（必須）:
   - テスト内で生成する url_signature, run_id 等は uuid4 で一意化
   - 例: url_sig = f"sig_{uuid.uuid4().hex[:8]}"

3. 明示的なflush/close（必須）:
   - DuckDBへの書込み後は flush() を呼ぶ
   - 読み込み前に close() + 再オープン を推奨

4. 不安定テスト禁止:
   - "再実行して通る" は禁止
   - 不安定なテストは即座に修正対象

これらを守らない新規テストはレビューで差し戻す。
============================================================
"""

import pytest
import os
import sys
from pathlib import Path
from typing import Generator
import tempfile
import uuid


# =============================================================================
# Path Setup
# =============================================================================

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


# =============================================================================
# Shared Fixtures
# =============================================================================

@pytest.fixture
def isolated_db_path(tmp_path: Path) -> Path:
    """
    Provide an isolated database path for testing.
    
    Usage:
        def test_something(isolated_db_path):
            db = DuckDBClient(str(isolated_db_path))
            ...
    """
    return tmp_path / f"test_{uuid.uuid4().hex[:8]}.duckdb"


@pytest.fixture
def isolated_temp_directory(tmp_path: Path) -> Path:
    """
    Provide an isolated temp directory for DuckDB.
    
    Usage:
        def test_something(isolated_db_path, isolated_temp_directory):
            db = DuckDBClient(str(isolated_db_path), temp_directory=str(isolated_temp_directory))
            ...
    """
    temp_dir = tmp_path / f"duckdb_tmp_{uuid.uuid4().hex[:8]}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


@pytest.fixture
def unique_run_id() -> str:
    """
    Provide a unique run_id for testing.
    
    Usage:
        def test_something(unique_run_id):
            run_id = unique_run_id
            ...
    """
    return f"test_run_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_url_signature() -> str:
    """
    Provide a unique url_signature for testing.
    
    Usage:
        def test_something(unique_url_signature):
            sig = unique_url_signature
            ...
    """
    return f"sig_{uuid.uuid4().hex}"


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def sample_logs_dir(project_root: Path) -> Path:
    """Return the sample_logs directory."""
    return project_root / "sample_logs"


@pytest.fixture
def schemas_dir(project_root: Path) -> Path:
    """Return the schemas directory."""
    return project_root / "schemas"


# =============================================================================
# Test Markers
# =============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end tests"
    )
    config.addinivalue_line(
        "markers", "llm: marks tests that require LLM API access"
    )


# =============================================================================
# Hooks
# =============================================================================

def pytest_collection_modifyitems(config, items):
    """
    Automatically mark tests based on their names/paths.
    """
    for item in items:
        # Mark tests in test_e2e_*.py as e2e
        if "test_e2e" in item.nodeid:
            item.add_marker(pytest.mark.e2e)
        
        # Mark tests with 'llm' in name as llm
        if "llm" in item.name.lower():
            item.add_marker(pytest.mark.llm)
