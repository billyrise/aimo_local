"""
AIMO Analysis Engine - Database Migrations

Handles schema migrations for DuckDB.
DuckDB has limited ALTER TABLE support, so migrations use:
- Add new columns (safe)
- Keep deprecated columns for backward compatibility
- Idempotent design: check if column exists before adding

Usage:
    from db.migrations import apply_migrations
    apply_migrations(connection)
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# Migration definitions
# Each migration is a dict with:
#   - id: Unique migration identifier
#   - description: Human-readable description
#   - check_sql: SQL to check if migration is needed (returns 0 if needed, >0 if already done)
#   - apply_sql: List of SQL statements to apply

MIGRATIONS = [
    # Migration 1: Add AIMO Standard versioning to runs (v1.5)
    {
        "id": "001_runs_aimo_standard_version",
        "description": "Add AIMO Standard versioning columns to runs table",
        "check_sql": """
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = 'runs' AND column_name = 'aimo_standard_version'
        """,
        "apply_sql": [
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS aimo_standard_version VARCHAR",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS aimo_standard_commit VARCHAR",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS aimo_standard_artifacts_dir_sha256 VARCHAR",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS aimo_standard_artifacts_zip_sha256 VARCHAR",
        ]
    },
    
    # Migration 2: Add 8-dimension taxonomy columns to analysis_cache (v1.6)
    {
        "id": "002_analysis_cache_8dim_taxonomy",
        "description": "Add 8-dimension taxonomy array columns to analysis_cache",
        "check_sql": """
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = 'analysis_cache' AND column_name = 'fs_code'
        """,
        "apply_sql": [
            # Single-value dimensions
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS fs_code VARCHAR",
            # im_code may already exist, add if not
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS im_code VARCHAR",
            # Array dimensions (JSON strings)
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS uc_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS dt_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ch_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS rs_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ev_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ob_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS taxonomy_schema_version VARCHAR",
        ]
    },
    
    # Migration 3: Add 8-dimension taxonomy columns to signature_stats (v1.6)
    {
        "id": "003_signature_stats_8dim_taxonomy",
        "description": "Add 8-dimension taxonomy array columns to signature_stats",
        "check_sql": """
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = 'signature_stats' AND column_name = 'fs_code'
        """,
        "apply_sql": [
            # Single-value dimensions
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS fs_code VARCHAR",
            # im_code may already exist, add if not
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS im_code VARCHAR",
            # Array dimensions (JSON strings)
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS uc_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS dt_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS ch_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS rs_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS ev_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS ob_codes_json VARCHAR DEFAULT '[]'",
            "ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS taxonomy_schema_version VARCHAR",
        ]
    },
    
    # Migration 4: Migrate legacy fs_uc_code to fs_code where possible
    {
        "id": "004_migrate_fs_uc_code_to_fs_code",
        "description": "Migrate legacy fs_uc_code to fs_code in analysis_cache",
        "check_sql": """
            SELECT COUNT(*) FROM analysis_cache 
            WHERE fs_uc_code IS NOT NULL AND fs_uc_code != '' 
            AND (fs_code IS NULL OR fs_code = '')
        """,
        "apply_sql": [
            # Only migrate if fs_uc_code looks like an FS code (starts with FS-)
            """
            UPDATE analysis_cache 
            SET fs_code = fs_uc_code,
                status = CASE 
                    WHEN status = 'active' AND is_human_verified = FALSE 
                    THEN 'needs_review' 
                    ELSE status 
                END
            WHERE fs_uc_code IS NOT NULL 
            AND fs_uc_code LIKE 'FS-%'
            AND (fs_code IS NULL OR fs_code = '')
            """,
        ]
    },
]


def check_migration_needed(conn, migration: dict) -> bool:
    """
    Check if a migration is needed.
    
    Args:
        conn: DuckDB connection
        migration: Migration definition dict
    
    Returns:
        True if migration is needed, False if already applied
    """
    check_sql = migration.get("check_sql")
    if not check_sql:
        return True  # No check, always apply
    
    try:
        result = conn.execute(check_sql).fetchone()
        count = result[0] if result else 0
        # If check returns 0, migration is needed
        return count == 0
    except Exception as e:
        logger.warning(f"Migration check failed for {migration['id']}: {e}")
        return True  # Assume needed if check fails


def apply_migration(conn, migration: dict) -> bool:
    """
    Apply a single migration.
    
    Args:
        conn: DuckDB connection
        migration: Migration definition dict
    
    Returns:
        True if applied successfully, False otherwise
    """
    migration_id = migration["id"]
    description = migration.get("description", "")
    apply_sql = migration.get("apply_sql", [])
    
    if not apply_sql:
        logger.warning(f"Migration {migration_id} has no apply_sql, skipping")
        return True
    
    try:
        for sql in apply_sql:
            sql = sql.strip()
            if sql:
                conn.execute(sql)
        
        logger.info(f"Applied migration {migration_id}: {description}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to apply migration {migration_id}: {e}")
        return False


def apply_migrations(conn, migrations: Optional[list] = None) -> dict:
    """
    Apply all pending migrations.
    
    Args:
        conn: DuckDB connection
        migrations: List of migrations to check/apply (default: MIGRATIONS)
    
    Returns:
        dict with 'applied', 'skipped', 'failed' counts
    """
    if migrations is None:
        migrations = MIGRATIONS
    
    result = {
        "applied": 0,
        "skipped": 0,
        "failed": 0,
        "details": []
    }
    
    for migration in migrations:
        migration_id = migration["id"]
        
        if check_migration_needed(conn, migration):
            if apply_migration(conn, migration):
                result["applied"] += 1
                result["details"].append({"id": migration_id, "status": "applied"})
            else:
                result["failed"] += 1
                result["details"].append({"id": migration_id, "status": "failed"})
        else:
            result["skipped"] += 1
            result["details"].append({"id": migration_id, "status": "skipped"})
    
    if result["applied"] > 0:
        logger.info(f"Migrations complete: {result['applied']} applied, {result['skipped']} skipped")
    
    return result


def get_schema_version(conn) -> str:
    """
    Get the current schema version based on which migrations have been applied.
    
    Returns:
        Version string (e.g., "1.6")
    """
    # Check for v1.6 features (8-dimension taxonomy)
    try:
        result = conn.execute("""
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = 'analysis_cache' AND column_name = 'fs_code'
        """).fetchone()
        if result and result[0] > 0:
            return "1.6"
    except Exception:
        pass
    
    # Check for v1.5 features (AIMO Standard versioning)
    try:
        result = conn.execute("""
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = 'runs' AND column_name = 'aimo_standard_version'
        """).fetchone()
        if result and result[0] > 0:
            return "1.5"
    except Exception:
        pass
    
    # Default to v1.4
    return "1.4"
