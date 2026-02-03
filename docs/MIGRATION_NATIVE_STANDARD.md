# Migration to AIMO Standard v0.1.1 Native Integration

This document describes the migration path from legacy 7-code taxonomy to AIMO Standard v0.1.1 native 8-dimension taxonomy.

## Overview

### Before Migration (Legacy)
- **Taxonomy**: 7 codes (`fs_uc_code`, `dt_code`, `ch_code`, `im_code`, `rs_code`, `ob_code`, `ev_code`)
- **Format**: Single values per dimension
- **Standard**: Not version-locked

### After Migration (Native Standard v0.1.1)
- **Taxonomy**: 8 dimensions (`FS`, `IM`, `UC`, `DT`, `CH`, `RS`, **`LG`** (Log/Event Type), `OB`). **EV** is reserved for Evidence artifact IDs (EP-01..EP-07).
- **Format**: Arrays for UC/DT/CH/RS/LG/OB (cardinality 1+ or 0+)
- **Standard**: Locked to AIMO Standard v0.1.1 with artifact SHA verification
- **Evidence Bundle root structure (v0.1)**: Root must have `manifest.json`, `objects/`, `payloads/`, `signatures/`, `hashes/`. Payloads (run_manifest, evidence_pack_manifest, logs, analysis, dictionary.json, summary.json, change_log.json) live under `payloads/`.

## Schema Changes

### New Columns (added)
| Table | Column | Type | Description |
|-------|--------|------|-------------|
| `runs` | `aimo_standard_version` | VARCHAR | Standard version (e.g., "0.1.1") |
| `runs` | `aimo_standard_commit` | VARCHAR | Git commit hash |
| `runs` | `aimo_standard_artifacts_dir_sha256` | VARCHAR | Artifacts SHA |
| `analysis_cache` | `fs_code` | VARCHAR | Functional Scope (single) |
| `analysis_cache` | `uc_codes_json` | VARCHAR | Use Case Class (JSON array) |
| `analysis_cache` | `dt_codes_json` | VARCHAR | Data Type (JSON array) |
| `analysis_cache` | `ch_codes_json` | VARCHAR | Channel (JSON array) |
| `analysis_cache` | `rs_codes_json` | VARCHAR | Risk Surface (JSON array) |
| `analysis_cache` | `ev_codes_json` / `lg_codes_json` | VARCHAR | Log/Event Type (LG) — JSON array; legacy column name `ev_codes_json` retained for compatibility |
| `analysis_cache` | `ob_codes_json` | VARCHAR | Outcome/Benefit (JSON array) |
| `analysis_cache` | `taxonomy_schema_version` | VARCHAR | Schema version |

### Legacy Columns (deprecated, retained for compatibility)
| Column | Status | Migration |
|--------|--------|-----------|
| `fs_uc_code` | DEPRECATED | Set to "DEPRECATED" for new records |
| `dt_code` | DEPRECATED | Populated with first element of `dt_codes_json` |
| `ch_code` | DEPRECATED | Populated with first element of `ch_codes_json` |
| `rs_code` | DEPRECATED | Populated with first element of `rs_codes_json` |
| `ob_code` | DEPRECATED | Populated with first element of `ob_codes_json` |
| `ev_code` | DEPRECATED | Populated with first element of `ev_codes_json` |

## Backward Compatibility

### Reading Records

The compatibility layer (`src/db/compat.py`) normalizes records on read:

```python
from db.compat import normalize_taxonomy_record, TaxonomyRecord

# Normalize a DB row (handles both new and legacy format)
record = normalize_taxonomy_record(row_dict)

# Access normalized values
print(record.fs_code)       # Single value
print(record.uc_codes)      # List of codes
print(record.needs_review)  # True if legacy fallback was used
```

**Priority Order:**
1. New columns (`uc_codes_json`, etc.) if non-empty → Use as-is
2. Legacy columns (`dt_code`, etc.) → Convert to single-element array
3. Mark record with `needs_review=True` if legacy fallback used

### Writing Records

New records are always written in new format:
- Array dimensions stored as canonical JSON (`["UC-001","UC-002"]`)
- Legacy columns populated for backward compatibility
- `fs_uc_code` set to "DEPRECATED" for new records

```python
from utils.json_canonical import classification_to_db_record

# Convert classification to DB format
db_record = classification_to_db_record(classification, "0.1.1")
```

## Evidence Bundle Output

All Evidence Bundle outputs use the new 8-dimension format:

- `evidence_pack_manifest.json`: Codes in Standard format
- `analysis/taxonomy_assignments.json`: 8-dimension codes
- `logs/shadow_ai_discovery.jsonl`: Standard schema

Evidence Bundle root (v0.1): `manifest.json`, `objects/`, `payloads/`, `signatures/`, `hashes/`. All run_manifest, evidence_pack_manifest, logs, analysis, dictionary.json, summary.json, change_log.json are under `payloads/`.

Legacy outputs are placed in `payloads/derived/`:
- `payloads/derived/evidence_pack_summary.json`: Legacy format (for reference)
- `payloads/derived/evidence_pack_summary.xlsx`: Legacy Excel format

## Migration Procedures

### Automatic Migration (on startup)

The DB client automatically applies migrations on startup:

```python
from db.duckdb_client import DuckDBClient

# Migrations applied automatically
client = DuckDBClient("data/aimo.duckdb")
```

Migrations are idempotent (safe to run multiple times).

### Re-analyzing Existing Records

Records with legacy-only data can be re-analyzed:

```sql
-- Find records needing re-analysis
SELECT url_signature 
FROM analysis_cache 
WHERE uc_codes_json = '[]' 
  AND fs_uc_code IS NOT NULL 
  AND fs_uc_code != 'DEPRECATED';

-- Mark for re-analysis
UPDATE analysis_cache 
SET status = 'needs_review'
WHERE uc_codes_json = '[]' 
  AND fs_uc_code IS NOT NULL;
```

Then run a new analysis pass to re-classify these signatures.

### Full Re-run

For a complete refresh with new taxonomy:

```bash
# Create new run with same input
python -m src.main \
  --input-dir data/input/your_logs \
  --force-rerun \
  --aimo-standard-version 0.1.1
```

This creates a new run (new `run_id`) with fresh classifications.

## Deprecation Timeline

| Phase | Status | Actions |
|-------|--------|---------|
| Phase 1 | **Current** | New columns added, legacy retained |
| Phase 2 | Planned | Stop writing to legacy columns |
| Phase 3 | Planned | Remove legacy columns from schema |

**Note:** Phase 2 and 3 will be announced with migration notice.

## Validation

### Checking Migration Status

```python
from db.compat import get_migration_status

status = get_migration_status(row_dict)
print(status)
# {
#     "has_new_format": True,
#     "has_legacy_only": False,
#     "needs_migration": False,
#     "missing_dimensions": []
# }
```

### CI Validation

The CI pipeline validates:
- Standard submodule is present and at v0.1.1
- Artifacts sync successfully
- Evidence Bundle passes validation

See `.github/workflows/ci.yml` for details.

## Troubleshooting

### "Missing dimension" errors

If a record is missing required dimensions (UC, DT, etc.):
1. Check if legacy columns have data
2. If yes, the compat layer will use fallback (but mark `needs_review`)
3. Re-run classification to populate new columns

### Bundle validation fails

If Evidence Bundle validation fails:
1. Check `validation_result.json` for specific errors
2. Common issues:
   - Empty required arrays (UC/DT/CH/RS/EV must have 1+ codes)
   - Invalid code format (must be XX-NNN)
3. Re-classify affected signatures

### Cache coherence issues

If results differ between runs:
1. Check `run_manifest.json` for `aimo_standard` section
2. Verify same Standard version and artifacts SHA
3. Different Standard versions = different `run_key` = no cache reuse

## References

- [AIMO Standard v0.1.1](../third_party/aimo-standard/)
- [Taxonomy Dictionary](../third_party/aimo-standard/artifacts/taxonomy/)
- [Evidence Bundle Schema](../third_party/aimo-standard/schemas/jsonschema/evidence_pack_manifest.schema.json)
