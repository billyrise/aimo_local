## Summary

<!-- Brief description of the changes (1-3 bullet points) -->

-

## AIMO Standard v0.1.7 Compliance Checklist

<!-- All items must be checked before merge -->

### Required

- [ ] **Standard version fixed**: All code references `aimo_standard_version = "0.1.7"` (no `latest` or dynamic versions)
- [ ] **Submodule intact**: `third_party/aimo-standard` points to v0.1.7 tag/commit
- [ ] **Artifacts sync**: `python scripts/sync_aimo_standard.py --version 0.1.7` succeeds
- [ ] **Tests pass**: `pytest -q` passes (all tests green)

### Evidence Bundle (if applicable)

- [ ] **Bundle validator pass**: Generated Evidence Bundle passes `validator_runner.run_validation()`
- [ ] **run_manifest includes SHA**: `run_manifest.json` contains:
  - `aimo_standard.version`: "0.1.7"
  - `aimo_standard.artifacts_dir_sha256`: (non-empty)
- [ ] **8-dimension taxonomy**: All taxonomy assignments use 8-dimension format (FS/IM/UC/DT/CH/RS/EV/OB)

### Code Quality

- [ ] **ruff check**: `ruff check .` passes (no errors)
- [ ] **No hardcoded taxonomy**: Taxonomy codes loaded from Standard artifacts, not hardcoded
- [ ] **Backward compatibility**: Legacy fields populated for DB compatibility (if modifying classification)

## Test Plan

<!-- How to verify the changes work correctly -->

1.
2.
3.

## Breaking Changes

<!-- List any breaking changes, or write "None" -->

None

## Related Issues

<!-- Link related issues: Fixes #123, Closes #456 -->

