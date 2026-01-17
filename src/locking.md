# File Locking Strategy

## Purpose

AIMO uses file-based locking to prevent concurrent runs that could corrupt:
- DuckDB database
- Parquet files
- Cache state

## Implementation

### Recommended Library

Use `filelock` for cross-platform compatibility:

```python
from filelock import FileLock, Timeout

lock = FileLock("data/cache/aimo.lock", timeout=60)

try:
    with lock:
        # Run processing
        orchestrator.run()
except Timeout:
    logger.error("Could not acquire lock - another process is running")
    sys.exit(1)
```

### Alternative: `portalocker`

If `filelock` has issues, `portalocker` is also reliable:

```python
import portalocker

with portalocker.Lock("data/cache/aimo.lock", timeout=60):
    orchestrator.run()
```

## Lock Scope

| Level | Scope | Use Case |
|-------|-------|----------|
| Global | Entire run | Default - prevents any concurrent processing |
| Vendor | Per-vendor | Future - parallel vendor processing |
| File | Per input file | Future - parallel file ingestion |

**Start with global lock** and refine if performance requires.

## Lock File Location

```
data/cache/aimo.lock
```

This is alongside the DuckDB database to ensure they're protected together.

## Stale Lock Detection

If a process crashes, the lock file may remain. Detection strategy:

```python
import os
import psutil

def is_lock_stale(lock_file: str, pid_file: str) -> bool:
    """Check if lock is held by a dead process."""
    if not os.path.exists(pid_file):
        return True
    
    with open(pid_file) as f:
        pid = int(f.read().strip())
    
    return not psutil.pid_exists(pid)
```

### Recovery Procedure

1. Check if lock file exists
2. Read PID from companion `.pid` file
3. If process is dead, remove lock
4. Log warning about stale lock recovery

## Writer Queue Lock

For LLM Worker → DB writes, use a separate lock:

```python
write_lock = threading.Lock()

def write_to_db(batch):
    with write_lock:
        db.upsert(batch)
```

This allows parallel LLM calls while serializing database writes.

## Runbook Reference

See `ops/runbook.md` for lock-related troubleshooting.
