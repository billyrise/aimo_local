#!/bin/bash
# AIMO Analysis Engine - Production Wrapper Script
# 
# This script is called by launchd and handles:
# - Process-level locking (prevents duplicate execution)
# - Input directory validation
# - Logging setup
# - Artifact organization
#
# Usage: run_aimo.sh [--input-dir <dir>] [--vendor <vendor>] [--dry-run]

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOCK_FILE="${PROJECT_ROOT}/data/cache/aimo.lock"
PID_FILE="${PROJECT_ROOT}/data/cache/aimo.pid"
LOG_DIR="${PROJECT_ROOT}/logs"
INPUT_DIR="${PROJECT_ROOT}/data/input"
OUTPUT_DIR="${PROJECT_ROOT}/data/output"
STABLE_WAIT_SECONDS=60  # Wait for file stability (Box sync)

# Parse arguments
DRY_RUN=false
VENDOR="paloalto"
INPUT_DIR_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --input-dir)
            INPUT_DIR_OVERRIDE="$2"
            shift 2
            ;;
        --vendor)
            VENDOR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ -n "$INPUT_DIR_OVERRIDE" ]]; then
    INPUT_DIR="$INPUT_DIR_OVERRIDE"
fi

# Ensure directories exist
mkdir -p "${LOG_DIR}"
mkdir -p "${PROJECT_ROOT}/data/cache"
mkdir -p "${OUTPUT_DIR}"

# Log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/run_${TIMESTAMP}.log"
ERROR_LOG="${LOG_DIR}/run_${TIMESTAMP}.err"

# Redirect stdout/stderr to log files
exec 1> >(tee -a "${LOG_FILE}")
exec 2> >(tee -a "${ERROR_LOG}" >&2)

echo "=== AIMO Analysis Engine Run ==="
echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "Project root: ${PROJECT_ROOT}"
echo "Input directory: ${INPUT_DIR}"
echo "Vendor: ${VENDOR}"
echo ""

# Check for stale lock
if [[ -f "${LOCK_FILE}" ]]; then
    if [[ -f "${PID_FILE}" ]]; then
        PID=$(cat "${PID_FILE}")
        if ps -p "${PID}" > /dev/null 2>&1; then
            echo "ERROR: Lock file exists and process ${PID} is running" >&2
            echo "       Another AIMO process is already running." >&2
            exit 1
        else
            echo "WARNING: Stale lock detected (PID ${PID} not running)" >&2
            echo "         Removing stale lock..." >&2
            rm -f "${LOCK_FILE}" "${PID_FILE}"
        fi
    else
        echo "WARNING: Lock file exists without PID file" >&2
        echo "         Removing stale lock..." >&2
        rm -f "${LOCK_FILE}"
    fi
fi

# Acquire lock (using fcntl via Python for cross-platform compatibility)
# We'll use a Python helper to acquire the lock atomically
LOCK_ACQUIRED=false
python3 << EOF
import fcntl
import os
import sys
import time

lock_file_path = "${LOCK_FILE}"
pid_file_path = "${PID_FILE}"

try:
    # Open lock file in append mode (create if not exists)
    lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND)
    
    # Try to acquire exclusive lock (non-blocking)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        # Write current PID
        with open(pid_file_path, 'w') as f:
            f.write(str(os.getpid()))
        
        # Keep lock file open (lock is held while file descriptor is open)
        # We'll close it in the cleanup trap
        sys.exit(0)
    except IOError:
        # Lock is held by another process
        os.close(lock_fd)
        sys.exit(1)
except Exception as e:
    print(f"ERROR: Failed to acquire lock: {e}", file=sys.stderr)
    sys.exit(1)
EOF

if [[ $? -ne 0 ]]; then
    echo "ERROR: Could not acquire lock - another process may be running" >&2
    exit 1
fi

LOCK_ACQUIRED=true

# Cleanup function
cleanup() {
    if [[ "$LOCK_ACQUIRED" == "true" ]]; then
        rm -f "${LOCK_FILE}" "${PID_FILE}"
        echo "Lock released"
    fi
}

# Register cleanup on exit
trap cleanup EXIT

echo "Lock acquired (PID: $$)"
echo ""

# Check input directory
if [[ ! -d "${INPUT_DIR}" ]]; then
    echo "ERROR: Input directory does not exist: ${INPUT_DIR}" >&2
    exit 1
fi

# Find input files (wait for stability if needed)
INPUT_FILES=()
if [[ -d "${INPUT_DIR}" ]]; then
    # List CSV/log files
    while IFS= read -r -d '' file; do
        INPUT_FILES+=("$file")
    done < <(find "${INPUT_DIR}" -type f \( -name "*.csv" -o -name "*.log" -o -name "*.txt" \) -print0 2>/dev/null || true)
fi

if [[ ${#INPUT_FILES[@]} -eq 0 ]]; then
    echo "INFO: No input files found in ${INPUT_DIR}"
    echo "      Skipping execution (this is normal if no new logs are available)"
    exit 0
fi

echo "Found ${#INPUT_FILES[@]} input file(s):"
for file in "${INPUT_FILES[@]}"; do
    echo "  - ${file}"
done
echo ""

# Wait for file stability (Box sync)
if [[ "$DRY_RUN" != "true" ]]; then
    echo "Checking file stability (waiting up to ${STABLE_WAIT_SECONDS}s for Box sync)..."
    for file in "${INPUT_FILES[@]}"; do
        if [[ ! -f "$file" ]]; then
            continue
        fi
        
        initial_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "0")
        initial_mtime=$(stat -f%m "$file" 2>/dev/null || stat -c%Y "$file" 2>/dev/null || echo "0")
        
        stable_count=0
        for i in $(seq 1 ${STABLE_WAIT_SECONDS}); do
            sleep 1
            current_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "0")
            current_mtime=$(stat -f%m "$file" 2>/dev/null || stat -c%Y "$file" 2>/dev/null || echo "0")
            
            if [[ "$current_size" == "$initial_size" ]] && [[ "$current_mtime" == "$initial_mtime" ]]; then
                stable_count=$((stable_count + 1))
                if [[ $stable_count -ge 5 ]]; then
                    echo "  ✓ ${file} is stable"
                    break
                fi
            else
                stable_count=0
                initial_size="$current_size"
                initial_mtime="$current_mtime"
            fi
            
            if [[ $i -eq ${STABLE_WAIT_SECONDS} ]]; then
                echo "  WARNING: ${file} may still be syncing (proceeding anyway)"
            fi
        done
    done
    echo ""
fi

# Change to project root
cd "${PROJECT_ROOT}"

# Run AIMO for each input file
EXIT_CODE=0
for input_file in "${INPUT_FILES[@]}"; do
    echo "Processing: ${input_file}"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would execute: python3 -m src.main \"${input_file}\" --vendor \"${VENDOR}\""
        continue
    fi
    
    # Run main.py
    if python3 -m src.main "${input_file}" --vendor "${VENDOR}" --db-path "${PROJECT_ROOT}/data/cache/aimo.duckdb" --output-dir "${OUTPUT_DIR}"; then
        echo "  ✓ Successfully processed: ${input_file}"
    else
        echo "  ✗ Failed to process: ${input_file}" >&2
        EXIT_CODE=1
    fi
    echo ""
done

# Organize artifacts
if [[ "$DRY_RUN" != "true" ]]; then
    echo "Organizing artifacts..."
    
    # Move reports to timestamped directory (optional)
    # mkdir -p "${OUTPUT_DIR}/${TIMESTAMP}"
    # mv "${OUTPUT_DIR}"/run_*.json "${OUTPUT_DIR}/${TIMESTAMP}/" 2>/dev/null || true
    
    echo "  Reports: ${OUTPUT_DIR}"
    echo "  Logs: ${LOG_DIR}"
fi

echo ""
echo "=== Run Complete ==="
echo "Exit code: ${EXIT_CODE}"

exit ${EXIT_CODE}
