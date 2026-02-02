#!/usr/bin/env bash
set -euo pipefail

# ========== Config ==========
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="$REPO_ROOT/ops/logs"
STATE_DIR="$REPO_ROOT/ops/state"
LOCK_FILE="$STATE_DIR/aimo.engine.lock"
PID_FILE="$STATE_DIR/aimo.engine.pid"

# 入力（Box同期先など）を運用で変更できるようにする
INPUT_FILE="${1:-$REPO_ROOT/sample_logs/paloalto_sample.csv}"
VENDOR="${2:-paloalto}"

# ========== Prepare ==========
mkdir -p "$LOG_DIR" "$STATE_DIR"

ts="$(date '+%Y%m%d_%H%M%S')"
OUT_LOG="$LOG_DIR/run_${ts}.out.log"
ERR_LOG="$LOG_DIR/run_${ts}.err.log"

cd "$REPO_ROOT"

# ========== Lock (no double-run) ==========
# macOS標準の flock は環境差があるため、簡易ロックとして mkdir を利用（原子的）
# 既にlock dirがあれば起動中とみなす
LOCK_DIR="${LOCK_FILE}.d"
if mkdir "$LOCK_DIR" 2>/dev/null; then
  echo $$ > "$PID_FILE"
  trap 'rm -rf "$LOCK_DIR"; rm -f "$PID_FILE"' EXIT
else
  echo "[SKIP] Another run appears active. lock=$LOCK_DIR" >> "$ERR_LOG"
  exit 0
fi

# ========== Run ==========
# 重要: キー類は echo しない。環境は main.py の .env.local ロードに任せる
{
  echo "=== AIMO run started: $(date -Iseconds) ==="
  echo "repo_root=$REPO_ROOT"
  echo "input_file=$INPUT_FILE"
  echo "vendor=$VENDOR"
  echo "python=$(python3 --version)"
  echo "pwd=$(pwd)"
} >> "$OUT_LOG"

# 実行
# 既存の main.py が .env.local を優先ロードする想定
# ここではあえて export はしない（運用で必要ならlaunchd側に入れる）
python3 src/main.py "$INPUT_FILE" --vendor "$VENDOR" >> "$OUT_LOG" 2>> "$ERR_LOG" || {
  echo "[ERROR] main.py returned non-zero: $?" >> "$ERR_LOG"
  exit 1
}

echo "=== AIMO run finished: $(date -Iseconds) ===" >> "$OUT_LOG"
