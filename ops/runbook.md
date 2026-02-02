# AIMO Operations Runbook

## Overview

This runbook provides procedures for operating and troubleshooting the AIMO Analysis Engine.

---

## Launchd Setup (Phase 6)

### Initial Setup

1. **Configure environment variables**:
   - Create `.env.local` in the repository root (if not exists)
   - Add `GEMINI_API_KEY=your_gemini_api_key_here` to `.env.local`
   - **Important**: `.env.local` is automatically loaded by `src/main.py` at startup
   - The wrapper script (`ops/bin/run_aimo.sh`) relies on this automatic loading

2. **Make wrapper script executable**:
   ```bash
   chmod +x ops/bin/run_aimo.sh
   ```

3. **Update plist with absolute paths**:
   - Edit `ops/launchd/aimo.engine.plist`
   - Replace all `/ABSOLUTE/PATH/TO/REPO` with actual repository path
   - Example: `/Volumes/JetDrive256/AIMO_Engine_local`
   - **Note**: Environment variables (e.g., `GEMINI_API_KEY`) are loaded from `.env.local` automatically.
     If you need to override via plist, uncomment the `EnvironmentVariables` section in the plist.

4. **Install launchd plist**:
   ```bash
   # For user-level (recommended)
   cp ops/launchd/aimo.engine.plist ~/Library/LaunchAgents/
   
   # Or for system-level (requires root)
   # sudo cp ops/launchd/aimo.engine.plist /Library/LaunchDaemons/
   ```

5. **Load and start**:
   ```bash
   launchctl load ~/Library/LaunchAgents/aimo.engine.plist
   launchctl start com.aimo.analysis.engine
   ```

6. **Verify status**:
   ```bash
   launchctl list | grep aimo
   ```

### Environment Variables Handling

**Default behavior (recommended)**:
- `src/main.py` automatically loads `.env.local` from the repository root at startup
- The wrapper script (`ops/bin/run_aimo.sh`) does not export environment variables
- All API keys should be stored in `.env.local` (which is gitignored)

**Alternative (plist-based)**:
- If you prefer to set environment variables in the plist file:
  1. Uncomment the `EnvironmentVariables` section in `ops/launchd/aimo.engine.plist`
  2. Set `GEMINI_API_KEY` (or `OPENAI_API_KEY` for fallback) in the plist
  3. **Security note**: Never commit plist files with actual API keys
  4. Consider using macOS Keychain or a secure secrets manager instead

### Management Commands

```bash
# Check status
launchctl list | grep aimo

# View logs
tail -f ops/logs/launchd.out.log
tail -f ops/logs/launchd.err.log
tail -f ops/logs/run_*.out.log

# Stop service
launchctl stop com.aimo.analysis.engine

# Unload service
launchctl unload ~/Library/LaunchAgents/aimo.engine.plist

# Reload after plist changes
launchctl unload ~/Library/LaunchAgents/aimo.engine.plist
launchctl load ~/Library/LaunchAgents/aimo.engine.plist
```

### Manual Run (Testing)

```bash
# Test wrapper script directly
ops/bin/run_aimo.sh sample_logs/paloalto_sample.csv paloalto

# Check logs
ls -la ops/logs/
cat ops/logs/run_*.out.log
cat ops/logs/run_*.err.log

# Verify no double-run (lock should prevent)
# Run twice quickly - second should skip
ops/bin/run_aimo.sh sample_logs/paloalto_sample.csv paloalto &
ops/bin/run_aimo.sh sample_logs/paloalto_sample.csv paloalto
# Second run should exit with "[SKIP] Another run appears active"
```

### Log Locations

- **Wrapper script logs**: `ops/logs/run_YYYYMMDD_HHMMSS.out.log` / `.err.log`
- **Launchd logs**: `ops/logs/launchd.out.log` / `launchd.err.log`
- **Lock/PID files**: `ops/state/aimo.engine.lock.d` / `aimo.engine.pid`

### Troubleshooting Launchd

**Issue**: Service not starting
- Check plist syntax: `plutil -lint ~/Library/LaunchAgents/aimo.engine.plist`
- Check launchd logs: `log show --predicate 'process == "launchd"' --last 5m`

**Issue**: Double-run detected
- Check lock directory: `ls -la ops/state/`
- If stale lock exists: `rm -rf ops/state/aimo.engine.lock.d`

**Issue**: Permission denied
- Ensure script is executable: `chmod +x ops/bin/run_aimo.sh`
- Check file ownership: `ls -la ops/bin/run_aimo.sh`

---

## Quick Reference

| Issue | Section |
|-------|---------|
| Run stuck / lock not released | [Lock Issues](#lock-issues) |
| LLM costs too high | [Cost Management](#cost-management) |
| Parse errors increasing | [Ingestion Issues](#ingestion-issues) |
| Cache hit rate dropping | [Cache Issues](#cache-issues) |
| Report generation failed | [Report Issues](#report-issues) |

---

## DuckDB Configuration

### temp_directory設定（必須・運用品質固定）

DuckDBはWALや.tmpディレクトリ等を作成するため、DBとtemp_directoryはローカルSSDの書込み可能領域に固定する。

**規約**:
- `temp_directory`はDBと同じディレクトリ配下に配置（デフォルト: `{db_pathの親ディレクトリ}/duckdb_tmp`）
- 起動時に必ず `SET temp_directory` を実行
- パスをログに出力（監査用：JSON形式）

**実装**:
- `src/db/duckdb_client.py`で自動設定
- ログ出力: `DuckDB initialized: {"db_path": "...", "temp_directory": "...", "note": "..."}`

**確認方法**:
```bash
# 実行ログで確認
grep "DuckDB initialized" <execution_log>
grep "DuckDB temp_directory" <execution_log>
```

### UPSERT仕様（恒久対策として固定）

DuckDB UPSERTは `INSERT ... ON CONFLICT DO UPDATE SET ...` を使用する。
`INSERT OR REPLACE` は禁止（DELETE→INSERT相当のため監査・来歴が破壊される）。

**仕様**:
- UPDATE句の右辺は必ず `EXCLUDED.<col>` を使用（直接値埋め込み禁止）
- 以下の列は強制除外:
  - conflict_cols（衝突ターゲット列）
  - PK列
  - indexed_columns（インデックス付き列：例 `status`, `usage_type`）
  - 許可リスト外の列（`TABLE_UPDATABLE_COLS`）
- 除外時はWARNログを出力
- 監査用にUPSERT情報をJSONログで記録（DEBUGレベル）

**許可リスト**:
- `RUNS_UPDATABLE_COLS`: runs テーブルで更新可能な列
- `SIGNATURE_STATS_UPDATABLE_COLS`: signature_stats テーブルで更新可能な列
- `ANALYSIS_CACHE_UPDATABLE_COLS`: analysis_cache テーブルで更新可能な列
- `INPUT_FILES_UPDATABLE_COLS`: input_files テーブルで更新可能な列

**インデックス列（更新禁止）**:
- `analysis_cache`: status, usage_type, updated_at, is_human_verified
- `runs`: status, started_at

**確認方法**:
```bash
# WARNログで除外列を確認
grep "UPSERT.*Excluded columns" <execution_log>

# DEBUGログで監査情報を確認
grep "UPSERT audit" <execution_log>
```

**根拠**:
- DuckDB公式: https://duckdb.org/docs/sql/statements/insert#on-conflict-clause
- DuckDBインデックス制限: UPDATEがDELETE+INSERTに変換されるケースがあり、インデックス/制約と組み合わせて制約違反になる可能性がある

---

## 受入テストゲート（Phase 6 固定）

### 概要

コード変更後の受入確認は以下のゲートを順番に通過すること。
失敗時は「再実行して通る」を禁止し、不安定テストは即座に修正対象とする。

### ゲート1: 全テスト実行（必須）

```bash
# 受入の唯一の合否判定コマンド
python3 -m pytest -q
```

**合格条件**: すべてのテストがPASS（skipped/warnings は許容）

**失敗時の対応**:
- 失敗したテストを特定: `python3 -m pytest --tb=short`
- 不安定テストは修正対象（再実行で通ることを期待してはならない）

### ゲート2: Phase 6 追加テストの単独実行

```bash
# Phase 6 で追加したテストを単独で確認
python3 -m pytest -q \
    tests/test_gemini_schema_sanitizer.py \
    tests/test_llm_rate_limit_policy.py \
    tests/test_llm_coverage_counts.py \
    tests/test_vendor_ingestion_smoke.py \
    tests/test_e2e_report_integrity.py
```

**合格条件**: すべてPASS
**これがPASSしない限り、E2Eテストに進まない。**

### ゲート3: E2E 成果物の整合性チェック

E2E実行後に、生成されたレポートの整合性を自動検証する。

```bash
# E2E実行（LLMなし）
python3 src/main.py sample_logs/paloalto/normal.csv --vendor paloalto

# レポート整合性テスト
python3 -m pytest tests/test_e2e_report_integrity.py::TestLatestReportIntegrity -v
```

**検証内容**:
- a) report_summary.schema.json に合致
- b) rule_hit + unknown_count == total_signatures
- c) llm_analyzed_count / needs_review_count / skipped_count の整合

**スタンドアロン検証（オプション）**:
```bash
# 特定のレポートファイルを直接検証
python3 tests/test_e2e_report_integrity.py data/output/<run_id>_summary.json
```

### ゲート4: 成果物の存在確認

E2E成功時に以下が生成されていることを確認:

```bash
# 実行ログの存在
ls -la ops/logs/run_*.out.log

# レポートJSONの存在
ls -la data/output/*_summary.json

# 整合性テストがPASSしていること（ゲート3で確認済み）
```

### 受入チェックリスト

| # | チェック項目 | コマンド/確認方法 |
|---|------------|-----------------|
| 1 | 全テストPASS | `python3 -m pytest -q` |
| 2 | Phase 6テストPASS | ゲート2のコマンド |
| 3 | E2Eレポート整合性 | `pytest tests/test_e2e_report_integrity.py` |
| 4 | 実行ログ出力 | `ls ops/logs/` |
| 5 | レポートJSON生成 | `ls data/output/*_summary.json` |

---

## テスト規約（Phase 6 固定）

### DB分離（必須）

すべてのDBテストは `tmp_path` を使用してテストごとに分離する。

```python
def test_something(tmp_path):
    db_path = tmp_path / "test.duckdb"
    temp_dir = tmp_path / "duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    db = DuckDBClient(str(db_path), temp_directory=str(temp_dir))
    ...
```

**共通フィクスチャ** (`tests/conftest.py`):
- `isolated_db_path`: 分離されたDBパス
- `isolated_temp_directory`: 分離されたtemp_directory
- `unique_run_id`: 一意なrun_id
- `unique_url_signature`: 一意なurl_signature

### ユニークキー（必須）

テスト内で生成する `url_signature`, `run_id` 等は `uuid4` で一意化する。

```python
import uuid

def test_upsert():
    url_sig = f"sig_{uuid.uuid4().hex[:8]}"
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    ...
```

### 明示的なflush/close（必須）

DuckDBへの書込み後は `flush()` を呼び、読込み前に `close()` + 再オープンを推奨。

```python
db_client.upsert("table", data)
db_client.flush()
db_client.close()

# 再オープンして読み込み
db_client = DuckDBClient(db_path, temp_directory=temp_dir)
result = db_client.get_reader().execute("SELECT ...").fetchall()
```

### 不安定テスト禁止

- 「再実行して通る」は禁止
- 不安定なテストは即座に修正対象
- これらを守らない新規テストはレビューで差し戻す

---

## Daily Operations

### Health Check

```bash
# Check last run status
python -m src.cli status --last

# View recent runs
python -m src.cli runs --limit 5

# Check cache statistics
python -m src.cli cache-stats
```

### Manual Run

```bash
# Standard run (LLM無し)
python src/main.py <input_file> --vendor <vendor>

# Standard run (LLM有り - Geminiデフォルト)
# .env.local に GEMINI_API_KEY を設定している場合、自動的に読み込まれる
python src/main.py <input_file> --vendor <vendor>

# または環境変数で明示的に指定
export GEMINI_API_KEY=<your_gemini_api_key>
python src/main.py <input_file> --vendor <vendor>

# 実行ログの確認
# - duckdb_path / temp_directory
# - rule_hit / unknown_count
# - llm_analyzed_count / needs_review_count / cache_hit_rate
# - thresholds_used / seed(run_id)
```

### E2E検証

```bash
# A) LLM無しE2E検証
python src/main.py sample_logs/paloalto_sample.csv --vendor paloalto

# B) LLM有りE2E検証（Geminiデフォルト）
# 方法1: .env.local を使用（推奨）
# .env.local に GEMINI_API_KEY=your_key を設定
python src/main.py sample_logs/paloalto_sample.csv --vendor paloalto

# 方法2: 環境変数で明示的に指定
export GEMINI_API_KEY=<your_gemini_api_key>
python src/main.py sample_logs/paloalto_sample.csv --vendor paloalto

# C) LLM有りE2E検証（OpenAI fallback）
# OpenAI を使用する場合は OPENAI_API_KEY を設定
export OPENAI_API_KEY=<your_openai_api_key>
python src/main.py sample_logs/paloalto_sample.csv --vendor paloalto

# レポートのバリデーション確認
python -m pytest tests/test_e2e_validation.py -v
```

**Note**: 
- **Gemini is the default LLM provider**. Set `GEMINI_API_KEY` in `.env.local` for automatic loading.
- OpenAI is available as a fallback option if `OPENAI_API_KEY` is set and `GEMINI_API_KEY` is not available.
- The `.env.local` file is automatically loaded by `src/main.py` at startup (see [Environment Variables Handling](#environment-variables-handling) above).

---

## Troubleshooting

### Lock Issues

**Symptom**: Run fails with "Could not acquire lock" or "[SKIP] Another run appears active"

**Diagnosis**:
```bash
# Check if lock directory exists
ls -la ops/state/aimo.engine.lock.d

# Check if any AIMO process is running
ps aux | grep aimo

# Check PID file
cat ops/state/aimo.engine.pid
```

**Resolution**:

1. If process is running, wait for completion
2. If no process running (stale lock):
   ```bash
   # Verify no process
   ps aux | grep aimo
   
   # Check PID from file
   if [ -f ops/state/aimo.engine.pid ]; then
     PID=$(cat ops/state/aimo.engine.pid)
     if ! ps -p $PID > /dev/null 2>&1; then
       echo "PID $PID is not running - removing stale lock"
       rm -rf ops/state/aimo.engine.lock.d
       rm -f ops/state/aimo.engine.pid
     fi
   else
     # No PID file but lock exists - remove stale lock
     rm -rf ops/state/aimo.engine.lock.d
   fi
   
   # Retry run
   ops/bin/run_aimo.sh <input_file> <vendor>
   ```

3. If process is stuck:
   ```bash
   # Get PID
   if [ -f ops/state/aimo.engine.pid ]; then
     PID=$(cat ops/state/aimo.engine.pid)
     
     # Kill gracefully
     kill $PID
     
     # Wait a moment
     sleep 5
     
     # If still stuck, force kill
     if ps -p $PID > /dev/null 2>&1; then
       kill -9 $PID
     fi
     
     # Clean up
     rm -rf ops/state/aimo.engine.lock.d
     rm -f ops/state/aimo.engine.pid
   else
     # No PID file - just remove lock
     rm -rf ops/state/aimo.engine.lock.d
   fi
   ```

---

### Cost Management

**Symptom**: LLM API costs exceeding budget

**Diagnosis**:
```bash
# Check daily costs
python -m src.cli costs --today

# Check cost by run
python -m src.cli costs --run-id <run_id>
```

**Resolution**:

1. **Immediate**: Reduce budget temporarily
   ```bash
   export LLM_DAILY_BUDGET_USD=5.0
   ```

2. **Investigate high costs**:
   ```sql
   -- Check cost distribution
   SELECT model, SUM(cost_usd_estimated), SUM(input_tokens), SUM(output_tokens)
   FROM api_costs
   WHERE recorded_at > CURRENT_DATE
   GROUP BY model;
   
   -- Check unknown rate
   SELECT 
     COUNT(*) as total,
     COUNT(*) FILTER (WHERE status = 'active' AND service_name IS NULL) as unknown
   FROM analysis_cache;
   ```

3. **Optimize**:
   - Increase rule coverage for common services
   - Reduce C sample rate (config/thresholds.yaml)
   - Use smaller LLM model for initial pass

---

### Ingestion Issues

**Symptom**: Parse error count increasing for a vendor

**Diagnosis**:
```bash
# Check recent parse errors
python -m src.cli parse-errors --vendor <vendor> --last 100

# View sample raw log
head -20 data/work/<run_id>/raw/<file>
```

**Resolution**:

1. **Schema change**: Vendor updated log format
   ```bash
   # Compare with expected schema
   cat schemas/vendors/<vendor>/mapping.yaml
   
   # Update mapping if needed
   # Add new field candidates
   ```

2. **Encoding issue**: File encoding mismatch
   ```bash
   # Check file encoding
   file -I data/input/<file>
   
   # If not UTF-8, specify encoding in ingestor
   ```

3. **Corrupted file**: Partial upload/sync
   ```bash
   # Check file size stability
   ls -la data/input/<file>
   sleep 60
   ls -la data/input/<file>
   
   # If still changing, wait for Box sync
   ```

---

### Cache Issues

**Symptom**: Cache hit rate dropping

**Diagnosis**:
```sql
-- Check cache hit rate trend
SELECT 
  DATE(updated_at) as date,
  COUNT(*) as new_entries
FROM analysis_cache
GROUP BY DATE(updated_at)
ORDER BY date DESC
LIMIT 14;

-- Check signature version distribution
SELECT signature_version, COUNT(*)
FROM analysis_cache
GROUP BY signature_version;
```

**Resolution**:

1. **Signature version change**: Expected after normalization update
   - Verify change was intentional
   - Monitor for stabilization

2. **New traffic patterns**: Unusual services appearing
   ```sql
   -- Top new domains
   SELECT dest_domain, COUNT(*)
   FROM signature_stats
   WHERE run_id = '<latest_run_id>'
   GROUP BY dest_domain
   ORDER BY COUNT(*) DESC
   LIMIT 20;
   ```

3. **Rule coverage**: Add rules for common new services

---

### Report Issues

**Symptom**: Excel generation fails or is slow

**Diagnosis**:
```bash
# Check memory usage during generation
top -pid $(pgrep -f "excel_writer")

# Check output file size
ls -la data/output/
```

**Resolution**:

1. **Memory exhaustion**:
   - Verify constant_memory=True in excel_template_spec.json
   - Reduce EXCEL_MAX_ROWS in .env.local
   - Export large tables to Parquet instead

2. **Disk space**:
   ```bash
   df -h data/output/
   
   # Clean old reports
   find data/output/ -name "*.xlsx" -mtime +30 -delete
   ```

---

## Scheduled Maintenance

### Weekly

- [ ] Review LLM cost trends
- [ ] Check cache hit rate
- [ ] Review `needs_review` queue
- [ ] Clean old work directories

### Monthly

- [ ] Update Public Suffix List
  ```bash
  curl -o data/psl/public_suffix_list.dat https://publicsuffix.org/list/public_suffix_list.dat
  sha256sum data/psl/public_suffix_list.dat
  # Update docs/domain_parsing.md with new hash
  ```

- [ ] Review and update base_rules.json
- [ ] Archive old Parquet partitions
- [ ] Review error patterns

### Quarterly

- [ ] Run full regression test suite
- [ ] Review golden signature set
- [ ] Audit PII detection effectiveness
- [ ] Update LLM pricing in config

---

## Alerts

### Critical (Immediate Response)

| Alert | Threshold | Action |
|-------|-----------|--------|
| Authentication failure | Any | Check API keys |
| Lock timeout > 1 hour | 60 min | Investigate stuck process |
| Parse error rate > 10% | 10% | Check vendor schema |

### Warning (Next Business Day)

| Alert | Threshold | Action |
|-------|-----------|--------|
| Daily budget > 80% | 80% | Review cost drivers |
| Cache hit rate < 70% | 70% | Check signature stability |
| Unknown rate > 30% | 30% | Expand rule coverage |

### Info (Weekly Review)

| Alert | Threshold | Action |
|-------|-----------|--------|
| New GenAI service detected | Any | Add to base_rules.json |
| needs_review queue > 100 | 100 | Schedule manual review |

---

## Contact

- **Primary**: [Your Name] - [email]
- **Escalation**: [Manager] - [email]
- **Vendor Support**: Check vendor documentation links in schemas/vendors/
