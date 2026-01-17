# AIMO Operations Runbook

## Overview

This runbook provides procedures for operating and troubleshooting the AIMO Analysis Engine.

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

### temp_directory設定（必須）

DuckDBはWALや.tmpディレクトリ等を作成するため、DBとtemp_directoryはローカルSSDの書込み可能領域に固定する。

**規約**:
- `temp_directory`はDBと同じディレクトリ配下に配置（デフォルト: `{db_pathの親ディレクトリ}/duckdb_tmp`）
- 起動時に必ず `SET temp_directory` を実行
- パスをログに出力（監査用）

**実装**:
- `src/db/duckdb_client.py`で自動設定
- ログ出力: `DuckDB temp_directory: {path}`

**確認方法**:
```bash
# 実行ログで確認
grep "DuckDB temp_directory" <execution_log>
```

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

# Standard run (LLM有り)
export OPENAI_API_KEY=<your_key>
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

# B) LLM有りE2E検証
export OPENAI_API_KEY=<your_key>
python src/main.py sample_logs/paloalto_sample.csv --vendor paloalto

# レポートのバリデーション確認
python -m pytest tests/test_e2e_validation.py -v
```

---

## Troubleshooting

### Lock Issues

**Symptom**: Run fails with "Could not acquire lock"

**Diagnosis**:
```bash
# Check if lock file exists
ls -la data/cache/aimo.lock

# Check if any AIMO process is running
ps aux | grep aimo
```

**Resolution**:

1. If process is running, wait for completion
2. If no process running (stale lock):
   ```bash
   # Verify no process
   ps aux | grep aimo
   
   # Remove stale lock
   rm data/cache/aimo.lock
   
   # Retry run
   python -m src.main run
   ```

3. If process is stuck:
   ```bash
   # Get PID
   cat data/cache/aimo.pid
   
   # Kill gracefully
   kill <pid>
   
   # If still stuck, force kill
   kill -9 <pid>
   
   # Clean up
   rm data/cache/aimo.lock data/cache/aimo.pid
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
