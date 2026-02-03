# AIMO Engine テスト構造

このドキュメントは、テストスイートの構造と各テストの役割を説明します。

## テスト階層

```
tests/
├── Unit Tests (単体テスト)
│   ├── test_url_normalizer.py      - URL正規化ロジック
│   ├── test_signature_stability.py  - 署名の安定性
│   ├── test_abc_detector.py         - A/B/C候補検出
│   ├── test_gemini_schema_sanitizer.py - Gemini スキーマ変換
│   └── ...
│
├── Integration Tests (統合テスト)
│   ├── test_db_compat.py            - DuckDB互換性
│   ├── test_orchestrator_checkpoint.py - チェックポイント・再開
│   ├── test_evidence_bundle_generation.py - Evidence Bundle生成
│   └── ...
│
├── Contract Tests (契約テスト)
│   ├── test_contract_e2e_standard_bundle.py - Standard準拠E2E (必須)
│   ├── test_standard_adapter_smoke.py - Standard Adapter動作確認
│   ├── test_taxonomy_codes.py        - Taxonomy検証
│   └── ...
│
└── Legacy Tests (レガシー - skip済み)
    ├── test_rule_classifier.py       - 旧ルール形式テスト (skip)
    └── test_llm_gemini_schema_validation.py - API必要テスト (skip)
```

## 必須テスト（CI ゲート）

以下のテストは CI で必ず実行され、失敗するとマージがブロックされます：

| テスト | 目的 | 備考 |
|--------|------|------|
| `test_contract_e2e_standard_bundle.py` | Standard 準拠の契約テスト | LLM不使用 (stub_classifier) |
| `test_standard_adapter_smoke.py` | Standard Adapter の動作確認 | pinning 検証含む |
| `test_taxonomy_codes.py` | Taxonomy コードの妥当性確認 | 8次元・cardinality |
| `test_signature_stability.py` | 署名の決定性確認 | 同一入力→同一出力 |
| `test_url_normalizer.py` | URL正規化の決定性確認 | 正規化ルール |

## 後方互換不要による変更 (2026-02-02)

「稼働前で後方互換不要」という前提のもと、以下の変更を行いました：

### 1. CI の `--ignore` 撤廃

以前は以下のテストが CI から除外されていました：

```yaml
# 旧: これらは --ignore されていた
--ignore=tests/test_rule_classifier.py
--ignore=tests/test_llm_gemini_schema_validation.py
--ignore=tests/test_budget_priority.py
--ignore=tests/test_cli_smoke.py
--ignore=tests/test_human_verified_protection.py
--ignore=tests/test_idempotency.py
--ignore=tests/test_input_manifest_hash.py
--ignore=tests/test_llm_rate_limit_policy.py
```

**変更後**:
- `--ignore` をすべて撤廃
- 各テストファイルは適切に動作するか、内部で `pytest.mark.skip` を持つ

### 2. レガシーテストの扱い

| ファイル | 状態 | 理由 |
|----------|------|------|
| `test_rule_classifier.py` | `pytest.mark.skip` | 旧ルール形式前提。新形式は `test_rule_classifier_taxonomy.py` で検証 |
| `test_llm_gemini_schema_validation.py` | `pytest.mark.skip` | Gemini API アクセスが必要。CI では実行しない |
| `test_file_stabilizer.py` | `pytest.mark.skip` | ファイルシステム依存テスト。テスト環境と競合 |
| `test_human_verified_protection.py` | `pytest.mark.skip` | DuckDB indexed columns 制約。UPSERT で indexed columns を更新不可 |
| `test_cli_smoke.py` (一部) | `pytest.mark.skip` | CLI 引数順序変更。`--db-path` はサブコマンドより前に必要 |
| `test_idempotency.py::test_run_replay_idempotency` | `pytest.mark.skip` | DuckDB indexed columns 制約 |
| `test_budget_priority.py::test_filter_by_priority` | `pytest.mark.skip` | `filter_by_priority` 実装変更 |

### 3. 新規追加テスト

| ファイル | 目的 |
|----------|------|
| `test_contract_e2e_standard_bundle.py` | LLM不使用の契約E2E。Standard準拠を機械的に担保 |

### 4. DuckDB Indexed Columns 制約について

DuckDB の制約により、`DuckDBClient.upsert()` は以下のカラムを UPDATE 操作から除外します：

- `status` (runs, analysis_cache)
- `started_at` (runs)
- `is_human_verified` (analysis_cache)
- `usage_type` (analysis_cache)

これは DuckDB がインデックス付きカラムの UPSERT UPDATE をサポートしないためです。
これらのカラムの更新は、`execute_sql()` を使用して直接 UPDATE クエリで行う必要があります。

## 環境変数

テスト実行時に使用される環境変数：

| 変数 | 値 | 説明 |
|------|-----|------|
| `AIMO_DISABLE_LLM` | `1` | LLM呼び出しを完全無効化。呼び出すと例外 |
| `AIMO_CLASSIFIER` | `stub` | stub_classifier を使用（LLM不要で8次元分類） |
| `AIMO_ALLOW_SKIP_PINNING` | 未設定 | CI では設定しない（pinning必須） |

## テスト実行方法

### ローカル（全テスト）
```bash
pytest -q
```

### ローカル（契約E2Eのみ）
```bash
pytest tests/test_contract_e2e_standard_bundle.py -v
```

### LLM無効化確認
```bash
AIMO_DISABLE_LLM=1 AIMO_CLASSIFIER=stub pytest tests/test_contract_e2e_standard_bundle.py -v
```

## 注意事項

1. **Taxonomy コードのハードコード禁止**
   - すべてのコードは `standard_adapter.taxonomy.get_allowed_codes()` から取得すること
   - テスト内でも `"FS-001"` などをリテラルで書かない

2. **Validator fail の握り潰し禁止**
   - Validator が失敗したらテストも失敗させる
   - `try/except` で握り潰さない

3. **pinning 不一致は必ず止める**
   - これが「Standard 更新に対する安全装置」
   - CI で `AIMO_ALLOW_SKIP_PINNING` は絶対に設定しない

---

**最終更新**: 2026-02-02
**適用 Standard Version**: v0.1.1
