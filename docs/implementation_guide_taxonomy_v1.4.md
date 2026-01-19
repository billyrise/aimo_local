# AIMO Analysis Engine v1.4 Taxonomyセット対応 実装ガイド

## 実装完了項目

### 1. DBスキーマ拡張
- ✅ `runs`テーブルに`taxonomy_version`, `evidence_pack_version`, `engine_spec_version`を追加
- ✅ `analysis_cache`テーブルに7コード（`fs_uc_code`, `dt_code`, `ch_code`, `im_code`, `rs_code`, `ob_code`, `ev_code`）と`taxonomy_version`を追加
- ✅ `signature_stats`テーブルに7コードと`taxonomy_version`を追加

### 2. run_key計算拡張
- ✅ `Orchestrator.compute_run_key()`に`taxonomy_version`, `evidence_pack_version`, `engine_spec_version`を含める
- ✅ 同一入力でもTaxonomyバージョンが異なれば異なる`run_id`に収束

### 3. is_human_verified上書き禁止
- ✅ `DuckDBClient._execute_upsert()`で`is_human_verified=true`の行を上書き禁止
- ✅ RULE/LLM/自動処理が人手確定を上書きしない

### 4. LLM JSON Schema拡張
- ✅ `llm/schemas/analysis_output.schema.json`に7コードと`taxonomy_version`を必須項目として追加
- ✅ LLMプロンプトテンプレートを更新（7コード必須の説明を追加）

### 5. RuleClassifier拡張
- ✅ `RuleClassifier._build_classification()`で7コードを返す（ルール定義に含まれている場合）
- ✅ Unknownの場合でも列は欠落させず、空文字列で整合を取る

### 6. LLM出力処理拡張
- ✅ `src/main.py`の`_stage_4_llm_analysis()`で7コードと`taxonomy_version`を保存
- ✅ `src/main.py`の`_stage_3_rule_classification()`で7コードと`taxonomy_version`を保存
- ✅ LLM出力で7コードが欠落している場合のデフォルト値処理を追加

### 7. signature_stats更新
- ✅ Stage 3とStage 4の後に`analysis_cache`から7コードを取得して`signature_stats`を更新

### 8. Evidence Pack生成
- ✅ `src/reporting/evidence_pack_generator.py`を作成
- ✅ `evidence_pack_summary.json`（機械可読）を生成
- ✅ `evidence_pack_summary.xlsx`（人間可読）を生成
- ✅ 7コード＋`taxonomy_version`を必須列として含める（列欠落禁止）

### 9. run_manifest.json生成
- ✅ `EvidencePackGenerator.generate_run_manifest()`で全バージョン情報を記録
- ✅ `engine_spec_version`, `taxonomy_version`, `evidence_pack_version`, `signature_version`, `rule_version`, `prompt_version`を含める

### 10. テスト追加
- ✅ `tests/test_taxonomy_codes.py`: Taxonomyセット関連のテスト
- ✅ `tests/test_human_verified_protection.py`: is_human_verified上書き禁止のテスト

## 実行手順

### 1. 環境準備

```bash
# 仮想環境の有効化（必要に応じて）
source venv/bin/activate  # または適切な仮想環境

# 依存関係のインストール
pip install -r requirements.txt
```

### 2. DBスキーマ更新

既存のDuckDBデータベースがある場合、スキーマを更新する必要があります。

```bash
# スキーマ更新スクリプトを実行（手動でALTER TABLEを実行）
python -c "
import duckdb
conn = duckdb.connect('./data/cache/aimo.duckdb')
# runsテーブルに列を追加
conn.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS taxonomy_version VARCHAR')
conn.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS evidence_pack_version VARCHAR')
conn.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS engine_spec_version VARCHAR')
# analysis_cacheテーブルに列を追加
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS taxonomy_version VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS fs_uc_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS dt_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ch_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS im_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS rs_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ob_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ev_code VARCHAR')
# signature_statsテーブルに列を追加
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS taxonomy_version VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS fs_uc_code VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS dt_code VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS ch_code VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS im_code VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS rs_code VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS ob_code VARCHAR')
conn.execute('ALTER TABLE signature_stats ADD COLUMN IF NOT EXISTS ev_code VARCHAR')
conn.close()
print('Schema updated successfully')
"
```

**注意**: 新規インストールの場合は、`src/db/schema.sql`を実行するだけでOKです。

### 3. 実行

```bash
# Box同期モードで実行
python src/main.py --use-box-sync --vendor paloalto

# または、直接ファイルを指定
python src/main.py data/input/test_stabilization.csv --vendor paloalto
```

### 4. 出力確認

実行後、以下のファイルが生成されます：

- `data/output/<run_id>/run_<run_id>_summary.json`: レポートJSON
- `data/output/<run_id>/run_<run_id>_report.xlsx`: Excelレポート
- `data/output/<run_id>/run_<run_id>_sanitized.csv`: サニタイズCSV
- `data/output/<run_id>/evidence_pack/evidence_pack_summary.json`: Evidence Pack JSON
- `data/output/<run_id>/evidence_pack/evidence_pack_summary.xlsx`: Evidence Pack Excel
- `data/output/<run_id>/run_manifest.json`: バージョン情報マニフェスト

### 5. テスト実行

```bash
# Taxonomyセット関連のテスト
pytest tests/test_taxonomy_codes.py -v

# is_human_verified上書き禁止のテスト
pytest tests/test_human_verified_protection.py -v

# 全テスト実行
pytest tests/ -v
```

## 必要な環境変数

`.env.local`ファイルに以下を設定（必要に応じて）：

```bash
# LLM API Keys
GOOGLE_API_KEY=your_api_key_here
# または
GEMINI_API_KEY=your_api_key_here
OPENAI_API_KEY=your_api_key_here  # OpenAI使用時

# Budget設定（オプション、デフォルト値あり）
DAILY_BUDGET_USD=10.0
```

## 後方互換性

- DBスキーマ拡張は`ALTER TABLE ADD COLUMN`で後方互換
- 既存データは`NULL`で開始（nullable開始）
- 出力列は必須（欠落禁止）だが、既存データは`NULL`可
- 既存のrunは再実行時にTaxonomyバージョンが含まれる新しい`run_id`になる可能性がある（仕様通り）

## 移行方法

1. **DBスキーマ更新**: 上記のスキーマ更新スクリプトを実行
2. **コード更新**: リポジトリを最新版に更新
3. **既存データ移行**: 不要（新規列は`NULL`で開始、新規runから7コード付与開始）

## トラブルシューティング

### 問題: DBスキーマエラー

**解決策**: スキーマ更新スクリプトを実行（上記参照）

### 問題: LLM出力に7コードが含まれない

**解決策**: 
- LLM JSON Schemaが正しく更新されているか確認
- LLMプロンプトテンプレートが更新されているか確認
- LLM出力のデフォルト値処理が動作しているか確認

### 問題: Evidence Packに7コードが欠落

**解決策**:
- `analysis_cache`に7コードが保存されているか確認
- `signature_stats`が`analysis_cache`から7コードを取得しているか確認
- Evidence Pack生成時のクエリを確認

## 変更ファイル一覧

### 新規ファイル
- `src/reporting/evidence_pack_generator.py`
- `tests/test_taxonomy_codes.py`
- `tests/test_human_verified_protection.py`
- `docs/implementation_gap_analysis_v1.4_taxonomy.md`
- `docs/implementation_guide_taxonomy_v1.4.md`

### 変更ファイル
- `src/db/schema.sql`: Taxonomy列追加
- `src/orchestrator.py`: Taxonomyバージョンをrun_key計算に含める
- `src/db/duckdb_client.py`: is_human_verified上書き禁止チェック
- `src/main.py`: Taxonomyコード保存、Evidence Pack生成
- `src/classifiers/rule_classifier.py`: 7コード返却
- `src/llm/client.py`: 7コードデフォルト値処理
- `src/llm/prompt_templates.py`: 7コード必須の説明追加
- `llm/schemas/analysis_output.schema.json`: 7コード必須項目追加
