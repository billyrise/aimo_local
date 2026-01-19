# AIMO Analysis Engine v1.4 実装差分特定結果（Taxonomyセット対応）

## 1. 実装済み項目

### 1.1 Box同期ファイル安定化
- ✅ `src/orchestrator/file_stabilizer.py`: 実装済み
- ✅ 60秒安定化待機、workディレクトリへのコピー
- ✅ `main.py`で`--use-box-sync`オプション対応

### 1.2 run冪等性
- ✅ `src/orchestrator.py`: `compute_run_key()`実装済み
- ✅ `input_manifest_hash`計算実装済み
- ✅ `last_completed_stage`チェックポイント実装済み
- ✅ `data/cache/aimo.lock`による排他制御実装済み

### 1.3 URL正規化・署名
- ✅ `src/normalize/url_normalizer.py`: 正規化手順順序固定実装済み
- ✅ 追跡パラメータ除去、クエリ順序固定、ID抽象化
- ✅ PII検知と`pii_audit`記録
- ✅ `src/signatures/signature_builder.py`: 署名生成実装済み

### 1.4 Candidate抽出（A/B/C）
- ✅ `src/detectors/abc_detector.py`: A/B/C抽出実装済み
- ✅ バースト・累積集計、seed固定サンプリング
- ✅ サイズ閾値単独除外禁止（構造的に防止）

### 1.5 DuckDBスキーマ
- ✅ `src/db/schema.sql`: 基本テーブル定義済み
- ✅ `runs/input_files/analysis_cache/signature_stats/api_costs/performance_metrics/pii_audit`

### 1.6 Writer Queue
- ✅ `src/db/duckdb_client.py`: 単一Writer実装済み
- ✅ バッチコミット、並列Worker対応

### 1.7 LLM（JSON Schema検証＋再試行）
- ✅ `src/llm/client.py`: JSON Schema検証実装済み
- ✅ 最大2回再試行、永続失敗skipped遷移
- ✅ `needs_review`は自動再送しない

### 1.8 Budget（Token Bucket）
- ✅ `src/llm/budget.py`: DAILY_BUDGET_USD実装済み
- ✅ 優先順位（A/B優先、C停止）実装済み

### 1.9 Reporting（監査説明）
- ✅ `src/reporting/excel_writer.py`: 監査説明セクション実装済み
- ✅ `constant_memory=True`実装済み
- ✅ サニタイズCSV生成実装済み

## 2. 未実装項目（Taxonomyセット対応）

### 2.1 DBスキーマ拡張（必須）
**現状**: `schema.sql`にTaxonomy関連列が存在しない

**必要実装**:
- `runs`テーブルに以下を追加:
  - `taxonomy_version VARCHAR`
  - `evidence_pack_version VARCHAR`
  - `engine_spec_version VARCHAR`
- `analysis_cache`テーブルに以下を追加:
  - `fs_uc_code VARCHAR` (nullable開始可)
  - `dt_code VARCHAR`
  - `ch_code VARCHAR`
  - `im_code VARCHAR`
  - `rs_code VARCHAR`
  - `ob_code VARCHAR`
  - `ev_code VARCHAR`
  - `taxonomy_version VARCHAR`
- `signature_stats`テーブルに以下を追加:
  - `fs_uc_code VARCHAR`
  - `dt_code VARCHAR`
  - `ch_code VARCHAR`
  - `im_code VARCHAR`
  - `rs_code VARCHAR`
  - `ob_code VARCHAR`
  - `ev_code VARCHAR`
  - `taxonomy_version VARCHAR`

**対応方針**: `schema.sql`を更新し、マイグレーションスクリプトを提供

### 2.2 run_key計算拡張（必須）
**現状**: `src/orchestrator.py`の`compute_run_key()`にTaxonomy関連バージョンが含まれていない

**必要実装**:
```python
run_key_input = (
    f"{input_manifest_hash}|"
    f"{target_range}|"
    f"{self.signature_version}|"
    f"{self.rule_version}|"
    f"{self.prompt_version}|"
    f"{self.taxonomy_version}|"  # 追加
    f"{self.evidence_pack_version}|"  # 追加
    f"{self.engine_spec_version}"  # 追加
)
```

**対応方針**: `Orchestrator`クラスにTaxonomy関連バージョンを追加し、`compute_run_key()`を更新

### 2.3 LLM JSON Schema拡張（必須）
**現状**: `llm/schemas/analysis_output.schema.json`に7コードが含まれていない

**必要実装**:
- `fs_uc_code`, `dt_code`, `ch_code`, `im_code`, `rs_code`, `ob_code`, `ev_code`を必須項目として追加
- `taxonomy_version`を必須項目として追加
- Unknownの場合でも列は欠落させず、空文字列または"Unknown"で整合を取る

**対応方針**: JSON Schemaを更新し、LLMプロンプトテンプレートも更新

### 2.4 RuleClassifier拡張（必須）
**現状**: `src/classifiers/rule_classifier.py`で7コードを付与していない

**必要実装**:
- `rules/base_rules.json`に7コードフィールドを追加可能にする
- RuleClassifierで7コードを付与可能な場合は付与する
- Unknownの場合でも列は欠落させず、空文字列で整合を取る

**対応方針**: RuleClassifierの`classify()`メソッドで7コードを返すように拡張

### 2.5 LLM出力処理拡張（必須）
**現状**: `src/main.py`の`_stage_4_llm_analysis()`で7コードを保存していない

**必要実装**:
- LLM出力から7コードとtaxonomy_versionを取得
- `analysis_cache`へのUPSERT時に7コードとtaxonomy_versionを保存
- JSON Schema検証で7コードの必須チェック

**対応方針**: `_stage_4_llm_analysis()`と`_stage_3_rule_classification()`を更新

### 2.6 Evidence Pack生成（必須）
**現状**: Evidence Pack出力が全く実装されていない

**必要実装**:
- `data/output/<run_id>/evidence_pack/`ディレクトリを作成
- `evidence_pack_summary.json`（機械可読）を生成
- `evidence_pack_summary.xlsx`または`csv`（人間可読）を生成
- 上記は必ず7コード＋taxonomy_versionを必須列として含める（欠落禁止）

**対応方針**: 新規モジュール`src/reporting/evidence_pack_generator.py`を作成

### 2.7 run_manifest.json生成（必須）
**現状**: run_manifest.jsonが生成されていない

**必要実装**:
- `data/output/<run_id>/run_manifest.json`を生成
- 以下のバージョン情報を必ず記録:
  - `engine_spec_version`
  - `taxonomy_version`
  - `evidence_pack_version`
  - `signature_version`
  - `rule_version`
  - `prompt_version`

**対応方針**: `ReportBuilder`または新規モジュールで生成

### 2.8 is_human_verified上書き禁止（必須）
**現状**: `src/db/duckdb_client.py`のUPSERT時に`is_human_verified=true`のチェックがない

**必要実装**:
- `_execute_upsert()`でUPSERT前に`is_human_verified=true`をチェック
- `is_human_verified=true`の場合は上書きをスキップ（警告ログ出力）

**対応方針**: `DuckDBClient._execute_upsert()`を更新

## 3. 仕様不一致項目

### 3.1 status=skipped送信禁止
**現状**: `src/main.py`の`_stage_4_llm_analysis()`で`status=skipped`のチェックは実装済み
**確認**: ✅ 実装済み（`status=skipped`は送信対象外）

### 3.2 needs_review自動再送禁止
**現状**: `src/main.py`の`_stage_4_llm_analysis()`で`needs_review`の自動再送は実装済み
**確認**: ✅ 実装済み（`needs_review`は人手確認キュー）

## 4. テスト要件

### 4.1 未実装テスト
- Taxonomyセット関連:
  - 7コード必須列チェックテスト
  - Evidence Pack生成テスト
  - Versioning整合性テスト
- is_human_verified上書き禁止テスト

### 4.2 既存テスト（確認済み）
- ✅ 署名決定性テスト: `test_signature_stability.py`
- ✅ run冪等性テスト: `test_idempotency.py`
- ✅ C枠再現性テスト: `test_abc_detector.py`
- ✅ LLM壊れテスト: `test_llm_gemini_schema_validation.py`
- ✅ 監査説明必須項目テスト: `test_report_contains_audit_fields.py`

## 5. 実装優先順位

1. **最優先**: DBスキーマ拡張（Taxonomy列追加）
2. **最優先**: run_key計算拡張（Taxonomyバージョン含める）
3. **最優先**: is_human_verified上書き禁止
4. **高**: LLM JSON Schema拡張（7コード必須）
5. **高**: RuleClassifier拡張（7コード付与）
6. **高**: LLM出力処理拡張（7コード保存）
7. **高**: Evidence Pack生成
8. **中**: run_manifest.json生成
9. **中**: テスト追加

## 6. 後方互換性

- DBスキーマ拡張は`ALTER TABLE ADD COLUMN`で後方互換
- 既存データは`NULL`で開始（nullable開始）
- 出力列は必須（欠落禁止）だが、既存データは`NULL`可

## 7. 移行方法

1. DBスキーマ更新: `schema.sql`を実行（既存データは`NULL`で開始）
2. コード更新: Taxonomy関連バージョンを設定ファイルまたは環境変数で管理
3. 既存データ移行: 不要（新規列は`NULL`で開始、新規runから7コード付与開始）
