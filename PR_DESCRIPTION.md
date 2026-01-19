# PR説明文: AIMO Analysis Engine v1.4 Taxonomyセット対応

## 概要

AIMO Analysis Engineを仕様書v1.4に完全準拠させ、Taxonomyセット（7コード：FS-UC/DT/CH/IM/RS/OB/EV＋Evidence Pack＋Versioning）と矛盾なく連動するように改修しました。

## 変更点

### 1. Taxonomyセット対応（必須）

#### DBスキーマ拡張
- `runs`テーブルに`taxonomy_version`, `evidence_pack_version`, `engine_spec_version`を追加
- `analysis_cache`テーブルに7コード（`fs_uc_code`, `dt_code`, `ch_code`, `im_code`, `rs_code`, `ob_code`, `ev_code`）と`taxonomy_version`を追加
- `signature_stats`テーブルに7コードと`taxonomy_version`を追加

#### run_key計算拡張
- `Orchestrator.compute_run_key()`にTaxonomy関連バージョンを含める
- 同一入力でもTaxonomyバージョンが異なれば異なる`run_id`に収束（冪等性維持）

#### LLM JSON Schema拡張
- `llm/schemas/analysis_output.schema.json`に7コードと`taxonomy_version`を必須項目として追加
- LLMプロンプトテンプレートを更新（7コード必須の説明を追加）
- LLM出力で7コードが欠落している場合のデフォルト値処理を追加

#### RuleClassifier拡張
- `RuleClassifier._build_classification()`で7コードを返す（ルール定義に含まれている場合）
- Unknownの場合でも列は欠落させず、空文字列で整合を取る（列欠落禁止）

#### データ保存
- `analysis_cache`へのUPSERT時に7コードと`taxonomy_version`を保存
- `signature_stats`を`analysis_cache`から7コードを取得して更新

### 2. Evidence Pack生成（必須）

- `src/reporting/evidence_pack_generator.py`を新規作成
- `data/output/<run_id>/evidence_pack/evidence_pack_summary.json`（機械可読）を生成
- `data/output/<run_id>/evidence_pack/evidence_pack_summary.xlsx`（人間可読）を生成
- 7コード＋`taxonomy_version`を必須列として含める（列欠落禁止）

### 3. run_manifest.json生成（必須）

- `EvidencePackGenerator.generate_run_manifest()`で全バージョン情報を記録
- `engine_spec_version`, `taxonomy_version`, `evidence_pack_version`, `signature_version`, `rule_version`, `prompt_version`を含める

### 4. is_human_verified上書き禁止（必須）

- `DuckDBClient._execute_upsert()`で`is_human_verified=true`の行を上書き禁止
- RULE/LLM/自動処理が人手確定を上書きしない（仕様9.4準拠）

## 後方互換性

### DBスキーマ
- 新規列は`ALTER TABLE ADD COLUMN`で追加（後方互換）
- 既存データは`NULL`で開始（nullable開始）
- 出力列は必須（欠落禁止）だが、既存データは`NULL`可

### run_id収束
- 既存のrunは再実行時にTaxonomyバージョンが含まれる新しい`run_id`になる可能性がある
- これは仕様通り（Taxonomyバージョンが異なれば異なるrunとして扱う）

## 移行方法

### 既存DBのスキーマ更新

```bash
python -c "
import duckdb
conn = duckdb.connect('./data/cache/aimo.duckdb')
# runsテーブル
conn.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS taxonomy_version VARCHAR')
conn.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS evidence_pack_version VARCHAR')
conn.execute('ALTER TABLE runs ADD COLUMN IF NOT EXISTS engine_spec_version VARCHAR')
# analysis_cacheテーブル
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS taxonomy_version VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS fs_uc_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS dt_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ch_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS im_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS rs_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ob_code VARCHAR')
conn.execute('ALTER TABLE analysis_cache ADD COLUMN IF NOT EXISTS ev_code VARCHAR')
# signature_statsテーブル
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

### 既存データへの影響

- 既存の`analysis_cache`レコードは7コードが`NULL`のまま（新規runから7コード付与開始）
- 既存の`signature_stats`レコードは7コードが`NULL`のまま（新規runから7コード付与開始）
- 既存の`runs`レコードはTaxonomyバージョンが`NULL`のまま（新規runからバージョン記録開始）

## リスクと回避策

### リスク1: DBスキーマ更新失敗

**回避策**: 
- スキーマ更新スクリプトを実行前にバックアップを取得
- `IF NOT EXISTS`を使用して既存列がある場合はスキップ

### リスク2: LLM出力に7コードが欠落

**回避策**:
- LLM JSON Schema検証で必須チェック
- LLM出力のデフォルト値処理で空文字列を設定
- プロンプトテンプレートで7コード必須を明示

### リスク3: Evidence Packに7コードが欠落

**回避策**:
- Evidence Pack生成時に7コードの存在チェック
- テストで列欠落を検証

### リスク4: is_human_verified上書き

**回避策**:
- UPSERT前に`is_human_verified=true`をチェック
- テストで上書き禁止を検証

## テスト

### 追加テスト
- `tests/test_taxonomy_codes.py`: Taxonomyセット関連のテスト
  - LLM JSON Schemaに7コードが必須であることを確認
  - RuleClassifierが7コードを返すことを確認
  - analysis_cacheに7コードが保存されることを確認
  - Evidence Packに7コードが含まれることを確認（列欠落禁止）
  - run_manifest.jsonに全バージョン情報が含まれることを確認
  - run_key計算にTaxonomyバージョンが含まれることを確認

- `tests/test_human_verified_protection.py`: is_human_verified上書き禁止のテスト
  - RULE分類が`is_human_verified=true`を上書きしないことを確認
  - LLM分類が`is_human_verified=true`を上書きしないことを確認
  - `is_human_verified=false`は上書き可能であることを確認

### 既存テスト
- 既存のテストは全て通過（後方互換性維持）

## 実行手順

### 1. 環境準備

```bash
pip install -r requirements.txt
```

### 2. DBスキーマ更新（既存DBの場合）

上記のスキーマ更新スクリプトを実行

### 3. 実行

```bash
python src/main.py --use-box-sync --vendor paloalto
```

### 4. 出力確認

- `data/output/<run_id>/evidence_pack/evidence_pack_summary.json`
- `data/output/<run_id>/evidence_pack/evidence_pack_summary.xlsx`
- `data/output/<run_id>/run_manifest.json`

### 5. テスト実行

```bash
pytest tests/test_taxonomy_codes.py -v
pytest tests/test_human_verified_protection.py -v
```

## 変更ファイル一覧

### 新規ファイル
- `src/reporting/evidence_pack_generator.py`
- `tests/test_taxonomy_codes.py`
- `tests/test_human_verified_protection.py`
- `docs/implementation_gap_analysis_v1.4_taxonomy.md`
- `docs/implementation_guide_taxonomy_v1.4.md`
- `PR_DESCRIPTION.md`

### 変更ファイル
- `src/db/schema.sql`
- `src/orchestrator.py`
- `src/db/duckdb_client.py`
- `src/main.py`
- `src/classifiers/rule_classifier.py`
- `src/llm/client.py`
- `src/llm/prompt_templates.py`
- `llm/schemas/analysis_output.schema.json`

## 関連ドキュメント

- `docs/implementation_gap_analysis_v1.4_taxonomy.md`: 差分特定結果
- `docs/implementation_guide_taxonomy_v1.4.md`: 実装ガイド
- `AIMO_Detail.md`: 仕様書v1.4
