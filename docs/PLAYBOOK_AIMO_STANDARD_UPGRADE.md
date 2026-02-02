# AIMO Standard Upgrade Playbook (Engine開発者向け)

このPlaybookは、AIMO Standard のバージョンアップグレード時に Engine 開発者が従うべき手順と方針を定義します。

## 1. 基本原則 (Non-negotiables)

以下は絶対に守るべきルールです。例外はありません。

### 正は常に AIMO Standard 英語版
- Taxonomy、Schema、Validator のすべてにおいて、AIMO Standard (英語版) が唯一の正
- Engine はこれらをハードコードしない。必ず Standard artifacts から読み込む

### Run ごとに Standard を固定
- 各 Run は使用した Standard の version / commit / sha を `runs` テーブルと `run_manifest.json` に刻む
- 同じ入力 + 同じ Standard = 同じ出力 (再現性保証)

### /latest は参照しない
- 監査上の「正」が揺れるため、latest や HEAD への追従は禁止
- Standard リポジトリの `main` ブランチを直接参照しない

### Standard 側の過去タグは不変
- v0.1.7 等のタグは決して書き換えない
- commit hash が変わった場合は Standard リポジトリの運用問題として報告する

## 2. アップグレードが必要になるトリガー

| トリガー | 対応レベル | Engine への影響 |
|----------|------------|-----------------|
| Standard Patch (0.1.7 → 0.1.8) | 低 | 後方互換。pin 更新のみ |
| Standard Minor (0.1.x → 0.2.0) | 中 | 新機能追加。Adapter 拡張が必要な場合あり |
| Standard Major (0.x → 1.0) | 高 | 破壊的変更。Engine 側も大改修が必要 |

### 破壊的変更の具体例
- Taxonomy dimension の追加/削除 (例: 8次元 → 9次元)
- 既存コードの廃止 (例: UC-001 が deprecated)
- Evidence Schema の構造変更
- Validator ルールの厳格化

## 3. アップグレード手順 (チェックリスト)

### Step 1: 準備
- [ ] Standard リポジトリの Release Notes を確認
- [ ] 破壊的変更の有無を確認
- [ ] 新しいブランチを作成: `feat/upgrade-standard-vX.Y.Z`

### Step 2: Submodule 更新
```bash
# アップグレードスクリプトを実行
./scripts/upgrade_standard_version.sh --version X.Y.Z

# または手動で:
cd third_party/aimo-standard
git fetch --all --tags
git checkout vX.Y.Z
cd ../..
```

### Step 3: Pinning 値の更新
`src/standard_adapter/pinning.py` を更新:
```python
PINNED_STANDARD_VERSION = "X.Y.Z"
PINNED_STANDARD_COMMIT = "<new commit hash>"
PINNED_ARTIFACTS_DIR_SHA256 = "<new sha256>"
```

### Step 4: Default バージョンの更新
`src/standard_adapter/constants.py` を更新:
```python
AIMO_STANDARD_VERSION_DEFAULT = "X.Y.Z"
```

### Step 5: Adapter 対応
変更があれば以下を更新:
- [ ] `taxonomy.py`: 辞書の場所/フォーマット変更への対応
- [ ] `schemas.py`: JSON Schema の場所/構造変更への対応
- [ ] `validator_runner.py`: Validator ルール変更への対応

### Step 6: 分類ロジック対応
- [ ] `llm/schemas/analysis_output.schema.json`: 新 taxonomy 構造への対応
- [ ] `llm/prompt_templates.py`: プロンプトの cardinality 変更への対応
- [ ] `classifiers/rule_classifier.py`: ルールの taxonomy 対応

### Step 7: DB マイグレーション
必要であれば:
- [ ] `db/migrations.py` に新しいマイグレーションを追加
- [ ] 新カラムの追加 / 既存データの変換

### Step 8: テスト実行
```bash
# sync が成功すること
python scripts/sync_aimo_standard.py --version X.Y.Z

# pin 検証が通ること
python -c "from src.standard_adapter.resolver import resolve_standard_artifacts; resolve_standard_artifacts('X.Y.Z')"

# 全テストが通ること
pytest -q

# E2E で Evidence Bundle が生成されること
python -c "..." # (E2E smoke test)
```

### Step 9: PR 作成
- [ ] 変更内容を記載
- [ ] Standard v0.1.7 → vX.Y.Z の差分を説明
- [ ] 破壊的変更があれば移行手順を明記

## 4. Major アップデート時の必須対応

### 何が壊れるか
| 変更種別 | 影響範囲 | 対応 |
|----------|----------|------|
| Dimension 追加 | Taxonomy Adapter, LLM Schema, DB | 新 dimension の列追加 |
| Cardinality 変更 | LLM Schema, Validator | minItems/maxItems 調整 |
| コード廃止 | Rule Classifier, 既存データ | 移行マップ作成、旧→新変換 |
| Schema 構造変更 | Evidence Generator | 生成ロジック全面改修 |

### どこを直すか
```
src/
├── standard_adapter/
│   ├── taxonomy.py          # 辞書読み込み
│   ├── schemas.py           # Schema 読み込み
│   └── validator_runner.py  # Validator 実行
├── llm/
│   ├── schemas/
│   │   └── analysis_output.schema.json  # LLM 出力 Schema
│   ├── prompt_templates.py  # プロンプト
│   └── client.py            # 結果検証
├── classifiers/
│   └── rule_classifier.py   # ルール分類
├── db/
│   ├── schema.sql           # DB スキーマ
│   ├── migrations.py        # マイグレーション
│   └── compat.py            # 互換性レイヤー
└── reporting/
    └── standard_evidence_bundle_generator.py  # Bundle 生成
```

### 旧 Run の扱い
- 旧 Run は旧 Standard で再現できるように保つ
- `run_manifest.json` に刻まれた Standard version で判別
- 移行マップ (旧コード → 新コード) を Evidence に同梱する場合あり

## 5. リリース/監査観点

### run_manifest.json に残すべき項目
```json
{
  "run_id": "...",
  "aimo_standard": {
    "version": "0.1.7",
    "commit": "88ab75d286a2...",
    "artifacts_dir_sha256": "057228a570b5..."
  },
  "input_manifest_hash": "...",
  "versions": {
    "signature_version": "1.0",
    "rule_version": "1",
    "prompt_version": "1"
  },
  "extraction_parameters": {
    "a_threshold_bytes": 10000000,
    "sample_seed": "..."
  }
}
```

### Evidence Bundle の自己検証
- 生成後に必ず `validator_runner.run_validation()` を実行
- 失敗したら `status = "failed"` を返す (曖昧な "partial" にしない)
- `validation_result.json` を Bundle に含める

## 6. よくある事故と対処

### タグが動いた (commit が変わった)
**症状**: pin 検証で「Commit mismatch」エラー
**原因**: Standard リポジトリでタグが force push された
**対処**:
1. Standard リポジトリのメンテナに報告
2. タグを修正してもらう
3. **pin を更新してはならない** (事故の隠蔽になる)

### artifacts zip の中身が変わった
**症状**: pin 検証で「Artifacts SHA mismatch」エラー
**原因**: Standard リリースプロセスの問題
**対処**:
1. Standard リポジトリのリリース手順を確認
2. 再リリースを依頼
3. 確認後に pin を更新

### Schema の場所が変わった
**症状**: `FileNotFoundError` や Schema ロード失敗
**原因**: Standard の構造変更
**対処**:
1. `standard_adapter/schemas.py` の探索パスを修正
2. テストで新しいパスが動くことを確認

### Validator のルールが変わり落ちた
**症状**: `validation_result.json` で fail
**原因**: Standard の Validator が厳格化された
**対処**:
1. **これは正しい動作**。Validator が正
2. Engine 側の生成物を Standard に合わせて修正
3. 生成ロジックを修正後、再テスト

---

**作成日**: 2026-02-02
**適用 Standard Version**: v0.1.7
**次回更新予定**: Standard Major バージョンアップ時
