# Developer Handoff: AIMO Standard Native Integration

新規参加開発者向けの申し送りドキュメントです。

## 1. リポジトリ構造と責務

```
aimo_local/
├── third_party/
│   └── aimo-standard/          # [CRITICAL] AIMO Standard (Git submodule)
│                                 # 英語版が「正」。タグで固定。触らない。
├── src/
│   ├── standard_adapter/        # Standard との接続層
│   │   ├── resolver.py          # artifacts の解決 + pin 検証
│   │   ├── pinning.py           # [CRITICAL] 固定値。勝手に変えない
│   │   ├── taxonomy.py          # Taxonomy 辞書の読み込み
│   │   ├── schemas.py           # JSON Schema の読み込み
│   │   └── validator_runner.py  # Validator の実行
│   │
│   ├── llm/                     # LLM 分類
│   │   ├── schemas/             # LLM 出力 Schema (Standard 準拠)
│   │   └── prompt_templates.py  # プロンプト (Standard codes を使用)
│   │
│   ├── classifiers/             # ルール分類
│   │   └── rule_classifier.py   # ルールベース分類 (8 次元出力)
│   │
│   ├── db/                      # データベース
│   │   ├── schema.sql           # DuckDB スキーマ
│   │   ├── migrations.py        # マイグレーション
│   │   └── compat.py            # 旧→新フォーマット互換
│   │
│   └── reporting/               # レポート/Evidence 出力
│       └── standard_evidence_bundle_generator.py  # Standard 準拠 Bundle
│
├── scripts/
│   ├── sync_aimo_standard.py    # Standard 同期スクリプト
│   └── upgrade_standard_version.sh  # アップグレード補助
│
└── docs/
    ├── PLAYBOOK_AIMO_STANDARD_UPGRADE.md  # 必読
    ├── MIGRATION_NATIVE_STANDARD.md        # DB 移行ガイド
    └── ADR/
        └── ADR-0001-*.md                   # 設計決定の記録
```

## 2. Run の再現性 (最重要)

### run_key の構成

```python
run_key = sha256(
    input_manifest_hash +      # 入力ファイルの hash
    signature_version +        # URL 正規化バージョン
    rule_version +             # ルールバージョン
    prompt_version +           # プロンプトバージョン
    aimo_standard_version +    # Standard バージョン
    artifacts_sha256           # artifacts の hash
)
```

**同じ run_key = 同じ結果** が保証される。

### Standard 固定

各 Run は使用した Standard の情報を記録:

```json
{
  "aimo_standard": {
    "version": "0.1.7",
    "commit": "88ab75d286a2...",
    "artifacts_dir_sha256": "057228a570b5..."
  }
}
```

### Sample Seed

サンプリングには `run_id` 由来の seed を使用:
```python
seed = sha256(run_id)[:16]
```

## 3. 禁止事項 (絶対厳守)

### ❌ latest 追従
```python
# これは禁止
resolve_standard_artifacts("latest")
```
必ず固定バージョンを指定する。

### ❌ Pin 無視
```python
# これは禁止
resolve_standard_artifacts("0.1.7", skip_pinning_check=True)
```
`skip_pinning_check=True` は upgrade script 以外で使用禁止。

### ❌ Validator fail の握り潰し
```python
# これは禁止
if not validation_result.passed:
    pass  # 無視して続行
```
fail したら必ず `status="failed"` を返す。

### ❌ 旧タグ改変
```bash
# Standard repo でこれは禁止
git tag -f v0.1.7 new-commit
```
タグは不変。改変が発覚したら報告。

## 4. 作業の基本フロー

### 通常の開発作業

```bash
# 1. ブランチ作成
git checkout -b feat/your-feature

# 2. Standard 同期 (pin 検証付き)
python scripts/sync_aimo_standard.py --version 0.1.7

# 3. テスト実行
pytest -q

# 4. E2E 確認 (Evidence Bundle 生成)
# ... your E2E test ...

# 5. PR 作成
git push -u origin HEAD
gh pr create
```

### Standard アップグレード作業

```bash
# 1. Upgrade script 実行
./scripts/upgrade_standard_version.sh --version X.Y.Z

# 2. pinning.py の変更を確認
git diff src/standard_adapter/pinning.py

# 3. テスト実行
pytest -q

# 4. PR 作成 (タイトル例: "chore: upgrade standard to vX.Y.Z")
```

## 5. トラブルシューティング

### "Commit mismatch" エラー

```
StandardPinningError: Commit mismatch
```

**原因**: Standard repo でタグが改変された
**対処**: Standard メンテナに報告。pin は更新しない。

### "Artifacts SHA mismatch" エラー

```
StandardPinningError: Artifacts SHA mismatch
```

**原因**: artifacts の内容が変更された
**対処**: Standard repo のリリースプロセスを確認。

### Validator fail

```
validation_result.json: {"passed": false, "errors": [...]}
```

**原因**: Engine の出力が Standard 仕様と不一致
**対処**: Engine 側を修正。Validator が正。

## 6. 連絡先

- Standard repo: github.com/billyrise/aimo-standard
- Engine repo: github.com/billyrise/aimo_local
- Playbook: docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md

---

**最終更新**: 2026-02-02
**対象 Standard Version**: v0.1.7
