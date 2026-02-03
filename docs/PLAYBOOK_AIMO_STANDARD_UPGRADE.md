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
- v0.1.1 等のタグは決して書き換えない
- commit hash が変わった場合は Standard リポジトリの運用問題として報告する

## 2. アップグレードが必要になるトリガー

| トリガー | 対応レベル | Engine への影響 |
|----------|------------|-----------------|
| Standard Patch (0.1.1 → 0.1.2) | 低 | 後方互換。pin 更新のみ |
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
- [ ] Standard v0.1.1 → vX.Y.Z の差分を説明
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
    "version": "0.1.1",
    "commit": "556fa4ddb1bc...",
    "artifacts_dir_sha256": "02c8f5460290..."
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

## 7. 禁止事項（絶対に守ること）

以下は **絶対に禁止** されている事項です。違反した場合、CI が失敗し、本番デプロイがブロックされます。

### 7.1 pinning 無効化の禁止

| 禁止事項 | 理由 | 例外 |
|----------|------|------|
| `skip_pinning_check=True` の使用 | 監査再現性が失われる | AIMO_ALLOW_SKIP_PINNING=1 設定時のみ（開発検証の短時間のみ許可） |
| `AIMO_ALLOW_SKIP_PINNING=1` を CI に設定 | 品質ゲートが無効になる | **絶対禁止** |
| `AIMO_ALLOW_SKIP_PINNING=1` を本番環境に設定 | 監査耐性が失われる | **絶対禁止** |
| `--skip-pin-check` フラグの常用 | 不一致を見逃す | アップグレードスクリプト実行時のみ |

### 7.2 /latest 追従の禁止

| 禁止事項 | 理由 |
|----------|------|
| Standard リポジトリの `main` ブランチ直接参照 | 監査上の「正」が揺れる |
| `HEAD` や `/latest` へのシンボリックリンク | 実行ごとに結果が変わる可能性 |
| 日次自動更新スクリプト | pinning の意味がなくなる |

### 7.3 タグ書き換えの禁止

| 禁止事項 | 理由 | 対処 |
|----------|------|------|
| v0.1.1 タグの commit hash 変更 | commit mismatch が発生 | Standard リポジトリのメンテナに報告 |
| artifacts zip の内容変更 | SHA mismatch が発生 | 再リリースを依頼 |
| pin 値のみ更新して commit 確認をスキップ | 事故の隠蔽になる | **絶対禁止** |

### 7.4 違反時の動作

```
┌─────────────────────────────────────────────────────────────────────┐
│ skip_pinning_check=True を渡した場合:                                │
│                                                                     │
│ 1. AIMO_ALLOW_SKIP_PINNING=1 が設定されていない → ValueError で即失敗 │
│ 2. AIMO_ALLOW_SKIP_PINNING=1 が設定されている → WARNING 付きで続行   │
│                                                                     │
│ CI では AIMO_ALLOW_SKIP_PINNING を設定しないため、                   │
│ skip_pinning_check=True のコードは必ず失敗する。                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 7.5 正しいアップグレード手順（再掲）

1. 新しいブランチを作成
2. `AIMO_ALLOW_SKIP_PINNING=1 ./scripts/upgrade_standard_version.sh --version X.Y.Z`
3. 新しい commit/SHA を `pinning.py` に記録
4. `AIMO_ALLOW_SKIP_PINNING` を **削除** してテスト実行
5. CI で全テストが通ることを確認
6. PR を作成してレビューを受ける

---

## 8. Triage: When CI fails after bumping AIMO Standard

CIが失敗した時、開発者が"症状から原因へ"最短で辿り着くためのトリアージ表です。

**重要な区別:**
- **🚨 事故系**: Standard運用側の問題。Engine側でpin更新してはならない（原則）
- **✅ 正しい落ち方**: Standardが更新されEngineが追従対応すべき状態
- **⚠️ 境界系**: 原因切り分けが重要。正常か異常かは文脈による

### 8.1 トリアージ表

#### A) 事故系（運用不正 — Engine側でpin更新禁止）

| Severity | Symptom (where it fails) | What it usually means | Quick checks (copy/paste) | Typical fix | Done when |
|:--------:|--------------------------|----------------------|---------------------------|-------------|-----------|
| **P0** | `Commit mismatch: expected X, got Y` in `enforce_pinning()` | Standard側でタグがforce-pushされた（**事故**） | `cd third_party/aimo-standard && git log --oneline -5 v0.1.1` | Standard運用チームに報告。**Engine側でpin更新してはならない** | Standardがタグを修正し、再checkout後にcommitが一致 |
| **P0** | `Artifacts SHA mismatch: expected X, got Y` | 同一versionなのにzipや辞書ファイルが差し替えられた（**事故**） | `python scripts/sync_aimo_standard.py --version 0.1.1` で表示されるSHAを確認 | Standard側でリリース手順を是正・再リリース | 再syncでSHAが `pinning.py` と一致 |
| **P1** | `FileNotFoundError: third_party/aimo-standard/...` or `submodule not initialized` | CIでsubmodulesが取得されていない。checkout設定不備 | `git submodule status` | `.github/workflows/ci.yml` で `submodules: true` を設定。`sync_aimo_standard.py` でdist生成手順確認 | submoduleが存在し、artifacts dirが生成済み |
| **P1** | `KeyError: 'taxonomy'` or `Artifacts directory not found` | `sync_aimo_standard.py` 未実行 or 生成失敗 | `ls -la ~/.cache/aimo/standard/v0.1.1/` | `python scripts/sync_aimo_standard.py --version 0.1.1` を実行 | artifacts dirが存在しschema/辞書が読める |

#### B) 正しい落ち方系（Standard更新 — Engine追従対応が必要）

| Severity | Symptom (where it fails) | What it usually means | Quick checks (copy/paste) | Typical fix | Done when |
|:--------:|--------------------------|----------------------|---------------------------|-------------|-----------|
| **P1** | `validate_assignment failed: code 'XX-999' not in allowed codes` | コードが廃止または定義変更された | `python -c "from src.standard_adapter.taxonomy import TaxonomyAdapter; a=TaxonomyAdapter(); print(a.get_allowed_codes('XX'))"` | `rule_classifier.py`、`stub_classifier.py`、LLMプロンプトのコード参照を更新 | `validate_assignment()`が全コードでpass |
| **P1** | `validate_assignment failed: cardinality violation (expected 1+, got 0)` | Cardinality定義が変更された（例: OB optional→required） | `python -c "from src.standard_adapter.taxonomy import TaxonomyAdapter; a=TaxonomyAdapter(); print(a.get_cardinality_rules())"` | `llm/schemas/analysis_output.schema.json` の minItems/maxItems を更新 | 全dimensionでcardinality検証pass |
| **P1** | `jsonschema.ValidationError` in `standard_evidence_bundle_generator.py` | Evidence schemaが構造変更された（必須フィールド追加等） | `python -c "import pathlib; p=pathlib.Path('~/.cache/aimo/standard/v0.1.1').expanduser(); print('\\n'.join(str(x) for x in p.rglob('*.schema.json')))"` | `reporting/standard_evidence_bundle_generator.py` を新schema構造に合わせて改修 | Bundle生成後のschema validation pass |
| **P2** | `validation_result.json` shows `"status": "failed"` with rule failures | Validatorのルールが追加または厳格化された。生成物の整合性不足 | `python -c "from src.standard_adapter.validator_runner import run_validation; print(run_validation('<bundle_dir>'))"` | 不足フィールドの追加、checksum/manifest計算の見直し | `run_validation()` が `"status": "passed"` を返す |
| **P2** | `FileNotFoundError` or `KeyError` in `TaxonomyAdapter` / `SchemaAdapter` | artifacts内部構造が変更された（パス名・配置変更） | `find ~/.cache/aimo/standard/v0.1.1 -type f -name '*.json'` | `standard_adapter/taxonomy.py`、`schemas.py` の探索パスを更新 | Adapterが全辞書・schemaを正常ロード |
| **P2** | Dimension追加で `KeyError: 'XX_codes'` | 新dimensionが追加された（8次元→9次元等） | `python -c "from src.standard_adapter.taxonomy import TaxonomyAdapter; a=TaxonomyAdapter(); print(a.get_dimensions())"` | DB schema、LLM schema、Bundle生成、Rule分類器に新dimension追加 | 新dimension含めた全次元処理がpass |

#### C) 境界系（原因切り分けが重要）

| Severity | Symptom (where it fails) | What it usually means | Quick checks (copy/paste) | Typical fix | Done when |
|:--------:|--------------------------|----------------------|---------------------------|-------------|-----------|
| **P2** | Contract E2E fails only in CI (ローカルは通る) | submodule未取得、キャッシュdir権限問題、HOME設定違い | `env \| grep -E '^(HOME\|AIMO_\|XDG_)'` (CIログで確認) | CI yml で `HOME`、cache dir 設定を明示。submodules: true 確認 | CIとローカルで同一結果 |
| **P2** | `run_key changed unexpectedly` or cache misses spike | Standard SHA混入による当然の変化（正常）か、SHA計算の不安定（異常） | `python -c "from src.standard_adapter.resolver import resolve_standard_artifacts; r=resolve_standard_artifacts('0.1.1'); print(r.artifacts_dir_sha256)"` | SHA計算のcanonical化、run_key構成レビュー。正常なら「Standard更新で当然」と判断 | run_keyが同一入力で安定 |
| **P3** | Performance regression (validator slow, schema load slow) | artifacts肥大化、探索の総当たりが遅い | `time python -c "from src.standard_adapter.taxonomy import TaxonomyAdapter; TaxonomyAdapter()"` | 探索結果キャッシュ導入、インデックスファイル追加（ただしStandardが正の原則を崩さない） | ロード時間が許容範囲内 |

### 8.2 診断を早くする共通コマンド集

以下のコマンドをコピペして診断に使用してください。

#### 1) Standard解決とpin確認

```bash
# 現在のresolve結果を表示（version, commit, SHA含む）
python -c "from src.standard_adapter.resolver import resolve_standard_artifacts; print(resolve_standard_artifacts('0.1.1'))"
```

#### 2) artifacts SHA表示（sync）

```bash
# sync実行 + SHA確認（pinning.pyの値と比較）
python scripts/sync_aimo_standard.py --version 0.1.1
```

#### 3) artifacts内のschema探索

```bash
# キャッシュ内のschemaファイル一覧を表示
python -c "import pathlib; p=pathlib.Path('~/.cache/aimo/standard/v0.1.1').expanduser(); print('\\n'.join(str(x) for x in p.rglob('*.schema.json')))"

# 辞書ファイル一覧を表示
python -c "import pathlib; p=pathlib.Path('~/.cache/aimo/standard/v0.1.1').expanduser(); print('\\n'.join(str(x) for x in p.rglob('*.csv')))"
```

#### 4) taxonomy許可コード確認

```bash
# 全dimensionの許可コード一覧
python -c "
from src.standard_adapter.taxonomy import TaxonomyAdapter
adapter = TaxonomyAdapter()
for dim in ['FS', 'IM', 'UC', 'DT', 'CH', 'RS', 'LG', 'OB']:
    codes = adapter.get_allowed_codes(dim)
    print(f'{dim}: {len(codes)} codes')
"
```

#### 5) contract E2E（LLM無効＋stub分類）

```bash
# LLM呼び出しなしでエンジン実行。Bundle生成とvalidator実行まで確認
AIMO_DISABLE_LLM=1 AIMO_CLASSIFIER=stub python src/main.py sample_logs/paloalto_sample.csv --vendor paloalto
```

#### 6) validator単体実行

```bash
# 特定のEvidence Bundleディレクトリを検証
python -c "
import sys
from src.standard_adapter.validator_runner import run_validation
result = run_validation(sys.argv[1])
print(result)
" <evidence_bundle_dir>
```

#### 7) submodule状態確認

```bash
# submoduleのチェックアウト状態
git submodule status

# submoduleの最新コミット確認
cd third_party/aimo-standard && git log --oneline -3 && cd ../..
```

#### 8) pinning.py の現在値確認

```bash
# 現在pinされているversion/commit/SHA
grep -E "^PINNED_" src/standard_adapter/pinning.py
```

### 8.3 判断フローチャート

```
CIが失敗した
    │
    ├─ "Commit mismatch" or "SHA mismatch" ?
    │   └─ YES → 🚨 事故系。Standard運用チームに報告。Engine側でpin更新禁止
    │
    ├─ "submodule not initialized" or "FileNotFoundError: third_party/..." ?
    │   └─ YES → CI設定を確認。submodules: true、sync実行
    │
    ├─ "validate_assignment failed" or "cardinality violation" ?
    │   └─ YES → ✅ 正しい落ち方。Taxonomy定義を確認し、Engine側のコード/schema更新
    │
    ├─ "jsonschema.ValidationError" in evidence bundle generator ?
    │   └─ YES → ✅ 正しい落ち方。Evidence schemaを確認し、生成ロジック更新
    │
    ├─ "validation_result.json shows failed" ?
    │   └─ YES → ✅ 正しい落ち方。Validatorが正。生成物をStandardに合わせる
    │
    └─ ローカルは通るがCIだけ落ちる ?
        └─ YES → ⚠️ 環境差異。HOME/cache dir/submodule状態を確認
```

---

**作成日**: 2026-02-02
**適用 Standard Version**: v0.1.1（0.1.1 準拠・EV→LG マイグレーション反映済み）
**次回更新予定**: Standard Major バージョンアップ時
