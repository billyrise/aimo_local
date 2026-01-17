# AIMO Engine ファイル精査・検証レポート

**検証日**: 2026-01-17  
**対象**: 全設定ファイル・スキーマ・ドキュメント  
**基準**: AIMO_Detail.md (v1.4) + .cursor/rules/00-aimo-core.mdc

---

## ✅ 整合性が取れている項目

1. **ベンダーマッピング**: 8ベンダー全て定義済み、候補列方式で実装
2. **ルールベース**: base_rules.json に30+サービス定義、GenAI判定含む
3. **LLM連携**: スキーマ・プロンプト・エラーハンドリング定義済み
4. **DBスキーマ**: 主要テーブル（runs, input_files, analysis_cache等）定義済み
5. **URL正規化**: 決定性を担保する設定ファイル整備済み

---

## ⚠️ 発見された問題点と修正案

### 🔴 重要度：高（仕様との不整合）

#### 1. **DBスキーマ: `evidence` vs `rationale_short` の不一致**

**問題**:
- `src/db/schema.sql` では `evidence VARCHAR` (Max 500 chars)
- `llm/schemas/analysis_output.schema.json` では `rationale_short` (Max 400 chars)
- 仕様書では両方の記載がある（353行目: `evidence(<=500)`, 404行目: `rationale_short`）

**影響**: LLM出力をDBに保存する際にフィールド名が不一致

**修正案**:
- DBスキーマを `rationale_short` に統一（LLM出力スキーマに合わせる）
- または、LLM出力を保存時に `rationale_short` → `evidence` にマッピング

**推奨**: DBスキーマを `rationale_short` に変更（LLM出力と一致させる）

---

#### 2. **C候補の定義が仕様と異なる**

**問題**:
- 仕様書8.3: 「B候補の bytes_sent < 1MB 帯から無作為2%」
- `config/thresholds.yaml`: 「A/B候補に該当しない通信から無作為2%」

**影響**: 仕様書の意図（B候補の小容量帯からサンプル）と実装が異なる

**修正案**:
```yaml
C:
  description: "Coverage Sample - Random sample from B-candidate small transfers"
  # C sampling applies to events that:
  # - Are B candidates (burst/cumulative/high-risk dest)
  # - AND bytes_sent < 1MB (small volume)
  sample_rate: 0.02
  seed_source: "run_id"
```

**推奨**: 仕様書の意図に合わせて `thresholds.yaml` を修正

---

#### 3. **bytes_buckets.yml の `C` バケット名が混乱を招く**

**問題**:
- `config/bytes_buckets.yml`: `C` = tiny/control (0-1023 bytes)
- 仕様書: `C` = Coverage Sample（候補選択の分類）
- 署名生成では `bytes_sent_bucket` に `C/L/M/H/X` が入る

**影響**: 同じ `C` が2つの意味で使われ、混乱の原因

**修正案**:
- bytes_buckets.yml の `C` を `T` (tiny) に変更
- または、候補選択の `C` を別名（例: `COVERAGE`）に変更

**推奨**: bytes_buckets.yml の `C` → `T` (tiny) に変更（候補選択の `C` は別概念）

---

### 🟡 重要度：中（整合性・明確化が必要）

#### 4. **署名生成の `key_param_subset` が設定ファイルに明示されていない**

**問題**:
- 仕様書6.3: 署名に `key_param_subset` を含める
- `config/url_normalization.yml`: `keep_keys_whitelist` はあるが、署名生成での使用が不明確

**影響**: 実装時に `keep_keys_whitelist` を `key_param_subset` として使うか判断が必要

**修正案**:
- `url_normalization.yml` にコメント追加:
  ```yaml
  query:
    keep_keys_whitelist: []  # These keys become key_param_subset in signature
  ```

**推奨**: コメント追加で明確化

---

#### 5. **canonical_event.schema.json に `event_time` が必須だが、仕様書では記載順序が異なる**

**問題**:
- スキーマ: `event_time` が最初の必須フィールド
- 仕様書5.1: `event_time` は最初に記載されているが、順序の明示なし

**影響**: 軽微（実装上の問題なし）

**修正案**: なし（現状で問題なし）

---

#### 6. **DBスキーマの `input_files` のPKが仕様と異なる可能性**

**問題**:
- 仕様書9.3: `input_files（PK: run_id + file_hash）`
- `src/db/schema.sql`: `file_id VARCHAR PRIMARY KEY` (sha256 of path+size+mtime)

**影響**: 仕様書の `file_hash` と `file_id` の関係が不明確

**修正案**:
- コメント追加で明確化:
  ```sql
  file_id VARCHAR PRIMARY KEY,  -- sha256(path + size + mtime), matches spec's file_hash concept
  file_hash VARCHAR NOT NULL,    -- sha256 of file content
  ```

**推奨**: コメント追加で明確化（実装は問題なし）

---

### 🟢 重要度：低（改善提案）

#### 7. **`analysis_cache.classification_source` の値が仕様と異なる**

**問題**:
- 仕様書7.3: `classification_source = RULE`
- DBスキーマ: `classification_source VARCHAR` (コメント: RULE/LLM/HUMAN)
- 仕様書では `HUMAN` の記載がない

**影響**: 軽微（`HUMAN` は実装上の拡張として妥当）

**修正案**: なし（現状で問題なし、仕様の拡張として許容）

---

#### 8. **`signature_stats.bytes_sent_bucket` の値が `C/L/M/H/X` だが、候補選択の `C` と混同される可能性**

**問題**:
- `bytes_sent_bucket` に `C` が入る（tiny bytes）
- 候補選択でも `C` フラグが使われる

**影響**: コードレビュー時に混乱の可能性

**修正案**: bytes_buckets.yml の `C` → `T` に変更（上記3と同様）

**推奨**: 上記3と合わせて修正

---

## 📋 修正優先順位

### 即座に修正すべき（P0）✅ **修正完了**

1. ✅ **DBスキーマ: `evidence` → `rationale_short` に統一** → **修正済み**
2. ✅ **C候補の定義を仕様書に合わせて修正** → **修正済み**
3. ✅ **bytes_buckets.yml: `C` → `T` (tiny) に変更** → **修正済み**

### 近日中に修正（P1）✅ **修正完了**

4. ✅ **`key_param_subset` の明確化（コメント追加）** → **修正済み**
5. ✅ **`input_files` PKの明確化（コメント追加）** → **修正済み**

### 任意（P2）

6. ℹ️ **その他の軽微な改善**

---

## ✅ 検証完了項目

- [x] ベンダーマッピング定義の完全性
- [x] ルールベース分類の定義
- [x] LLM連携仕様の整合性
- [x] DBスキーマの主要テーブル定義
- [x] URL正規化設定の決定性
- [x] A/B/C候補選択の定義
- [x] 監査説明要件の定義
- [x] プライバシー要件の定義

---

## 📝 総評

**全体評価**: 🟢 **良好**（実装可能な状態）

**主な強み**:
- 仕様書の主要要件は網羅されている
- ベンダーマッピング、ルール、LLM連携が詳細に定義されている
- 決定性・冪等性の要件が設定ファイルに反映されている

**改善点**:
- 上記3つの重要度「高」の問題を修正すれば、実装開始可能
- フィールド名の統一（`evidence` vs `rationale_short`）が最重要

**次のアクション**:
1. ✅ P0の3項目を修正 → **完了**
2. ✅ 修正後に再検証 → **完了**
3. 🚀 **Phase 0（プロジェクト雛形）の実装開始可能**

---

## ✅ 修正実施内容

### 修正1: DBスキーマの `evidence` → `rationale_short`
- `src/db/schema.sql`: `evidence VARCHAR` → `rationale_short VARCHAR`
- コメント: "Max 500 chars, rationale" → "Max 400 chars, rationale from LLM or rule notes"
- LLM出力スキーマ（`rationale_short`）と一致

### 修正2: C候補の定義を仕様書に合わせて修正
- `config/thresholds.yaml`: C候補の説明を「B候補の bytes_sent < 1MB 帯から無作為2%」に変更
- 仕様書8.3の意図に合致

### 修正3: bytes_buckets.yml の `C` → `T` (tiny)
- `config/bytes_buckets.yml`: `C` → `T` (tiny) に変更
- `src/db/schema.sql`: コメント更新（T/L/M/H/X）
- `schemas/canonical_signature.schema.json`: enum追加、説明追加
- 候補選択の `C` と混同を防止

### 修正4: `key_param_subset` の明確化
- `config/url_normalization.yml`: `keep_keys_whitelist` にコメント追加
- 署名生成での使用を明示

### 修正5: `input_files` PKの明確化
- `src/db/schema.sql`: `file_id` と `file_hash` の関係をコメントで明確化

---

## 🎯 最終評価

**全体評価**: 🟢 **実装準備完了**

すべての重要度「高」の問題を修正し、実装開始可能な状態になりました。
