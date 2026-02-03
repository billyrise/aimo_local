# AIMO Standard 0.1.1 準拠 — 変更計画

**目的**: AIMO Analysis Engine を Standard 0.1.1（サイト https://standard.aimoaas.com/0.1.1/）に準拠させる。  
**前提**: 変更一覧は `docs/STANDARD_0.1.1_CHANGE_LIST.md` を参照。

**準拠の正として参照しているもの**: 公開サイト **https://standard.aimoaas.com/0.1.1/** の内容（Taxonomy, Evidence Bundle 構造, 09-evidence-bundle-structure, Validator, EP-01..EP-07, Minimum Evidence 等）を取得済みであり、本計画はこのサイトの仕様に合わせることを前提としている。サイトは参照可能である。

---

## 前提条件・ブロッカー（サイトは見えている；以下はリポ・artifacts の整合のみ）

**リポジトリ**: [billyrise/aimo-standard](https://github.com/billyrise/aimo-standard) — **v0.1.1 が Latest リリース**（2026-02-03）。Releases から `aimo-standard-artifacts.zip` 等が取得可能。

| # | 項目 | 状態 | 備考 |
|---|------|------|------|
| P1 | aimo-standard に **v0.1.1 タグ／リリース**があるか | ✅ 確認済 | [Releases](https://github.com/billyrise/aimo-standard/releases) に v0.1.1 (Latest) が存在。Engine の sync は `--version 0.1.1` でリリース資材を参照可能 |
| P2 | 0.1.1 用 **artifacts**（zip／辞書・スキーマ）が取得できるか | 要確認 | リリースに `aimo-standard-artifacts.zip` がある。`sync_aimo_standard.py --version 0.1.1` が submodule のタグ v0.1.1 と dist/ または Releases から取得する想定で動作するか確認する |
| P3 | 取得する **Taxonomy 辞書**に LG 次元（LG-001..LG-015）が含まれるか | 要確認 | サイトには LG 定義あり。辞書に無い場合は Engine 側で LG を暫定定義して進める |
| P4 | バンドルルート用 **manifest の JSON Schema** が Standard から提供されているか | 要確認 | サイトの 09-evidence-bundle-structure に必須フィールドは記載済み。未提供ならその表を元に Engine で暫定スキーマを定義して検証する |

**注意**: サイト https://standard.aimoaas.com/0.1.1/ は参照可能。**準拠対象はサイト 0.1.1 の仕様**。Engine のピンは **v0.1.1**（[aimo-standard Releases](https://github.com/billyrise/aimo-standard/releases) の v0.1.1 タグ・commit・artifacts）に合わせる。

---

## フェーズ概要

| Phase | 名前 | 目的 | 成果物 | 依存 |
|-------|------|------|--------|------|
| 0 | 事前確認 | ブロッカー解消と方針確定 | 確認メモ・方針メモ | — |
| 1 | バージョン・Taxonomy (EV→LG) | 標準バージョンと 8 次元目を LG に統一 | ピン更新、EV→LG 一括変更、テスト更新 | P1〜P3 |
| 2 | Evidence Bundle ルート構造 | v0.1 必須のルート構造を実装 | manifest.json, objects/, payloads/, signatures/, hashes/ | Phase 1 |
| 3 | Evidence Pack と必須アーティファクト | EP/LG 対応と dictionary / Summary / Change Log | file_id EP-01..EP-07、dictionary、Summary、Change Log | Phase 2 |
| 4 | ドキュメント・検証 | ドキュメント更新と最終検証 | 更新ドキュメント、検証チェックリスト完了 | Phase 3 |

---

## Phase 0: 事前確認

**目標**: 準拠対象バージョンの決定とブロッカー解消。

### タスク

| ID | タスク | 担当目安 | 完了条件 |
|----|--------|----------|----------|
| 0.1 | aimo-standard のタグ一覧を確認し、0.1.1 とサイトの対応関係をメモする | 1h | 採用する Standard バージョンが決まっている |
| 0.2 | `scripts/sync_aimo_standard.py --version 0.1.1` を実行し、artifacts が取得できるか確認する | 0.5h | 取得可能、または「0.1.7 を維持しサイト記載の構造のみ取り入れる」方針が決まっている |
| 0.3 | 取得した（または 0.1.7 の）artifacts 内に taxonomy の **LG** 次元が含まれるか確認する。含まれない場合は「Engine 側で LG を定義する」か「Standard 側のリリースを待つ」かを決める | 0.5h | LG の扱いが決まっている |
| 0.4 | バンドルルート用 manifest のスキーマが Standard から提供されているか確認する。未提供なら「暫定スキーマを Engine で持つ」方針を決める | 0.5h | スキーマの扱いが決まっている |
| 0.5 | 上記を踏まえ、**準拠対象バージョン**（0.1.1 固定か、0.1.7 のまま構造のみ 0.1.1 に合わせるか）を決定し、`docs/STANDARD_0.1.1_CHANGE_LIST.md` の「0.1.7 → 0.1.1」表記を必要に応じて修正する | 0.5h | 変更一覧・計画のバージョン表記が一貫している |

**成果物**: 事前確認メモ（Confluence / 共有ドキュメントまたは `docs/STANDARD_0.1.1_PREREQS.md` に記載推奨）。

---

## Phase 1: バージョン・Taxonomy (EV→LG)

**目標**: 採用バージョンへのピン更新と、Taxonomy の 8 番目を EV から LG へ完全移行。既存テストをすべて通す。

### 1.1 バージョン・定数

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 1.1.1 | デフォルトバージョン定数を更新 | `src/standard_adapter/constants.py` | `AIMO_STANDARD_VERSION_DEFAULT` が採用バージョン |
| 1.1.2 | ピン値を更新（タグ・commit・artifacts SHA） | `src/standard_adapter/pinning.py` | `PINNED_*` が採用バージョンと一致 |
| 1.1.3 | sync スクリプトのデフォルトバージョンを更新 | `scripts/sync_aimo_standard.py` | 先頭のデフォルトが採用バージョン |

### 1.2 Taxonomy: EV → LG（コア）

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 1.2.1 | 次元定義を EV → LG に変更 | `src/standard_adapter/taxonomy.py` | `DIMENSION_CARDINALITY["LG"]`, `ALL_DIMENSIONS` に LG、docstring 更新 |
| 1.2.2 | `validate_assignment` / `get_allowed_codes` 等の引数・キーを `lg_codes` / `"LG"` に | `src/standard_adapter/taxonomy.py` | 全呼び出しが lg_codes / LG で動作 |
| 1.2.3 | 辞書探索で LG 次元を読む（Standard に LG がある場合） | `src/standard_adapter/taxonomy.py` | `_load_taxonomy` が LG を正しく読み、`get_allowed_codes("LG")` が返る |

### 1.3 DB 層

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 1.3.1 | スキーマに `lg_codes_json` を追加（または `ev_codes_json` の意味を LG に統一） | `src/db/schema.sql` | analysis_cache / signature_stats に LG 用カラムまたは解釈が明確 |
| 1.3.2 | 互換レイヤーを LG 対応に | `src/db/compat.py` | `TaxonomyRecord.lg_codes`, `to_dict()["LG"]`, `normalize_*` が ev と lg 両方から読める（後方互換） |
| 1.3.3 | DuckDB クライアントのカラム・キーを LG 対応に | `src/db/duckdb_client.py` | 読み書きが lg_codes_json / lg_code と整合 |
| 1.3.4 | マイグレーション追加（必要なら） | `src/db/migrations.py` | 既存 DB から lg へ移行可能 |

### 1.4 LLM・分類器・ユーティリティ

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 1.4.1 | プロンプトと例を lg_codes / LG に | `src/llm/prompt_templates.py` | ev_codes/EV-* が lg_codes/LG-* に統一 |
| 1.4.2 | 分析出力スキーマを lg_codes に | `src/llm/schemas/`（該当 JSON） | ev_codes → lg_codes |
| 1.4.3 | クライアントのフォールバック・正規化を lg_codes に | `src/llm/client.py` | DB 書き込み・正規化が lg 対応 |
| 1.4.4 | ルール分類器の出力を lg_codes に | `src/classifiers/rule_classifier.py` | ev_codes → lg_codes、EV → LG |
| 1.4.5 | スタブ分類器を lg_codes に | `src/classifiers/stub_classifier.py` | "EV" → "LG", ev_codes → lg_codes |
| 1.4.6 | JSON 正規化を lg 対応に | `src/utils/json_canonical.py` | ev_codes/ev_code → lg_codes/lg_code, "EV" → "LG" |

### 1.5 レポート・メイン

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 1.5.1 | Bundle 生成の集約・出力を LG に | `src/reporting/standard_evidence_bundle_generator.py` | codes["LG"], taxonomy_assignments が LG |
| 1.5.2 | Evidence Pack サマリを LG に | `src/reporting/evidence_pack_generator.py` | 列・キーが lg 対応 |
| 1.5.3 | main の参照を lg に | `src/main.py` | ev_code/ev_codes 参照が lg に |

### 1.6 テスト更新（Phase 1）

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 1.6.1 | バージョン・LG に合わせる | `tests/test_standard_adapter_smoke.py` | get_allowed_codes("LG"), version 0.1.1（または採用版） |
| 1.6.2 | lg_codes / lg_codes_json に合わせる | `tests/test_taxonomy_codes.py` | EV → LG、全テスト通過 |
| 1.6.3 | lg に合わせる | `tests/test_db_compat.py` | ev_codes/ev_code → lg_codes/lg_code |
| 1.6.4 | lg に合わせる | `tests/test_rule_classifier_taxonomy.py` | ev_codes → lg_codes |
| 1.6.5 | lg に合わせる | `tests/test_fallback_code_resolution.py` | ev_codes/EV → lg_codes/LG |
| 1.6.6 | モック応答を lg に | `tests/test_llm_rate_limit_policy.py` | ev_codes → lg_codes |
| 1.6.7 | 契約テストをピン・LG に合わせる | `tests/test_contract_e2e_standard_bundle.py` | ピン・LG 反映、E2E 通過 |

**Phase 1 完了条件**:  
- `pytest tests/` がすべて成功（既存の必須テストを含む）。  
- `sync_aimo_standard.py --version <採用版>` とピン検証が通る。

---

## Phase 2: Evidence Bundle ルート構造（v0.1）

**目標**: バンドルルートに manifest.json + objects/ + payloads/ + signatures/ + hashes/ を用意し、Validator がルート manifest を前提に動作するようにする。

### 2.1 バンドル生成

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 2.1.1 | ルート用 `manifest.json` を生成（bundle_id, bundle_version, created_at, scope_ref, object_index, payload_index, hash_chain, signing） | `src/reporting/standard_evidence_bundle_generator.py` | 必須フィールドがすべて入り、Validator が受け付ける |
| 2.1.2 | `objects/` を作成し、少なくとも index 相当のオブジェクトを配置 | 同上 | objects/ が存在し object_index と一致 |
| 2.1.3 | `payloads/` を作成し、既存の run_manifest / evidence_pack_manifest / logs / analysis 等を配置 | 同上 | 既存成果物が payloads 配下にあり payload_index と一致 |
| 2.1.4 | `signatures/` を作成し、manifest を参照する署名エントリを 1 件以上追加 | 同上 | signing.signatures に targets に manifest.json を含むエントリ、signatures/ にファイル存在 |
| 2.1.5 | `hashes/` を作成し、hash_chain（algorithm, head, covers 等）を満たす | 同上 | hash_chain が manifest.json と objects をカバー |
| 2.1.6 | object_index / payload_index の各エントリに path + sha256 を含める | 同上 | 全エントリに path と sha256（64 文字小 hex） |

### 2.2 Validator

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 2.2.1 | 検証対象を「ルートの manifest.json」に統一 | `src/standard_adapter/validator_runner.py` | ディレクトリ指定時はルート manifest.json を読み、object_index / payload_index / hash_chain / signing を検証 |
| 2.2.2 | バンドルルート用スキーマがあればロードして検証に利用 | `src/standard_adapter/schemas.py` | 必要に応じて bundle_manifest 等のスキーマを追加 |

### 2.3 テスト更新（Phase 2）

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 2.3.1 | 生成物がルート構造を満たすことを検証 | `tests/test_evidence_bundle_generation.py` | manifest.json, objects/, payloads/, signatures/, hashes/ の存在と内容 |
| 2.3.2 | Validator がルート manifest で PASS することを検証 | `tests/test_evidence_bundle_validator_pass.py` | ルート構造を出力するフィクスチャ/モックで Validator 通過 |

**Phase 2 完了条件**:  
- 生成した evidence_bundle が Standard の「Evidence Bundle root structure (v0.1)」を満たす。  
- Validator（Standard CLI またはフォールバック）が当該バンドルで成功する。

---

## Phase 3: Evidence Pack と必須アーティファクト

**目標**: file_id を EP-01..EP-07、ev_type を LG に揃え、dictionary / Summary / Change Log をバンドルに含める。

### 3.1 Evidence Pack 形式

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 3.1.1 | evidence_files[].file_id を EP-01..EP-07 に合わせる | `src/reporting/standard_evidence_bundle_generator.py` | デフォルト EV-01 等を廃止し EP-* に |
| 3.1.2 | ev_type を LG コード（例 LG-001）に合わせるかスキーマに合わせて削除/変更 | 同上 | 標準スキーマと矛盾しない |

### 3.2 必須アーティファクト

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 3.2.1 | Standard から dictionary を取得し payloads に dictionary.json を配置 | `src/reporting/standard_evidence_bundle_generator.py` | payloads に dictionary.json が存在し、payload_index に記載 |
| 3.2.2 | Summary（1 ページ概要）を生成して payloads に含める | 同上 | Summary が payloads にあり index と一致 |
| 3.2.3 | Change Log（または参照）を payloads に含める | 同上 | Change Log が payloads にあり index と一致 |

### 3.3 スキーマ URL

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 3.3.1 | evidence_pack_manifest の $schema を標準推奨 URL に（バージョン付きがあれば使用） | `src/reporting/standard_evidence_bundle_generator.py` | $schema が Standard 推奨に一致 |

**Phase 3 完了条件**:  
- バンドルに dictionary.json / Summary / Change Log が含まれる。  
- Evidence Pack の file_id が EP-01..EP-07、ev_type が LG と整合している。

---

## Phase 4: ドキュメント・最終検証

**目標**: ドキュメントを 0.1.1 準拠と LG・ルート構造に合わせ、全体の検証チェックリストを完了する。

### 4.1 ドキュメント更新

| ID | タスク | ファイル | 完了条件 |
|----|--------|----------|----------|
| 4.1.1 | バージョンとルート構造を記載 | `docs/DEVELOPER_HANDOFF_NATIVE_STANDARD.md` | 0.1.1（または採用版）と manifest/objects/payloads/signatures/hashes を記載 |
| 4.1.2 | 0.1.1 向け手順と EV→LG マイグレーションを追記 | `docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md` | アップグレード手順に 0.1.1 と LG を反映 |
| 4.1.3 | 0.1.1 準拠の変更履歴を追記 | `docs/CHANGELOG_NATIVE_STANDARD.md` | 日付・内容が記録されている |
| 4.1.4 | 0.1.1 ルート構造と LG を反映 | `docs/MIGRATION_NATIVE_STANDARD.md` | 移行手順が現状と一致 |
| 4.1.5 | ピン例を 0.1.1 に（必要なら） | `docs/ADR/ADR-0001-standard-pinning-and-run-reproducibility.md` | 記載がピン方針と一致 |

### 4.2 最終検証

| ID | タスク | 完了条件 |
|----|--------|----------|
| 4.2.1 | フルテストスイート実行 | `pytest tests/` がすべて成功 |
| 4.2.2 | サンプル run で Evidence Bundle を生成し、Validator で検証 | 生成バンドルで Validator が成功 |
| 4.2.3 | 変更一覧・計画の「完了」タスクをチェックし、漏れがないか確認 | 本計画の全 Phase が完了している |

**Phase 4 完了条件**:  
- 上記ドキュメントが更新済み。  
- テスト・Validator による検証がすべて通っている。

---

## リスク・軽減

| リスク | 影響 | 軽減策 |
|--------|------|--------|
| Standard に v0.1.1 タグがない / サイトとバージョン体系が違う | ピン・sync が決められない | Phase 0 で「採用バージョン」を明確にし、必要なら 0.1.7 のまま構造のみ 0.1.1 に合わせる |
| Taxonomy 辞書に LG が無い | EV→LG 移行で辞書検証が使えない | Standard のリリースを待つか、Engine 側で LG 許可リストを暫定定義 |
| バンドルルート manifest のスキーマが無い | Validator の厳密なスキーマ検証ができない | 標準の 09-evidence-bundle-structure の表を元に Engine で暫定スキーマを定義 |
| 既存 DB に ev_codes のみのデータが残る | 読み出しで lg が空になる | compat で ev_codes_json/ev_code を lg_codes にマッピングし後方互換を維持 |

---

## 進捗チェックリスト（コピー用）

```text
Phase 0: [x] 0.1 [x] 0.2 [x] 0.3 [x] 0.4 [x] 0.5
Phase 1: [x] 1.1.1–1.1.3 [x] 1.2.1–1.2.3 [x] 1.3.1–1.3.4 [x] 1.4.1–1.4.6 [x] 1.5.1–1.5.3 [x] 1.6.1–1.6.7
Phase 2: [x] 2.1.1–2.1.6 [x] 2.2.1–2.2.2 [x] 2.3.1–2.3.2
Phase 3: [x] 3.1.1–3.1.2 [x] 3.2.1–3.2.3 [x] 3.3.1
Phase 4: [x] 4.1.1–4.1.5 [x] 4.2.1–4.2.3
```

---

## 参照

- 変更すべき箇所一覧: `docs/STANDARD_0.1.1_CHANGE_LIST.md`
- 標準サイト 0.1.1: https://standard.aimoaas.com/0.1.1/
- Evidence Bundle root structure: https://standard.aimoaas.com/0.1.1/standard/current/09-evidence-bundle-structure/
- Validator: https://standard.aimoaas.com/0.1.1/validator/
