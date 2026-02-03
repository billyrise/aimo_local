# AIMO Standard 0.1.1 準拠 — 変更すべき箇所一覧

## 1. 標準サイト 0.1.1 の主な変更点（理解メモ）

- **構成**: Standard / Artifacts / Validator / Examples / Releases / Governance に再編。
- **Taxonomy**: 8次元・91コード。**EV-** は Evidence *artifact* ID 専用。ログ/イベント種別の次元は **LG** (LG-001..LG-015)。
- **Evidence Bundle ルート構造 (v0.1 必須)**:
  - **manifest.json**（バンドルマニフェスト）: `bundle_id`, `bundle_version`, `created_at`, `scope_ref`, **object_index**, **payload_index**, **hash_chain**, **signing`
  - **objects/** — 列挙オブジェクト（例: index.json）
  - **payloads/** — ペイロード（EV JSON、Evidence Pack ファイル等）
  - **signatures/** — 少なくとも1件の署名が manifest を参照
  - **hashes/** — ハッシュチェーン/整合性記録
- **Evidence Pack**: EP-01..EP-07 がドキュメント種別。Pack は payloads/ 側の概念。Bundle ルートの manifest と Pack マニフェストは別。
- **Validator**: バンドルディレクトリまたは root 用 JSON を入力に、上記ルート構造・スキーマ・辞書一貫性を検証。`--format json` / `sarif` 対応。
- **Minimum Evidence Requirements**: Request / Review・Approval / Exception / Renewal / Change Log / Integrity & Access のライフサイクルで MUST 項目定義。

---

## 2. 変更すべき箇所（ファイル単位・優先度付き）

### 2.1 バージョン・ピン・定数（0.1.7 → 0.1.1 に合わせる場合）

| ファイル | 変更内容 |
|----------|----------|
| `src/standard_adapter/constants.py` | `AIMO_STANDARD_VERSION_DEFAULT = "0.1.7"` → `"0.1.1"`（または採用するバージョン） |
| `src/standard_adapter/pinning.py` | `PINNED_STANDARD_VERSION`, `PINNED_STANDARD_COMMIT`, `PINNED_ARTIFACTS_DIR_SHA256` を 0.1.1 用に更新（Standard リポジトリのタグ・コミット・artifacts 確定後に設定） |
| `scripts/sync_aimo_standard.py` | 先頭の `AIMO_STANDARD_VERSION_DEFAULT = "0.1.7"` → 採用バージョンに統一 |

### 2.2 Taxonomy: EV 次元 → LG 次元（必須）

標準 0.1.1: **EV-** は Evidence アーティファクト ID 専用。8 番目の次元は **LG (Log/Event Type)**。

| ファイル | 変更内容 |
|----------|----------|
| `src/standard_adapter/taxonomy.py` | `DIMENSION_CARDINALITY` の `"EV"` を `"LG"` に変更。`ALL_DIMENSIONS` の `"EV"` → `"LG"`。docstring の "Evidence Type" → "Log/Event Type"。`_load_taxonomy` は Standard 側の辞書が LG を返す前提でそのまま利用可能か確認。 |
| `src/standard_adapter/taxonomy.py` | `validate_assignment` / `get_allowed_codes` 等の引数・キー: `ev_codes` → `lg_codes`, `"EV"` → `"LG"`。 |
| `src/db/schema.sql` | `ev_codes_json` → `lg_codes_json`（または後方互換のため `ev_codes_json` を残しつつ `lg_codes_json` を追加し、意味を LG に寄せる）。`ev_code` 非推奨のまま LG 用に読み替えするか、`lg_code` を追加するか方針決定。 |
| `src/db/compat.py` | `TaxonomyRecord.ev_codes` → `lg_codes`。`to_dict()` の `"EV"` → `"LG"`。`normalize_taxonomy_record` で `ev_codes_json`/`ev_code` を `lg_codes` にマッピング（後方互換）。`is_complete` の `ev_codes` チェック → `lg_codes`。 |
| `src/db/duckdb_client.py` | カラム名・キー: `ev_code` / `ev_codes_json` を LG 用に読み替えまたは `lg_codes_json` に変更。 |
| `src/db/migrations.py` | 新規マイグレーションで `lg_codes_json` 追加、または既存 `ev_codes_json` の意味を LG に統一。 |
| `src/llm/prompt_templates.py` | プロンプト内の `ev_codes` → `lg_codes`。例の `"ev_codes": ["EV-001"]` → `"lg_codes": ["LG-001"]` 等。EV/LG の説明を「EV は artifact ID、LG は Log/Event Type」に合わせる。 |
| `src/llm/schemas/`（分析出力スキーマ） | `ev_codes` → `lg_codes`、EV-* → LG-* の例に更新。 |
| `src/classifiers/rule_classifier.py` | 出力キー `ev_codes` → `lg_codes`。`DEFAULT_*` や EV 参照を LG に。 |
| `src/classifiers/stub_classifier.py` | `ev_codes` / `"EV"` → `lg_codes` / `"LG"`。 |
| `src/llm/client.py` | フォールバック・正規化の `ev_codes` → `lg_codes`。DB 書き込み時のカラム名を `lg_codes_json` 等に合わせる。 |
| `src/utils/json_canonical.py` | `ev_codes` → `lg_codes`、`ev_code` → `lg_code`、`"EV"` → `"LG"`。 |
| `src/reporting/standard_evidence_bundle_generator.py` | `_aggregate_codes` の `"EV"` → `"LG"`。`codes["EV"]` / `ev_codes` 参照を `lg_codes` に。taxonomy_assignments の `codes.EV` → `codes.LG`。 |
| `src/reporting/evidence_pack_generator.py` | 列・キー `ev_code` / `ev_codes` を LG 用に変更または読み替え。 |
| `src/main.py` | `ev_code` / `ev_codes` 参照を `lg_code` / `lg_codes` に合わせる。 |
| `src/reporting/standard_evidence_bundle_generator.py` | Evidence Pack 内の `file_id`: 標準では EP-01..EP-07 がドキュメント種別。`ev_type` は LG コード（例 LG-001）と揃えるか、標準スキーマに合わせて修正。 |

### 2.3 Evidence Bundle ルート構造（v0.1 必須）の実装

現在は `evidence_bundle/` 直下に `run_manifest.json`, `evidence_pack_manifest.json`, `logs/`, `analysis/` 等を置いている。標準 0.1.1 では次が必須:

- ルート **manifest.json**（バンドル用）: `bundle_id`, `bundle_version`, `created_at`, `scope_ref`, `object_index`, `payload_index`, `hash_chain`, `signing`
- **objects/** ディレクトリ
- **payloads/** ディレクトリ
- **signatures/** ディレクトリ（少なくとも 1 件が manifest を参照）
- **hashes/** ディレクトリ

| ファイル | 変更内容 |
|----------|----------|
| `src/reporting/standard_evidence_bundle_generator.py` | (1) `evidence_bundle/` 直下に `manifest.json`（バンドル用）を新規生成: `bundle_id` (UUID), `bundle_version`, `created_at`, `scope_ref`, `object_index`, `payload_index`, `hash_chain`, `signing` を満たす。(2) `objects/`, `payloads/`, `signatures/`, `hashes/` を作成。(3) 既存の `run_manifest.json`, `evidence_pack_manifest.json`, `logs/`, `analysis/` 等は **payloads/** 以下に配置するか、object_index/payload_index で参照する形に変更。(4) `object_index` / `payload_index` の各エントリに `path` と `sha256` を含める。(5) `hash_chain`: `algorithm`, `head`, `covers` 等を定義し、少なくとも manifest.json と objects/index.json（または同等）をカバー。(6) `signing.signatures`: 少なくとも 1 件が `targets` に `manifest.json` を含む。実際の署名ファイルを `signatures/` に配置（v0.1 では「存在と参照」のみで可）。 |
| `src/standard_adapter/validator_runner.py` | バンドル検証時、ルートの **manifest.json**（バンドル manifest）を期待するように変更。`evidence_pack_manifest.json` は payloads 内の一要素として扱う。Validator がディレクトリを渡されたときに、ルートの `manifest.json` を読んで object_index / payload_index / hash_chain / signing をチェックする流れに合わせる。 |
| `src/standard_adapter/schemas.py` | バンドルルート用の JSON スキーマ（manifest.json 用）が Standard から配布されていれば読み込み・検証に利用する。 |

### 2.4 Evidence Pack の位置づけと file_id / ev_type

- 標準: Evidence Pack のドキュメント種別は **EP-01..EP-07**。**EV-** は Evidence アーティファクト ID 用。
- ログ/イベントの分類は **LG-** コード（LG-001 Request Record 等）。

| ファイル | 変更内容 |
|----------|----------|
| `src/reporting/standard_evidence_bundle_generator.py` | `evidence_files[].file_id` を EP-01..EP-07 のいずれか（または標準で許容される形式）に合わせる。`ev_type` を LG コード（例 LG-001）に合わせるか、スキーマに合わせて削除/変更。デフォルトの `"EV-01"` 等は EP-01 等に変更。 |

### 2.5 スキーマ URL と Standard 参照

| ファイル | 変更内容 |
|----------|----------|
| `src/reporting/standard_evidence_bundle_generator.py` | `$schema`: `https://standard.aimoaas.com/schemas/evidence_pack_manifest.schema.json` を、標準で推奨されるバージョン付き URL（例 `.../0.1.1/...`）があればそれに合わせる。バンドル manifest 用のスキーマ URL が Standard に用意されていればそれも利用。 |

### 2.6 Validator の入出力

- 標準: `python validator/src/validate.py examples/evidence_bundle_v01_minimal` のようにディレクトリを渡す。ルートに `manifest.json` がある v0.1 最小構成を期待。

| ファイル | 変更内容 |
|----------|----------|
| `src/standard_adapter/validator_runner.py` | 検証対象を「ルートの manifest.json があるバンドルディレクトリ」に統一。`evidence_pack_manifest.json` のみを探すのではなく、まずルートの `manifest.json` を探し、その上で Standard CLI またはフォールバックで object_index / payload_index / hash_chain / signing を検証する。 |

### 2.7 辞書・Summary・Change Log（Evidence Bundle TOC）

- 標準: Bundle には **dictionary.json**（aimo-dictionary.schema.json）、**Summary**、**Change Log** が必須。

| ファイル | 変更内容 |
|----------|----------|
| `src/reporting/standard_evidence_bundle_generator.py` | (1) Standard から辞書を取得して **payloads/** に `dictionary.json` を配置するか、object_index で参照する。(2) **Summary**（1 ページ概要）を生成して payloads に含める。(3) **Change Log**（またはその参照）を payloads に含める。 |

### 2.8 テストの更新

| ファイル | 変更内容 |
|----------|----------|
| `tests/test_evidence_bundle_validator_pass.py` | バージョンを 0.1.1 に合わせる。バンドル構造を v0.1 ルート構造（manifest.json, objects/, payloads/, signatures/, hashes/）に合わせて生成するようモックまたはフィクスチャを変更。LG 次元の検証に合わせて `ev_codes` → `lg_codes`。 |
| `tests/test_evidence_bundle_generation.py` | 上記同様。生成物にルート manifest / object_index / payload_index / hash_chain / signing が含まれることを検証。 |
| `tests/test_contract_e2e_standard_bundle.py` | ピン 0.1.1、LG 次元、バンドルルート構造の契約を反映。 |
| `tests/test_standard_adapter_smoke.py` | バージョン 0.1.1。`get_allowed_codes("EV", ...)` → `get_allowed_codes("LG", ...)`。 |
| `tests/test_taxonomy_codes.py` | `ev_codes` / `ev_codes_json` → `lg_codes` / `lg_codes_json`。次元名 EV → LG。 |
| `tests/test_db_compat.py` | `ev_codes` / `ev_code` → `lg_codes` / `lg_code`。 |
| `tests/test_rule_classifier_taxonomy.py` | `ev_codes` → `lg_codes`、EV-* → LG-*。 |
| `tests/test_fallback_code_resolution.py` | `ev_codes` / EV フォールバック → `lg_codes` / LG。 |
| `tests/test_llm_rate_limit_policy.py` | モック応答の `ev_codes` → `lg_codes`。 |

### 2.9 ドキュメントの更新

| ファイル | 変更内容 |
|----------|----------|
| `docs/DEVELOPER_HANDOFF_NATIVE_STANDARD.md` | 記載バージョンを 0.1.1 に。Evidence Bundle のルート構造（manifest, objects/, payloads/, signatures/, hashes/）を記載。 |
| `docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md` | 0.1.1 向けの手順に更新。EV→LG のマイグレーションを追記。 |
| `docs/CHANGELOG_NATIVE_STANDARD.md` | 0.1.1 準拠の変更履歴を追記。 |
| `docs/MIGRATION_NATIVE_STANDARD.md` | 0.1.1 のルート構造と LG 次元への移行を反映。 |
| `docs/ADR/ADR-0001-standard-pinning-and-run-reproducibility.md` | ピン 0.1.1 の例に更新（必要なら）。 |

### 2.10 その他

| ファイル | 変更内容 |
|----------|----------|
| `src/orchestrator.py` | コメント・デフォルトの `aimo_standard_version` を 0.1.1 に。 |
| `src/main.py` | Evidence Pack / Standard に関するコメントを 0.1.1 と v0.1 バンドル構造に合わせる。 |
| `report/` 以下 | 監査レポートやサニタイズ仕様で Standard / Evidence を参照している箇所があれば、0.1.1 と LG に言及。 |
| `llm/` 以下 | 分析出力スキーマやプロンプトで `ev_codes` / EV を参照していれば `lg_codes` / LG に統一。 |

---

## 3. 実施順序の提案

1. **Phase 1 — バージョン・Taxonomy (EV→LG)**  
   - 定数・ピンのバージョン更新（Standard の 0.1.1 タグ/artifacts 確定後）。  
   - taxonomy: EV → LG のリネームと DB/API/プロンプト/分類器の一括変更。  
   - 単体・契約テストの更新。

2. **Phase 2 — Evidence Bundle ルート構造**  
   - ルート `manifest.json`（bundle_id, object_index, payload_index, hash_chain, signing）の生成。  
   - `objects/`, `payloads/`, `signatures/`, `hashes/` の作成と既存成果物の payloads 配置。  
   - Validator の入力を「ルート manifest を持つバンドル」に合わせる。

3. **Phase 3 — Evidence Pack と必須アーティファクト**  
   - file_id を EP-01..EP-07、ev_type を LG に合わせる。  
   - dictionary.json / Summary / Change Log の追加。

4. **Phase 4 — ドキュメント・運用**  
   - 上記ドキュメントの更新。  
   - Minimum Evidence Requirements との対応は、必要に応じてライフサイクル別の出力を追加する。

---

## 4. 参照 URL（0.1.1）

- トップ: https://standard.aimoaas.com/0.1.1/
- Taxonomy: https://standard.aimoaas.com/0.1.1/standard/current/03-taxonomy/
- Evidence Bundle: https://standard.aimoaas.com/0.1.1/artifacts/evidence-bundle/
- Evidence Bundle root structure: https://standard.aimoaas.com/0.1.1/standard/current/09-evidence-bundle-structure/
- Minimum Evidence: https://standard.aimoaas.com/0.1.1/artifacts/minimum-evidence/
- EV Template (EP-01..EP-07): https://standard.aimoaas.com/0.1.1/standard/current/06-ev-template/
- Validator: https://standard.aimoaas.com/0.1.1/validator/
