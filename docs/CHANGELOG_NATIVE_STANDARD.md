# CHANGELOG: Native AIMO Standard v0.1.7 Integration

**開始日**: 2026-02-02  
**ブランチ**: `feat/native-standard-v0.1.7`  
**目的**: AIMO Standard v0.1.7（英語版）を唯一の正として、Analysis Engineの出力・DB・キャッシュ・Evidenceを無矛盾化する

---

## 1. 改修の狙い

### 1.1 Standard固定による再現性・監査耐性

- AIMO Standard v0.1.7（英語版）を**唯一の正**として採用
- `/latest` を正として扱わない（バージョン固定方式）
- Runごとに参照Standardを `runs` テーブルに記録し、再現性と監査耐性を維持
- Standard成果物は submodule + 展開キャッシュで管理（巨大バイナリはリポジトリにコミットしない）

### 1.2 Taxonomy体系の刷新

- **`fs_uc_code` の廃止**: v0.1.7では廃止予定のため、新しい体系に移行
- **コード体系の配列化**: 単一コードから配列形式への移行（複数分類対応）
- **Evidence構造の刷新**: v0.1.7仕様に準拠した新しいEvidence Pack形式

### 1.3 出力・DB・キャッシュの整合性

- DB スキーマを v0.1.7 に準拠した形式に更新
- キャッシュエントリに `standard_version` を追加
- 出力フォーマット（Excel/JSON/Evidence Pack）を v0.1.7 仕様に統一

---

## 2. 破壊的変更点（BREAKING CHANGES）

### 2.1 Taxonomy Code体系

| 項目 | 変更前（v1.4/v1.5） | 変更後（v0.1.7準拠） |
|------|---------------------|----------------------|
| `fs_uc_code` | 単一文字列 | **廃止** → 新体系に移行 |
| Taxonomy codes | 単一値 | **配列化**（複数分類対応） |
| `taxonomy_version` | "1.0" 固定 | Standard version参照 |

### 2.2 Evidence Pack構造

| 項目 | 変更前 | 変更後 |
|------|--------|--------|
| Evidence形式 | 独自形式 | v0.1.7 Evidence Spec準拠 |
| メタデータ | 最小限 | Standard version, 署名方式等を完全記録 |

### 2.3 DB スキーマ変更

- `analysis_cache` テーブル: `fs_uc_code` カラムを廃止、新コード体系カラム追加
- `signature_stats` テーブル: 同上
- `runs` テーブル: `aimo_standard_version` カラム追加

### 2.4 LLM出力スキーマ

- `llm/schemas/analysis_output.schema.json`: v0.1.7準拠に更新
- プロンプトテンプレート: 新Taxonomy体系を反映

---

## 3. 主要ファイル一覧（改修対象）

### 3.1 DB・スキーマ関連

| ファイル | 改修内容 |
|----------|----------|
| `src/db/schema.sql` | Taxonomy列の刷新、`aimo_standard_version` 追加 |
| `src/db/duckdb_client.py` | 新スキーマ対応、マイグレーション処理 |
| `schemas/canonical_event.schema.json` | v0.1.7準拠のフィールド追加 |
| `schemas/canonical_signature.schema.json` | Taxonomy体系の更新 |

### 3.2 分類・LLM関連

| ファイル | 改修内容 |
|----------|----------|
| `src/classifiers/rule_classifier.py` | 新Taxonomy体系対応、`fs_uc_code` 廃止 |
| `src/llm/prompt_templates.py` | v0.1.7 Taxonomy用プロンプト |
| `src/llm/client.py` | Standard version参照ロジック |
| `llm/schemas/analysis_output.schema.json` | v0.1.7準拠のスキーマ |
| `rules/base_rules.json` | 新Taxonomy体系の適用 |
| `rules/rule.schema.json` | 新Taxonomy体系のスキーマ定義 |

### 3.3 レポーティング・Evidence関連

| ファイル | 改修内容 |
|----------|----------|
| `src/reporting/evidence_pack_generator.py` | v0.1.7 Evidence形式への刷新 |
| `src/reporting/excel_writer.py` | 新Taxonomy列の出力 |
| `src/reporting/report_builder.py` | Standard version表示 |
| `report/dashboard_output.schema.json` | 出力スキーマの更新 |

### 3.4 オーケストレーション・コア

| ファイル | 改修内容 |
|----------|----------|
| `src/orchestrator.py` | Standard version固定ロジック |
| `src/main.py` | Standard version引数追加 |
| `src/signatures/signature_builder.py` | 署名にStandard version含有 |

### 3.5 設定・構成ファイル

| ファイル | 改修内容 |
|----------|----------|
| `config/thresholds.yaml` | Standard version参照設定 |
| `pyproject.toml` | Standard submodule依存追加（予定） |

### 3.6 テスト

| ファイル | 改修内容 |
|----------|----------|
| `tests/test_rule_classifier.py` | 新Taxonomy体系テスト |
| `tests/test_taxonomy_codes.py` | `fs_uc_code` 廃止に伴う更新 |
| `tests/test_llm_gemini_schema_validation.py` | 新スキーマバリデーション |
| `tests/test_signature_stability.py` | Standard version含有テスト |

---

## 4. 実装フェーズ

### Phase 1: 基盤整備
- [ ] Standard v0.1.7 仕様の詳細分析
- [ ] DB マイグレーション設計
- [ ] 新Taxonomy体系の定義

### Phase 2: スキーマ・DB更新
- [ ] `schema.sql` の更新
- [ ] JSON Schema の更新
- [ ] マイグレーションスクリプト作成

### Phase 3: 分類ロジック更新
- [ ] `rule_classifier.py` の改修
- [ ] LLMプロンプトの更新
- [ ] ルールファイルの移行

### Phase 4: Evidence Pack刷新
- [ ] 新Evidence形式の実装
- [ ] レポート出力の更新

### Phase 5: テスト・検証
- [ ] 全テストの更新
- [ ] 回帰テスト実行
- [ ] 監査証跡の検証

---

## 5. 注意事項

### 5.1 後方互換性

- 既存のキャッシュエントリは `fs_uc_code` を含む可能性あり
- マイグレーション時に既存データを新形式に変換
- `is_human_verified=true` のエントリは変換時も保護

### 5.2 Standard管理

- Engine repoにPDF等の巨大バイナリをコミットしない
- Standard成果物は submodule + 展開キャッシュで扱う
- Run実行時にStandard versionを `runs` テーブルに記録

### 5.3 テスト要件

- 変更は必ずテスト追加・更新とセットで行う
- 署名安定性テストは必須
- 冪等性テストは必須

---

## 6. 参考資料

- AIMO Standard v0.1.7（英語版）: [TBD: submodule path]
- Engine仕様書 v1.5: `AIMO_Analysis_Engine_Specification_v1.5.md`
- 実装ステータス: `IMPLEMENTATION_STATUS.md`

---

**最終更新**: 2026-02-02
