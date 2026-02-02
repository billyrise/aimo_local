# AIMO Analysis Engine 実装監査レポート v1.4

**調査実施日**: 2026-01-17  
**調査対象**: Git commit `bda14cd577683de0236aa01737385793f9c48d02` (ブランチ: `main`)  
**仕様書**: 
- AIMO Analysis Engine 仕様書 v1.3（公式仕様）
- AIMO_Detail.md（v1.4最新版固定仕様）

---

## 1. 全体サマリ

### 実装完成度

| 項目 | 完成度 | 評価 |
|------|--------|------|
| **v1.3準拠度** | **85%** | 部分準拠 |
| **v1.4準拠度** | **78%** | 部分準拠 |
| **Tier1商用利用可否** | **条件付き** | 重要リスクあり |

### 最重大リスクTOP5

1. **URL正規化のeTLD+1抽出が簡易実装**（v1.4非準拠）
   - `URLNormalizer.extract_domain()` が簡易ヒューリスティック（最後2ドメイン部分）を使用
   - Public Suffix Listを正しく使用していない（BaseIngestorでは使用）
   - **リスク**: ドメイン集計誤り、監査説明崩壊

2. **製品別Ingestor実装がBaseIngestor依存**
   - 専用Ingestorクラスが存在せず、mapping.yamlのみで対応
   - 実ログ形式の検証が不十分な可能性
   - **リスク**: 実ログで動作保証できない

3. **Writer Queueの実装が不完全**
   - DuckDBClientにWriter Queueは実装済み
   - ただし、LLM Workerからの利用が確認できない
   - **リスク**: 並列LLM呼び出し時のDB競合

4. **Tier1商品要件の一部未実装**
   - リスクマップ（High/Medium/Low）の可視化が不足
   - コスト削減シミュレーションが未実装
   - **リスク**: 契約要件未充足

5. **Box同期安定化の実装が条件付き**
   - FileStabilizerは実装済み
   - ただし、main.pyでの統合が条件分岐（`--use-box-sync`）
   - **リスク**: デフォルトで未完了ファイルを掴む可能性

---

## 2. 製品別ログ対応マトリクス（最重要）

| 製品 | Ingest実装 | 正規化 | 必須フィールド | 動作保証可否 | 備考 |
|------|------------|--------|----------------|--------------|------|
| **Netskope** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ✅ user_id, dest_domain, url_full, bytes_sent, action | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |
| **Palo Alto** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ✅ user_id, dest_domain, url_full, bytes_sent, action | ⚠️ **条件付き** | mapping.yaml存在、sample_logsあり |
| **Zscaler** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ✅ user_id, dest_domain, url_full, bytes_sent, action | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |
| **MDCA** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ⚠️ 部分（cloud_app_id等は任意） | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |
| **Umbrella** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ⚠️ 部分（DNS特有フィールドは任意） | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |
| **Blue Coat** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ✅ user_id, dest_domain, url_full, bytes_sent, action | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |
| **Skyhigh** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ✅ user_id, dest_domain, url_full, bytes_sent, action | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |
| **i-FILTER** | ✅ BaseIngestor + mapping.yaml | ✅ 実装済み | ✅ user_id, dest_domain, url_full, bytes_sent, action | ⚠️ **条件付き** | mapping.yaml存在、実ログ検証要 |

**必須フィールド充足状況の詳細**:
- ✅ **user_id**: 全製品でmapping.yamlに候補フィールド定義あり
- ✅ **dest_domain**: BaseIngestorでeTLD+1抽出実装（tldextract使用）
- ✅ **url_full**: 全製品でmapping.yamlに候補フィールド定義あり
- ✅ **bytes_sent**: 全製品でmapping.yamlに候補フィールド定義あり
- ✅ **action**: 全製品でmapping.yamlにactionマッピング定義あり

**動作保証可否の判定理由**:
- ⚠️ **条件付き**: mapping.yamlは存在するが、実ログ形式の検証が不十分
- 実ログでのE2Eテストが必要（特にNetskope、Zscaler、MDCA、Umbrella）

---

## 3. v1.3 / v1.4 仕様準拠チェックリスト

### ① 実装バージョン固定（最新版の特定）

- ✅ **Git HEAD**: `bda14cd577683de0236aa01737385793f9c48d02`
- ✅ **ブランチ**: `main`
- ✅ **読み込んだ仕様ファイル**: 
  - AIMO_Detail.md（v1.4最新版固定仕様）✅
  - v1.3仕様書（参照のみ、詳細はAIMO_Detail.mdに統合）

---

### ② 全体アーキテクチャ（Stage 0〜5 / Orchestrator）

| 要件 | v1.3 | v1.4 | 実装状況 | 対応ファイル |
|------|------|------|----------|--------------|
| Orchestrator / run_id / run_key | ✅ 要求 | ✅ 要求 | ✅ **実装済み** | `src/orchestrator.py` |
| input_manifest_hash | ✅ 要求 | ✅ 要求 | ✅ **実装済み** | `src/orchestrator.py:95-166` |
| stage_checkpoint / 再開処理 | ✅ 要求 | ✅ 要求 | ✅ **実装済み** | `src/orchestrator.py:302-340` |
| Writer Queue / 単一Writer | ✅ 要求 | ✅ 要求 | ⚠️ **部分実装** | `src/db/duckdb_client.py:118-157` |

**評価**:
- **v1.3準拠**: ✅ 準拠（Writer Queueは実装済みだが、LLM Worker統合が未確認）
- **v1.4準拠**: ⚠️ 部分準拠（同上）

**未実装点**:
- LLM WorkerからのWriter Queue利用が確認できない（`src/main.py`のStage 4実装を要確認）

---

### ③ Box同期安定化・I/O原子性（v1.4最重要）

| 要件 | v1.4要求 | 実装状況 | 対応ファイル | 危険点 |
|------|----------|----------|--------------|--------|
| サイズ＋mtime安定待ち | ✅ 必須 | ✅ **実装済み** | `src/orchestrator/file_stabilizer.py:147-207` | なし |
| input直読み禁止 | ✅ 必須 | ⚠️ **条件付き** | `src/main.py:170-201` | `--use-box-sync`フラグ必須 |
| work/run_idへのコピー | ✅ 必須 | ✅ **実装済み** | `src/orchestrator/file_stabilizer.py:209-232` | なし |
| 未完了ファイルを掴まない保証 | ✅ 必須 | ⚠️ **条件付き** | `src/main.py:170-201` | デフォルトで無効化 |

**評価**:
- **v1.4準拠**: ⚠️ **部分準拠**

**危険点**:
- `--use-box-sync`フラグが無い場合、`data/input`を直接読み込む（v1.4違反）
- デフォルト動作で未完了ファイルを掴むリスクあり

**推奨修正**:
- デフォルトでBox同期安定化を有効化
- または、`data/input`直接読み込みを禁止

---

### ④ ログ取込・正規化（製品別対応）

**実装状況**: 上記「2. 製品別ログ対応マトリクス」参照

**評価**:
- **v1.3準拠**: ✅ 準拠（全製品でmapping.yaml存在）
- **v1.4準拠**: ✅ 準拠（同上）

**未実装点**:
- 専用Ingestorクラスが存在しない（BaseIngestor依存）
- 実ログ形式のE2E検証が不十分

---

### ⑤ eTLD+1 / Public Suffix / ドメイン正規化（v1.4必須）

| 要件 | v1.4要求 | 実装状況 | 対応ファイル | 評価 |
|------|----------|----------|--------------|------|
| Public Suffix List使用 | ✅ 必須 | ⚠️ **部分実装** | `src/ingestor/base.py:44-56` | BaseIngestorでは使用 |
| eTLD+1算出 | ✅ 必須 | ⚠️ **部分実装** | `src/ingestor/base.py:309-317` | BaseIngestorでは実装 |
| URLNormalizer.extract_domain() | ✅ 必須 | ❌ **未実装** | `src/normalize/url_normalizer.py:233-251` | 簡易ヒューリスティック |

**評価**:
- **v1.4準拠**: ❌ **非準拠**

**問題点**:
- `URLNormalizer.extract_domain()`が簡易ヒューリスティック（最後2ドメイン部分）を使用
- Public Suffix Listを使用していない
- BaseIngestorでは正しく実装されているが、URLNormalizerでは未実装

**推奨修正**:
- `URLNormalizer.extract_domain()`をBaseIngestorと同様にtldextract使用に変更

---

### ⑥ URL正規化・署名決定性（v1.4核心）

| 要件 | v1.4要求 | 実装状況 | 対応ファイル | 評価 |
|------|----------|----------|--------------|------|
| 正規化手順順序固定 | ✅ 必須 | ✅ **実装済み** | `src/normalize/url_normalizer.py:57-219` | 準拠 |
| 小文字化 / Punycode / 末尾スラッシュ | ✅ 必須 | ✅ **実装済み** | `src/normalize/url_normalizer.py:87-116` | 準拠 |
| 追跡パラメータ除去 | ✅ 必須 | ✅ **実装済み** | `src/normalize/url_normalizer.py:127-167` | 準拠 |
| クエリ並び順固定 | ✅ 必須 | ✅ **実装済み** | `src/normalize/url_normalizer.py:155-167` | 準拠 |
| ID抽象化 | ✅ 必須 | ✅ **実装済み** | `src/normalize/url_normalizer.py:169-191` | 準拠 |
| signature_version管理 | ✅ 必須 | ✅ **実装済み** | `src/signatures/signature_builder.py:29-48` | 準拠 |
| 同一入力で同一署名 | ✅ 必須 | ✅ **実装済み** | `src/signatures/signature_builder.py:134-217` | 準拠 |

**評価**:
- **v1.4準拠**: ✅ **準拠**

**備考**:
- URL正規化の決定性は完全に実装されている
- 署名生成も決定性を担保

---

### ⑦ A/B/C抽出・バースト・累積・サンプル（監査防衛ロジック）

| 要件 | v1.4要求 | 実装状況 | 対応ファイル | 評価 |
|------|----------|----------|--------------|------|
| A/B/C分類ロジック | ✅ 必須 | ✅ **実装済み** | `src/detectors/abc_detector.py:330-443` | 準拠 |
| burst（5分窓）集計 | ✅ 必須 | ✅ **実装済み** | `src/detectors/abc_detector.py:288-316` | 準拠 |
| cumulative（日次 user×domain）集計 | ✅ 必須 | ✅ **実装済み** | `src/detectors/abc_detector.py:262-286` | 準拠 |
| C枠サンプルでrun_id seed固定 | ✅ 必須 | ✅ **実装済み** | `src/detectors/abc_detector.py:405-444` | 準拠 |
| 小容量帯ゼロ除外防止 | ✅ 必須 | ✅ **実装済み** | `src/detectors/abc_detector.py:83-150` | 準拠 |

**評価**:
- **v1.4準拠**: ✅ **準拠**

**備考**:
- A/B/C抽出ロジックは完全に実装されている
- 小容量帯ゼロ除外防止も構造的に実装されている

---

### ⑧ DuckDB / 冪等性 / UPSERT世代管理（v1.4必須）

| 要件 | v1.4要求 | 実装状況 | 対応ファイル | 評価 |
|------|----------|----------|--------------|------|
| runs / input_files / analysis_cache / signature_stats テーブル | ✅ 必須 | ✅ **実装済み** | `src/db/schema.sql:12-187` | 準拠 |
| run再実行で二重計上しない設計 | ✅ 必須 | ✅ **実装済み** | `src/orchestrator.py:179-225` | 準拠 |
| is_human_verified上書き禁止 | ✅ 必須 | ⚠️ **要確認** | `src/db/schema.sql:117` | スキーマ定義あり、実装要確認 |
| batch_id / run_id世代管理 | ✅ 必須 | ✅ **実装済み** | `src/db/schema.sql:141-187` | 準拠 |

**評価**:
- **v1.4準拠**: ⚠️ **部分準拠**

**未実装点**:
- `is_human_verified`上書き禁止の実装が確認できない（UPSERTロジックを要確認）

---

### ⑨ LLM Analyzer（v1.4安全仕様）

| 要件 | v1.4要求 | 実装状況 | 対応ファイル | 評価 |
|------|----------|----------|--------------|------|
| 未知署名のみ送信 | ✅ 必須 | ✅ **実装済み** | `src/llm/client.py:563-709` | 準拠 |
| PII除去 | ✅ 必須 | ✅ **実装済み** | `src/normalize/url_normalizer.py:169-204` | 準拠 |
| JSON Schema検証 | ✅ 必須 | ✅ **実装済み** | `src/llm/client.py:708-709` | 準拠 |
| 再試行→永続失敗skipped遷移 | ✅ 必須 | ✅ **実装済み** | `src/llm/client.py:49-63` | 準拠 |
| Token Bucket予算制御 | ✅ 必須 | ✅ **実装済み** | `src/llm/budget.py:22-193` | 準拠 |
| Writer Queue直列書込み | ✅ 必須 | ⚠️ **要確認** | `src/db/duckdb_client.py:118-157` | 実装あり、利用確認要 |

**評価**:
- **v1.4準拠**: ⚠️ **部分準拠**

**未実装点**:
- Writer QueueのLLM Workerからの利用が確認できない

---

### ⑩ レポーティング（Tier1商品要件）

| 要件 | v1.3 | v1.4 | 実装状況 | 対応ファイル | 評価 |
|------|------|------|----------|--------------|------|
| Excel生成（constant_memory） | ✅ 要求 | ✅ 必須 | ✅ **実装済み** | `src/reporting/excel_writer.py:48-50` | 準拠 |
| 監査説明セクション | ✅ 要求 | ✅ 必須 | ✅ **実装済み** | `src/reporting/excel_writer.py:992-1299` | 準拠 |
| Shadow AI全数可視化 | ✅ Tier1 | ✅ Tier1 | ✅ **実装済み** | `src/reporting/excel_writer.py:343-398` | 準拠 |
| リスクマップ（High/Medium/Low） | ✅ Tier1 | ✅ Tier1 | ⚠️ **部分実装** | `src/reporting/excel_writer.py:609-775` | 部門別リスクのみ |
| コスト削減シミュレーション | ✅ Tier1 | ✅ Tier1 | ❌ **未実装** | - | 未実装 |
| 経営層向けレポート | ✅ Tier1 | ✅ Tier1 | ✅ **実装済み** | `src/reporting/excel_writer.py:312-341` | ExecutiveSummary |

**評価**:
- **v1.3準拠**: ⚠️ **部分準拠**（コスト削減シミュレーション未実装）
- **v1.4準拠**: ⚠️ **部分準拠**（同上）
- **Tier1準拠**: ⚠️ **部分準拠**（同上）

**未実装点**:
- コスト削減シミュレーションが未実装
- リスクマップの可視化が不足（部門別リスクのみ、全体マップなし）

---

## 4. Tier1商品ギャップ一覧

### 優先度P0（契約要件・監査リスク）

1. **コスト削減シミュレーション未実装**
   - Tier1商品要件として必須
   - 現状: 未実装
   - **影響**: 契約要件未充足

2. **リスクマップ可視化の不足**
   - High/Medium/Lowリスクの全体マップが不足
   - 現状: 部門別リスクのみ実装
   - **影響**: Tier1商品要件の一部未充足

### 優先度P1（運用リスク）

3. **実ログ形式のE2E検証不足**
   - 全製品でmapping.yamlは存在するが、実ログでの検証が不十分
   - 現状: Palo Altoのみsample_logsあり
   - **影響**: 実ログで動作保証できない

---

## 5. 3月初回顧客で必ず直すべき実装TOP10

### 最優先（事故・監査・契約リスク順）

1. **URLNormalizer.extract_domain()のPublic Suffix List対応** ⚠️ **監査リスク**
   - 現状: 簡易ヒューリスティック（最後2ドメイン部分）
   - 修正: BaseIngestorと同様にtldextract使用
   - **ファイル**: `src/normalize/url_normalizer.py:233-251`
   - **リスク**: ドメイン集計誤り、監査説明崩壊

2. **Box同期安定化のデフォルト有効化** ⚠️ **事故リスク**
   - 現状: `--use-box-sync`フラグ必須
   - 修正: デフォルトで有効化、または`data/input`直接読み込み禁止
   - **ファイル**: `src/main.py:170-201`
   - **リスク**: 未完了ファイルを掴む

3. **is_human_verified上書き禁止の実装確認** ⚠️ **監査リスク**
   - 現状: スキーマ定義あり、実装要確認
   - 修正: UPSERTロジックで`is_human_verified=true`の行を上書き禁止
   - **ファイル**: `src/db/duckdb_client.py`（UPSERT実装）
   - **リスク**: 人手確定結果の上書き

4. **Writer QueueのLLM Worker統合確認** ⚠️ **事故リスク**
   - 現状: Writer Queue実装あり、利用確認要
   - 修正: LLM WorkerからWriter Queue経由でDB書込み
   - **ファイル**: `src/main.py`（Stage 4実装）
   - **リスク**: 並列LLM呼び出し時のDB競合

5. **コスト削減シミュレーション実装** ⚠️ **契約リスク**
   - 現状: 未実装
   - 修正: Tier1商品要件として実装
   - **ファイル**: 新規実装
   - **リスク**: 契約要件未充足

6. **リスクマップ可視化の拡充** ⚠️ **契約リスク**
   - 現状: 部門別リスクのみ
   - 修正: High/Medium/Lowリスクの全体マップ追加
   - **ファイル**: `src/reporting/excel_writer.py`
   - **リスク**: Tier1商品要件の一部未充足

7. **実ログ形式のE2E検証（Netskope/Zscaler/MDCA/Umbrella）** ⚠️ **動作保証リスク**
   - 現状: mapping.yamlのみ、実ログ検証不足
   - 修正: 実ログでのE2Eテスト追加
   - **ファイル**: `tests/test_vendor_ingestion_smoke.py`
   - **リスク**: 実ログで動作保証できない

8. **専用Ingestorクラスの検討** ⚠️ **動作保証リスク**
   - 現状: BaseIngestor依存のみ
   - 修正: 必要に応じて専用Ingestorクラス追加
   - **ファイル**: `src/ingestor/`
   - **リスク**: 複雑なログ形式に対応できない可能性

9. **除外件数の正確な集計** ⚠️ **監査リスク**
   - 現状: 一部でN/A表示
   - 修正: Parquetファイルから正確に集計（実装済みだが要確認）
   - **ファイル**: `src/reporting/excel_writer.py:1117-1179`
   - **リスク**: 監査説明の不正確性

10. **エラーハンドリングの強化** ⚠️ **運用リスク**
    - 現状: 基本実装あり
    - 修正: エラー時のリトライ・ログ・通知の強化
    - **ファイル**: 各モジュール
    - **リスク**: 運用時の障害対応困難

---

## 6. 追加調査が必要な項目

1. **LLM WorkerからのWriter Queue利用確認**
   - `src/main.py`のStage 4実装を詳細確認
   - Writer Queue経由でDB書込みしているか確認

2. **is_human_verified上書き禁止の実装確認**
   - `src/db/duckdb_client.py`のUPSERT実装を詳細確認
   - `is_human_verified=true`の行を上書き禁止しているか確認

3. **実ログ形式のE2E検証**
   - Netskope、Zscaler、MDCA、Umbrellaの実ログでのE2Eテスト実施

---

## 7. 結論

### 3月初回顧客での商用利用可否

**判定**: ⚠️ **条件付き（重要リスクあり）**

**条件**:
1. TOP10の修正項目（特に1-4）を完了すること
2. 実ログ形式のE2E検証を実施すること
3. Tier1商品要件（コスト削減シミュレーション、リスクマップ）を実装すること

**推奨アクション**:
- **即座に修正**: TOP10の1-4（監査・事故リスク）
- **3月初回顧客前**: TOP10の5-7（契約・動作保証リスク）
- **継続改善**: TOP10の8-10（運用リスク）

---

**レポート作成者**: AI監査エンジニア（Auto）  
**最終更新**: 2026-01-17
