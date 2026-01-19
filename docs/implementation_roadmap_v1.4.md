# AIMO Analysis Engine - 実装ロードマップ v1.4

**作成日**: 2026-01-17  
**基準**: AIMO_Detail.md (v1.4)  
**目的**: 現在の実装状況と仕様書v1.4の差分を分析し、今後の実装優先順位を明確化

---

## 📊 実装状況サマリ

### ✅ 実装完了（Phase 0-6の大部分）

| 項目 | 実装状況 | 備考 |
|------|---------|------|
| **URL正規化** | ✅ 完了 | 決定性保証、PII検知含む |
| **署名生成** | ✅ 完了 | signature_version管理、決定性保証 |
| **A/B/C検出** | ✅ 完了 | 累積・バースト・サンプリング実装済み |
| **ルール分類** | ✅ 完了 | base_rules.json駆動 |
| **LLM分析** | ✅ 完了 | JSON Schema検証、再試行、Writer Queue |
| **DuckDBスキーマ** | ✅ 完了 | 全テーブル定義済み |
| **Writer Queue** | ✅ 完了 | 単一Writer保証 |
| **冪等性** | ✅ 完了 | run_id決定性、UPSERT実装 |
| **ファイルロック** | ✅ 完了 | 排他制御実装済み |

### ⚠️ 部分実装・未実装

| 項目 | 実装状況 | 優先度 | 仕様書参照 |
|------|---------|--------|-----------|
| **Stage 0: Orchestrator** | ✅ 完了 | **P0（最優先）** | 3.2.3 チェックポイント再開 |
| **Parquet Hiveパーティション** | ✅ 完了 | **P0** | 12.1 増分処理の基盤 |
| **Excel生成** | ✅ 完了 | **P0** | 11.4 constant_memory必須 |
| **サニタイズCSV** | ✅ 完了 | **P1** | 11.5 外部共有用 |
| **Box同期安定化** | ✅ 完了 | **P1** | 2.4 原子性保証 |
| **監査説明セクション** | ⚠️ 部分 | **P0** | 11.3 必須項目（Phase 14で完全実装予定） |
| **Token Bucket予算制御** | ✅ 完了 | **P1** | 3.2.4 A/B優先制御 |
| **パフォーマンスメトリクス** | ⚠️ 部分 | **P1** | 16.2 観測性（Phase 16で完全実装予定） |
| **Excel主要集計** | ⚠️ 部分 | **P0** | 11.2 必須集計（Phase 15で完全実装予定） |
| **JSONL構造化ログ** | ❌ 未実装 | **P1** | 16.1 ログ（Phase 17で実装予定） |
| **増分処理** | ❌ 未実装 | **P2** | 12.1 増分処理（Phase 18で実装予定） |

---

## 🎯 実装優先順位（Phase別）

### **Phase 7: Orchestrator + チェックポイント再開（P0）** ✅ 完了

**目的**: 途中失敗からの安全な再開を実現

**実装内容**:
1. **`src/orchestrator.py` の作成**
   - `run_id` の決定論的生成（`input_manifest_hash` ベース）
   - ステージ別チェックポイント管理
   - `last_completed_stage` に基づく再開ロジック
   - `data/work/run_id/` での中間生成物管理

2. **`main.py` のリファクタリング**
   - Orchestratorへの移行
   - 各ステージを独立した関数に分離
   - チェックポイント更新（各ステージ完了時）

3. **再開テスト**
   - 途中失敗シミュレーション
   - 再実行で二重計上しないことを確認

**受け入れ基準**:
- ✅ 同一 `run_key` で再実行しても二重計上しない
- ✅ `last_completed_stage` から再開できる
- ✅ 中間生成物（Parquet等）が `data/work/run_id/` に保存される

**仕様書参照**: 3.2.3, 13.2

---

### **Phase 8: Parquet Hiveパーティション出力（P0）** ✅ 完了

**目的**: 増分処理の基盤と翌月以降の高速化

**実装内容**:
1. **`src/ingestor/parquet_writer.py` の作成**
   - Hiveパーティション形式: `data/processed/vendor=<v>/date=<YYYY-MM-DD>/...snappy.parquet`
   - `pyarrow` を使用したParquet生成
   - 原子性保証（`.tmp` → `rename()`）

2. **`main.py` の統合**
   - Stage 1完了時にParquet出力
   - `run_id` 世代管理（上書き時は原子置換）

3. **増分処理の準備**
   - 日付パーティションでのフィルタリング
   - 既存Parquetとのマージ（将来）

**受け入れ基準**:
- ✅ Hiveパーティション形式で出力される
- ✅ ファイル出力が原子的（`.tmp` → `rename()`）
- ✅ `data/processed/` に保存される

**仕様書参照**: 12.1, 13.2

---

### **Phase 9: Excel生成（constant_memory）（P0）** ✅ 完了

**目的**: 監査レポートの完全実装

**実装内容**:
1. **`src/reporting/excel_writer.py` の作成**
   - `xlsxwriter` を使用（`constant_memory=True` 必須）
   - 複数シート: サマリ、Top Shadow AI Apps、High Risk Users、部門別リスク、時系列
   - グラフ生成（集計表の縮約範囲を参照）

2. **大規模表の処理**
   - DuckDBで集計 → 1,000行ずつフェッチ → 書込み
   - 明細はExcelに全件載せない（CSV/Parquetに退避）

3. **監査説明セクション（必須）**
   - A/B/C件数・割合・bytes帯
   - 除外条件と除外件数
   - サンプル率・方式・seed
   - LLM利用範囲・PII送信禁止の説明

**受け入れ基準**:
- ✅ `constant_memory=True` で巨大表でも落ちない
- ✅ 監査説明セクションに全必須項目が含まれる
- ✅ グラフが正しく生成される

**仕様書参照**: 11.3, 11.4

---

### **Phase 10: サニタイズCSV出力（P1）** ✅ 完了

**目的**: 外部支援・デバッグ共有用の完全匿名化

**実装内容**:
1. **`src/reporting/sanitized_export.py` の作成**
   - `user_id`, `src_ip`, `device_id` を不可逆ハッシュ化
   - URL内PII疑い部分を不可逆化
   - `url_signature` と統計のみ残す

2. **`main.py` の統合**
   - Stage 5でサニタイズCSV生成
   - `data/output/run_{run_id}_sanitized.csv`

**受け入れ基準**:
- ✅ PIIが完全に不可逆化されている
- ✅ `url_signature` と統計のみ残る
- ✅ 外部共有可能な形式

**仕様書参照**: 11.5

---

### **Phase 11: Box同期安定化処理（P1）** ✅ 完了

**目的**: 未完了ファイルを掴まない原子性保証

**実装内容**:
1. **`src/orchestrator/file_stabilizer.py` の作成**
   - ファイルサイズ・最終更新時刻の監視
   - 60秒変化なしで「安定化」と判定
   - `data/work/run_id/raw/` へコピー

2. **`config/box_sync.yaml` の活用**
   - 設定ファイルの読み込み
   - 安定化条件の適用

3. **`main.py` の統合**
   - 入力検知 → 安定化 → コピー → 処理開始

**受け入れ基準**:
- ✅ 安定化条件を満たすまで待機
- ✅ `data/input/` は読み取り専用扱い
- ✅ `data/work/run_id/raw/` のみを処理

**仕様書参照**: 2.4, 13.1

---

### **Phase 12: Token Bucket予算制御の強化（P1）** ✅ 完了

**目的**: 予算枯渇時の優先順位制御（A/B優先、C停止）

**実装内容**:
1. **`src/llm/budget.py` の作成**
   - `BudgetController` クラスの実装
   - 予算枯渇時の優先順位制御
   - A/Bを優先し、C（Coverage Sample）を停止
   - 日次予算の追跡と自動リセット

2. **`src/llm/client.py` の拡張**
   - `BudgetController` の統合
   - `_check_budget()` を優先順位対応に拡張
   - バッチリクエストでの優先順位判定

3. **`main.py` の統合**
   - `_stage_4_llm_analysis()` で優先順位フィルタリング
   - `signature_stats` から `candidate_flags` を取得
   - スキップされた署名を `analysis_cache` に記録

4. **テストの追加**
   - `tests/test_budget_priority.py` を作成
   - 優先順位制御の動作を確認するテストを追加

**受け入れ基準**:
- ✅ 予算枯渇時にA/Bを優先
- ✅ Cは予算が十分な場合のみ送信
- ✅ 優先順位に基づくフィルタリングが正しく動作
- ✅ スキップされた署名が適切に記録される

**仕様書参照**: 3.2.4, 10.7

---

### **Phase 13: パフォーマンスメトリクス記録（P2）** ✅ 完了

**目的**: 観測性の向上

**実装内容**:
1. **`src/orchestrator/metrics.py` の作成** ✅
   - ステージ別処理時間・rows/sec・I/O量
   - `performance_metrics` テーブルへの記録
   - `MetricsRecorder` クラスの実装
   - コンテキストマネージャーによる自動計測

2. **`main.py` の統合** ✅
   - 各ステージ完了時にメトリクス記録
   - 全ステージ（ingest, normalize, abc_cache, rule_classification, llm, report）に対応

3. **テストの追加** ✅
   - `tests/test_performance_metrics.py` を作成
   - `tests/test_e2e_metrics.py` を作成
   - 全テストがパス

**受け入れ基準**:
- ✅ 全ステージのメトリクスが記録される
- ✅ `performance_metrics` テーブルに保存される
- ✅ 処理時間、スループット、I/O量が記録される
- ✅ カスタムメトリクスも記録可能

**仕様書参照**: 16.2

---

## 📋 実装チェックリスト

### P0（最優先・必須）

- [x] **Orchestrator + チェックポイント再開** ✅ Phase 7完了
  - [x] `src/orchestrator.py` 作成
  - [x] `main.py` リファクタリング
  - [x] 再開テスト

- [x] **Parquet Hiveパーティション出力** ✅ Phase 8完了
  - [x] `src/ingestor/parquet_writer.py` 作成
  - [x] `main.py` 統合
  - [x] 原子性保証

- [x] **Excel生成（constant_memory）** ✅ Phase 9完了
  - [x] `src/reporting/excel_writer.py` 作成
  - [x] 監査説明セクション実装
  - [x] 大規模表テスト

### P1（重要・次フェーズ）

- [x] **サニタイズCSV出力** ✅ Phase 10完了
  - [x] `src/reporting/sanitized_export.py` 作成
  - [x] `main.py` 統合
  - [x] PII不可逆化実装

- [x] **Box同期安定化処理** ✅ Phase 11完了
  - [x] `src/orchestrator/file_stabilizer.py` 作成
  - [x] `config/box_sync.yaml` の活用
  - [x] `main.py` 統合
- [x] **Token Bucket予算制御の強化** ✅ Phase 12完了
  - [x] `src/llm/budget.py` 作成（予算管理と優先順位制御）
  - [x] `src/llm/client.py` 拡張（優先順位に基づく予算チェック）
  - [x] `src/main.py` の `_stage_4_llm_analysis()` 修正（A/B/C優先順位制御）
  - [x] テスト追加（予算枯渇時の優先順位制御を確認）

### P2（改善・将来）

- [x] **パフォーマンスメトリクス記録** ✅ Phase 13完了

---

## 🆕 追加実装項目（仕様書v1.4との差分）

詳細は `docs/implementation_gap_analysis_v1.4.md` を参照。

### P0（最優先・必須）

- [ ] **Phase 14: 監査説明セクションの完全実装**
  - [ ] 対象母集団の明示（全量メタ集計、抽出件数・割合）
  - [ ] 除外条件と除外件数の正確な集計
  - [ ] 小容量ゼロ除外の数値証明

- [ ] **Phase 15: Excel主要集計の完全実装**
  - [ ] 部門別リスクスコアの実装
  - [ ] 時系列集計の完全実装

### P1（重要・次フェーズ）

- [ ] **Phase 16: パフォーマンスメトリクスの完全実装**
  - [ ] メモリ使用量の記録
  - [ ] LLMコストと予算消化の記録

- [ ] **Phase 17: JSONL構造化ログの実装**
  - [ ] JSONLログ出力
  - [ ] ログローテーション

### P2（改善・将来）

- [ ] **Phase 18: 増分処理の実装**
  - [ ] 既存Parquetとのマージ
  - [ ] 増分処理ロジック

---

## 🔄 実装順序の推奨

1. ✅ **Phase 7（Orchestrator）** → 基盤整備 **完了**
2. ✅ **Phase 8（Parquet）** → データ永続化 **完了**
3. ✅ **Phase 9（Excel）** → 監査レポート完成 **完了**
4. ✅ **Phase 10-12（P1項目）** → 運用耐性向上 **完了**
   - ✅ Phase 10: サニタイズCSV出力
   - ✅ Phase 11: Box同期安定化処理
   - ✅ Phase 12: Token Bucket予算制御の強化
5. ✅ **Phase 13（P2項目）** → 観測性向上 **完了**
6. 🔄 **Phase 14-15（P0項目）** → 監査耐性の完全性 **次フェーズ**
7. 🔄 **Phase 16-17（P1項目）** → 観測性の向上 **次フェーズ**
8. 🔄 **Phase 18（P2項目）** → 月次BPOの効率化 **将来**

---

## 📝 注意事項

1. **決定性・冪等性の維持**
   - 全実装で決定性・冪等性を最優先
   - テストで二重計上防止を確認

2. **原子性保証**
   - 全ファイル出力は `.tmp` → `rename()` 方式
   - Parquet・Excel・CSV全てに適用

3. **監査説明の完全性**
   - Excelレポートに全必須項目を含める
   - A/B/C・サンプル率・除外条件を明記

4. **テスト要件**
   - 各Phase完了時に受け入れ基準を満たすテストを追加
   - 回帰テストで既存機能の破壊を防止

---

## 🎯 成功条件（Definition of Done）

全Phase完了時に以下を満たすこと:

1. ✅ **監査耐性**: 決定性・来歴・抽出設計を説明可能
2. ✅ **信頼性**: 冪等性・途中失敗からの安全な再開・クラッシュセーフ
3. ✅ **プライバシー**: PII送信ゼロ・ローカルマスク・監査ログ
4. ✅ **性能**: Parquet Hiveパーティション・constant_memory Excel・キャッシュ最適化
5. ✅ **完全性**: Excel・JSON・サニタイズCSVの全出力形式

---

**次ステップ**: Phase 14（監査説明セクションの完全実装）の開始

詳細な差分分析は `docs/implementation_gap_analysis_v1.4.md` を参照。
