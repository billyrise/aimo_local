# AIMO Analysis Engine 仕様書 v1.5（Cursor実装用・完全版）

**改訂日**: 2026-01-17（v1.5）  
**前版**: v1.4（AIMO_Detail.md）  
**対象**: AIMOaaS Tier2（月次分析BPO）獲得および運用効率化の中核エンジン  
**設計思想**: Learning-First（初回＝学習優先）／Local-First, Cloud-Smart／監査耐性・運用耐性・コスト統制を最優先

---

## 目次

1. [目的と成功条件](#1-目的と成功条件)
2. [スコープ](#2-スコープ)
3. [実行環境・フォルダ・運用前提](#3-実行環境フォルダ運用前提)
4. [全体アーキテクチャ（段階処理）](#4-全体アーキテクチャ段階処理)
5. [データ提出要件（初回＝学習優先・取りこぼし防止）](#5-データ提出要件初回学習優先取りこぼし防止)
6. [標準スキーマ（AIMO Canonical Schema）](#6-標準スキーマaimo-canonical-schema)
7. [URL正規化（コスト削減と見逃し防止の両立）](#7-url正規化コスト削減と見逃し防止の両立)
8. [ルールベース分類（Stage 1）](#8-ルールベース分類stage-1)
9. [監査耐性の抽出ロジック（Stage 2）](#9-監査耐性の抽出ロジックstage-2)
10. [キャッシュ／DB設計（DuckDB中心）](#10-キャッシュdb設計duckdb中心)
11. [LLM分析（Stage 4）](#11-llm分析stage-4)
12. [レポーティング（Stage 5）](#12-レポーティングstage-5)
13. [学習資産（翌月以降の短縮要件）](#13-学習資産翌月以降の短縮要件)
14. [運用フロー](#14-運用フロー)
15. [セキュリティ・プライバシー](#15-セキュリティプライバシー)
16. [性能・コストのSLO（目標）](#16-性能コストのslo目標)
17. [観測性（ログ・メトリクス）](#17-観測性ログメトリクス)
18. [品質保証（QA）](#18-品質保証qa)
19. [実装構成（リポジトリ）](#19-実装構成リポジトリ)
20. [Cursor実装手順（v1.5）](#20-cursor実装手順v15)
21. [v1.4からの変更点](#21-v14からの変更点)
22. [付録](#22-付録)

---

## 1. 目的と成功条件

### 1.1 目的（初回・翌月以降を分けて最適化）

本エンジンは、企業のWebアクセスログ（SWG / CASB / FW / DNS 等）を解析し、以下を実現する。

#### 現状監査（初回）
- Shadow AI（未許可の生成AI等）の利用実態（誰が・何を・どれくらい）を可視化
- リスク（データ持ち出し兆候、規程違反、重要部門・重要ユーザーの高リスク行動）を可視化
- 「なぜ見逃したのか」と責められない説明可能性（監査耐性）を担保

#### 月次BPO（翌月以降）
- 初回の学習資産（キャッシュ／辞書／ルール／モデル）により、処理時間とAPIコストを大幅に削減
- 増分処理（新規ログのみ）で定常運用を安定化
- 再現性（同じ入力なら同じ出力）・冪等性（途中失敗しても再実行で破綻しない）を担保

### 1.2 成功条件（Acceptance Criteria）

#### 決定性・冪等性（必須）
- **署名の決定性**: 同一URL入力は常に同一 `url_signature`（同一 `signature_version` 前提）
- **冪等性**: 同一入力（同一 `input_manifest_hash`）で再実行しても、DBと成果物が二重計上されない
- **run_id決定性**: 同一入力・同一設定は必ず同一 `run_id` に収束

#### プライバシー・セキュリティ（必須）
- **PII送信ゼロ**: 外部API送信ペイロードに `user_id` / `src_ip` / `device_id` / PII疑い文字列を含めない
- **PII検知**: URL内のPII疑いを検知し、ローカルでマスクし、`pii_audit` に記録

#### 監査説明（必須）
- **抽出設計の説明**: A/B/C抽出、バースト・累積・サンプル率、除外条件と件数をレポートに必ず明記
- **小容量ゼロ除外防止**: 構造的に小容量帯をゼロ除外しない設計（監査説明で数値証明）

#### 性能・信頼性（必須）
- **大規模耐性**: Excel生成は `constant_memory=True` で落ちない（巨大表は集計に縮約し、詳細はCSV/Parquetに退避）
- **Writer Queue**: DuckDB書込みは単一Writerで直列化（並列Worker可、DB更新は直列）
- **チェックポイント再開**: 途中失敗時は `last_completed_stage` から再開可能

#### Tier1商品要件（必須）
- **コスト削減シミュレーション**: 翌月以降のコスト削減見積もりをレポートに含める
- **リスクマップ可視化**: High/Medium/Lowリスクの全体マップを可視化
- **Evidence Pack**: 監査証跡パックを生成（`run_manifest.json` + 集計統計）

---

## 2. スコープ

### 2.1 対象入力（ログソース）

以下の製品のログを処理対象とする：

| 製品 | 略称 | ログ形式 | 備考 |
|------|------|----------|------|
| Netskope | netskope | CSV/JSON | SWG/CASB |
| Zscaler | zscaler | CSV/JSON | ZIA/NSS |
| Microsoft Defender for Cloud Apps | mdca | CSV/JSON | 旧MCAS |
| Palo Alto Networks / Prisma Access | paloalto | CSV/CEF | SWG/FW |
| Cisco Umbrella | umbrella | CSV | DNS/SWG |
| Symantec / Blue Coat | bluecoat | W3C/CEF | ProxySG |
| McAfee / Skyhigh | skyhigh | CSV/JSON | Trellix |
| i-FILTER | ifilter | CSV | プロキシ |

その他：上記に準ずるSWG/Proxy/FW/DNSログ（CSV/W3C/JSON/CEF 等）

### 2.2 対象出力

#### 必須出力
- **Excel監査レポート**: 複数シート、グラフ含む（`constant_memory=True` 必須）
- **ダッシュボード用JSON**: 集計結果、時系列、ドリルダウンキー
- **サニタイズ済み共有用CSV**: 外部支援/デバッグ用（完全匿名化）

#### Tier1商品追加出力
- **Evidence Pack**: `run_manifest.json` + 集計統計（監査証跡）
- **コスト削減シミュレーション**: 翌月以降のコスト削減見積もり

### 2.3 非対象（v1.5では実装しない）

- リアルタイムストリーミング（Kafka/Flink等）
- 多テナント同居クラウド処理（原則ローカル運用）
- 高度な差分プライバシー（必要時はオプション設計として別紙）

---

## 3. 実行環境・フォルダ・運用前提

### 3.1 実行環境（本番）

- **ハードウェア**: Mac Studio（常時稼働、ローカル処理）
- **OS**: macOS（launchd による定期実行を推奨）
- **Python**: 3.11以上（型ヒント、パフォーマンス最適化を活用）

### 3.2 開発環境

- **ハードウェア**: MacBook Pro
- **IDE**: Cursor
- **Python**: 3.11以上

### 3.3 入出力フォルダ（Box Drive同期）

```
AIMO_Engine_local/
├── data/
│   ├── input/              # 顧客がBoxへアップロードしたログが同期
│   ├── processed/          # 正規化後Parquet（Hiveパーティション）
│   │   └── vendor=<vendor>/
│   │       └── date=<YYYY-MM-DD>/
│   │           └── *.snappy.parquet
│   ├── cache/              # DuckDBデータベース、各種キャッシュ
│   │   ├── aimo.duckdb
│   │   ├── aimo.duckdb.wal
│   │   └── aimo.lock       # 排他制御用ロックファイル
│   ├── work/                # 作業領域（run_id別）
│   │   └── <run_id>/
│   │       ├── raw/        # 安定化後の入力ファイルコピー
│   │       ├── parquet/     # 中間Parquet（必要時）
│   │       └── checkpoint/ # チェックポイント（必要時）
│   └── output/              # Excelレポート、JSON、サニタイズCSV
│       ├── run_<run_id>_report.xlsx
│       ├── run_<run_id>_dashboard.json
│       ├── run_<run_id>_sanitized.csv
│       └── run_<run_id>_manifest.json
├── logs/                    # 実行ログ、監査ログ（JSONL推奨）
│   └── run_<run_id>.jsonl
├── rules/                   # ルール定義（顧客別オーバーレイ可）
│   ├── base_rules.json
│   └── customer_overrides/
│       └── <customer_id>.json
├── schemas/                 # ベンダー別フィールドマッピング（YAML/JSON）
│   └── vendors/
│       ├── paloalto/
│       │   └── mapping.yaml
│       └── ...
└── config/                  # 設定ファイル
    ├── url_normalization.yml
    ├── bytes_buckets.yml
    ├── thresholds.yaml
    ├── llm_providers.yaml
    └── allowlist.yaml
```

### 3.4 Box同期の安定化要件（v1.5必須・デフォルト有効化）

**重要**: Box Drive同期フォルダのファイルを直接 ingest しない。必ず以下の手順で「安定化」する。

#### 3.4.1 安定化検知（必須）

1. **入力検知**: `data/input` に新規ファイルを検知
2. **安定化条件**（いずれも満たすまで待機）:
   - ファイルサイズが N 秒（既定：60秒）変化しない
   - 最終更新時刻（mtime）が N 秒（既定：60秒）変化しない
3. **安定化後にローカル作業領域へコピー**:
   - `data/work/<run_id>/raw/<original_filename>`
4. **以後、解析は work 配下のみを参照**（`input` は読み取り専用扱い）

#### 3.4.2 実装要件（v1.5改善点）

- **デフォルト有効化**: `--use-box-sync` フラグは不要（常に有効）
- **直接読み込み禁止**: `data/input` を直接読み込む処理は禁止
- **安定化待機**: `FileStabilizer` クラスを使用し、安定化を待機
- **エラーハンドリング**: 安定化待機中のタイムアウト（既定：300秒）を設定

#### 3.4.3 実装例

```python
from orchestrator.file_stabilizer import FileStabilizer

stabilizer = FileStabilizer(
    input_dir=Path("data/input"),
    work_dir=Path(f"data/work/{run_id}/raw"),
    stability_seconds=60,
    timeout_seconds=300
)

# 安定化を待機し、work領域へコピー
stabilized_files = stabilizer.wait_and_copy()
```

---

## 4. 全体アーキテクチャ（段階処理）

### 4.1 ステージ構成（v1.5）

| Stage | 名称 | 処理内容 | 出力 |
|-------|------|----------|------|
| **Stage 0** | Orchestrator | 実行管理、run_id生成、チェックポイント | `runs` テーブル |
| **Stage 1** | Ingestion & Normalization | 高速取込・正規化 | Parquet（Hiveパーティション） |
| **Stage 2** | Risk Candidate Selection | 監査防衛の抽出設計（A/B/C） | `signature_stats` テーブル |
| **Stage 3** | Signature & Dedup | 署名化・重複排除・キャッシュ照合 | `analysis_cache` 照合結果 |
| **Stage 4** | LLM Analysis | 未知署名のみ・バッチ処理 | `analysis_cache` 更新 |
| **Stage 5** | Reporting | 全量結合・集計・Excel/JSON/サニタイズ出力 | Excel/JSON/CSV |

### 4.2 Stage 0：Orchestrator（実行管理）

#### 4.2.1 Runキー（冪等性の中核）

**決定性を担保するrun_id生成**:

```python
# 1. 入力ファイルのマニフェストハッシュ
input_manifest = {
    "files": [
        {"path": "file1.csv", "size": 12345, "hash": "sha256..."},
        ...
    ]
}
input_manifest_hash = sha256(json.dumps(input_manifest, sort_keys=True))

# 2. run_key生成（決定性保証）
run_key = sha256(
    input_manifest_hash +
    target_range_start +
    target_range_end +
    signature_version +
    rule_version +
    prompt_version +
    taxonomy_version +      # v1.5追加
    evidence_pack_version + # v1.5追加
    engine_spec_version     # v1.5追加（"1.5"）
)

# 3. run_id（短縮表現）
run_id = base32_encode(run_key)[:16]  # 例: "a1b2c3d4e5f6g7h8"
```

**同一入力・同一設定は必ず同一 `run_id` に収束**。

#### 4.2.2 排他制御（必須）

- **同時実行禁止（原則）**: ファイルロック（`data/cache/aimo.lock`）を取得できたrunのみ実行
- **ロック取得失敗時**: 既存プロセスが実行中である旨をログ出力し、終了
- **例外**: LLM Workerは同一run内で並列可（Writer Queueで直列書込み）

#### 4.2.3 ステージ別チェックポイント（必須）

- **`runs.status`**: `running` / `succeeded` / `failed` / `partial`
- **`runs.last_completed_stage`**: 最後に完了したステージ番号（0-5）
- **再実行時**: 未完了ステージから再開可能
- **再開時の中間生成物**: `run_id` 世代で管理（`data/work/<run_id>/`）

#### 4.2.4 予算制御（Token Bucket）

- **`DAILY_BUDGET_USD`**: 日次予算を設定（環境変数または設定ファイル）
- **LLM送信前**: 推定コスト分の予算確保を行う
- **枯渇時の優先順位**: A/Bを優先し、C（Coverage Sample）を停止

---

## 5. データ提出要件（初回＝学習優先・取りこぼし防止）

### 5.1 共通要件（必須）

- **期間**: 直近1年（可能なら日次/週次で分割）
- **allow / block の両方**: 抑止の証跡として重要
- **可能な限り、上り/下り（upload/download相当）bytesを含める**
- **ユーザー識別子**: 匿名化可（不可逆ハッシュ＋saltは顧客保持）が、同一性が維持されること

### 5.2 初回抽出設計（提出側で可能なら推奨、難しければAIMO側で実施）

#### A系統：High-Volume
- **条件**: `bytes_sent`（upload相当）>= 1MB（既定、設定で変更可）

#### B系統：High-Risk Small
- **条件**: `bytes_sent`が小さくても、以下のいずれか
  - `write_method`（POST/PUT/PATCH相当）
  - **バースト**: `user` × `domain` × 5分窓で回数 >= 20（既定）
  - **累積**: `user` × `domain` × 日で `sum(bytes_sent)` >= 20MB（既定）
  - 宛先カテゴリが GenAI/AI/Unknown/Uncategorized

#### C系統：学習用カバレッジ枠（必須）
- **条件**: B候補の `bytes_sent` < 1MB 帯から無作為2%（既定）
- **seed固定**: `run_id` をseedとして使用（再現性保証）

### 5.3 提出が重すぎる場合の代替（必須の3点セット）

- **全量メタ集計**: 母集団の説明（件数、bytes帯分布等）
- **高価値イベント全量**: bytes_sent上位、AI/Unknown、block、DLP等
- **層化サンプル**: bytes帯を層化し各層から無作為抽出、率を明記

---

## 6. 標準スキーマ（AIMO Canonical Schema）

### 6.1 Canonical Event（標準イベント行）

すべての入力ログは最終的に以下のカラムへ正規化される（型は推奨）。

| カラム名 | 型 | 必須 | 説明 |
|---------|-----|------|------|
| `event_time` | TIMESTAMP | ✅ | UTC推奨、ISO8601 → 変換 |
| `vendor` | VARCHAR | ✅ | paloalto / zscaler / netskope / mdca / umbrella / bluecoat / skyhigh / ifilter / other |
| `log_type` | VARCHAR | ✅ | web / proxy / traffic / url / dns / cloudapp / dlp 等 |
| `user_id` | VARCHAR | ✅ | 匿名化済み識別子 |
| `user_dept` | VARCHAR | ❌ | 任意、匿名化可 |
| `device_id` | VARCHAR | ❌ | 任意、匿名化可 |
| `src_ip` | VARCHAR | ❌ | 任意、匿名化可 |
| `dest_host` | VARCHAR | ✅ | FQDN |
| `dest_domain` | VARCHAR | ✅ | eTLD+1（Public Suffix Listにより算出） |
| `url_full` | VARCHAR | ✅ | 可能なら完全URL、無い場合は組立 |
| `url_path` | VARCHAR | ✅ | 正規化後 |
| `url_query` | VARCHAR | ✅ | 正規化後、追跡パラメータ除去 |
| `http_method` | VARCHAR | ❌ | GET/POST/PUT等 |
| `status_code` | INTEGER | ❌ | HTTPステータスコード |
| `action` | VARCHAR | ✅ | allow / block / warn / observe 等 |
| `app_name` | VARCHAR | ❌ | アプリケーション名 |
| `app_category` | VARCHAR | ❌ | AI/GenAI/Business/Storage/Unknown 等 |
| `bytes_sent` | BIGINT | ✅ | upload相当 |
| `bytes_received` | BIGINT | ✅ | download相当 |
| `content_type` | VARCHAR | ❌ | MIMEタイプ |
| `user_agent` | VARCHAR | ❌ | User-Agent文字列 |
| `raw_event_id` | VARCHAR | ❌ | 元ログの一意キーがある場合 |
| `ingest_file` | VARCHAR | ✅ | 入力ファイル名 |
| `ingest_lineage_hash` | VARCHAR | ✅ | 行のハッシュ：改ざん検知・再処理抑止 |

### 6.2 Canonical Signature（署名行）

| カラム名 | 型 | 必須 | 説明 |
|---------|-----|------|------|
| `url_signature` | VARCHAR | ✅ | PK、Stableであることが最重要 |
| `signature_version` | VARCHAR | ✅ | 署名バージョン |
| `norm_host` | VARCHAR | ✅ | 正規化済みホスト |
| `norm_path_template` | VARCHAR | ✅ | 正規化済みパステンプレート（ID抽象化後） |
| `path_depth` | INTEGER | ✅ | パスの深さ |
| `param_count` | INTEGER | ✅ | クエリパラメータ数 |
| `has_auth_token_like` | BOOLEAN | ✅ | 認証トークン疑いフラグ |
| `bytes_sent_bucket` | VARCHAR | ✅ | L/M/H/C 等（T=tiny, not candidate C） |
| `candidate_flags` | VARCHAR | ✅ | A/B/C, burst, cumulative, sampled 等 |

---

## 7. URL正規化（コスト削減と見逃し防止の両立）

### 7.1 正規化の目的

- **細かすぎると**: キャッシュが効かずコスト増
- **粗すぎると**: 誤判定増
- **v1.5は**: 「追跡パラメータ除去＋危険ID抽象化＋決定性の完全明文化」を標準

### 7.2 決定性を担保する正規化手順（順序厳守・v1.5改善点）

#### 7.2.1 入力の前処理（順序固定）

1. **文字列トリム**: 前後の空白を除去
2. **スキーム除去**: `http://` / `https://` を除去
3. **Host部分を小文字化**: 決定性保証
4. **Punycode正規化**: IDN対応（`xn--` 形式を正規化）
5. **デフォルトポート除去**: `:80` / `:443` を除去
6. **連続スラッシュ正規化**: `//` → `/`
7. **末尾スラッシュ規則**: パスが `/` のみの場合を除き、末尾 `/` は除去

#### 7.2.2 クエリ正規化（順序固定）

1. **追跡パラメータ除去（必須）**:
   - `utm_*`, `gclid`, `fbclid`, `ref`, `session`, `sid`, `phpsessid`, `mc_cid`, `mc_eid` 等
   - 追加リストは設定ファイル（`config/url_normalization.yml`）で顧客別に拡張
2. **残すパラメータ（key_param_subset）**:
   - 既定は空（保持しない）
   - 必要時のみホワイトリスト方式で保持（例：`api_version` 等）
3. **クエリ並び順**: キー昇順で固定（決定性）

#### 7.2.3 ID/トークン抽象化（必須・v1.5改善点）

1. **UUID検知**: `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}` → `:uuid`
2. **長いhex検知**: 32文字以上のhex → `:hex`
3. **長いbase64風検知**: base64風の長い文字列 → `:token`
4. **数値連番検知**: 連続する数値ID → `:id`
5. **email検知**: `[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}` → `:email`
6. **IPv4検知**: `\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}` → `:ip`
7. **置換前後の検知ログ**: `pii_audit` に記録（外部送信監査）

### 7.3 eTLD+1算出（v1.5必須改善点）

**重要**: `URLNormalizer.extract_domain()` は **Public Suffix List を使用すること**（簡易ヒューリスティック禁止）。

#### 7.3.1 実装要件

- **ライブラリ**: `tldextract` または `publicsuffix2` を使用
- **Public Suffix List**: `data/psl/public_suffix_list.dat` を読み込み
- **eTLD+1算出**: `tldextract.extract(host).registered_domain` を使用
- **フォールバック**: Public Suffix Listが読み込めない場合はエラー（簡易ヒューリスティック禁止）

#### 7.3.2 実装例

```python
import tldextract

def extract_domain(host: str) -> str:
    """
    Extract eTLD+1 from host using Public Suffix List.
    
    Args:
        host: FQDN (e.g., "www.example.co.uk")
    
    Returns:
        eTLD+1 (e.g., "example.co.uk")
    """
    extractor = tldextract.TLDExtract(
        cache_file="data/psl/public_suffix_list.dat",
        fallback_to_snapshot=False  # フォールバック禁止
    )
    extracted = extractor(host)
    return extracted.registered_domain  # e.g., "example.co.uk"
```

### 7.4 署名生成（v1.5）

```python
# 署名生成（決定性保証）
url_signature = sha256(
    norm_host + "|" +
    norm_path_template + "|" +
    key_param_subset + "|" +
    method_group + "|" +        # GET / WRITE / OTHER
    bytes_bucket + "|" +         # L/M/H/C
    signature_version
)
```

**同一入力は常に同一 `url_signature`**。

---

## 8. ルールベース分類（Stage 1）

### 8.1 目的

- 既知の安全通信や業務SaaSを高確度で分類し、LLMへ送らない
- 翌月以降の高速化のため、分類結果をキャッシュへ蓄積

### 8.2 ルール定義

- **`rules/base_rules.json`**: 共通ルール
- **`rules/customer_overrides/<customer_id>.json`**: 顧客別追加（任意）
- **ルールは以下を含む**:
  - `pattern`（host/domain/path regex）
  - `service_name`, `category`, `default_risk`, `usage_type`（Corporate/Safe等）
  - `rule_id`, `rule_version`
  - **Taxonomy codes（v1.5追加）**: `fs_uc_code`, `dt_code`, `ch_code`, `im_code`, `rs_code`, `ob_code`, `ev_code`
- **ルール適用**: 「最長一致＋優先度」で決定（曖昧性排除）

### 8.3 出力

- `classification_source = "RULE"`
- `rule_id`, `rule_version` を記録
- `analysis_cache` に UPSERT（`is_human_verified` が `true` の行は上書き禁止）

---

## 9. 監査耐性の抽出ロジック（Stage 2）

### 9.1 原則

- **サイズ閾値「単独」での除外は禁止**
- **A/B/C＋バースト＋累積＋サンプルを必須**とし「小容量ゼロ除外」を構造的に防止

### 9.2 必須集計（決定性のためSQL定義を固定）

#### 累積（必須）
```sql
-- user_id × dest_domain × day の sum(bytes_sent)
SELECT
    user_id,
    dest_domain,
    DATE(event_time) as day,
    SUM(bytes_sent) as cumulative_bytes
FROM canonical_events
GROUP BY user_id, dest_domain, day
```

#### バースト（必須）
```sql
-- user_id × dest_domain × 5min window の count(write_methods)
SELECT
    user_id,
    dest_domain,
    DATE_TRUNC('minute', event_time) / 5 * 5 as window_start,
    COUNT(*) as burst_count
FROM canonical_events
WHERE http_method IN ('POST', 'PUT', 'PATCH')
GROUP BY user_id, dest_domain, window_start
HAVING COUNT(*) >= 20
```

#### カテゴリ（可能なら）
- AI/Unknown を優先

### 9.3 A/B/C（既定値：設定で変更可）

- **A（High-Volume）**: `bytes_sent >= 1MB`
- **B（High-Risk Small）**:
  - `write_method` かつ（AI/Unknown宛 または `burst>=20` または `cumulative>=20MB`）
- **C（Coverage Sample）**:
  - B候補の `bytes_sent < 1MB` から無作為2%
  - 乱数seedは `run_id` で固定（再現性）

### 9.4 監査説明用の記録（必須）

- A/B/Cの件数、割合、bytes帯
- 除外条件と除外件数（存在する場合）
- サンプル率、サンプル方式、乱数seed（`run_id`）

---

## 10. キャッシュ／DB設計（DuckDB中心）

### 10.1 採用方針

- ローカルで高速な分析SQLと大規模Parquetスキャンを実現
- 書込みは単一Writer（Producer-Consumer）で安定運用
- 再実行冪等のため、`run_id` と主キー制約を厳格化

### 10.2 DBファイル

- **`data/cache/aimo.duckdb`**: メインデータベース
- **`data/cache/aimo.duckdb.wal`**: Write-Ahead Log（自動生成）

### 10.3 主要テーブル（必須）

#### runs（PK: run_id）
- `run_id`, `run_key`, `started_at`, `finished_at`
- `target_range_start`, `target_range_end`
- `vendor_scope`
- `status`, `last_completed_stage`
- `code_version`（git hash等）
- `rule_version`, `signature_version`, `prompt_version`
- `taxonomy_version`, `evidence_pack_version`, `engine_spec_version`（v1.5追加）
- `input_manifest_hash`

#### input_files（PK: file_id = sha256(path + size + mtime)）
- `run_id`, `file_path`, `file_size`, `file_hash`
- `vendor`, `log_type`
- `min_time`, `max_time`, `row_count_estimate`, `ingested_at`

#### analysis_cache（PK: url_signature）
- `url_signature`, `service_name`, `usage_type`, `risk_level`, `category`
- `confidence`, `classification_source`, `evidence`(<=500)
- `analysis_date`
- `signature_version`, `rule_version`, `prompt_version`, `taxonomy_version`
- `status`（active/needs_review/skipped）
- **`is_human_verified`（上書き禁止フラグ・v1.5必須）**
- `error_type`, `error_reason`, `retry_after`, `failure_count`, `last_error_at`
- **Taxonomy codes（v1.5追加）**: `fs_uc_code`, `dt_code`, `ch_code`, `im_code`, `rs_code`, `ob_code`, `ev_code`

#### signature_stats（PK: run_id + url_signature）
- `run_id`, `url_signature`, `norm_host`, `norm_path_template`, `bytes_sent_bucket`
- `access_count`, `unique_users`
- `bytes_sent_sum`, `bytes_sent_p95`, `bytes_sent_max`
- `burst_max_5min`, `cumulative_user_domain_day_max`
- `candidate_flags`, `sampled`
- **Taxonomy codes（v1.5追加）**: 同上

#### api_costs（run_id + timestamp）
- `provider`, `model`, `request_tokens`, `response_tokens`, `cost_usd_estimated`, `latency_ms`

#### performance_metrics（run_id + timestamp）
- `stage`, `metric_name`, `value`, `unit`

#### pii_audit（run_id + detected_at）
- `url_signature`, `pii_type`, `redaction_applied`

### 10.4 UPSERT/上書きルール（v1.5必須改善点）

#### is_human_verified上書き禁止（必須）

**重要**: `is_human_verified=true` の行は **絶対に上書き禁止**。

```python
def upsert_analysis_cache(self, records: List[Dict[str, Any]]):
    """
    UPSERT analysis_cache with is_human_verified protection.
    
    Rules:
    1. If existing row has is_human_verified=true, skip update
    2. If new row has is_human_verified=true, always insert/update
    3. Otherwise, normal UPSERT
    """
    for record in records:
        existing = self.get_reader().execute(
            "SELECT is_human_verified FROM analysis_cache WHERE url_signature = ?",
            [record["url_signature"]]
        ).fetchone()
        
        if existing and existing[0]:  # is_human_verified=true
            logger.warning(
                f"Skipping update for {record['url_signature']}: "
                "is_human_verified=true (human verification protection)"
            )
            continue
        
        # Normal UPSERT
        self._upsert_record("analysis_cache", record)
```

#### その他のUPSERTルール

- `analysis_cache` は原則UPSERT
- `status='skipped'` は送信禁止（永続失敗の安全弁）

---

## 11. LLM分析（Stage 4）

### 11.1 送信対象（厳格）

- `analysis_cache` 未登録、または未分類（`risk_level='Unknown'`等）で `status='active'` の `url_signature` のみ
- `status='skipped'` は送信禁止
- `status='needs_review'` でも、永続失敗（`context_length_exceeded`等）は送信禁止
- 再送は `retry_after` に従う

### 11.2 PII送信禁止（必須）

**送信ペイロードに以下を含めない**:
- `user_id`, `src_ip`, `device_id`
- 生URLのPII疑い部分

**送信は原則、`norm_host` / `norm_path_template` / 統計のみ**。

### 11.3 バッチ設計

- 1リクエストに 10〜20署名（可変）
- 事前にトークン量見積り→予算確保できた分のみ送信

### 11.4 プロンプト要件（幻覚防止＋JSON厳格）

LLMへ必ず指示する：
- 不明なものは推測しない
- 判断不能は `Unknown` とする
- 出力は厳格なJSON（スキーマ準拠）のみ
- `confidence` と `rationale_short` を必須
- **Taxonomy codes（v1.5追加）**: 7コード（FS-UC/DT/CH/IM/RS/OB/EV）を必須
- 返答JSONはローカルで JSON Schema 検証し、失敗時は最大2回まで再試行（同一バッチで）

### 11.5 失敗時の取り扱い（無限ループ防止）

- `error_type` / `error_reason` を記録
- **永続失敗**（`context_length_exceeded` 等）は `status='skipped'` に自動遷移
- **一時失敗**（`rate_limit`/`timeout`）は `retry_after` を設定して再試行
- `needs_review` は「人手確認キュー」であり、自動再送フラグではない

### 11.6 書込み競合対策（Writer Queue方式：v1.5必須改善点）

#### 11.6.1 実装要件

- **Workerは並列可**: LLM呼び出しは並列実行可能
- **DuckDB書込みは単一Writer**: 全てのDB更新はWriter Queue経由で直列化
- **Producer（Worker）→ Async Queue → Writer Thread（単一）→ UPSERT**
- **Writerはバッチコミット**: 例：50〜200行
- **Writerクラッシュ時**: 再実行で復旧できるよう `run_id` と `batch_id` を持つ

#### 11.6.2 実装例

```python
# LLM Worker（並列実行可能）
def analyze_signatures_batch(signatures: List[str], llm_client: LLMClient, db_client: DuckDBClient):
    results = llm_client.analyze_batch(signatures)
    
    # Writer Queue経由でDB書込み（直列化）
    for result in results:
        db_client.queue_write("analysis_cache", result)  # 非同期キューへ投入
    
    # Writer Threadが自動的にバッチコミット

# DuckDBClient（Writer Queue実装）
class DuckDBClient:
    def queue_write(self, table: str, record: Dict[str, Any]):
        """Queue write operation (non-blocking)."""
        self._write_queue.put({
            "operation": "upsert",
            "table": table,
            "record": record
        })
    
    def _start_writer(self):
        """Start single writer thread."""
        def writer_loop():
            batch = []
            while not self._shutdown_event.is_set():
                item = self._write_queue.get(timeout=1.0)
                batch.append(item)
                
                if len(batch) >= 50:  # バッチサイズ
                    self._process_batch(batch)
                    batch = []
            
            # 残りを処理
            if batch:
                self._process_batch(batch)
```

### 11.7 予算制御（Token Bucket）

- `DAILY_BUDGET_USD` を設定
- 推定コスト（入力/出力トークン×単価）で予算を消費
- 予算枯渇時は C枠停止、A/B優先

---

## 12. レポーティング（Stage 5）

### 12.1 全量結合

- Parquet（イベント）に `analysis_cache` を `url_signature` で結合
- UnknownはUnknownのまま表示（無理に埋めない）

### 12.2 主要集計（必須）

- **Top 10 Shadow AI Apps**: ユーザー数、アクセス数、bytes_sent合計、リスク別内訳
- **Top 10 High Risk Users**: アップロード量、High/Critical宛先数、burst回数
- **部門別リスクスコア**: ユーザー数で正規化、High/Critical割合
- **時系列（週次/月次）**: Unknown率、AIカテゴリ率、High/Critical比率、ブロック率
- **ブロック/許可のポリシー差分が疑われる宛先一覧**

### 12.3 監査説明（必須：レポートに明記）

- 初回抽出設計（A/B/C、バースト、累積、サンプル率、seed）
- 対象母集団（全量メタ集計）と抽出件数・割合
- 除外がある場合：除外条件と除外件数
- 小容量をゼロ除外していない説明
- LLM利用範囲（未知署名のみ）とPII送信禁止の説明

### 12.4 Excel生成（大規模耐性）

- **XlsxWriter**: `constant_memory=True` を必須
- **大規模表**: DuckDBで集計→1,000行ずつフェッチ→書込み
- **グラフ**: 集計表の縮約範囲を参照（明細はExcelに全件載せない）

### 12.5 サニタイズ（完全匿名化）エクスポート（必須）

- 外部支援/デバッグ共有用に、不可逆ハッシュ化済みサンプルCSVを生成
- `user_id`、`src_ip`、`device_id`、URL内PII疑い部分は不可逆化
- `url_signature` と統計のみ残す

### 12.6 Tier1商品追加機能（v1.5追加）

#### 12.6.1 コスト削減シミュレーション（必須）

**目的**: 翌月以降のコスト削減見積もりをレポートに含める。

**計算ロジック**:
```python
# 初回コスト
initial_cost = sum(api_costs.cost_usd_estimated)

# キャッシュ命中率（既知署名 / 全署名）
cache_hit_rate = cache_hit_count / unique_signatures

# 翌月以降の推定コスト（キャッシュ命中分は0）
estimated_future_cost = initial_cost * (1 - cache_hit_rate)

# コスト削減額
cost_reduction = initial_cost - estimated_future_cost
```

**レポート出力**:
- 初回コスト（USD）
- キャッシュ命中率（%）
- 翌月以降の推定コスト（USD）
- コスト削減額（USD）
- コスト削減率（%）

#### 12.6.2 リスクマップ可視化（必須）

**目的**: High/Medium/Lowリスクの全体マップを可視化。

**実装要件**:
- **全体リスクマップ**: ユーザー × 宛先ドメインのリスクマトリクス
- **部門別リスクマップ**: 部門 × リスクレベルの集計
- **時系列リスクトレンド**: 週次/月次のリスク推移
- **Excel出力**: リスクマップシートを追加

#### 12.6.3 Evidence Pack生成（v1.5追加）

**目的**: 監査証跡パックを生成。

**出力内容**:
- **`run_manifest.json`**: run_id、実行日時、入力ファイル、設定バージョン等
- **集計統計**: A/B/C件数、キャッシュ命中率、LLM送信数等
- **監査ログ**: PII検知ログ、エラーログ等

**ファイル構成**:
```
data/output/run_<run_id>_evidence_pack/
├── run_manifest.json
├── summary_statistics.json
├── pii_audit.jsonl
└── error_log.jsonl
```

---

## 13. 学習資産（翌月以降の短縮要件）

### 13.1 ParquetのHiveパーティション（必須）

- **パス形式**: `data/processed/vendor=<vendor>/date=<YYYY-MM-DD>/...snappy.parquet`
- **翌月以降**: `date` パーティションのみ増分処理

### 13.2 キャッシュ命中率最大化（必須）

- 署名の安定性を最優先（署名ブレはKPI悪化）
- `analysis_cache` は `is_human_verified` を最優先

### 13.3 ML Pre-Filter（オプション）

- 初回後に段階導入（評価AUC等を満たす場合のみ）
- 低信頼は必ずLLMまたはHumanへ回す

---

## 14. 運用フロー

### 14.1 日次バッチ（例：毎日AM 2:00）

1. **Box同期**: 顧客アップロード → `data/input`
2. **Orchestrator**: 安定化検知→workへコピー→run作成
3. **Ingest**: →Parquet生成
4. **A/B/C抽出**: →署名統計生成
5. **キャッシュ照合**: →未知署名のみLLM
6. **Writer Queue**: でDB反映
7. **レポート生成**: →`data/output`→Box共有

### 14.2 冪等性

- 入力ファイルhashを記録し同一入力を二重処理しない
- `run_id`単位で成果物と中間生成物を管理し、途中失敗でも再実行で回復
- Parquet再生成は `run_id` 世代で管理（上書き時は原子置換）

### 14.3 スケジューリング（macOS）

- **launchd（推奨）**: 標準出力/標準エラーを `logs/` にリダイレクト
- **設定ファイル**: `ops/jp.riseby.aimo.plist`

---

## 15. セキュリティ・プライバシー

### 15.1 データ局所性

- 生ログはローカルSSDとメモリ内に留める
- 外部へ送るのは署名と統計のみ

### 15.2 PII検知と送信禁止

- URL内のemail/IP/トークン疑いを検知しマスク
- `pii_audit` に記録し、外部送信ペイロードを監査可能にする

### 15.3 ゼロ保持の方針

- 外部APIは「入力データを学習に使わない」契約形態を推奨
- APIキーは `.env.local` で管理し、リポジトリに含めない

---

## 16. 性能・コストのSLO（目標）

### 16.1 初回（学習優先）

- **優先度**: 監査説明可能性 ＞ 学習資産最大化 ＞ 速度

### 16.2 翌月以降（月次BPO）

- 増分Parquet＋高命中キャッシュにより、処理時間とAPIコストを大幅に削減
- **KPI**: キャッシュ命中率、未知率、LLM送信数/日、USD消化

---

## 17. 観測性（ログ・メトリクス）

### 17.1 ログ（JSONL推奨・v1.5必須）

#### 17.1.1 構造化ログ形式（必須）

**ファイル**: `logs/run_<run_id>.jsonl`

**ログエントリ例**:
```json
{
  "timestamp": "2026-01-17T10:00:00Z",
  "level": "INFO",
  "stage": "ingest",
  "run_id": "a1b2c3d4e5f6g7h8",
  "event": "file_processed",
  "file_path": "data/work/.../raw/file1.csv",
  "row_count": 1000,
  "duration_ms": 500
}
```

#### 17.1.2 必須ログイベント

- **run開始/終了**: `run_started`, `run_completed`, `run_failed`
- **入力ファイル**: `file_ingested`, `file_error`
- **対象件数**: `total_events`, `unique_signatures`
- **A/B/C件数**: `candidate_a_count`, `candidate_b_count`, `candidate_c_count`
- **未知署名数**: `unknown_signatures_count`
- **LLM送信数**: `llm_sent_count`, `llm_error_count`
- **失敗数**: `error_type`, `error_reason`（error_type別）
- **除外条件と件数**: `exclusion_condition`, `exclusion_count`（必ず記録）

### 17.2 メトリクス（DBへ保存・v1.5必須）

#### 17.2.1 必須メトリクス

- **stageごとの処理時間**: `duration_ms`
- **rows/sec**: `rows_per_sec`
- **I/O量**: `bytes_read`, `bytes_written`
- **メモリ使用量**: `memory_mb`（可能なら）
- **LLMコスト（推定）**: `cost_usd_estimated`
- **予算消化**: `budget_consumed_usd`, `budget_remaining_usd`

#### 17.2.2 実装例

```python
# メトリクス記録
db_client.record_metric(
    run_id=run_id,
    stage="ingest",
    metric_name="duration_ms",
    value=500.0,
    unit="ms"
)
```

---

## 18. 品質保証（QA）

### 18.1 テスト（必須）

- **URL正規化**: 追跡パラメータ除去、ID抽象化、順序固定
- **署名安定性**: 同じ入力→同じ署名
- **A/B/C抽出**: 小容量ゼロ除外にならない
- **Writer Queue整合性**: 重複UPSERT、部分失敗→再実行
- **error_typeによる再送制御**: 永続失敗が再送されない
- **監査説明セクション生成**: 必須項目チェック
- **is_human_verified上書き禁止**: 保護テスト（v1.5追加）
- **eTLD+1抽出**: Public Suffix List使用テスト（v1.5追加）

### 18.2 回帰（毎月）

- `signature_version`更新時: 命中率悪化がないか検証
- ルール更新: 誤分類増がないか検証

---

## 19. 実装構成（リポジトリ）

### 19.1 ディレクトリ構造

```
src/
├── main.py                    # エントリ
├── orchestrator.py            # Stage 0: Orchestrator
├── orchestrator/
│   └── file_stabilizer.py     # Box同期安定化
├── ingestor/
│   ├── base.py                # ベースIngestor
│   ├── paloalto.py            # Palo Alto専用（必要時）
│   └── ...
├── normalize/
│   ├── url_normalizer.py      # URL正規化（eTLD+1含む）
│   ├── pii_detector.py        # PII検知
│   └── domain_parser.py       # Public Suffix List対応
├── classifiers/
│   └── rule_classifier.py     # ルール分類
├── detectors/
│   └── abc_detector.py        # A/B/C検出
├── signatures/
│   ├── signature_builder.py   # 署名生成
│   └── deduplicator.py        # 重複排除
├── llm/
│   ├── client.py              # LLMクライアント
│   ├── budget.py              # Token Bucket
│   ├── writer_queue.py        # Writer Queue（必要時）
│   ├── prompt_templates.py    # プロンプトテンプレート
│   └── json_schema.py         # JSON Schema検証
├── db/
│   ├── schema.sql             # DuckDBスキーマ
│   └── duckdb_client.py       # DuckDBクライアント（Writer Queue含む）
└── reporting/
    ├── reporter.py            # レポーティング統合
    ├── excel_writer.py        # Excel生成
    ├── dashboard_json.py      # ダッシュボードJSON
    ├── sanitized_export.py    # サニタイズCSV
    └── evidence_pack_generator.py  # Evidence Pack生成（v1.5追加）

rules/
├── base_rules.json
└── customer_overrides/
    └── <customer_id>.json

schemas/
└── vendors/
    ├── paloalto/
    │   └── mapping.yaml
    └── ...

config/
├── url_normalization.yml
├── bytes_buckets.yml
├── thresholds.yaml
├── llm_providers.yaml
└── allowlist.yaml
```

### 19.2 依存ライブラリ（推奨）

- `polars`: ETL処理
- `duckdb`: 分析SQL
- `pyarrow`: Parquet/partition
- `python-dotenv`: 環境変数管理
- `regex`: 正規表現
- `httpx`: LLM呼び出し
- `tenacity`: バックオフ
- `xlsxwriter`: Excel生成（`constant_memory=True`）
- `psutil`: 監視（任意）
- `tldextract` または `publicsuffix2`: eTLD+1抽出（v1.5必須）
- `jsonschema`: LLM JSON検証
- `filelock`: 排他制御

---

## 20. Cursor実装手順（v1.5）

### Step 1：プロジェクト雛形＋DBスキーマ

- ディレクトリ構造、`.env.local`、DuckDBスキーマ、Run管理（`runs`/`input_files`）を整備
- **v1.5追加**: Taxonomy codes列、Evidence Pack列を追加

### Step 2：ベンダー別Ingestor（1製品から開始）

- Palo Alto または Zscaler を最初に実装し Canonical Event 正規化を確認
- 以降、各ベンダーへ拡張（`schemas/`のマッピング駆動）

### Step 3：URL正規化（決定性の厳格化）＋PII検知

- 正規化手順（7.2）を順序固定で実装
- **v1.5必須改善**: `extract_domain()` でPublic Suffix List使用（簡易ヒューリスティック禁止）
- `pii_audit` へ記録

### Step 4：A/B/C候補抽出（累積・バースト・サンプル）

- seed固定（`run_id`）で再現性確保
- 小容量帯ゼロ除外のテスト

### Step 5：署名・統計・キャッシュ照合

- `signature_version`管理
- `signature_stats`生成
- `analysis_cache`照合（既知除外）

### Step 6：LLM Analyzer（JSON Schema、Token Bucket、Writer Queue）

- 送信前に予算確保
- バッチ送信
- JSON検証→再試行→永続失敗skipped
- **v1.5必須改善**: Writer QueueでUPSERT（LLM Worker統合確認）
- **v1.5追加**: Taxonomy codes（7コード）を必須

### Step 7：Reporter（Excel/JSON＋監査説明＋サニタイズ）

- `constant_memory`でExcel
- 監査説明セクション必須
- サニタイズエクスポート必須
- **v1.5追加**: コスト削減シミュレーション、リスクマップ可視化、Evidence Pack生成

### Step 8：Box同期安定化（v1.5必須改善）

- **デフォルト有効化**: `--use-box-sync` フラグ不要（常に有効）
- 安定化待機→work領域へコピー
- `data/input` 直接読み込み禁止

### Step 9：is_human_verified上書き禁止（v1.5必須改善）

- UPSERTロジックで `is_human_verified=true` の行を上書き禁止
- テスト追加

### Step 10：JSONL構造化ログ（v1.5必須）

- 構造化ログ形式（JSONL）で記録
- 必須ログイベントを実装

---

## 21. v1.4からの変更点

### 21.1 必須改善点（監査・事故リスク）

1. **URLNormalizer.extract_domain()のPublic Suffix List対応**
   - v1.4: 簡易ヒューリスティック（最後2ドメイン部分）
   - v1.5: Public Suffix List必須（`tldextract` または `publicsuffix2` 使用）

2. **Box同期安定化のデフォルト有効化**
   - v1.4: `--use-box-sync` フラグ必須
   - v1.5: デフォルト有効化（常に有効）、`data/input` 直接読み込み禁止

3. **is_human_verified上書き禁止の実装**
   - v1.4: スキーマ定義あり、実装要確認
   - v1.5: UPSERTロジックで `is_human_verified=true` の行を上書き禁止（必須）

4. **Writer QueueのLLM Worker統合**
   - v1.4: Writer Queue実装あり、利用確認要
   - v1.5: LLM WorkerからWriter Queue経由でDB書込み（必須）

### 21.2 Tier1商品要件追加

5. **コスト削減シミュレーション実装**
   - v1.4: 未実装
   - v1.5: 翌月以降のコスト削減見積もりをレポートに含める（必須）

6. **リスクマップ可視化の拡充**
   - v1.4: 部門別リスクのみ
   - v1.5: High/Medium/Lowリスクの全体マップ追加（必須）

7. **Evidence Pack生成**
   - v1.4: 未実装
   - v1.5: `run_manifest.json` + 集計統計を生成（必須）

### 21.3 観測性強化

8. **JSONL構造化ログ**
   - v1.4: 未実装
   - v1.5: 構造化ログ形式（JSONL）で記録（必須）

9. **パフォーマンスメトリクスの詳細化**
   - v1.4: 部分実装
   - v1.5: 必須メトリクスを完全実装

### 21.4 Taxonomyセット対応（v1.5追加）

10. **Taxonomy codes（7コード）**
    - v1.4: 部分実装
    - v1.5: 7コード（FS-UC/DT/CH/IM/RS/OB/EV）を必須項目として完全実装

---

## 22. 付録

### 22.1 設定ファイル例

#### config/url_normalization.yml
```yaml
tracking_params:
  - utm_source
  - utm_medium
  - utm_campaign
  - gclid
  - fbclid
  # ... 追加可能

key_params:
  # 保持するパラメータ（ホワイトリスト）
  - api_version
```

#### config/thresholds.yaml
```yaml
abc_detection:
  a_threshold_bytes: 1048576  # 1MB
  b_burst_threshold: 20
  b_cumulative_threshold_bytes: 20971520  # 20MB
  c_sample_rate: 0.02  # 2%
```

### 22.2 環境変数例

#### .env.local
```bash
# LLM API Keys
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...

# Budget
DAILY_BUDGET_USD=100.0

# Database
DUCKDB_PATH=./data/cache/aimo.duckdb

# Box Sync
BOX_SYNC_STABILITY_SECONDS=60
BOX_SYNC_TIMEOUT_SECONDS=300
```

### 22.3 参考資料

- **AIMO_Detail.md**: v1.4仕様書（詳細版）
- **AUDIT_REPORT_v1.4.md**: 実装監査レポート
- **IMPLEMENTATION_STATUS.md**: 実装状況
- **docs/implementation_roadmap_v1.4.md**: 実装ロードマップ

---

**仕様書作成日**: 2026-01-17  
**バージョン**: v1.5  
**次回改訂予定**: 実装完了後の監査結果に基づき更新
