# 監査説明書（Audit Narrative）

## 分析概要

本レポートは、**{period_start}** から **{period_end}** の期間における企業Webアクセスログを対象に、Shadow AI（未許可の生成AI等）およびセキュリティリスクを分析した結果である。

### 分析対象

| 項目 | 値 |
|------|-----|
| 分析期間 | {period_start} 〜 {period_end} |
| 対象ベンダー | {vendor_scope} |
| 総イベント数 | {total_events:,} |
| ユニークユーザー数 | {unique_users:,} |
| ユニークドメイン数 | {unique_domains:,} |
| ユニーク署名数 | {unique_signatures:,} |

---

## 抽出方法論（A/B/C選定）

本分析では、**監査耐性**（小容量通信の見逃し防止）を担保するため、以下の3系統で候補を抽出している。

### A系統：大容量転送（High-Volume）

- **条件**: bytes_sent ≥ 1MB（単一イベント）
- **件数**: {count_a:,} 件
- **割合**: 全体の {pct_a:.2%}

### B系統：高リスク小容量（High-Risk Small）

以下のいずれかを満たす通信：

1. **書き込みメソッド** (POST/PUT/PATCH) かつ AI/Unknown宛先
2. **バースト検知**: user×domain×5分窓 で 20件以上
3. **累積検知**: user×domain×日 で合計 20MB以上

- **件数**: {count_b:,} 件
- **割合**: 全体の {pct_b:.2%}

### C系統：カバレッジサンプル（Coverage Sample）

A/B候補に該当しない通信から**無作為 {sample_rate:.1%}** を抽出。

- **件数**: {count_c:,} 件
- **サンプリングシード**: `{sample_seed}` (run_id由来、再現可能)

---

## 除外条件

{exclusions_section}

※ 除外がない場合は「除外条件の適用なし」と記載

---

## 分類手法

### ルールベース分類（優先）

既知のSaaS/サービスは事前定義ルール (`rules/base_rules.json`) により高速分類。

- ルールバージョン: {rule_version}
- ルールヒット数: {rule_hit_count:,} 件

### LLM分類（未知署名のみ）

ルールに該当しない**未知署名**のみを外部LLMへ送信。

- プロンプトバージョン: {prompt_version}
- LLM送信数: {llm_sent_count:,} 署名
- キャッシュヒット率: {cache_hit_rate:.1%}
- 推定コスト: ${llm_cost_usd:.2f}

### PII保護

LLMへの送信ペイロードには以下を**含めない**：

- user_id / src_ip / device_id
- URL内のPII疑い部分（事前にマスク済み）

送信内容は署名（正規化済みホスト・パステンプレート）と統計情報のみ。

---

## 署名の決定性

- **署名バージョン**: {signature_version}
- **正規化ルール**: `config/url_normalization.yml` に準拠
- **再現性**: 同一入力は必ず同一 `url_signature` に収束

---

## 実行情報

| 項目 | 値 |
|------|-----|
| Run ID | {run_id} |
| Run Key | {run_key} |
| 入力マニフェストハッシュ | {input_manifest_hash} |
| コードバージョン | {code_version} |
| 実行開始 | {started_at} |
| 実行完了 | {finished_at} |

---

## 結論

本分析は、A/B/C の多層抽出により**小容量通信をゼロ除外しない**設計を採用している。
サンプリングは `run_id` をシードとした決定的手法であり、同一入力に対して再現可能な結果を保証する。

---

*本レポートは AIMO Analysis Engine v1.4 により自動生成されました。*
