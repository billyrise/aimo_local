以下、添付の「AIMO Analysis Engine仕様書 v1.3」を前提に、実装可能性（正常動作の見込み）を検証し、信頼性（再現性・冪等性・障害復旧）と性能（I/O・メモリ・並列・コスト）を同時に強化した v1.4（最新版） を、Cursorがそのまま実装に使える粒度でマークダウン化して全文提示いたします。
なお、過去チャット履歴（AIMO/AIMOaaS）については personal_context から該当情報を取得できませんでしたため、今回の検証は本仕様書（添付）のみを根拠として実施しております。

⸻

1. v1.3のままでも「動く」可能性は高いが、運用で詰まり得る主要リスク

v1.3は「Local-First／Cloud-Smart」「署名＋統計のみ外部送信」「冪等性」「Writer Queue」「Token Bucket」「Parquet Hiveパーティション」など、運用耐性の骨格が揃っており、設計思想としては極めて妥当です。   

一方、実装に落とす際に仕様の未確定が原因で事故りやすい箇所が残っています（特に“日次・月次BPOでの安定運用”の観点）。

主要リスク（v1.3で起き得る不具合・劣化）
	1.	Box Drive同期の「未完了ファイル」を掴むリスク
　サイズが変動中のCSVを ingest すると、壊れた行・欠損・重複が混入し、監査説明が崩れます（v1.3はフォルダ前提のみで、安定化条件が未規定）。 
	2.	URL正規化と署名の「安定性」の定義不足
　大小文字、%エンコード、末尾スラッシュ、クエリ順、Punycode等の扱いが曖昧だと、署名がブレてキャッシュ命中率が落ちる（翌月以降の短縮が効かない）。   
	3.	eTLD+1算出（dest_domain）の実装依存が未規定
　Public Suffix List をどう扱うかが未規定で、ドメイン集計が不正確になり得ます。 
	4.	DuckDBのUPSERT/競合/世代管理の厳密仕様が未規定
　Writer Queueは方針として正しい一方、run再実行時の二重計上や、途中失敗→再開時の整合を担保するキー設計が要ります。 
	5.	LLM JSON厳格出力の“検証と再試行”が未規定
　「厳格JSONのみ」は要求されていますが、JSON Schema検証→自動リトライ→最終スキップの状態遷移が未確定だと、needs_review が溜まり続けます。 

⸻

2. 改善方針（v1.4の狙い）

v1.3の思想・構造は維持しつつ、以下を明文化し、Cursor実装でブレないようにします。
	•	I/Oの原子性：Box同期・Parquet生成・成果物出力の「未完了を掴まない」
	•	署名の完全決定性：正規化→抽象化→署名が常に同じ
	•	run再実行の完全冪等：同一入力は同一run_keyに収束、再実行で二重計上しない
	•	DB整合の厳密化：UPSERTキー、run別世代、ステージ別チェックポイント
	•	LLMの“壊れた出力”前提設計：JSON検証・再試行・永続失敗スキップ
	•	性能KPIの実測可能化：rows/sec、I/O量、未知率、キャッシュ命中率、コストを毎回記録

⸻

:::writing{id=“74019” variant=“standard”}

AIMO Analysis Engine 仕様書 v1.4（Cursor実装用・完全版）

改訂日：2026-01-17（v1.4）
対象：AIMOaaS Tier2（月次分析BPO）獲得および運用効率化の中核エンジン
設計思想：Learning-First（初回＝学習優先）／Local-First, Cloud-Smart／監査耐性・運用耐性・コスト統制を最優先

⸻

0. 目的と成功条件

0.1 目的（初回・翌月以降を分けて最適化）

本エンジンは、企業のWebアクセスログ（SWG / CASB / FW / DNS 等）を解析し、以下を実現する。

現状監査（初回）
	•	Shadow AI（未許可の生成AI等）の利用実態（誰が・何を・どれくらい）を可視化
	•	リスク（データ持ち出し兆候、規程違反、重要部門・重要ユーザーの高リスク行動）を可視化
	•	「なぜ見逃したのか」と責められない説明可能性（監査耐性）を担保

月次BPO（翌月以降）
	•	初回の学習資産（キャッシュ／辞書／ルール／モデル）により、処理時間とAPIコストを大幅に削減
	•	増分処理（新規ログのみ）で定常運用を安定化
	•	再現性（同じ入力なら同じ出力）・冪等性（途中失敗しても再実行で破綻しない）を担保

0.2 成功条件（Acceptance Criteria）
	•	署名の決定性：同一URL入力は常に同一 url_signature（同一 signature_version 前提）
	•	冪等性：同一入力（同一input_manifest_hash）で再実行しても、DBと成果物が二重計上されない
	•	PII送信ゼロ：外部API送信ペイロードに user_id / src_ip / PII疑い文字列を含めない
	•	監査説明：A/B/C抽出、バースト・累積・サンプル率、除外条件と件数をレポートに必ず明記
	•	大規模耐性：Excel生成は constant_memory=True で落ちない（巨大表は集計に縮約し、詳細はCSV/Parquetに退避）

⸻

1. スコープ

1.1 対象入力（ログソース）
	•	Netskope
	•	Zscaler（ZIA / NSS）
	•	Microsoft Defender for Cloud Apps（MDCA / 旧MCAS）
	•	Palo Alto Networks / Prisma Access
	•	Cisco Umbrella
	•	Symantec / Blue Coat（ProxySG 等）
	•	McAfee / Skyhigh（Trellix）
	•	i-FILTER
	•	その他：上記に準ずるSWG/Proxy/FW/DNSログ（CSV/W3C/JSON/CEF 等）

1.2 対象出力
	•	Excel監査レポート（複数シート、グラフ含む）
	•	ダッシュボード用JSON（集計結果、時系列、ドリルダウンキー）
	•	サニタイズ済み共有用CSV（外部支援/デバッグ用：完全匿名化）

1.3 非対象（v1.4では実装しない）
	•	リアルタイムストリーミング（Kafka/Flink等）
	•	多テナント同居クラウド処理（原則ローカル運用）
	•	高度な差分プライバシー（必要時はオプション設計として別紙）

⸻

2. 実行環境・フォルダ・運用前提

2.1 実行環境（本番）
	•	Mac Studio（常時稼働、ローカル処理）
	•	macOS：launchd による定期実行（推奨）

2.2 開発環境
	•	MacBook Pro + Cursor

2.3 入出力フォルダ（Box Drive同期）
	•	data/input/：顧客がBoxへアップロードしたログが同期
	•	data/processed/：正規化後Parquet（Hiveパーティション）
	•	data/cache/：DuckDBデータベース、各種キャッシュ
	•	data/output/：Excelレポート、JSON、サニタイズCSV
	•	logs/：実行ログ、監査ログ（JSONL推奨）
	•	rules/：ルール定義（顧客別オーバーレイ可）
	•	schemas/：ベンダー別フィールドマッピング（YAML/JSON）

2.4 Box同期の安定化要件（重要）

Box同期フォルダのファイルを直接 ingest しない。必ず以下の手順で「安定化」する。
	•	入力検知：data/input に新規ファイルを検知
	•	安定化条件（いずれも満たすまで待機）
	•	ファイルサイズが N 秒（既定：60秒）変化しない
	•	最終更新時刻が N 秒（既定：60秒）変化しない
	•	安定化後にローカル作業領域へコピー：
	•	data/work/run_id/raw/<original_filename>
	•	以後、解析は work 配下のみを参照（input は読み取り専用扱い）

⸻

3. 全体アーキテクチャ（段階処理）

3.1 ステージ構成（v1.4）
	•	Stage 0：Orchestrator（実行管理）
	•	Stage 1：Ingestion & Normalization（高速取込・正規化）
	•	Stage 2：Risk Candidate Selection（監査防衛の抽出設計）
	•	Stage 3：Signature & Dedup（署名化・重複排除・キャッシュ照合）
	•	Stage 4：LLM Analysis（未知署名のみ・バッチ処理）
	•	Stage 5：Reporting（全量結合・集計・Excel/JSON/サニタイズ出力）

3.2 Stage 0：Orchestrator（実行管理）

3.2.1 Runキー（冪等性の中核）
	•	input_manifest_hash：入力ファイルのパス・サイズ・sha256 を列挙し、正規化してhash化
	•	run_key = sha256(input_manifest_hash + target_range + signature_version + rule_version + prompt_version)
	•	run_id：run_key の短縮表現（例：先頭16桁base32）
同一入力・同一設定は必ず同一 run_id に収束する。

3.2.2 排他制御
	•	同時実行禁止（原則）：ファイルロック（data/cache/aimo.lock）を取得できたrunのみ実行
	•	例外：LLM Workerは同一run内で並列可（Writer Queueで直列書込み）

3.2.3 ステージ別チェックポイント
	•	runs.status：running/succeeded/failed/partial
	•	runs に stage_checkpoint（last_completed_stage）を持ち、再実行時は未完了ステージから再開可能
	•	再開時の中間生成物は run_id 世代で管理（data/work/run_id/）

3.2.4 予算制御（Token Bucket）
	•	DAILY_BUDGET_USD を設定し、LLM送信前に「推定コスト分の予算確保」を行う
	•	枯渇時の優先順位：A/Bを優先し、C（Coverage Sample）を停止

⸻

4. データ提出要件（初回＝学習優先・取りこぼし防止）

4.1 共通要件（必須）
	•	期間：直近1年（可能なら日次/週次で分割）
	•	allow / block の両方（抑止の証跡として重要）
	•	可能な限り、上り/下り（upload/download相当）bytesを含める
	•	ユーザー識別子は匿名化可（不可逆ハッシュ＋saltは顧客保持）が、同一性が維持されること

4.2 初回抽出設計（提出側で可能なら推奨、難しければAIMO側で実施）
	•	A系統：High-Volume
	•	bytes_sent（upload相当）>= 1MB（既定、設定で変更可）
	•	B系統：High-Risk Small
	•	bytes_sentが小さくても、以下のいずれか
	•	write_method（POST/PUT/PATCH相当）
	•	バースト：user×domain×5分窓で回数>=20（既定）
	•	累積：user×domain×日で sum(bytes_sent)>=20MB（既定）
	•	宛先カテゴリが GenAI/AI/Unknown/Uncategorized
	•	C系統：学習用カバレッジ枠（必須）
	•	B候補の bytes_sent < 1MB 帯から無作為2%（既定）

4.3 提出が重すぎる場合の代替（必須の3点セット）
	•	全量メタ集計（母集団の説明）
	•	高価値イベント全量（bytes_sent上位、AI/Unknown、block、DLP等）
	•	層化サンプル（bytes帯を層化し各層から無作為抽出、率を明記）

⸻

5. 標準スキーマ（AIMO Canonical Schema）

5.1 Canonical Event（標準イベント行）

すべての入力ログは最終的に以下のカラムへ正規化される（型は推奨）。
	•	event_time: TIMESTAMP（UTC推奨、ISO8601 → 変換）
	•	vendor: VARCHAR（paloalto / zscaler / netskope / mdca / umbrella / bluecoat / skyhigh / ifilter / other）
	•	log_type: VARCHAR（web / proxy / traffic / url / dns / cloudapp / dlp 等）
	•	user_id: VARCHAR（匿名化済み識別子）
	•	user_dept: VARCHAR（任意、匿名化可）
	•	device_id: VARCHAR（任意、匿名化可）
	•	src_ip: VARCHAR（任意、匿名化可）
	•	dest_host: VARCHAR（FQDN）
	•	dest_domain: VARCHAR（eTLD+1：Public Suffix List により算出）
	•	url_full: VARCHAR（可能なら完全URL、無い場合は組立）
	•	url_path: VARCHAR（正規化後）
	•	url_query: VARCHAR（正規化後、追跡パラメータ除去）
	•	http_method: VARCHAR（任意）
	•	status_code: INTEGER（任意）
	•	action: VARCHAR（allow / block / warn / observe 等）
	•	app_name: VARCHAR（任意）
	•	app_category: VARCHAR（AI/GenAI/Business/Storage/Unknown 等）
	•	bytes_sent: BIGINT（upload相当）
	•	bytes_received: BIGINT（download相当）
	•	content_type: VARCHAR（任意）
	•	user_agent: VARCHAR（任意）
	•	raw_event_id: VARCHAR（任意：元ログの一意キーがある場合）
	•	ingest_file: VARCHAR（入力ファイル名）
	•	ingest_lineage_hash: VARCHAR（行のハッシュ：改ざん検知・再処理抑止）

5.2 Canonical Signature（署名行）
	•	url_signature: VARCHAR（PK、Stableであることが最重要）
	•	signature_version: VARCHAR
	•	norm_host: VARCHAR
	•	norm_path_template: VARCHAR
	•	path_depth: INTEGER
	•	param_count: INTEGER
	•	has_auth_token_like: BOOLEAN
	•	bytes_sent_bucket: VARCHAR（L/M/H/C 等）
	•	candidate_flags: VARCHAR（A/B/C, burst, cumulative, sampled 等）

⸻

6. URL正規化（コスト削減と見逃し防止の両立）

6.1 正規化の目的
	•	細かすぎるとキャッシュが効かずコスト増
	•	粗すぎると誤判定増
	•	v1.4は「追跡パラメータ除去＋危険ID抽象化＋決定性の完全明文化」を標準

6.2 決定性を担保する正規化手順（順序厳守）
	1.	入力の前処理

	•	文字列トリム
	•	スキーム除去（http/https）
	•	Host部分を小文字化
	•	Punycode正規化（IDN対応）
	•	デフォルトポート除去（:80/:443）
	•	連続スラッシュ正規化（”//” → “/”）
	•	末尾スラッシュ規則：パスが “/” のみの場合を除き、末尾 “/” は除去

	2.	クエリ正規化

	•	追跡パラメータ除去（必須）
	•	utm_*, gclid, fbclid, ref, session, sid, phpsessid, mc_cid, mc_eid 等
	•	追加リストは設定ファイル（config/url_normalization.yml）で顧客別に拡張
	•	残すパラメータ（key_param_subset）
	•	既定は空（保持しない）
	•	必要時のみホワイトリスト方式で保持（例：api_version 等）
	•	クエリ並び順はキー昇順で固定（決定性）

	3.	ID/トークン抽象化（必須）

	•	UUID、長いhex、長いbase64風、数値連番等を :id に置換（ローカル処理）
	•	email / ip / user名らしきものがURL内に含まれる場合は検知しマスク
	•	置換前後の検知ログを pii_audit に記録（外部送信監査）

6.3 署名生成（v1.4）
	•	hashアルゴリズム：sha256（hex）を標準（衝突回避）
	•	method_group：GET / WRITE（POST/PUT/PATCH） / OTHER
	•	bytes_bucket：L/M/H/C（閾値は設定）
	•	url_signature = sha256(
norm_host + “|” +
norm_path_template + “|” +
key_param_subset + “|” +
method_group + “|” +
bytes_bucket + “|” +
signature_version
)

⸻

7. ルールベース分類（Stage 1）

7.1 目的
	•	既知の安全通信や業務SaaSを高確度で分類し、LLMへ送らない
	•	翌月以降の高速化のため、分類結果をキャッシュへ蓄積

7.2 ルール定義
	•	rules/base_rules.json：共通ルール
	•	rules/customer_overrides/<customer_id>.json：顧客別追加（任意）
	•	ルールは以下を含む：
	•	pattern（host/domain/path regex）
	•	service_name, category, default_risk, usage_type（Corporate/Safe等）
	•	rule_id, rule_version
	•	ルール適用は「最長一致＋優先度」で決定（曖昧性排除）

7.3 出力
	•	classification_source = RULE
	•	rule_id, rule_version を記録
	•	analysis_cache に UPSERT（is_human_verified が true の行は上書き禁止）

⸻

8. 監査耐性の抽出ロジック（Stage 2）

8.1 原則
	•	サイズ閾値「単独」での除外は禁止
	•	A/B/C＋バースト＋累積＋サンプルを必須とし「小容量ゼロ除外」を構造的に防止

8.2 必須集計（決定性のためSQL定義を固定）
	•	累積（必須）：user_id × dest_domain × day の sum(bytes_sent)
	•	バースト（必須）：user_id × dest_domain × 5min window の count(write_methods)
	•	カテゴリ（可能なら）：AI/Unknown を優先

8.3 A/B/C（既定値：設定で変更可）
	•	A（High-Volume）：bytes_sent >= 1MB
	•	B（High-Risk Small）：
	•	write_method かつ（AI/Unknown宛 または burst>=20 または cumulative>=20MB）
	•	C（Coverage Sample）：
	•	B候補の bytes_sent < 1MB から無作為2%
	•	乱数seedは run_id で固定（再現性）

8.4 監査説明用の記録（必須）
	•	A/B/Cの件数、割合、bytes帯
	•	除外条件と除外件数（存在する場合）
	•	サンプル率、サンプル方式、乱数seed（run_id）

⸻

9. キャッシュ／DB設計（DuckDB中心）

9.1 採用方針
	•	ローカルで高速な分析SQLと大規模Parquetスキャンを実現
	•	書込みは単一Writer（Producer-Consumer）で安定運用
	•	再実行冪等のため、run_id と主キー制約を厳格化

9.2 DBファイル
	•	data/cache/aimo.duckdb

9.3 主要テーブル（必須）
	•	runs（PK: run_id）
	•	run_id, run_key, started_at, finished_at
	•	target_range_start, target_range_end
	•	vendor_scope
	•	status, last_completed_stage
	•	code_version（git hash等）
	•	rule_version, signature_version, prompt_version
	•	input_manifest_hash
	•	input_files（PK: run_id + file_hash）
	•	run_id, file_path, file_size, file_hash, vendor, log_type
	•	min_time, max_time, row_count_estimate, ingested_at
	•	analysis_cache（PK: url_signature）
	•	url_signature, service_name, usage_type, risk_level, category
	•	confidence, classification_source, evidence(<=500)
	•	analysis_date
	•	signature_version, rule_version, prompt_version
	•	status（active/needs_review/skipped）
	•	is_human_verified（上書き禁止フラグ）
	•	error_type, error_reason, retry_after, failure_count, last_error_at
	•	signature_stats（PK: run_id + url_signature）
	•	run_id, url_signature, norm_host, norm_path_template, bytes_sent_bucket
	•	access_count, unique_users
	•	bytes_sent_sum, bytes_sent_p95, bytes_sent_max
	•	burst_max_5min, cumulative_user_domain_day_max
	•	candidate_flags, sampled
	•	api_costs（run_id + timestamp）
	•	provider, model, request_tokens, response_tokens, cost_usd_estimated, latency_ms
	•	performance_metrics（run_id + timestamp）
	•	stage, metric_name, value, unit
	•	pii_audit（run_id + detected_at）
	•	url_signature, pii_type, redaction_applied

9.4 UPSERT/上書きルール（重要）
	•	analysis_cache は原則UPSERT
	•	ただし is_human_verified=true の行は上書き禁止（人手確定を最優先）
	•	status=skipped は送信禁止（永続失敗の安全弁）

⸻

10. LLM分析（Stage 4）

10.1 送信対象（厳格）
	•	analysis_cache 未登録、または未分類（risk_level=Unknown等）で status=active の url_signature のみ
	•	status=skipped は送信禁止
	•	status=needs_review でも、永続失敗（context_length_exceeded等）は送信禁止
	•	再送は retry_after に従う

10.2 PII送信禁止（必須）

送信ペイロードに以下を含めない：
	•	user_id, src_ip, device_id
	•	生URLのPII疑い部分
送信は原則、norm_host / norm_path_template / 統計のみ。

10.3 バッチ設計
	•	1リクエストに 10〜20署名（可変）
	•	事前にトークン量見積り→予算確保できた分のみ送信

10.4 プロンプト要件（幻覚防止＋JSON厳格）

LLMへ必ず指示する：
	•	不明なものは推測しない
	•	判断不能は Unknown とする
	•	出力は厳格なJSON（スキーマ準拠）のみ
	•	confidence と rationale_short を必須
	•	返答JSONはローカルで JSON Schema 検証し、失敗時は最大2回まで再試行（同一バッチで）

10.5 失敗時の取り扱い（無限ループ防止）
	•	error_type / error_reason を記録
	•	永続失敗（context_length_exceeded 等）は status=‘skipped’ に自動遷移
	•	一時失敗（rate_limit/timeout）は retry_after を設定して再試行
	•	needs_review は「人手確認キュー」であり、自動再送フラグではない

10.6 書込み競合対策（Writer Queue方式：必須）
	•	Workerは並列可、DuckDB書込みは単一Writerで直列化
	•	Producer（Worker）→ Async Queue → Writer Thread（単一）→ UPSERT
	•	Writerはバッチコミット（例：50〜200行）
	•	Writerクラッシュ時も、再実行で復旧できるよう run_id と batch_id を持つ

10.7 予算制御（Token Bucket）
	•	DAILY_BUDGET_USD を設定
	•	推定コスト（入力/出力トークン×単価）で予算を消費
	•	予算枯渇時は C枠停止、A/B優先

⸻

11. レポーティング（Stage 5）

11.1 全量結合
	•	Parquet（イベント）に analysis_cache を url_signature で結合
	•	UnknownはUnknownのまま表示（無理に埋めない）

11.2 主要集計（必須）
	•	Top 10 Shadow AI Apps（ユーザー数、アクセス数、bytes_sent合計、リスク別内訳）
	•	Top 10 High Risk Users（アップロード量、High/Critical宛先数、burst回数）
	•	部門別リスクスコア（ユーザー数で正規化、High/Critical割合）
	•	時系列（週次/月次）：Unknown率、AIカテゴリ率、High/Critical比率、ブロック率
	•	ブロック/許可のポリシー差分が疑われる宛先一覧

11.3 監査説明（必須：レポートに明記）
	•	初回抽出設計（A/B/C、バースト、累積、サンプル率、seed）
	•	対象母集団（全量メタ集計）と抽出件数・割合
	•	除外がある場合：除外条件と除外件数
	•	小容量をゼロ除外していない説明
	•	LLM利用範囲（未知署名のみ）とPII送信禁止の説明

11.4 Excel生成（大規模耐性）
	•	XlsxWriter：constant_memory=True を必須
	•	大規模表：DuckDBで集計→1,000行ずつフェッチ→書込み
	•	グラフ：集計表の縮約範囲を参照（明細はExcelに全件載せない）

11.5 サニタイズ（完全匿名化）エクスポート（必須）
	•	外部支援/デバッグ共有用に、不可逆ハッシュ化済みサンプルCSVを生成
	•	user_id、src_ip、device_id、URL内PII疑い部分は不可逆化
	•	url_signature と統計のみ残す

⸻

12. 学習資産（翌月以降の短縮要件）

12.1 ParquetのHiveパーティション（必須）
	•	data/processed/vendor=/date=/…snappy.parquet
	•	翌月以降は date パーティションのみ増分処理

12.2 キャッシュ命中率最大化（必須）
	•	署名の安定性を最優先（署名ブレはKPI悪化）
	•	analysis_cache は is_human_verified を最優先

12.3 ML Pre-Filter（オプション）
	•	初回後に段階導入（評価AUC等を満たす場合のみ）
	•	低信頼は必ずLLMまたはHumanへ回す

⸻

13. 運用フロー

13.1 日次バッチ（例：毎日AM 2:00）
	1.	Box同期：顧客アップロード → data/input
	2.	Orchestrator：安定化検知→workへコピー→run作成
	3.	Ingest→Parquet生成
	4.	A/B/C抽出→署名統計生成
	5.	キャッシュ照合→未知署名のみLLM
	6.	Writer QueueでDB反映
	7.	レポート生成→data/output→Box共有

13.2 冪等性
	•	入力ファイルhashを記録し同一入力を二重処理しない
	•	run_id単位で成果物と中間生成物を管理し、途中失敗でも再実行で回復
	•	Parquet再生成は run_id 世代で管理（上書き時は原子置換）

13.3 スケジューリング（macOS）
	•	launchd（推奨）：標準出力/標準エラーを logs/ にリダイレクト

⸻

14. セキュリティ・プライバシー

14.1 データ局所性
	•	生ログはローカルSSDとメモリ内に留める
	•	外部へ送るのは署名と統計のみ

14.2 PII検知と送信禁止
	•	URL内のemail/IP/トークン疑いを検知しマスク
	•	pii_audit に記録し、外部送信ペイロードを監査可能にする

14.3 ゼロ保持の方針
	•	外部APIは「入力データを学習に使わない」契約形態を推奨
	•	APIキーは .env.local で管理し、リポジトリに含めない

⸻

15. 性能・コストのSLO（目標）

15.1 初回（学習優先）
	•	優先度：監査説明可能性 ＞ 学習資産最大化 ＞ 速度

15.2 翌月以降（月次BPO）
	•	増分Parquet＋高命中キャッシュにより、処理時間とAPIコストを大幅に削減
	•	KPI：キャッシュ命中率、未知率、LLM送信数/日、USD消化

⸻

16. 観測性（ログ・メトリクス）

16.1 ログ（JSONL推奨）
	•	run開始/終了、入力ファイル、対象件数、A/B/C件数、未知署名数、LLM送信数、失敗数（error_type別）
	•	除外条件と件数（必ず記録）

16.2 メトリクス（DBへ保存）
	•	stageごとの処理時間、rows/sec、I/O量、メモリ使用量（可能なら）
	•	LLMコスト（推定）と予算消化

⸻

17. 品質保証（QA）

17.1 テスト（必須）
	•	URL正規化（追跡パラメータ除去、ID抽象化、順序固定）
	•	署名安定性（同じ入力→同じ署名）
	•	A/B/C抽出（小容量ゼロ除外にならない）
	•	Writer Queue整合性（重複UPSERT、部分失敗→再実行）
	•	error_typeによる再送制御（永続失敗が再送されない）
	•	監査説明セクション生成の必須項目チェック

17.2 回帰（毎月）
	•	signature_version更新時：命中率悪化がないか検証
	•	ルール更新：誤分類増がないか検証

⸻

18. 実装構成（リポジトリ）

18.1 ディレクトリ構造

src/
	•	main.py（エントリ）
	•	orchestrator.py
	•	ingestor/
	•	base.py
	•	paloalto.py
	•	zscaler.py
	•	netskope.py
	•	mdca.py
	•	umbrella.py
	•	bluecoat.py
	•	skyhigh.py
	•	ifilter.py
	•	normalize/
	•	url_normalizer.py
	•	pii_detector.py
	•	domain_parser.py（Public Suffix List対応）
	•	rules/
	•	rule_engine.py
	•	candidates/
	•	selector.py
	•	signatures/
	•	signature_builder.py
	•	deduplicator.py
	•	llm/
	•	analyzer.py
	•	budget.py
	•	writer_queue.py
	•	prompt_templates.py
	•	json_schema.py
	•	db/
	•	schema.sql
	•	duckdb_client.py
	•	reporting/
	•	reporter.py
	•	excel_writer.py
	•	dashboard_json.py
	•	sanitized_export.py
	•	cli.py（init-db/runなど）

rules/
	•	base_rules.json
	•	customer_overrides/<customer_id>.json（任意）

schemas/
	•	/<log_type>.yml（フィールドマッピング）

18.2 依存ライブラリ（推奨）
	•	polars
	•	duckdb
	•	pyarrow（Parquet/partition）
	•	python-dotenv
	•	regex
	•	httpx（LLM呼び出し）
	•	tenacity（バックオフ）
	•	xlsxwriter
	•	psutil（任意：監視）
	•	publicsuffix2（または同等：eTLD+1）
	•	jsonschema（LLM JSON検証）

⸻

19. Cursor実装手順（v1.4）

Step 1：プロジェクト雛形＋DBスキーマ
	•	ディレクトリ構造、.env.local、DuckDBスキーマ、Run管理（runs/input_files）を整備

Step 2：ベンダー別Ingestor（1製品から開始）
	•	Palo Alto または Zscaler を最初に実装し Canonical Event 正規化を確認
	•	以降、各ベンダーへ拡張（schemas/のマッピング駆動）

Step 3：URL正規化（決定性の厳格化）＋PII検知
	•	正規化手順（6.2）を順序固定で実装
	•	pii_audit へ記録

Step 4：A/B/C候補抽出（累積・バースト・サンプル）
	•	seed固定（run_id）で再現性確保
	•	小容量帯ゼロ除外のテスト

Step 5：署名・統計・キャッシュ照合
	•	signature_version管理
	•	signature_stats生成
	•	analysis_cache照合（既知除外）

Step 6：LLM Analyzer（JSON Schema、Token Bucket、Writer Queue）
	•	送信前に予算確保
	•	バッチ送信
	•	JSON検証→再試行→永続失敗skipped
	•	Writer QueueでUPSERT

Step 7：Reporter（Excel/JSON＋監査説明＋サニタイズ）
	•	constant_memoryでExcel
	•	監査説明セクション必須
	•	サニタイズエクスポート必須

⸻

20. 付録

本仕様書は、別紙「製品別ログ提出依頼書（初回＝学習優先）」に準拠する。

以上。
:::—

3. 補足（実装検証の結論）
	•	v1.3の骨格は堅牢で、設計としては正常動作が十分見込めます（特に Stage 分割、署名キャッシュ、Writer Queue、Token Bucket）。   
	•	v1.4では、Box同期の原子性、署名決定性、run冪等性、LLM JSON検証、DB UPSERT規約を仕様として固定し、運用事故の確率を大きく下げています。

ご要望があれば、この v1.4 を前提に「Cursorへの投入用：Stepごとの“実装プロンプト（ファイル単位）”」まで分解した版（そのまま貼って開発できる粒度）も、同一仕様から矛盾なく生成いたします。