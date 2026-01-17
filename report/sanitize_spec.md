# サニタイズ仕様（Sanitized Export Specification）

## 目的

外部支援・デバッグ用に、**完全匿名化された**サンプルデータをエクスポートする。
このデータは第三者と共有可能であり、元データへの逆引きは不可能である。

## 匿名化方式

### ハッシュ化

PII（個人識別情報）を含むフィールドは、不可逆ハッシュで置換する。

```python
import hashlib

def anonymize(value: str, salt: str) -> str:
    """Irreversible hash for PII fields."""
    if not value:
        return ""
    return hashlib.sha256(f"{salt}{value}".encode()).hexdigest()[:16]
```

### Salt管理

- Salt は環境変数 `SANITIZE_SALT` で設定
- **Saltはレポートに含めない**（逆引き防止）
- 同一Saltを使用することで、同一ユーザーの追跡可能性は維持（分析目的）

## 対象フィールド

| フィールド | 処理 | 出力名 |
|-----------|------|--------|
| user_id | SHA256(salt + value)[:16] | user_hash |
| src_ip | SHA256(salt + value)[:16] | src_hash |
| device_id | SHA256(salt + value)[:16] | device_hash |
| URL内PII | 事前にマスク済み (:email, :ip等) | url_signature使用 |

## 出力カラム

サニタイズ済みCSVは以下のカラムのみを含む：

```csv
ts,dest_domain,url_signature,service_name,usage_type,risk_level,category,bytes_sent,bytes_received,action,user_hash
```

### カラム定義

| カラム | 型 | 説明 |
|--------|-----|------|
| ts | datetime | イベント時刻（ISO8601） |
| dest_domain | string | 宛先ドメイン（eTLD+1） |
| url_signature | string | 正規化済みURL署名 |
| service_name | string | サービス名 |
| usage_type | string | business/genai/devtools/storage/social/unknown |
| risk_level | string | low/medium/high |
| category | string | サービスカテゴリ |
| bytes_sent | integer | 送信バイト数 |
| bytes_received | integer | 受信バイト数 |
| action | string | allow/block/warn |
| user_hash | string | 匿名化済みユーザー識別子 |

## 出力ファイル

### ファイル名

```
AIMO_Sanitized_{run_id}_{date}.csv
```

### サンプルサイズ

- デフォルト: 全A/B候補 + C候補（サンプル済み）
- 大規模データ: 最大100,000行（設定可能）

## 使用シナリオ

1. **外部コンサルへの共有**: 分析支援を依頼する際のサンプルデータ
2. **デバッグ**: パース/分類エラーの調査
3. **ベンチマーク**: 性能測定用のテストデータ作成

## 禁止事項

以下は**絶対に出力に含めない**：

- 生の user_id / src_ip / device_id
- 生のURL（url_full, url_path, url_query）
- Salt値
- 任意の個人識別可能情報

## 検証

サニタイズ出力前に、以下を自動検証：

```python
def validate_sanitized(df):
    """Ensure no PII in sanitized output."""
    forbidden_columns = ['user_id', 'src_ip', 'device_id', 'url_full', 'url_path', 'url_query']
    for col in forbidden_columns:
        assert col not in df.columns, f"Forbidden column found: {col}"
    
    # Check for email patterns in string columns
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    for col in df.select_dtypes(include=['object']).columns:
        matches = df[col].str.contains(email_pattern, na=False).sum()
        assert matches == 0, f"Email pattern found in {col}"
```
