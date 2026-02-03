# ADR-0001: Standard Pinning and Run Reproducibility

**Status**: Accepted  
**Date**: 2026-02-02  
**Decision Makers**: Architecture Team

## Context

AIMO Analysis Engine は AIMO Standard (Taxonomy, Schema, Validator) に依存して動作する。Standard は独立したリポジトリで開発されており、継続的に更新される。

### 課題

1. **Standard の更新頻度**: Standard は独自のリリースサイクルを持ち、Engine とは非同期で更新される
2. **監査再現性**: 過去の Run を同一条件で再現できる必要がある (金融監査要件)
3. **英語版が正**: Standard は多言語対応だが、Engine は英語版を正として扱う
4. **破壊的変更**: Standard の Major バージョンアップで Taxonomy 構造が変わる可能性がある

### 要件

- 各 Run は使用した Standard の正確なバージョンを記録する
- 同じ入力 + 同じ Standard = 同じ出力 (決定論的)
- 過去の Run は当時の Standard version で再現可能
- Standard の意図しない変更 (タグ改変等) を検知する

## Decision

### 1. Git Submodule による Standard 固定

AIMO Standard を Git submodule として `third_party/aimo-standard` に配置し、特定のタグ (commit) に固定する。

```
third_party/
└── aimo-standard/  # git submodule, fixed to v0.1.1
```

**理由**:
- タグと commit の紐付けが明確
- Engine リポジトリのバージョン管理と統合
- オフライン環境でも動作可能

### 2. Pinning による事故防止

`src/standard_adapter/pinning.py` に以下を定義:

```python
PINNED_STANDARD_VERSION = "0.1.1"
PINNED_STANDARD_COMMIT = "556fa4ddb1bcce3c5169e3d36b7697a94a80a2ff"
PINNED_ARTIFACTS_DIR_SHA256 = "02c8f54602904174b68ba5e17b0b7c36123225a5f1c42baa197f64e866981db0"
```

Resolver は起動時にこれらと実際の値を照合し、不一致なら例外を投げる。

**検知できる事故**:
- タグの改変 (commit hash が変わった)
- Artifacts の差し替え (SHA が変わった)
- 誤った version の使用

### 3. Run ごとの Standard 固定

各 Run の `runs` テーブルと `run_manifest.json` に以下を記録:

```json
{
  "aimo_standard": {
    "version": "0.1.1",
    "commit": "556fa4ddb1bc...",
    "artifacts_dir_sha256": "02c8f5460290..."
  }
}
```

`run_key` の計算にも `aimo_standard_version` と `artifacts_sha256` を含め、Standard が異なれば別の Run として扱う (キャッシュ混線防止)。

### 4. 自動追従の禁止

`--version latest` や `/latest` への自動追従機能は**実装しない**。

Standard のバージョンアップは必ず:
1. 開発者が明示的に新バージョンを選択
2. Pinning 値を更新する PR を作成
3. CI で検証後にマージ

## Consequences

### メリット

| 観点 | 効果 |
|------|------|
| 監査再現性 | 過去 Run を正確に再現可能 |
| 安定性 | 意図しない Standard 変更による障害を防止 |
| 透明性 | 使用した Standard version が明確に記録される |
| 事故検知 | タグ改変や Artifacts 差し替えを即座に検知 |

### デメリット

| 観点 | 影響 | 対策 |
|------|------|------|
| 運用負荷 | Standard 更新時に手動対応が必要 | Upgrade Script で半自動化 |
| 追従遅延 | Standard の新機能/修正の適用に時間がかかる | 定期的なアップグレード計画 |
| 複雑性 | Pinning/検証のコードが増加 | 明確なドキュメントと Playbook |

## Alternatives Considered

### Alternative 1: /latest への自動追従

**却下理由**:
- 監査時に「どの version を使ったか」が曖昧になる
- Standard 側の意図しない変更で Engine が壊れる
- Run の再現性が保証できない

### Alternative 2: ネットワーク経由で Standard を取得

**却下理由**:
- オフライン環境で動作しない
- ネットワーク障害時に Engine が起動不可
- CDN キャッシュによる version 不整合リスク

### Alternative 3: Standard を Engine にコピー (vendor)

**却下理由**:
- Standard の更新履歴が失われる
- ライセンス/帰属の問題
- 変更の追跡が困難

## Related Documents

- [PLAYBOOK_AIMO_STANDARD_UPGRADE.md](../PLAYBOOK_AIMO_STANDARD_UPGRADE.md)
- [MIGRATION_NATIVE_STANDARD.md](../MIGRATION_NATIVE_STANDARD.md)
- [DEVELOPER_HANDOFF_NATIVE_STANDARD.md](../DEVELOPER_HANDOFF_NATIVE_STANDARD.md)

---

**Revision History**:
- 2026-02-02: Initial decision
