# AIMO Cursor Pack (A + B)

This bundle contains the completed files for:
- **A. Cursor development guardrails** (`.cursor/rules/*`, `docs/*`)
- **B. Data specs and normalization** (`schemas/*`, `config/*`, `data/psl/*`, `docs/domain_parsing.md`)

PSL source: `https://publicsuffix.org/list/public_suffix_list.dat`

Notes:
- Update `config/url_normalization.yml` and `config/bytes_buckets.yml` only with a corresponding `signature_version` bump.
- `schemas/*.json` are canonical contracts for canonical event and signature records.
PSL snapshot: 2026-01-17 sha256=4bd5bd3d1fd15a8ac7cbfff17a64ba16f423a9f96b307721a46d355b40de3663
