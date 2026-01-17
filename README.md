# AIMO Analysis Engine

GenAI usage analysis engine from firewall logs.

## Requirements

- **Python 3.11+** (required)
- See `requirements.txt` for dependencies

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment variables (required)
# The project uses .env.local for local configuration
# Copy env.example to .env.local and fill in your values:
# cp env.example .env.local
# Edit .env.local with your API keys and settings

# Run E2E smoke test
python3 src/main.py sample_logs/paloalto_sample.csv --vendor paloalto

# Run tests
python3 -m pytest tests/ -v
```

**Note**: The project uses `.env.local` for environment variables (not `.env`). See `env.example` for template.

## Project Structure

This bundle contains:
- **A. Cursor development guardrails** (`.cursor/rules/*`, `docs/*`)
- **B. Data specs and normalization** (`schemas/*`, `config/*`, `data/psl/*`, `docs/domain_parsing.md`)

PSL source: `https://publicsuffix.org/list/public_suffix_list.dat`

## Notes

- Update `config/url_normalization.yml` and `config/bytes_buckets.yml` only with a corresponding `signature_version` bump.
- `schemas/*.json` are canonical contracts for canonical event and signature records.
- PSL snapshot: 2026-01-17 sha256=4bd5bd3d1fd15a8ac7cbfff17a64ba16f423a9f96b307721a46d355b40de3663
