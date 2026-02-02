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
#
# IMPORTANT: LLM API Keys
# - GEMINI_API_KEY (REQUIRED): Default LLM provider is Gemini
#   Get your API key from: https://makersuite.google.com/app/apikey
# - OPENAI_API_KEY (OPTIONAL): Only needed if you want to use OpenAI instead

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

## Scheduled Execution (launchd)

For automated periodic execution on macOS, use launchd:

```bash
# 1. Make wrapper script executable
chmod +x ops/bin/run_aimo.sh

# 2. Update plist with absolute paths
# Edit ops/launchd/aimo.engine.plist and replace /ABSOLUTE/PATH/TO/REPO

# 3. Install and start
cp ops/launchd/aimo.engine.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/aimo.engine.plist
launchctl start com.aimo.analysis.engine

# 4. Check status
launchctl list | grep aimo
tail -f ops/logs/launchd.out.log
```

See `ops/runbook.md` for detailed operations procedures.

## AIMO Standard Integration

The AIMO Standard specification is referenced as a git submodule at `third_party/aimo-standard`. The Engine pins to a specific Standard version (currently v0.1.7) and records the version, commit hash, and SHA256 checksum in each run manifest to ensure audit reproducibility. To sync the Standard artifacts locally, run:

```bash
python scripts/sync_aimo_standard.py --version 0.1.7
```

## Notes

- Update `config/url_normalization.yml` and `config/bytes_buckets.yml` only with a corresponding `signature_version` bump.
- `schemas/*.json` are canonical contracts for canonical event and signature records.
- PSL snapshot: 2026-01-17 sha256=4bd5bd3d1fd15a8ac7cbfff17a64ba16f423a9f96b307721a46d355b40de3663
