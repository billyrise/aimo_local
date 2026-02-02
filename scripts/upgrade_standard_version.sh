#!/bin/bash
#
# AIMO Standard Upgrade Script
#
# Purpose: Assist developers in upgrading to a new AIMO Standard version.
# This script does NOT auto-follow to "latest". It only upgrades to a
# specified version and updates the pinning values.
#
# Usage:
#   ./scripts/upgrade_standard_version.sh --version X.Y.Z
#
# What this script does:
#   1. Checkout the submodule to the specified version tag
#   2. Run sync script to generate/extract artifacts
#   3. Update src/standard_adapter/pinning.py with new values
#   4. Show what changed and next steps
#
# What this script does NOT do:
#   - Auto-follow to "latest"
#   - Create commits (developer must review and commit)
#   - Update any other files (Adapter, Schema, etc.)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SUBMODULE_PATH="third_party/aimo-standard"
PINNING_FILE="src/standard_adapter/pinning.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
VERSION=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --version|-v)
            VERSION="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 --version X.Y.Z"
            echo ""
            echo "Upgrades AIMO Standard submodule to the specified version"
            echo "and updates pinning values in pinning.py."
            echo ""
            echo "Options:"
            echo "  --version, -v   Target version (e.g., 0.2.0)"
            echo "  --help, -h      Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate version argument
if [ -z "$VERSION" ]; then
    echo -e "${RED}Error: --version is required${NC}"
    echo "Usage: $0 --version X.Y.Z"
    exit 1
fi

echo "========================================"
echo "AIMO Standard Upgrade Script"
echo "========================================"
echo "Target version: v${VERSION}"
echo "Submodule path: ${SUBMODULE_PATH}"
echo "========================================"
echo ""

# Step 1: Check submodule exists
cd "$PROJECT_ROOT"
if [ ! -d "$SUBMODULE_PATH" ]; then
    echo -e "${YELLOW}Submodule not found. Initializing...${NC}"
    git submodule update --init "$SUBMODULE_PATH"
fi

# Step 2: Checkout target version
echo "Checking out v${VERSION}..."
cd "$SUBMODULE_PATH"
git fetch --all --tags

# Try with v prefix first
TAG="v${VERSION}"
if ! git rev-parse "$TAG" >/dev/null 2>&1; then
    TAG="${VERSION}"
    if ! git rev-parse "$TAG" >/dev/null 2>&1; then
        echo -e "${RED}Error: Version ${VERSION} not found in repository${NC}"
        echo "Available tags:"
        git tag -l | tail -10
        exit 1
    fi
fi

git checkout "$TAG"
COMMIT=$(git rev-parse HEAD)
echo -e "${GREEN}Checked out $TAG at commit: ${COMMIT:0:12}${NC}"

cd "$PROJECT_ROOT"

# Step 3: Run sync script (skip pin check since we're upgrading)
echo ""
echo "Running sync script..."
python scripts/sync_aimo_standard.py --version "$VERSION" --skip-pin-check --json > /tmp/sync_result.json

# Extract values from sync result
ARTIFACTS_SHA=$(python -c "import json; print(json.load(open('/tmp/sync_result.json'))['directory_sha256'])")

echo -e "${GREEN}Sync complete.${NC}"
echo "  Commit: ${COMMIT:0:12}"
echo "  Artifacts SHA: ${ARTIFACTS_SHA:0:16}..."

# Step 4: Update pinning.py
echo ""
echo "Updating ${PINNING_FILE}..."

# Create backup
cp "$PINNING_FILE" "${PINNING_FILE}.bak"

# Update values using sed
sed -i.tmp "s/PINNED_STANDARD_VERSION = .*/PINNED_STANDARD_VERSION = \"${VERSION}\"/" "$PINNING_FILE"
sed -i.tmp "s/PINNED_STANDARD_COMMIT = .*/PINNED_STANDARD_COMMIT = \"${COMMIT}\"/" "$PINNING_FILE"
sed -i.tmp "s/PINNED_ARTIFACTS_DIR_SHA256 = .*/PINNED_ARTIFACTS_DIR_SHA256 = \"${ARTIFACTS_SHA}\"/" "$PINNING_FILE"

rm -f "${PINNING_FILE}.tmp"

echo -e "${GREEN}Updated pinning values:${NC}"
echo "  PINNED_STANDARD_VERSION = \"${VERSION}\""
echo "  PINNED_STANDARD_COMMIT = \"${COMMIT}\""
echo "  PINNED_ARTIFACTS_DIR_SHA256 = \"${ARTIFACTS_SHA}\""

# Step 5: Show diff
echo ""
echo "========================================"
echo "Changes to ${PINNING_FILE}:"
echo "========================================"
diff "${PINNING_FILE}.bak" "$PINNING_FILE" || true
rm -f "${PINNING_FILE}.bak"

# Step 6: Show next steps
echo ""
echo "========================================"
echo -e "${GREEN}Upgrade preparation complete!${NC}"
echo "========================================"
echo ""
echo "NEXT STEPS:"
echo ""
echo "1. Update constants.py if needed:"
echo "   src/standard_adapter/constants.py"
echo "   AIMO_STANDARD_VERSION_DEFAULT = \"${VERSION}\""
echo ""
echo "2. Review Standard release notes for breaking changes"
echo ""
echo "3. Run tests:"
echo "   python scripts/sync_aimo_standard.py --version ${VERSION}"
echo "   pytest -q"
echo ""
echo "4. If breaking changes exist, update:"
echo "   - src/standard_adapter/taxonomy.py"
echo "   - src/standard_adapter/schemas.py"
echo "   - llm/schemas/analysis_output.schema.json"
echo "   - See: docs/PLAYBOOK_AIMO_STANDARD_UPGRADE.md"
echo ""
echo "5. Commit changes:"
echo "   git add ."
echo "   git commit -m 'chore: upgrade AIMO Standard to v${VERSION}'"
echo ""
echo "========================================"
