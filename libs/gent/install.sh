#!/bin/bash
# Gent (Git Ent) Installation Script
# Installs gent as an editable uv tool and sets up shell integration.
#
# For non-editable installs, use pip/uv directly and then run:
#   wt setup-shell

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}wt (Git Ent) Installation${NC}"
echo -e "${BLUE}=========================${NC}"
echo "Tree shepherds for your working trees"
echo ""

# Get path to this script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if we're in a worktree structure and warn if not in main
set +e
CHECK_RESULT=$(python3 "$SCRIPT_DIR/check_install_location.py" 2>&1)
CHECK_EXIT_CODE=$?
set -e

if [ $CHECK_EXIT_CODE -ne 0 ]; then
    MAIN_GENT=$(echo "$CHECK_RESULT" | cut -d'|' -f2)
    echo -e "${YELLOW}⚠️  Warning: Installing from non-main worktree${NC}"
    echo "Install from the main worktree instead:"
    echo "  cd $MAIN_GENT && ./install.sh"
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    echo ""
fi

# Install as editable uv tool
echo -e "${BLUE}Installing wt package (editable)...${NC}"
if [ -f "$SCRIPT_DIR/pyproject.toml" ]; then
    uv tool install --force --editable "$SCRIPT_DIR"
else
    echo "Error: Could not find pyproject.toml in $SCRIPT_DIR"
    exit 1
fi
echo -e "${GREEN}✓${NC} wt package installed"
echo ""

# Set up shell integration. uv tool install drops the binary in its bin dir
# but does not modify the running shell's PATH, so invoke wt by absolute path.
WT_BIN_DIR="$(uv tool dir --bin 2>/dev/null || echo "$HOME/.local/bin")"
"$WT_BIN_DIR/wt" setup-shell
