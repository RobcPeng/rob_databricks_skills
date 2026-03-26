#!/usr/bin/env bash
# install-practice-skill.sh
# Installs the databricks-practice skill into your Claude Code project.
#
# Usage:
#   bash install-practice-skill.sh
#   # or
#   curl -sSL <raw-url>/install-practice-skill.sh | bash

set -euo pipefail

SKILL_NAME="databricks-practice"
SKILL_DIR=".claude/skills/${SKILL_NAME}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Installing: ${SKILL_NAME} skill"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check we're in a project directory
if [ ! -d ".claude" ]; then
    echo ""
    echo "⚠️  No .claude directory found in $(pwd)"
    echo "   Either run this from your AI Dev Kit project directory,"
    echo "   or create the directory first:"
    echo ""
    echo "   mkdir -p .claude/skills"
    echo ""
    read -p "Create .claude/skills now? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Aborted."
        exit 1
    fi
    mkdir -p .claude/skills
fi

# Create skill directory
mkdir -p "${SKILL_DIR}/references"

echo "📁 Created ${SKILL_DIR}/"

# Copy files (if running from the skill directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "${SCRIPT_DIR}/SKILL.md" ]; then
    # Running from the skill source directory
    cp "${SCRIPT_DIR}/SKILL.md" "${SKILL_DIR}/SKILL.md"
    cp "${SCRIPT_DIR}/references/exercise-bank.md" "${SKILL_DIR}/references/exercise-bank.md"
    cp "${SCRIPT_DIR}/references/review-framework.md" "${SKILL_DIR}/references/review-framework.md"
else
    echo "❌ Cannot find skill files. Run this script from the skill directory,"
    echo "   or copy the files manually:"
    echo ""
    echo "   cp SKILL.md ${SKILL_DIR}/"
    echo "   cp references/* ${SKILL_DIR}/references/"
    exit 1
fi

echo "✅ Installed SKILL.md"
echo "✅ Installed references/exercise-bank.md"
echo "✅ Installed references/review-framework.md"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Installation complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Now in Claude Code, try:"
echo "  • \"Let's practice Spark\""
echo "  • \"Quiz me on Delta Lake\""
echo "  • \"Give me a PySpark challenge\""
echo "  • \"Review my code\" (paste any Databricks code)"
echo "  • \"Mock exam for DE Associate\""
echo ""

# Check if AI Dev Kit is also installed
if [ -f ".claude/mcp.json" ]; then
    echo "🔗 AI Dev Kit MCP detected — exercises will run against your live workspace!"
else
    echo "💡 Tip: Install the AI Dev Kit for live exercise validation:"
    echo "   bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh)"
fi
