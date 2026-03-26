#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Databricks AI Dev Kit + Custom Skills — Update & Install
#
# 1. Pulls latest AI Dev Kit from upstream (git submodule)
# 2. Runs the AI Dev Kit interactive installer (MCP server, skills, hooks, etc.)
# 3. Copies custom skills into the same target directories
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CUSTOM_SKILLS_DIR="$SCRIPT_DIR/custom-skills"
SUBMODULE_DIR="$SCRIPT_DIR/ai-dev-kit"

# Tool-specific skill directory names
TOOL_SKILL_DIRS=(
    ".claude/skills"
    ".cursor/skills"
    ".github/skills"
    ".agents/skills"
    ".gemini/skills"
)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    echo -e "${GREEN}▸${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# ── Step 1: Pull latest AI Dev Kit ──────────────────────────────────────────

print_header "Step 1/3 — Updating AI Dev Kit submodule"

if [ ! -d "$SUBMODULE_DIR/.git" ] && [ ! -f "$SUBMODULE_DIR/.git" ]; then
    print_step "Initializing submodule..."
    git -C "$SCRIPT_DIR" submodule update --init --recursive
else
    print_step "Pulling latest from upstream..."
    git -C "$SCRIPT_DIR" submodule update --remote --merge
fi

# Show version
if [ -f "$SUBMODULE_DIR/VERSION" ]; then
    DEVKIT_VERSION=$(cat "$SUBMODULE_DIR/VERSION")
    print_step "AI Dev Kit version: ${GREEN}$DEVKIT_VERSION${NC}"
else
    print_warn "Could not determine AI Dev Kit version"
fi

# Count custom skills
CUSTOM_COUNT=$(find "$CUSTOM_SKILLS_DIR" -name "SKILL.md" -maxdepth 2 | wc -l | tr -d ' ')
print_step "Custom skills found: ${GREEN}$CUSTOM_COUNT${NC}"
echo ""

# ── Step 2: Run AI Dev Kit installer ────────────────────────────────────────

print_header "Step 2/3 — Running AI Dev Kit installer"
print_step "Launching interactive installer — answer the prompts below."
echo ""

# Run their installer from the submodule directory
# Pass through any args the user provided (e.g., --force, --global)
bash "$SUBMODULE_DIR/install.sh" "$@"

INSTALL_EXIT=$?
if [ $INSTALL_EXIT -ne 0 ]; then
    print_error "AI Dev Kit installer exited with code $INSTALL_EXIT"
    exit $INSTALL_EXIT
fi

echo ""

# ── Step 3: Install custom skills ──────────────────────────────────────────

print_header "Step 3/3 — Installing custom skills"

# Build list of current custom skill names
CURRENT_CUSTOM_SKILLS=()
for skill in "$CUSTOM_SKILLS_DIR"/*/; do
    if [ -f "$skill/SKILL.md" ]; then
        CURRENT_CUSTOM_SKILLS+=("$(basename "$skill")")
    fi
done

# Manifest file tracks previously installed custom skills for cleanup
MANIFEST_NAME=".custom-skills-manifest"

INSTALLED_CUSTOM=0

detect_and_install() {
    local base_dir="$1"
    local scope_label="$2"

    for tool_dir in "${TOOL_SKILL_DIRS[@]}"; do
        local target="$base_dir/$tool_dir"
        if [ -d "$target" ]; then
            print_step "Found $scope_label skills at: $target"

            # Clean up stale custom skills from previous installs
            local manifest="$target/$MANIFEST_NAME"
            if [ -f "$manifest" ]; then
                while IFS= read -r old_skill; do
                    # Skip empty lines
                    [ -z "$old_skill" ] && continue
                    # Check if this old skill is still in current custom-skills/
                    local still_exists=false
                    for current in "${CURRENT_CUSTOM_SKILLS[@]}"; do
                        if [ "$current" = "$old_skill" ]; then
                            still_exists=true
                            break
                        fi
                    done
                    if [ "$still_exists" = false ] && [ -d "$target/$old_skill" ]; then
                        rm -rf "$target/$old_skill"
                        echo "    removed stale → $old_skill"
                    fi
                done < "$manifest"
            fi

            # Copy current custom skills
            for skill in "$CUSTOM_SKILLS_DIR"/*/; do
                if [ -f "$skill/SKILL.md" ]; then
                    skill_name=$(basename "$skill")
                    cp -r "$skill" "$target/$skill_name"
                    echo "    copied → $skill_name"
                    INSTALLED_CUSTOM=$((INSTALLED_CUSTOM + 1))
                fi
            done

            # Write manifest of what we just installed
            printf '%s\n' "${CURRENT_CUSTOM_SKILLS[@]}" > "$manifest"
        fi
    done
}

# Check project scope first (current directory)
detect_and_install "$(pwd)" "project"

# Check global scope
detect_and_install "$HOME" "global"

# If nothing was detected, try to infer from .ai-dev-kit state
if [ $INSTALLED_CUSTOM -eq 0 ]; then
    print_warn "No existing skill directories found. Checking AI Dev Kit state..."

    # Check project-level state
    if [ -f ".ai-dev-kit/.installed-skills" ]; then
        # Parse the manifest to find tool directories
        while IFS='|' read -r dir _; do
            if [ -n "$dir" ] && [ -d "$dir" ]; then
                parent_dir=$(dirname "$dir")
                if [ -d "$parent_dir" ]; then
                    print_step "Detected skill directory: $parent_dir"
                    # Clean stale skills
                    local manifest="$parent_dir/$MANIFEST_NAME"
                    if [ -f "$manifest" ]; then
                        while IFS= read -r old_skill; do
                            [ -z "$old_skill" ] && continue
                            local still_exists=false
                            for current in "${CURRENT_CUSTOM_SKILLS[@]}"; do
                                [ "$current" = "$old_skill" ] && still_exists=true && break
                            done
                            if [ "$still_exists" = false ] && [ -d "$parent_dir/$old_skill" ]; then
                                rm -rf "$parent_dir/$old_skill"
                                echo "    removed stale → $old_skill"
                            fi
                        done < "$manifest"
                    fi
                    # Copy current skills
                    for skill in "$CUSTOM_SKILLS_DIR"/*/; do
                        if [ -f "$skill/SKILL.md" ]; then
                            skill_name=$(basename "$skill")
                            cp -r "$skill" "$parent_dir/$skill_name"
                            echo "    copied → $skill_name"
                            INSTALLED_CUSTOM=$((INSTALLED_CUSTOM + 1))
                        fi
                    done
                    # Write manifest
                    printf '%s\n' "${CURRENT_CUSTOM_SKILLS[@]}" > "$manifest"
                    break
                fi
            fi
        done < ".ai-dev-kit/.installed-skills"
    fi
fi

if [ $INSTALLED_CUSTOM -eq 0 ]; then
    print_warn "No skill installation targets found."
    print_warn "Custom skills are available in: $CUSTOM_SKILLS_DIR"
    print_warn "You can manually copy them with:"
    echo ""
    echo "    cp -r $CUSTOM_SKILLS_DIR/* .claude/skills/"
    echo ""
else
    echo ""
    print_step "Installed ${GREEN}$CUSTOM_COUNT${NC} custom skills into ${GREEN}$((INSTALLED_CUSTOM / CUSTOM_COUNT))${NC} tool target(s)"
fi

# ── Summary ─────────────────────────────────────────────────────────────────

print_header "Done"

echo "  AI Dev Kit:     updated to latest"
if [ -n "${DEVKIT_VERSION:-}" ]; then
    echo "  Version:        $DEVKIT_VERSION"
fi
echo "  Custom skills:  $CUSTOM_COUNT installed"
echo ""
echo "  Custom skills included:"
for skill in "$CUSTOM_SKILLS_DIR"/*/; do
    if [ -f "$skill/SKILL.md" ]; then
        echo "    • $(basename "$skill")"
    fi
done
echo ""
