#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Databricks AI Dev Kit + Custom Skills — Update & Install
#
# 1. Pulls latest AI Dev Kit from upstream (git submodule)
# 2. Runs the AI Dev Kit interactive installer (MCP server, skills, hooks, etc.)
# 3. Prompts user to select which custom skills to install
# 4. Copies selected custom skills into the same target directories
#
# Compatible with bash 3.2+ (macOS default)
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

# ── Custom skill registry ──────────────────────────────────────────────────
# Skills listed here default to OFF and must be opted-in.
# All other skills in custom-skills/ default to ON.
DEFAULT_OFF_SKILLS="databricks-practice-skill databricks-free-tier-guardrails data-api-poc-builder"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
BOLD='\033[1m'
DIM='\033[2m'
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

is_default_off() {
    local skill_name="$1"
    for off_skill in $DEFAULT_OFF_SKILLS; do
        if [ "$skill_name" = "$off_skill" ]; then
            return 0
        fi
    done
    return 1
}

# ── Step 1: Pull latest AI Dev Kit ──────────────────────────────────────────

print_header "Step 1/4 — Updating AI Dev Kit submodule"

if [ ! -d "$SUBMODULE_DIR/.git" ] && [ ! -f "$SUBMODULE_DIR/.git" ]; then
    print_step "Initializing submodule..."
    git -C "$SCRIPT_DIR" submodule update --init --recursive
else
    print_step "Pulling latest from upstream..."
    git -C "$SCRIPT_DIR" submodule update --remote --merge
fi

if [ -f "$SUBMODULE_DIR/VERSION" ]; then
    DEVKIT_VERSION=$(cat "$SUBMODULE_DIR/VERSION")
    print_step "AI Dev Kit version: ${GREEN}$DEVKIT_VERSION${NC}"
else
    DEVKIT_VERSION=""
    print_warn "Could not determine AI Dev Kit version"
fi

echo ""

# ── Step 2: Run AI Dev Kit installer ────────────────────────────────────────

print_header "Step 2/4 — Running AI Dev Kit installer"
print_step "Launching interactive installer — answer the prompts below."
echo ""

bash "$SUBMODULE_DIR/install.sh" "$@"

INSTALL_EXIT=$?
if [ $INSTALL_EXIT -ne 0 ]; then
    print_error "AI Dev Kit installer exited with code $INSTALL_EXIT"
    exit $INSTALL_EXIT
fi

echo ""

# ── Step 3: Select custom skills ───────────────────────────────────────────

print_header "Step 3/4 — Select custom skills"

# Discover all available custom skills
ALL_SKILLS=()
SKILL_SELECTED=()
for skill in "$CUSTOM_SKILLS_DIR"/*/; do
    if [ -f "$skill/SKILL.md" ]; then
        skill_name=$(basename "$skill")
        ALL_SKILLS+=("$skill_name")
        if is_default_off "$skill_name"; then
            SKILL_SELECTED+=(0)
        else
            SKILL_SELECTED+=(1)
        fi
    fi
done

SKILL_COUNT=${#ALL_SKILLS[@]}

if [ $SKILL_COUNT -eq 0 ]; then
    print_warn "No custom skills found in $CUSTOM_SKILLS_DIR"
    exit 0
fi

# Extract short description from SKILL.md frontmatter
get_skill_desc() {
    local skill_path="$CUSTOM_SKILLS_DIR/$1/SKILL.md"
    local desc
    desc=$(sed -n 's/^description: *"\{0,1\}\(.*\)"\{0,1\}$/\1/p' "$skill_path" 2>/dev/null | head -1)
    if [ ${#desc} -gt 60 ]; then
        desc="${desc:0:57}..."
    fi
    echo "$desc"
}

# Interactive selection loop
while true; do
    echo ""
    echo -e "  ${BOLD}Custom skills available:${NC}"
    echo ""

    idx=0
    while [ $idx -lt $SKILL_COUNT ]; do
        skill_name="${ALL_SKILLS[$idx]}"
        display_num=$((idx + 1))

        if [ "${SKILL_SELECTED[$idx]}" -eq 1 ]; then
            state="${GREEN}[x]${NC}"
        else
            state="${DIM}[ ]${NC}"
        fi

        default_tag=""
        if is_default_off "$skill_name"; then
            default_tag="${DIM} (opt-in)${NC}"
        fi

        desc=$(get_skill_desc "$skill_name")

        echo -e "    ${BOLD}${display_num}${NC}) $state $skill_name${default_tag}"
        if [ -n "$desc" ]; then
            echo -e "         ${DIM}${desc}${NC}"
        fi

        idx=$((idx + 1))
    done

    echo ""
    echo -e "  ${BOLD}Commands:${NC}  Enter number to toggle, ${BOLD}a${NC}=all on, ${BOLD}n${NC}=all off, ${BOLD}d${NC}=defaults, ${BOLD}Enter${NC}=confirm"
    echo ""
    read -r -p "  > " choice < /dev/tty

    if [ -z "$choice" ]; then
        break
    fi

    case "$choice" in
        a|A)
            idx=0
            while [ $idx -lt $SKILL_COUNT ]; do
                SKILL_SELECTED[$idx]=1
                idx=$((idx + 1))
            done
            ;;
        n|N)
            idx=0
            while [ $idx -lt $SKILL_COUNT ]; do
                SKILL_SELECTED[$idx]=0
                idx=$((idx + 1))
            done
            ;;
        d|D)
            idx=0
            while [ $idx -lt $SKILL_COUNT ]; do
                if is_default_off "${ALL_SKILLS[$idx]}"; then
                    SKILL_SELECTED[$idx]=0
                else
                    SKILL_SELECTED[$idx]=1
                fi
                idx=$((idx + 1))
            done
            ;;
        *[0-9]*)
            if [ "$choice" -ge 1 ] 2>/dev/null && [ "$choice" -le $SKILL_COUNT ] 2>/dev/null; then
                toggle_idx=$((choice - 1))
                if [ "${SKILL_SELECTED[$toggle_idx]}" -eq 1 ]; then
                    SKILL_SELECTED[$toggle_idx]=0
                else
                    SKILL_SELECTED[$toggle_idx]=1
                fi
            else
                print_warn "Invalid number. Enter 1-${SKILL_COUNT}"
            fi
            ;;
        *)
            print_warn "Invalid input. Enter a number, a, n, d, or press Enter to confirm."
            ;;
    esac
done

# Build final list of selected skills
SELECTED_SKILLS=()
SKIPPED_SKILLS=()
idx=0
while [ $idx -lt $SKILL_COUNT ]; do
    if [ "${SKILL_SELECTED[$idx]}" -eq 1 ]; then
        SELECTED_SKILLS+=("${ALL_SKILLS[$idx]}")
    else
        SKIPPED_SKILLS+=("${ALL_SKILLS[$idx]}")
    fi
    idx=$((idx + 1))
done

echo ""
if [ ${#SELECTED_SKILLS[@]} -eq 0 ]; then
    print_warn "No custom skills selected. Skipping custom skill installation."
else
    print_step "Selected ${GREEN}${#SELECTED_SKILLS[@]}${NC} custom skill(s):"
    for s in "${SELECTED_SKILLS[@]}"; do
        echo "    • $s"
    done
fi

echo ""

# ── Step 4: Install selected custom skills ─────────────────────────────────

print_header "Step 4/4 — Installing custom skills"

MANIFEST_NAME=".custom-skills-manifest"
INSTALLED_CUSTOM=0

# Check if a skill name is in the selected list
is_selected() {
    local name="$1"
    for s in "${SELECTED_SKILLS[@]+"${SELECTED_SKILLS[@]}"}"; do
        if [ "$s" = "$name" ]; then
            return 0
        fi
    done
    return 1
}

install_to_target() {
    local target="$1"
    local scope_label="$2"

    print_step "Found $scope_label skills at: $target"

    # Clean up stale custom skills from previous installs
    local manifest="$target/$MANIFEST_NAME"
    if [ -f "$manifest" ]; then
        while IFS= read -r old_skill; do
            [ -z "$old_skill" ] && continue
            if ! is_selected "$old_skill" && [ -d "$target/$old_skill" ]; then
                rm -rf "$target/$old_skill"
                echo "    removed → $old_skill"
            fi
        done < "$manifest"
    fi

    # Copy selected custom skills
    for skill_name in "${SELECTED_SKILLS[@]+"${SELECTED_SKILLS[@]}"}"; do
        if [ -f "$CUSTOM_SKILLS_DIR/$skill_name/SKILL.md" ]; then
            cp -r "$CUSTOM_SKILLS_DIR/$skill_name" "$target/$skill_name"
            echo "    copied → $skill_name"
            INSTALLED_CUSTOM=$((INSTALLED_CUSTOM + 1))
        fi
    done

    # Write manifest
    if [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
        printf '%s\n' "${SELECTED_SKILLS[@]}" > "$manifest"
    else
        > "$manifest"
    fi
}

# Check project scope (current directory)
for tool_dir in "${TOOL_SKILL_DIRS[@]}"; do
    target="$(pwd)/$tool_dir"
    if [ -d "$target" ]; then
        install_to_target "$target" "project"
    fi
done

# Check global scope
for tool_dir in "${TOOL_SKILL_DIRS[@]}"; do
    target="$HOME/$tool_dir"
    if [ -d "$target" ]; then
        install_to_target "$target" "global"
    fi
done

# Fallback: infer from .ai-dev-kit state
if [ $INSTALLED_CUSTOM -eq 0 ] && [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
    print_warn "No existing skill directories found. Checking AI Dev Kit state..."
    if [ -f ".ai-dev-kit/.installed-skills" ]; then
        while IFS='|' read -r dir _; do
            if [ -n "$dir" ] && [ -d "$dir" ]; then
                parent_dir=$(dirname "$dir")
                if [ -d "$parent_dir" ]; then
                    install_to_target "$parent_dir" "detected"
                    break
                fi
            fi
        done < ".ai-dev-kit/.installed-skills"
    fi
fi

if [ $INSTALLED_CUSTOM -eq 0 ] && [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
    print_warn "No skill installation targets found."
    print_warn "You can manually copy them with:"
    echo ""
    echo "    cp -r $CUSTOM_SKILLS_DIR/<skill-name> .claude/skills/"
    echo ""
fi

# ── Summary ─────────────────────────────────────────────────────────────────

print_header "Done"

echo "  AI Dev Kit:     updated to latest"
if [ -n "${DEVKIT_VERSION:-}" ]; then
    echo "  Version:        $DEVKIT_VERSION"
fi
echo ""
echo "  Custom skills installed:"
if [ ${#SELECTED_SKILLS[@]} -eq 0 ]; then
    echo "    (none)"
else
    for s in "${SELECTED_SKILLS[@]}"; do
        echo "    • $s"
    done
fi

if [ ${#SKIPPED_SKILLS[@]} -gt 0 ]; then
    echo ""
    echo "  Skipped (not selected):"
    for s in "${SKIPPED_SKILLS[@]}"; do
        echo "    • $s"
    done
fi
echo ""
