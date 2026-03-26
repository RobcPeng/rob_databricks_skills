#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Databricks AI Dev Kit + Custom Skills — Update & Install
#
# 1. Pulls latest AI Dev Kit from upstream (git submodule)
# 2. Runs the AI Dev Kit interactive installer (MCP server, skills, hooks, etc.)
# 3. Prompts user to select which custom skills to install
# 4. Copies selected custom skills into the same target directories
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
DEFAULT_OFF_SKILLS=(
    "databricks-practice-skill"
    "databricks-free-tier-guardrails"
    "data-api-poc-builder"
)

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

# Check if a skill is in the default-off list
is_default_off() {
    local skill_name="$1"
    for off_skill in "${DEFAULT_OFF_SKILLS[@]}"; do
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

# Show version
if [ -f "$SUBMODULE_DIR/VERSION" ]; then
    DEVKIT_VERSION=$(cat "$SUBMODULE_DIR/VERSION")
    print_step "AI Dev Kit version: ${GREEN}$DEVKIT_VERSION${NC}"
else
    print_warn "Could not determine AI Dev Kit version"
fi

echo ""

# ── Step 2: Run AI Dev Kit installer ────────────────────────────────────────

print_header "Step 2/4 — Running AI Dev Kit installer"
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

# ── Step 3: Select custom skills ───────────────────────────────────────────

print_header "Step 3/4 — Select custom skills"

# Discover all available custom skills
ALL_SKILLS=()
for skill in "$CUSTOM_SKILLS_DIR"/*/; do
    if [ -f "$skill/SKILL.md" ]; then
        ALL_SKILLS+=("$(basename "$skill")")
    fi
done

if [ ${#ALL_SKILLS[@]} -eq 0 ]; then
    print_warn "No custom skills found in $CUSTOM_SKILLS_DIR"
    exit 0
fi

# Build selection state: 1=selected, 0=not selected
declare -A SKILL_SELECTED
for skill_name in "${ALL_SKILLS[@]}"; do
    if is_default_off "$skill_name"; then
        SKILL_SELECTED[$skill_name]=0
    else
        SKILL_SELECTED[$skill_name]=1
    fi
done

# Extract short description from SKILL.md frontmatter
get_skill_desc() {
    local skill_path="$CUSTOM_SKILLS_DIR/$1/SKILL.md"
    # Get the description line, strip quotes and truncate
    local desc
    desc=$(sed -n 's/^description: *"\{0,1\}\(.*\)"\{0,1\}$/\1/p' "$skill_path" 2>/dev/null | head -1)
    # Truncate to 60 chars
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

    local_idx=1
    for skill_name in "${ALL_SKILLS[@]}"; do
        local state
        if [ "${SKILL_SELECTED[$skill_name]}" -eq 1 ]; then
            state="${GREEN}[x]${NC}"
        else
            state="${DIM}[ ]${NC}"
        fi

        local default_tag=""
        if is_default_off "$skill_name"; then
            default_tag="${DIM} (opt-in)${NC}"
        fi

        local desc
        desc=$(get_skill_desc "$skill_name")

        echo -e "    ${BOLD}$local_idx${NC}) $state $skill_name${default_tag}"
        if [ -n "$desc" ]; then
            echo -e "         ${DIM}$desc${NC}"
        fi

        local_idx=$((local_idx + 1))
    done

    echo ""
    echo -e "  ${BOLD}Commands:${NC}  Enter number to toggle, ${BOLD}a${NC}=all on, ${BOLD}n${NC}=all off, ${BOLD}d${NC}=defaults, ${BOLD}Enter${NC}=confirm"
    echo ""
    read -r -p "  > " choice < /dev/tty

    # Empty input = confirm selection
    if [ -z "$choice" ]; then
        break
    fi

    case "$choice" in
        a|A)
            for skill_name in "${ALL_SKILLS[@]}"; do
                SKILL_SELECTED[$skill_name]=1
            done
            ;;
        n|N)
            for skill_name in "${ALL_SKILLS[@]}"; do
                SKILL_SELECTED[$skill_name]=0
            done
            ;;
        d|D)
            for skill_name in "${ALL_SKILLS[@]}"; do
                if is_default_off "$skill_name"; then
                    SKILL_SELECTED[$skill_name]=0
                else
                    SKILL_SELECTED[$skill_name]=1
                fi
            done
            ;;
        *[0-9]*)
            # Toggle the numbered skill
            if [ "$choice" -ge 1 ] 2>/dev/null && [ "$choice" -le ${#ALL_SKILLS[@]} ] 2>/dev/null; then
                local_idx=$((choice - 1))
                toggle_name="${ALL_SKILLS[$local_idx]}"
                if [ "${SKILL_SELECTED[$toggle_name]}" -eq 1 ]; then
                    SKILL_SELECTED[$toggle_name]=0
                else
                    SKILL_SELECTED[$toggle_name]=1
                fi
            else
                print_warn "Invalid number. Enter 1-${#ALL_SKILLS[@]}"
            fi
            ;;
        *)
            print_warn "Invalid input. Enter a number, a, n, d, or press Enter to confirm."
            ;;
    esac
done

# Build final list of selected skills
SELECTED_SKILLS=()
for skill_name in "${ALL_SKILLS[@]}"; do
    if [ "${SKILL_SELECTED[$skill_name]}" -eq 1 ]; then
        SELECTED_SKILLS+=("$skill_name")
    fi
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
                    # Check if this old skill is in the selected list
                    local still_selected=false
                    for current in "${SELECTED_SKILLS[@]+"${SELECTED_SKILLS[@]}"}"; do
                        if [ "$current" = "$old_skill" ]; then
                            still_selected=true
                            break
                        fi
                    done
                    if [ "$still_selected" = false ] && [ -d "$target/$old_skill" ]; then
                        rm -rf "$target/$old_skill"
                        echo "    removed → $old_skill"
                    fi
                done < "$manifest"
            fi

            # Copy selected custom skills
            for skill_name in "${SELECTED_SKILLS[@]+"${SELECTED_SKILLS[@]}"}"; do
                local skill="$CUSTOM_SKILLS_DIR/$skill_name"
                if [ -f "$skill/SKILL.md" ]; then
                    cp -r "$skill" "$target/$skill_name"
                    echo "    copied → $skill_name"
                    INSTALLED_CUSTOM=$((INSTALLED_CUSTOM + 1))
                fi
            done

            # Write manifest of what we just installed
            if [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
                printf '%s\n' "${SELECTED_SKILLS[@]}" > "$manifest"
            else
                # Empty manifest — all custom skills removed
                > "$manifest"
            fi
        fi
    done
}

# Check project scope first (current directory)
detect_and_install "$(pwd)" "project"

# Check global scope
detect_and_install "$HOME" "global"

# If nothing was detected, try to infer from .ai-dev-kit state
if [ $INSTALLED_CUSTOM -eq 0 ] && [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
    print_warn "No existing skill directories found. Checking AI Dev Kit state..."

    # Check project-level state
    if [ -f ".ai-dev-kit/.installed-skills" ]; then
        while IFS='|' read -r dir _; do
            if [ -n "$dir" ] && [ -d "$dir" ]; then
                parent_dir=$(dirname "$dir")
                if [ -d "$parent_dir" ]; then
                    print_step "Detected skill directory: $parent_dir"
                    # Clean stale skills
                    manifest="$parent_dir/$MANIFEST_NAME"
                    if [ -f "$manifest" ]; then
                        while IFS= read -r old_skill; do
                            [ -z "$old_skill" ] && continue
                            local still_selected=false
                            for current in "${SELECTED_SKILLS[@]}"; do
                                [ "$current" = "$old_skill" ] && still_selected=true && break
                            done
                            if [ "$still_selected" = false ] && [ -d "$parent_dir/$old_skill" ]; then
                                rm -rf "$parent_dir/$old_skill"
                                echo "    removed → $old_skill"
                            fi
                        done < "$manifest"
                    fi
                    # Copy selected skills
                    for skill_name in "${SELECTED_SKILLS[@]}"; do
                        local skill="$CUSTOM_SKILLS_DIR/$skill_name"
                        if [ -f "$skill/SKILL.md" ]; then
                            cp -r "$skill" "$parent_dir/$skill_name"
                            echo "    copied → $skill_name"
                            INSTALLED_CUSTOM=$((INSTALLED_CUSTOM + 1))
                        fi
                    done
                    # Write manifest
                    printf '%s\n' "${SELECTED_SKILLS[@]}" > "$manifest"
                    break
                fi
            fi
        done < ".ai-dev-kit/.installed-skills"
    fi
fi

if [ $INSTALLED_CUSTOM -eq 0 ] && [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
    print_warn "No skill installation targets found."
    print_warn "Custom skills are available in: $CUSTOM_SKILLS_DIR"
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

SKIPPED=()
for skill_name in "${ALL_SKILLS[@]}"; do
    if [ "${SKILL_SELECTED[$skill_name]}" -eq 0 ]; then
        SKIPPED+=("$skill_name")
    fi
done
if [ ${#SKIPPED[@]} -gt 0 ]; then
    echo ""
    echo "  Skipped (not selected):"
    for s in "${SKIPPED[@]}"; do
        echo "    • $s"
    done
fi
echo ""
