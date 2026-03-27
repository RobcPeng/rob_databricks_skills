#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Databricks AI Dev Kit + Custom Skills — Update & Install
#
# 1. Pulls latest AI Dev Kit from upstream (git submodule)
# 2. Runs the AI Dev Kit interactive installer (MCP server, skills, hooks, etc.)
# 3. Updates the Databricks MCP server packages from latest source
# 4. Optionally sets up the Databricks Builder App (local dev environment)
# 5. Prompts user to select which custom skills to install
# 6. Copies selected custom skills into the same target directories
# 7. Optionally deploys all skills to Genie Code in your workspace
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
DEFAULT_OFF_SKILLS="databricks-practice-skill databricks-free-tier-guardrails data-api-poc-builder update-skills-from-lessons"

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

print_header "Step 1/7 — Updating AI Dev Kit submodule"

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

print_header "Step 2/7 — Running AI Dev Kit installer"
print_step "Launching interactive installer — answer the prompts below."
echo ""

bash "$SUBMODULE_DIR/install.sh" "$@"

INSTALL_EXIT=$?
if [ $INSTALL_EXIT -ne 0 ]; then
    print_error "AI Dev Kit installer exited with code $INSTALL_EXIT"
    exit $INSTALL_EXIT
fi

echo ""

# ── Step 3: Update MCP server ─────────────────────────────────────────────

print_header "Step 3/7 — Updating Databricks MCP server"

MCP_INSTALL_DIR="${AIDEVKIT_HOME:-$HOME/.ai-dev-kit}"
MCP_VENV_DIR="$MCP_INSTALL_DIR/.venv"
MCP_VENV_PYTHON="$MCP_VENV_DIR/bin/python"
MCP_REPO_DIR="$MCP_INSTALL_DIR/repo"

# Detect and select Databricks profile from ~/.databrickscfg
DATABRICKS_PROFILE="DEFAULT"
CFG_FILE="$HOME/.databrickscfg"
if [ -f "$CFG_FILE" ]; then
    # Parse all profile names
    ALL_PROFILES=()
    while IFS= read -r line; do
        ALL_PROFILES+=("$line")
    done < <(sed -n 's/^\[\(.*\)\]$/\1/p' "$CFG_FILE")

    PROFILE_COUNT=${#ALL_PROFILES[@]}

    if [ $PROFILE_COUNT -eq 0 ]; then
        print_warn "No profiles found in ~/.databrickscfg — using DEFAULT"
    elif [ $PROFILE_COUNT -eq 1 ]; then
        DATABRICKS_PROFILE="${ALL_PROFILES[0]}"
        print_step "Using Databricks profile: ${GREEN}$DATABRICKS_PROFILE${NC}"
    else
        echo -e "  ${BOLD}Select Databricks profile:${NC}"
        echo ""
        idx=0
        while [ $idx -lt $PROFILE_COUNT ]; do
            display_num=$((idx + 1))
            profile_name="${ALL_PROFILES[$idx]}"
            # Extract host for this profile
            host=$(sed -n "/^\[$profile_name\]/,/^\[/{ s/^host *= *//p; }" "$CFG_FILE" | head -1)
            if [ -n "$host" ]; then
                echo -e "    ${BOLD}${display_num}${NC}) $profile_name ${DIM}($host)${NC}"
            else
                echo -e "    ${BOLD}${display_num}${NC}) $profile_name"
            fi
            idx=$((idx + 1))
        done
        echo ""
        read -r -p "  Enter number [1]: " profile_choice < /dev/tty
        if [ -z "$profile_choice" ]; then
            profile_choice=1
        fi
        if [ "$profile_choice" -ge 1 ] 2>/dev/null && [ "$profile_choice" -le $PROFILE_COUNT ] 2>/dev/null; then
            DATABRICKS_PROFILE="${ALL_PROFILES[$((profile_choice - 1))]}"
        else
            print_warn "Invalid selection — using ${ALL_PROFILES[0]}"
            DATABRICKS_PROFILE="${ALL_PROFILES[0]}"
        fi
        print_step "Using Databricks profile: ${GREEN}$DATABRICKS_PROFILE${NC}"
    fi
else
    print_warn "No ~/.databrickscfg found — using DEFAULT profile"
fi

# Patch MCP configs with the selected profile
if command -v python3 &>/dev/null; then
    # Patch the plugin .mcp.json
    PLUGIN_MCP_JSON="$SUBMODULE_DIR/.mcp.json"
    if [ -f "$PLUGIN_MCP_JSON" ]; then
        python3 -c "
import json
with open('$PLUGIN_MCP_JSON') as f: cfg = json.load(f)
srv = cfg.get('mcpServers', {}).get('databricks', {})
srv['env'] = {'DATABRICKS_CONFIG_PROFILE': '$DATABRICKS_PROFILE'}
cfg['mcpServers']['databricks'] = srv
with open('$PLUGIN_MCP_JSON', 'w') as f: json.dump(cfg, f, indent=2); f.write('\n')
" 2>/dev/null
        print_step "Patched plugin .mcp.json with profile ${GREEN}$DATABRICKS_PROFILE${NC}"
    fi

    # Patch ~/.claude.json (global + all per-project databricks MCP configs)
    CLAUDE_JSON="$HOME/.claude.json"
    if [ -f "$CLAUDE_JSON" ]; then
        python3 -c "
import json
with open('$CLAUDE_JSON') as f: cfg = json.load(f)
changed = False
# Fix global mcpServers
if 'mcpServers' in cfg and 'databricks' in cfg['mcpServers']:
    cfg['mcpServers']['databricks'].setdefault('env', {})['DATABRICKS_CONFIG_PROFILE'] = '$DATABRICKS_PROFILE'
    changed = True
# Fix all per-project mcpServers
if 'projects' in cfg:
    for path, proj in cfg['projects'].items():
        if isinstance(proj, dict) and 'mcpServers' in proj and 'databricks' in proj['mcpServers']:
            proj['mcpServers']['databricks'].setdefault('env', {})['DATABRICKS_CONFIG_PROFILE'] = '$DATABRICKS_PROFILE'
            changed = True
if changed:
    with open('$CLAUDE_JSON', 'w') as f: json.dump(cfg, f, indent=2); f.write('\n')
    print('updated')
" 2>/dev/null && print_step "Patched ~/.claude.json with profile ${GREEN}$DATABRICKS_PROFILE${NC}"
    fi
else
    print_warn "python3 not found — cannot patch MCP configs"
fi

# Update MCP server packages if installed
if [ -x "$MCP_VENV_PYTHON" ] && [ -d "$MCP_REPO_DIR/databricks-mcp-server" ]; then
    print_step "Found existing MCP installation at $MCP_INSTALL_DIR"
    print_step "Updating packages from latest source..."

    # Apple Silicon under Rosetta workaround
    arch_prefix=""
    if [ "$(sysctl -n hw.optional.arm64 2>/dev/null)" = "1" ] && [ "$(uname -m)" = "x86_64" ]; then
        if arch -arm64 python3 -c "pass" 2>/dev/null; then
            arch_prefix="arch -arm64"
            print_warn "Rosetta detected on Apple Silicon — forcing arm64 for Python"
        fi
    fi

    if command -v uv &>/dev/null; then
        $arch_prefix uv pip install --python "$MCP_VENV_PYTHON" \
            -e "$MCP_REPO_DIR/databricks-tools-core" \
            -e "$MCP_REPO_DIR/databricks-mcp-server" -q

        if "$MCP_VENV_PYTHON" -c "import databricks_mcp_server" 2>/dev/null; then
            print_step "MCP server updated ${GREEN}✓${NC}"
        else
            print_error "MCP server update failed — try re-running the AI Dev Kit installer"
        fi
    else
        print_warn "uv not found — skipping MCP update. Install uv: https://docs.astral.sh/uv/"
    fi
else
    print_warn "No existing MCP installation found at $MCP_INSTALL_DIR"
    print_warn "Run the AI Dev Kit installer (step 2) with MCP enabled to set it up."
fi

echo ""

# ── Step 4: Databricks Builder App (optional) ─────────────────────────────

print_header "Step 4/7 — Databricks Builder App (optional)"

BUILDER_APP_DIR="$SUBMODULE_DIR/databricks-builder-app"
BUILDER_INSTALLED=false

if [ -d "$BUILDER_APP_DIR" ]; then
    echo -e "  ${BOLD}Databricks Builder App${NC}"
    echo -e "  ${DIM}A web UI (React + FastAPI) that wraps Claude Code with integrated${NC}"
    echo -e "  ${DIM}Databricks tools. Chat interface for SQL, pipelines, file uploads, etc.${NC}"
    echo -e "  ${DIM}Requires: Python 3.11+, Node.js 18+, uv, and a Lakebase database.${NC}"
    echo ""
    read -r -p "  Install Builder App for local development? (y/N): " install_builder < /dev/tty

    if [ "$install_builder" = "y" ] || [ "$install_builder" = "Y" ]; then
        # Check prerequisites
        BUILDER_PREREQS_MET=true

        if ! command -v uv &>/dev/null; then
            print_error "uv not found — install: https://docs.astral.sh/uv/"
            BUILDER_PREREQS_MET=false
        fi

        if ! command -v node &>/dev/null; then
            print_error "Node.js not found — install: https://nodejs.org/"
            BUILDER_PREREQS_MET=false
        else
            NODE_VER=$(node -v | sed 's/v//' | cut -d. -f1)
            if [ "$NODE_VER" -lt 18 ] 2>/dev/null; then
                print_error "Node.js 18+ required (found $(node -v))"
                BUILDER_PREREQS_MET=false
            fi
        fi

        if [ "$BUILDER_PREREQS_MET" = true ]; then
            print_step "Installing backend dependencies..."
            (cd "$BUILDER_APP_DIR" && uv sync --quiet)

            REPO_ROOT="$SUBMODULE_DIR"
            if [ -d "$REPO_ROOT/databricks-tools-core" ] && [ -d "$REPO_ROOT/databricks-mcp-server" ]; then
                print_step "Installing sibling packages..."
                (cd "$BUILDER_APP_DIR" && uv pip install -e "$REPO_ROOT/databricks-tools-core" -e "$REPO_ROOT/databricks-mcp-server" --quiet)
            fi

            print_step "Installing frontend dependencies..."
            (cd "$BUILDER_APP_DIR/client" && npm install --silent 2>/dev/null || npm install)

            # Create .env.local from example if it doesn't exist
            if [ ! -f "$BUILDER_APP_DIR/.env.local" ]; then
                cp "$BUILDER_APP_DIR/.env.example" "$BUILDER_APP_DIR/.env.local"

                # Inject Databricks host from selected profile
                if [ -f "$CFG_FILE" ] && [ -n "$DATABRICKS_PROFILE" ]; then
                    host=$(sed -n "/^\[$DATABRICKS_PROFILE\]/,/^\[/{ s/^host *= *//p; }" "$CFG_FILE" | head -1)
                    if [ -n "$host" ]; then
                        sed -i.bak "s|^DATABRICKS_HOST=.*|DATABRICKS_HOST=$host|" "$BUILDER_APP_DIR/.env.local"
                        rm -f "$BUILDER_APP_DIR/.env.local.bak"
                        print_step "Set DATABRICKS_HOST to ${GREEN}$host${NC}"
                    fi
                fi

                echo ""
                echo -e "  ${YELLOW}╔══════════════════════════════════════════════════════════════╗${NC}"
                echo -e "  ${YELLOW}║  ACTION REQUIRED: Configure .env.local                      ║${NC}"
                echo -e "  ${YELLOW}╠══════════════════════════════════════════════════════════════╣${NC}"
                echo -e "  ${YELLOW}║                                                              ║${NC}"
                echo -e "  ${YELLOW}║  Edit: ${BOLD}$BUILDER_APP_DIR/.env.local${NC}${YELLOW}${NC}"
                echo -e "  ${YELLOW}║                                                              ║${NC}"
                echo -e "  ${YELLOW}║  Fill in:                                                    ║${NC}"
                echo -e "  ${YELLOW}║    1. DATABRICKS_TOKEN  — your personal access token         ║${NC}"
                echo -e "  ${YELLOW}║    2. LAKEBASE_ENDPOINT — or LAKEBASE_PG_URL                 ║${NC}"
                echo -e "  ${YELLOW}║                                                              ║${NC}"
                echo -e "  ${YELLOW}╚══════════════════════════════════════════════════════════════╝${NC}"
            else
                print_step ".env.local already exists — skipping"
            fi

            BUILDER_INSTALLED=true
            print_step "Builder App installed ${GREEN}✓${NC}"
            echo -e "  ${DIM}Start with: cd $BUILDER_APP_DIR && ./scripts/start_dev.sh${NC}"
        else
            print_error "Prerequisites not met — skipping Builder App install"
        fi
    else
        print_step "Skipping Builder App"
    fi
else
    print_warn "Builder App not found in submodule"
fi

echo ""

# ── Step 5: Select custom skills ───────────────────────────────────────────

print_header "Step 5/7 — Select custom skills"

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

# ── Step 6: Install selected custom skills ─────────────────────────────────

print_header "Step 6/7 — Installing custom skills"

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
            # Remove existing to prevent cp -r nesting into itself
            rm -rf "$target/$skill_name"
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

# ── Step 7: Deploy skills to Genie Code (optional) ────────────────────────

print_header "Step 7/7 — Deploy skills to Genie Code (optional)"

GENIE_DEPLOYED=false

echo -e "  ${BOLD}Genie Code Skills${NC}"
echo -e "  ${DIM}Deploys AI Dev Kit skills + selected custom skills to your Databricks${NC}"
echo -e "  ${DIM}workspace so Genie Code (Agent mode) can use them in any notebook/query.${NC}"
echo -e "  ${DIM}Skills are uploaded to /Workspace/Users/<you>/.assistant/skills/${NC}"
echo ""
read -r -p "  Deploy skills to Genie Code? (y/N): " deploy_genie < /dev/tty

if [ "$deploy_genie" = "y" ] || [ "$deploy_genie" = "Y" ]; then
    # Check databricks CLI is available
    if ! command -v databricks &>/dev/null; then
        print_error "Databricks CLI not found — install: https://docs.databricks.com/dev-tools/cli/install.html"
    else
        # Resolve user email
        USER_EMAIL=$(databricks current-user me --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null \
            | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('userName', ''))" 2>/dev/null || echo "")

        if [ -z "$USER_EMAIL" ]; then
            print_error "Could not determine user email — check your Databricks auth (profile: $DATABRICKS_PROFILE)"
        else
            GENIE_SKILLS_PATH="/Users/$USER_EMAIL/.assistant/skills"

            # Collect skills from canonical sources:
            #   - Databricks skills: ai-dev-kit submodule (authoritative)
            #   - MLflow + APX skills: installed copies (fetched from external repos by AI Dev Kit)
            #   - Custom skills: custom-skills/ directory
            GENIE_SKILL_SOURCES=()
            GENIE_DBX_COUNT=0
            GENIE_EXT_COUNT=0

            # 1. Databricks skills from submodule source
            DEVKIT_SKILLS_DIR="$SUBMODULE_DIR/databricks-skills"
            if [ -d "$DEVKIT_SKILLS_DIR" ]; then
                for skill_dir in "$DEVKIT_SKILLS_DIR"/*/; do
                    if [ -f "$skill_dir/SKILL.md" ]; then
                        skill_name=$(basename "$skill_dir")
                        [ "$skill_name" = "TEMPLATE" ] && continue
                        GENIE_SKILL_SOURCES+=("$skill_dir")
                        GENIE_DBX_COUNT=$((GENIE_DBX_COUNT + 1))
                    fi
                done
                print_step "Databricks skills (submodule): ${GREEN}$GENIE_DBX_COUNT${NC}"
            else
                print_warn "No Databricks skills found in submodule at $DEVKIT_SKILLS_DIR"
            fi

            # 2. MLflow + APX skills from installed copies (external repos, not in submodule)
            MLFLOW_APX_SKILLS="agent-evaluation analyze-mlflow-chat-session analyze-mlflow-trace databricks-mlflow-evaluation instrumenting-with-mlflow-tracing mlflow-onboarding querying-mlflow-metrics retrieving-mlflow-traces searching-mlflow-docs databricks-app-apx"
            INSTALLED_SKILLS_DIR=""
            if [ -d "$(pwd)/.claude/skills" ]; then
                INSTALLED_SKILLS_DIR="$(pwd)/.claude/skills"
            elif [ -d "$HOME/.claude/skills" ]; then
                INSTALLED_SKILLS_DIR="$HOME/.claude/skills"
            fi
            if [ -n "$INSTALLED_SKILLS_DIR" ]; then
                for ext_skill in $MLFLOW_APX_SKILLS; do
                    if [ -f "$INSTALLED_SKILLS_DIR/$ext_skill/SKILL.md" ]; then
                        GENIE_SKILL_SOURCES+=("$INSTALLED_SKILLS_DIR/$ext_skill/")
                        GENIE_EXT_COUNT=$((GENIE_EXT_COUNT + 1))
                    fi
                done
                if [ $GENIE_EXT_COUNT -gt 0 ]; then
                    print_step "MLflow + APX skills (installed): ${GREEN}$GENIE_EXT_COUNT${NC}"
                fi
            fi

            # 3. Selected custom skills from custom-skills/ directory
            for skill_name in "${SELECTED_SKILLS[@]+"${SELECTED_SKILLS[@]}"}"; do
                if [ -f "$CUSTOM_SKILLS_DIR/$skill_name/SKILL.md" ]; then
                    GENIE_SKILL_SOURCES+=("$CUSTOM_SKILLS_DIR/$skill_name/")
                fi
            done
            if [ ${#SELECTED_SKILLS[@]} -gt 0 ]; then
                print_step "Custom skills: ${GREEN}${#SELECTED_SKILLS[@]}${NC}"
            fi

            GENIE_SKILL_COUNT=${#GENIE_SKILL_SOURCES[@]}

            if [ $GENIE_SKILL_COUNT -eq 0 ]; then
                print_warn "No skills found to deploy. Run the AI Dev Kit installer first."
            else
                print_step "Found ${GREEN}$GENIE_SKILL_COUNT${NC} skills to deploy"

                # Clean out existing skills to remove stale/renamed/deselected versions
                print_step "Clearing existing Genie Code skills at ${DIM}$GENIE_SKILLS_PATH${NC}..."
                EXISTING_SKILLS=$(databricks workspace list "$GENIE_SKILLS_PATH" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null \
                    | python3 -c "
import sys, json
try:
    items = json.load(sys.stdin)
    for item in items:
        name = item.get('path', '').rsplit('/', 1)[-1]
        if name and not name.startswith('.'):
            print(name)
except: pass
" 2>/dev/null || echo "")

                if [ -n "$EXISTING_SKILLS" ]; then
                    while IFS= read -r old_skill; do
                        [ -z "$old_skill" ] && continue
                        databricks workspace delete "$GENIE_SKILLS_PATH/$old_skill" --profile "$DATABRICKS_PROFILE" --recursive 2>/dev/null || true
                        echo "    removed → $old_skill"
                    done <<< "$EXISTING_SKILLS"
                fi
                databricks workspace mkdirs "$GENIE_SKILLS_PATH" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true

                # Upload all skills
                print_step "Uploading skills to Genie Code (profile: ${GREEN}$DATABRICKS_PROFILE${NC})..."
                GENIE_UPLOAD_COUNT=0

                for skill_dir in "${GENIE_SKILL_SOURCES[@]}"; do
                    # Remove trailing slash
                    skill_dir="${skill_dir%/}"
                    skill_name=$(basename "$skill_dir")

                    databricks workspace mkdirs "$GENIE_SKILLS_PATH/$skill_name" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true

                    find "$skill_dir" -type f \( -name "*.md" -o -name "*.py" -o -name "*.yaml" -o -name "*.yml" -o -name "*.sh" \) | while read -r file; do
                        rel_path="${file#$skill_dir/}"
                        dest_path="$GENIE_SKILLS_PATH/$skill_name/$rel_path"
                        parent_dir=$(dirname "$dest_path")
                        if [ "$parent_dir" != "$GENIE_SKILLS_PATH/$skill_name" ]; then
                            databricks workspace mkdirs "$parent_dir" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true
                        fi
                        databricks workspace import "$dest_path" --file "$file" --profile "$DATABRICKS_PROFILE" --format AUTO --overwrite 2>/dev/null || true
                    done

                    echo "    uploaded → $skill_name"
                    GENIE_UPLOAD_COUNT=$((GENIE_UPLOAD_COUNT + 1))
                done

                GENIE_DEPLOYED=true
                print_step "Genie Code: ${GREEN}$GENIE_SKILL_COUNT${NC} skills deployed ${GREEN}✓${NC}"
                echo -e "  ${DIM}Location: /Workspace$GENIE_SKILLS_PATH${NC}"
            fi
        fi
    fi
else
    print_step "Skipping Genie Code deployment"
fi

echo ""

# ── Summary ─────────────────────────────────────────────────────────────────

print_header "Done"

echo "  AI Dev Kit:     updated to latest"
if [ -n "${DEVKIT_VERSION:-}" ]; then
    echo "  Version:        $DEVKIT_VERSION"
fi
if [ -x "$MCP_VENV_PYTHON" ]; then
    echo "  MCP server:     updated"
else
    echo "  MCP server:     not installed"
fi
if [ "$BUILDER_INSTALLED" = true ]; then
    echo "  Builder App:    installed"
else
    echo "  Builder App:    skipped"
fi
if [ "$GENIE_DEPLOYED" = true ]; then
    echo "  Genie Code:     deployed"
else
    echo "  Genie Code:     skipped"
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
