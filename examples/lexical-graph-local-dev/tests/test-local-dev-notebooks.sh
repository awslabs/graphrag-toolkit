#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# test-local-dev-notebooks.sh
#
# Full lifecycle test runner for lexical-graph-local-dev notebooks.
# Handles: env setup → AWS resources → Docker → notebook execution → report → cleanup
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NOTEBOOKS_DIR="$PROJECT_DIR/notebooks"
DOCKER_DIR="$PROJECT_DIR/docker"
AWS_DIR="$PROJECT_DIR/aws"
REPORT_DIR="${REPORT_DIR:-$PROJECT_DIR/test-results}"

# Configurable flags
SKIP_GITHUB="${SKIP_GITHUB:-true}"
SKIP_PPTX="${SKIP_PPTX:-true}"
SKIP_LONG_RUNNING="${SKIP_LONG_RUNNING:-true}"
CLEANUP="${CLEANUP:-true}"
DOCKER_MODE="standard"

# State tracking for cleanup
AWS_ACCOUNT=""
AWS_REGION=""
S3_BUCKET=""
DOCKER_STARTED=false
AWS_RESOURCES_CREATED=false
BEDROCK_PROMPTS_CREATED=false
ENV_CREATED=false
SYSTEM_PROMPT_ID=""
USER_PROMPT_ID=""
NOTEBOOK_EXIT_CODE=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*"; }

timer_start() { TIMER_START=$(date +%s); }
timer_end()   { echo "$(($(date +%s) - TIMER_START))s"; }

# =============================================================================
# Phase 1: Platform detection
# =============================================================================
detect_platform() {
    log "Detecting platform..."
    local arch
    arch=$(uname -m)
    if [[ "$arch" == "arm64" || "$arch" == "aarch64" ]]; then
        ok "ARM platform detected"
    else
        ok "x86 platform detected"
    fi
    DOCKER_FLAGS="--reset"
}

# =============================================================================
# Phase 2: Environment setup
# =============================================================================
setup_env() {
    log "Setting up environment..."
    timer_start

    AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
    S3_BUCKET="graphrag-toolkit-${AWS_ACCOUNT}"

    cp "$NOTEBOOKS_DIR/.env.template" "$NOTEBOOKS_DIR/.env"
    ENV_CREATED=true

    # Patch .env with detected values
    if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '' "s/^AWS_REGION=.*/AWS_REGION=${AWS_REGION}/" "$NOTEBOOKS_DIR/.env"
        sed -i '' "s/^S3_BUCKET_NAME=.*/S3_BUCKET_NAME=${S3_BUCKET}/" "$NOTEBOOKS_DIR/.env"
    else
        sed -i "s/^AWS_REGION=.*/AWS_REGION=${AWS_REGION}/" "$NOTEBOOKS_DIR/.env"
        sed -i "s/^S3_BUCKET_NAME=.*/S3_BUCKET_NAME=${S3_BUCKET}/" "$NOTEBOOKS_DIR/.env"
    fi

    ok "Environment configured (account=$AWS_ACCOUNT, region=$AWS_REGION, bucket=$S3_BUCKET) [$(timer_end)]"
}

# =============================================================================
# Phase 3: AWS resources
# =============================================================================
setup_aws() {
    log "Creating AWS resources..."
    timer_start

    # S3 bucket
    log "Creating S3 bucket: $S3_BUCKET"
    if [[ "$AWS_REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION" 2>/dev/null || true
    else
        aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION" \
            --create-bucket-configuration LocationConstraint="$AWS_REGION" 2>/dev/null || true
    fi
    AWS_RESOURCES_CREATED=true

    # Upload prompt files to S3
    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
print(data['variants'][0]['templateConfiguration']['text']['text'], end='')
" "$AWS_DIR/system_prompt.json" | aws s3 cp - "s3://$S3_BUCKET/prompts/system_prompt.txt" --region "$AWS_REGION"

    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    data = json.load(f)
print(data['variants'][0]['templateConfiguration']['text']['text'], end='')
" "$AWS_DIR/user_prompt.json" | aws s3 cp - "s3://$S3_BUCKET/prompts/user_prompt.txt" --region "$AWS_REGION"

    ok "S3 bucket created and prompts uploaded"

    # Bedrock managed prompts
    if [[ -f "$AWS_DIR/system_prompt.json" && -f "$AWS_DIR/user_prompt.json" ]]; then
        local sys_response usr_response
        sys_response=$(aws bedrock-agent create-prompt --region "$AWS_REGION" --cli-input-json file://"$AWS_DIR/system_prompt.json" 2>&1) || true
        usr_response=$(aws bedrock-agent create-prompt --region "$AWS_REGION" --cli-input-json file://"$AWS_DIR/user_prompt.json" 2>&1) || true

        SYSTEM_PROMPT_ID=$(echo "$sys_response" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null) || true
        USER_PROMPT_ID=$(echo "$usr_response" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])" 2>/dev/null) || true

        if [[ -n "$SYSTEM_PROMPT_ID" && -n "$USER_PROMPT_ID" ]]; then
            local sys_arn="arn:aws:bedrock:${AWS_REGION}:${AWS_ACCOUNT}:prompt/${SYSTEM_PROMPT_ID}"
            local usr_arn="arn:aws:bedrock:${AWS_REGION}:${AWS_ACCOUNT}:prompt/${USER_PROMPT_ID}"
            echo "SYSTEM_PROMPT_ARN=$sys_arn" >> "$NOTEBOOKS_DIR/.env"
            echo "USER_PROMPT_ARN=$usr_arn" >> "$NOTEBOOKS_DIR/.env"
            BEDROCK_PROMPTS_CREATED=true
            ok "Bedrock prompts created (system=$SYSTEM_PROMPT_ID, user=$USER_PROMPT_ID)"
        else
            warn "Could not extract Bedrock prompt IDs — prompt-based cells may fail"
        fi
    fi

    ok "AWS resources created [$(timer_end)]"
}

# =============================================================================
# Phase 4: Docker
# =============================================================================
start_docker() {
    log "Starting Docker containers ($DOCKER_MODE mode)..."
    timer_start

    # Container names (standard mode)
    JUPYTER_CONTAINER="jupyter-local"
    NEO4J_CONTAINER="neo4j-local"
    PGVECTOR_CONTAINER="pgvector-local"

    (cd "$DOCKER_DIR" && ./start-containers.sh $DOCKER_FLAGS)
    DOCKER_STARTED=true

    wait_for_containers
    ok "Docker containers running [$(timer_end)]"
}

wait_for_containers() {
    local max_wait=120
    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        local count
        count=$(docker ps --filter "name=$NEO4J_CONTAINER" --filter "name=$PGVECTOR_CONTAINER" --filter "name=$JUPYTER_CONTAINER" --format "{{.Names}}" | wc -l | tr -d ' ')
        if [[ "$count" -ge 3 ]]; then
            # Also verify jupyter is responsive
            if docker exec "$JUPYTER_CONTAINER" python3 -c "print('ready')" 2>/dev/null; then
                return 0
            fi
        fi
        sleep 5
        waited=$((waited + 5))
    done
    err "Containers did not start within ${max_wait}s"
    return 1
}

# =============================================================================
# Phase 5: Execute notebooks
# =============================================================================
run_notebooks() {
    log "Executing notebooks..."
    timer_start
    mkdir -p "$REPORT_DIR"

    JUPYTER_WORK_DIR="/home/jovyan/notebooks"

    # Copy runner script into container
    docker cp "$SCRIPT_DIR/run_notebooks.py" "$JUPYTER_CONTAINER:$JUPYTER_WORK_DIR/run_notebooks.py"

    # Execute
    docker exec "$JUPYTER_CONTAINER" \
        python3 "$JUPYTER_WORK_DIR/run_notebooks.py" \
        --skip-github="$SKIP_GITHUB" \
        --skip-pptx="$SKIP_PPTX" \
        --skip-long-running="$SKIP_LONG_RUNNING" \
    || NOTEBOOK_EXIT_CODE=$?

    # Collect reports
    docker cp "$JUPYTER_CONTAINER:$JUPYTER_WORK_DIR/execution_report.json" "$REPORT_DIR/" 2>/dev/null || true
    docker cp "$JUPYTER_CONTAINER:$JUPYTER_WORK_DIR/execution_report.md" "$REPORT_DIR/" 2>/dev/null || true

    if [[ $NOTEBOOK_EXIT_CODE -eq 0 ]]; then
        ok "All notebooks passed [$(timer_end)]"
    else
        err "Some notebooks failed (exit code $NOTEBOOK_EXIT_CODE) [$(timer_end)]"
    fi
}

# =============================================================================
# Phase 6: Cleanup
# =============================================================================
cleanup() {
    if [[ "$CLEANUP" != "true" ]]; then
        warn "Cleanup skipped (CLEANUP=$CLEANUP)"
        return 0
    fi
    log "Cleaning up resources..."

    # Docker
    if [[ "$DOCKER_STARTED" == "true" ]]; then
        (cd "$DOCKER_DIR" && docker compose -f "docker-compose.yml" down -v 2>/dev/null) || true
        ok "Docker containers removed"
    fi

    # S3
    if [[ "$AWS_RESOURCES_CREATED" == "true" && -n "$S3_BUCKET" ]]; then
        aws s3 rb "s3://$S3_BUCKET" --force --region "$AWS_REGION" 2>/dev/null || true
        ok "S3 bucket deleted"
    fi

    # Bedrock prompts
    if [[ "$BEDROCK_PROMPTS_CREATED" == "true" ]]; then
        [[ -n "$SYSTEM_PROMPT_ID" ]] && aws bedrock-agent delete-prompt --prompt-identifier "$SYSTEM_PROMPT_ID" --region "$AWS_REGION" 2>/dev/null || true
        [[ -n "$USER_PROMPT_ID" ]] && aws bedrock-agent delete-prompt --prompt-identifier "$USER_PROMPT_ID" --region "$AWS_REGION" 2>/dev/null || true
        ok "Bedrock prompts deleted"
    fi

    # Local .env
    if [[ "$ENV_CREATED" == "true" ]]; then
        rm -f "$NOTEBOOKS_DIR/.env"
        ok "Local .env removed"
    fi

    ok "Cleanup complete"
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "============================================================"
    echo "  lexical-graph-local-dev Notebook Test Runner"
    echo "============================================================"
    echo "  Mode: $DOCKER_MODE | GitHub: skip=$SKIP_GITHUB | PPTX: skip=$SKIP_PPTX | Long-running: skip=$SKIP_LONG_RUNNING"
    echo "  Cleanup: $CLEANUP | Reports: $REPORT_DIR"
    echo "============================================================"
    echo ""

    trap cleanup EXIT

    detect_platform
    setup_env
    setup_aws
    start_docker
    run_notebooks

    echo ""
    echo "============================================================"
    if [[ $NOTEBOOK_EXIT_CODE -eq 0 ]]; then
        ok "ALL TESTS PASSED"
    else
        err "SOME TESTS FAILED"
    fi
    echo "  Reports: $REPORT_DIR/execution_report.{json,md}"
    echo "============================================================"

    exit $NOTEBOOK_EXIT_CODE
}

main "$@"
