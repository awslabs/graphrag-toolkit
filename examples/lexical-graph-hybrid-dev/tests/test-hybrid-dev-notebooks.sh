#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# test-hybrid-dev-notebooks.sh
#
# Full lifecycle test runner for lexical-graph-hybrid-dev notebooks.
# Handles: env setup → AWS resources → Docker → notebook execution → report → cleanup
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NOTEBOOKS_DIR="$PROJECT_DIR/notebooks"
DOCKER_DIR="$PROJECT_DIR/docker"
AWS_DIR="$PROJECT_DIR/aws"
REPORT_DIR="${REPORT_DIR:-$PROJECT_DIR/test-results}"

# Configurable flags
SKIP_CUDA="${SKIP_CUDA:-true}"
SKIP_BATCH="${SKIP_BATCH:-true}"
CLEANUP="${CLEANUP:-true}"
DOCKER_MODE="standard"

# State tracking for cleanup
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BATCH_ROLE_NAME="bedrock-batch-inference-role-${TIMESTAMP}"
PROMPT_ROLE_NAME="bedrock-prompt-role-${TIMESTAMP}"
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
    JUPYTER_CONTAINER="jupyter-hybrid"
    NEO4J_CONTAINER="neo4j-hybrid"
    PGVECTOR_CONTAINER="pgvector-hybrid"
    JUPYTER_WORK_DIR="/home/jovyan/notebooks"
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
        sed -i '' "s/^AWS_ACCOUNT=.*/AWS_ACCOUNT=${AWS_ACCOUNT}/" "$NOTEBOOKS_DIR/.env"
        sed -i '' "s/^AWS_REGION=.*/AWS_REGION=${AWS_REGION}/" "$NOTEBOOKS_DIR/.env"
        sed -i '' "s/^S3_BUCKET_NAME=.*/S3_BUCKET_NAME=${S3_BUCKET}/" "$NOTEBOOKS_DIR/.env"
    else
        sed -i "s/^AWS_ACCOUNT=.*/AWS_ACCOUNT=${AWS_ACCOUNT}/" "$NOTEBOOKS_DIR/.env"
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

    # S3, DynamoDB, IAM role for batch inference
    (cd "$AWS_DIR" && BATCH_ROLE_NAME="$BATCH_ROLE_NAME" bash setup-bedrock-batch.sh) || true
    AWS_RESOURCES_CREATED=true

    # Bedrock prompts (optional — don't fail if scripts missing)
    if [[ -f "$AWS_DIR/create_prompt_role.sh" && -f "$AWS_DIR/create_custom_prompt.sh" ]]; then
        (cd "$AWS_DIR" && bash create_prompt_role.sh --role-name "$PROMPT_ROLE_NAME") || true

        local sys_output usr_output
        sys_output=$(cd "$AWS_DIR" && bash create_custom_prompt.sh system_prompt.json "$AWS_REGION" 2>&1) || true
        usr_output=$(cd "$AWS_DIR" && bash create_custom_prompt.sh user_prompt.json "$AWS_REGION" 2>&1) || true

        # Extract prompt IDs and set ARNs in .env
        SYSTEM_PROMPT_ID=$(echo "$sys_output" | grep -o '"id": *"[^"]*"' | head -1 | cut -d'"' -f4) || true
        USER_PROMPT_ID=$(echo "$usr_output" | grep -o '"id": *"[^"]*"' | head -1 | cut -d'"' -f4) || true

        if [[ -n "$SYSTEM_PROMPT_ID" && -n "$USER_PROMPT_ID" ]]; then
            echo "SYSTEM_PROMPT_ARN=arn:aws:bedrock:${AWS_REGION}:${AWS_ACCOUNT}:prompt/${SYSTEM_PROMPT_ID}" >> "$NOTEBOOKS_DIR/.env"
            echo "USER_PROMPT_ARN=arn:aws:bedrock:${AWS_REGION}:${AWS_ACCOUNT}:prompt/${USER_PROMPT_ID}" >> "$NOTEBOOKS_DIR/.env"
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
            return 0
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

    # Copy runner script into container
    docker cp "$SCRIPT_DIR/run_notebooks.py" "$JUPYTER_CONTAINER":"$JUPYTER_WORK_DIR/run_notebooks.py"

    # Pass AWS credentials to container
    local aws_env_flags=""
    [[ -n "${AWS_ACCESS_KEY_ID:-}" ]] && aws_env_flags="$aws_env_flags -e AWS_ACCESS_KEY_ID"
    [[ -n "${AWS_SECRET_ACCESS_KEY:-}" ]] && aws_env_flags="$aws_env_flags -e AWS_SECRET_ACCESS_KEY"
    [[ -n "${AWS_SESSION_TOKEN:-}" ]] && aws_env_flags="$aws_env_flags -e AWS_SESSION_TOKEN"
    [[ -n "${AWS_PROFILE:-}" ]] && aws_env_flags="$aws_env_flags -e AWS_PROFILE"
    [[ -n "${AWS_DEFAULT_REGION:-}" ]] && aws_env_flags="$aws_env_flags -e AWS_DEFAULT_REGION"

    # Execute
    # shellcheck disable=SC2086
    docker exec $aws_env_flags "$JUPYTER_CONTAINER" \
        python3 "$JUPYTER_WORK_DIR/run_notebooks.py" \
        --work-dir="$JUPYTER_WORK_DIR" \
        --skip-cuda="$SKIP_CUDA" \
        --skip-batch="$SKIP_BATCH" \
    || NOTEBOOK_EXIT_CODE=$?

    # Collect reports
    docker cp "$JUPYTER_CONTAINER":"$JUPYTER_WORK_DIR/execution_report.json" "$REPORT_DIR/" 2>/dev/null || true
    docker cp "$JUPYTER_CONTAINER":"$JUPYTER_WORK_DIR/execution_report.md" "$REPORT_DIR/" 2>/dev/null || true

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
        (cd "$DOCKER_DIR" && docker compose -f docker-compose.yml down -v 2>/dev/null) || true
        ok "Docker containers removed"
    fi

    # S3
    if [[ "$AWS_RESOURCES_CREATED" == "true" && -n "$S3_BUCKET" ]]; then
        aws s3 rb "s3://$S3_BUCKET" --force 2>/dev/null || true
        ok "S3 bucket deleted"
    fi

    # DynamoDB
    if [[ "$AWS_RESOURCES_CREATED" == "true" ]]; then
        aws dynamodb delete-table --table-name graphrag-toolkit-batch-table --region "$AWS_REGION" 2>/dev/null || true
        ok "DynamoDB table deleted"
    fi

    # IAM roles (timestamped — only deletes roles created by this run)
    if [[ "$AWS_RESOURCES_CREATED" == "true" ]]; then
        for role in "$BATCH_ROLE_NAME" "$PROMPT_ROLE_NAME"; do
            # Detach managed policies
            local policies
            policies=$(aws iam list-attached-role-policies --role-name "$role" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null) || true
            for arn in $policies; do
                aws iam detach-role-policy --role-name "$role" --policy-arn "$arn" 2>/dev/null || true
            done
            # Delete inline policies
            local inline
            inline=$(aws iam list-role-policies --role-name "$role" --query 'PolicyNames[]' --output text 2>/dev/null) || true
            for name in $inline; do
                aws iam delete-role-policy --role-name "$role" --policy-name "$name" 2>/dev/null || true
            done
            aws iam delete-role --role-name "$role" 2>/dev/null || true
        done
        ok "IAM roles deleted"
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
    echo "  lexical-graph-hybrid-dev Notebook Test Runner"
    echo "============================================================"
    echo "  Mode: $DOCKER_MODE | CUDA: skip=$SKIP_CUDA | Batch: skip=$SKIP_BATCH"
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
