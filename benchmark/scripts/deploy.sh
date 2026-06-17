#!/usr/bin/env bash
# deploy.sh — Deploy a benchmark stack (CloudFormation + Neptune + OpenSearch + SageMaker)
#              and run the full benchmark pipeline.
#
# Usage:
#   ./deploy.sh <dataset> [retriever]
#   ./deploy.sh --all                     # Deploy 4 stacks (one per dataset) with shared VPC
#
# Datasets: cuad, concurrentqa, wikihow, pga
#
# This script wraps integration-tests/build-tests.sh with benchmark-specific defaults
# and VPC reuse support. It provisions the infrastructure AND runs benchmarks.
#
# VPC Reuse:
#   Set EXISTING_VPC_ID and EXISTING_SUBNET_IDS in your .env to reuse an existing VPC.
#   This lets you run multiple dataset stacks in parallel without hitting the VPC limit.
#
# Examples:
#   # Deploy CUAD stack (creates new VPC)
#   ./deploy.sh cuad
#
#   # Deploy all datasets with shared VPC
#   export EXISTING_VPC_ID=vpc-0abc123def456
#   export EXISTING_SUBNET_IDS=subnet-111,subnet-222
#   ./deploy.sh --all
#
#   # Deploy PGA with specific retriever
#   ./deploy.sh pga topic-beam-chunk_only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INTEGRATION_TESTS_DIR="$REPO_ROOT/integration-tests"
BENCHMARK_DIR="$REPO_ROOT/benchmark"

# Source .env if present
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# --- Validate required environment variables ---
REQUIRED_VARS=(
    BUCKET_NAME
    AWS_REGION
    BENCHMARK_DATA_S3_URI
)

for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: Required environment variable '$var' is missing or empty." >&2
        echo "       See .env.example for the full list of required variables." >&2
        exit 1
    fi
done

# --- Parse arguments ---
DATASET="${1:-}"
RETRIEVER="${2:-}"
DEPLOY_ALL=false

if [[ "$DATASET" == "--all" ]]; then
    DEPLOY_ALL=true
    DATASET=""
fi

VALID_DATASETS="cuad concurrentqa wikihow pga"

if [[ "$DEPLOY_ALL" == false ]] && [[ -z "$DATASET" ]]; then
    echo "ERROR: Dataset name or --all required." >&2
    echo "Usage: $0 <dataset> [retriever]" >&2
    echo "       $0 --all" >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

if [[ "$DEPLOY_ALL" == false ]] && ! echo "$VALID_DATASETS" | grep -qw "$DATASET"; then
    echo "ERROR: Invalid dataset '$DATASET'." >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

# --- Defaults for benchmark stacks ---
NEPTUNE_INSTANCE_TYPE="${NEPTUNE_INSTANCE_TYPE:-db.r8g.2xlarge}"
TEST_EXTRACTION_LLM="${TEST_EXTRACTION_LLM:-us.anthropic.claude-haiku-4-5-20251001-v1:0}"
TEST_RESPONSE_LLM="${TEST_RESPONSE_LLM:-us.anthropic.claude-haiku-4-5-20251001-v1:0}"

# --- Dataset-specific settings ---
get_notebook_type() {
    local ds="$1"
    case "$ds" in
        concurrentqa|wikihow) echo "ml.m5.4xlarge" ;;
        *) echo "${NOTEBOOK_INSTANCE_TYPE:-ml.m5.xlarge}" ;;
    esac
}

get_stack_prefix() {
    local ds="$1"
    case "$ds" in
        concurrentqa) echo "cqa" ;;
        wikihow) echo "wiki" ;;
        *) echo "$ds" ;;
    esac
}

get_test_args() {
    local ds="$1"
    local retriever="$2"
    
    # Datasets with extraction (no pre-extracted data)
    case "$ds" in
        wikihow|pga)
            echo "--test-file $BENCHMARK_DIR/test-configs/benchmark.$ds"
            ;;
        cuad|concurrentqa)
            # Pre-extracted — just build + query + evaluate
            local prefix
            prefix=$(echo "$ds" | sed 's/concurrentqa/ConcurrentQa/' | sed 's/cuad/Cuad/')
            if [[ "$ds" == "cuad" ]]; then prefix="Cuad"; fi
            if [[ "$ds" == "concurrentqa" ]]; then prefix="ConcurrentQa"; fi
            
            local tests="benchmark_build.${prefix}BenchmarkBuild benchmark_query.${prefix}BenchmarkQuery benchmark_evaluate.${prefix}BenchmarkEvaluate"
            echo "--test \"$tests\""
            ;;
    esac
}

# --- Deploy function ---
deploy_dataset() {
    local ds="$1"
    local retriever="${2:-}"
    local notebook_type
    local stack_prefix
    
    notebook_type=$(get_notebook_type "$ds")
    stack_prefix=$(get_stack_prefix "$ds")
    
    echo ""
    echo "=============================================="
    echo " Deploying benchmark stack: $ds"
    echo " Stack prefix: $stack_prefix"
    echo " Neptune: $NEPTUNE_INSTANCE_TYPE"
    echo " Notebook: $notebook_type"
    echo " Region: $AWS_REGION"
    if [[ -n "${EXISTING_VPC_ID:-}" ]]; then
        echo " VPC: $EXISTING_VPC_ID (reusing)"
        echo " Subnets: $EXISTING_SUBNET_IDS"
    else
        echo " VPC: (creating new)"
    fi
    echo "=============================================="
    echo ""
    
    # Build the command
    local cmd="STACK_PREFIX=$stack_prefix"
    cmd="$cmd REGION_NAME=$AWS_REGION"
    cmd="$cmd BUCKET_NAME=$BUCKET_NAME"
    cmd="$cmd TEST_EXTRACTION_LLM=$TEST_EXTRACTION_LLM"
    cmd="$cmd TEST_RESPONSE_LLM=$TEST_RESPONSE_LLM"
    
    if [[ -n "$retriever" ]]; then
        cmd="$cmd BENCHMARK_RETRIEVER=$retriever"
    fi
    
    # Export for build-tests.sh
    export STACK_PREFIX="$stack_prefix"
    export REGION_NAME="$AWS_REGION"
    
    # Construct build-tests.sh arguments
    local args=""
    args="$args --neptune-instance-type $NEPTUNE_INSTANCE_TYPE"
    args="$args --notebook-instance-type $notebook_type"
    args="$args --benchmark-data-s3-uri $BENCHMARK_DATA_S3_URI"
    args="$args --region $AWS_REGION"
    args="$args --bucket $BUCKET_NAME"
    args="$args --extraction-llm $TEST_EXTRACTION_LLM"
    args="$args --response-llm $TEST_RESPONSE_LLM"
    
    # Add test specification
    local test_args
    test_args=$(get_test_args "$ds" "$retriever")
    args="$args $test_args"
    
    # Add VPC reuse if configured
    # NOTE: VPC reuse requires CloudFormation template changes (Task 1.1)
    # Once implemented, these will be passed as stack parameters
    if [[ -n "${EXISTING_VPC_ID:-}" ]] && [[ -n "${EXISTING_SUBNET_IDS:-}" ]]; then
        echo "NOTE: VPC reuse parameters set. Will use existing VPC: $EXISTING_VPC_ID"
        # These will be passed once CFN template supports them (Task 1.1)
        # args="$args --existing-vpc-id $EXISTING_VPC_ID --existing-subnet-ids $EXISTING_SUBNET_IDS"
    fi
    
    # Run from integration-tests directory (where build-tests.sh lives)
    echo "Running: sh build-tests.sh $args"
    cd "$INTEGRATION_TESTS_DIR"
    eval "sh build-tests.sh $args"
}

# --- Execute ---
if [[ "$DEPLOY_ALL" == true ]]; then
    echo "=== Deploying all benchmark stacks ==="
    
    if [[ -z "${EXISTING_VPC_ID:-}" ]]; then
        echo ""
        echo "WARNING: Deploying 4 stacks without VPC reuse will create 4 VPCs."
        echo "         Set EXISTING_VPC_ID and EXISTING_SUBNET_IDS to share a VPC."
        echo ""
        read -p "Continue? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 0
        fi
    fi
    
    for ds in $VALID_DATASETS; do
        deploy_dataset "$ds" "$RETRIEVER"
    done
    
    echo ""
    echo "=== All benchmark stacks deployed ==="
else
    deploy_dataset "$DATASET" "$RETRIEVER"
fi
