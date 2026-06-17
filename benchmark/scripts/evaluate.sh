#!/usr/bin/env bash
# evaluate.sh — Run the evaluation phase for a benchmark dataset.
# Usage: ./evaluate.sh <dataset> [retriever]
# Datasets: cuad, concurrentqa, wikihow, pga
# Retrievers: traversal, topic-beam-chunk_only, chunk_based_semantic,
#             semantic_guided, chunk_based, entity_network, entity_based,
#             topic_based, agentic
# If no retriever is specified, all retrievers are evaluated.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source .env if present
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# --- Validate required environment variables ---
REQUIRED_VARS=(
    NEPTUNE_ENDPOINT
    OPENSEARCH_ENDPOINT
    AWS_REGION
    BENCHMARK_DATA_S3_URI
    RESULTS_S3_URI
    BUCKET_NAME
    BENCHMARK_JUDGE_LLM
    STACK_PREFIX
)

for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: Required environment variable '$var' is missing or empty." >&2
        echo "       See .env.example for the full list of required variables." >&2
        exit 1
    fi
done

# --- Validate dataset parameter ---
DATASET="${1:-}"
VALID_DATASETS="cuad concurrentqa wikihow pga"

if [[ -z "$DATASET" ]]; then
    echo "ERROR: Dataset name required." >&2
    echo "Usage: $0 <dataset> [retriever]" >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

if ! echo "$VALID_DATASETS" | grep -qw "$DATASET"; then
    echo "ERROR: Invalid dataset '$DATASET'." >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

# --- Validate optional retriever parameter ---
RETRIEVER="${2:-all}"
VALID_RETRIEVERS="traversal topic-beam-chunk_only chunk_based_semantic semantic_guided chunk_based entity_network entity_based topic_based agentic all"

if ! echo "$VALID_RETRIEVERS" | grep -qw "$RETRIEVER"; then
    echo "ERROR: Invalid retriever '$RETRIEVER'." >&2
    echo "Valid retrievers: $VALID_RETRIEVERS" >&2
    exit 1
fi

# --- Map dataset to test class ---
declare -A EVALUATE_CLASSES=(
    [cuad]="benchmark_evaluate.CuadBenchmarkEvaluate"
    [concurrentqa]="benchmark_evaluate.ConcurrentQaBenchmarkEvaluate"
    [wikihow]="benchmark_evaluate.WikihowBenchmarkEvaluate"
    [pga]="benchmark_evaluate.PgaBenchmarkEvaluate"
)

TEST_CLASS="${EVALUATE_CLASSES[$DATASET]}"

echo "=== GraphRAG Benchmark: Evaluate ==="
echo "Dataset:    $DATASET"
echo "Test class: $TEST_CLASS"
echo "Retriever:  $RETRIEVER"
echo "Judge LLM:  $BENCHMARK_JUDGE_LLM"
echo "Region:     $AWS_REGION"
echo "===================================="

# --- Run evaluation ---
export STACK_PREFIX

# Set retriever filter if specified
if [[ "$RETRIEVER" != "all" ]]; then
    export BENCHMARK_RETRIEVER="$RETRIEVER"
fi

cd "$SCRIPT_DIR/../.."

python -m pytest "benchmark/${TEST_CLASS%%.*}.py::${TEST_CLASS##*.}" \
    --benchmark-data-s3-uri "$BENCHMARK_DATA_S3_URI" \
    -v --tb=short

echo "Evaluation complete for dataset: $DATASET, retriever: $RETRIEVER"
