#!/usr/bin/env bash
# run_all.sh — Run the full benchmark pipeline (extract → build → query → evaluate) for a dataset.
# Usage: ./run_all.sh <dataset> [retriever]
# Datasets: cuad, concurrentqa, wikihow, pga
# Retrievers: traversal, topic-beam-chunk_only, chunk_based_semantic, etc. (defaults to all)

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
    TEST_EXTRACTION_LLM
    TEST_RESPONSE_LLM
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

RETRIEVER="${2:-all}"

echo "=== GraphRAG Benchmark: Full Pipeline ==="
echo "Dataset:    $DATASET"
echo "Retriever:  $RETRIEVER"
echo "==========================================="
echo ""

# --- Step 1: Extract ---
echo ">>> Step 1/4: Extract"
"$SCRIPT_DIR/extract.sh" "$DATASET"
echo ""

# --- Step 2: Build ---
echo ">>> Step 2/4: Build"
"$SCRIPT_DIR/build.sh" "$DATASET"
echo ""

# --- Step 3: Query ---
echo ">>> Step 3/4: Query"
"$SCRIPT_DIR/query.sh" "$DATASET" "$RETRIEVER"
echo ""

# --- Step 4: Evaluate ---
echo ">>> Step 4/4: Evaluate"
"$SCRIPT_DIR/evaluate.sh" "$DATASET" "$RETRIEVER"
echo ""

echo "=== Full pipeline complete for dataset: $DATASET ==="
