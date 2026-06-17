#!/usr/bin/env bash
# build.sh — Run the graph build phase for a benchmark dataset.
# Usage: ./build.sh <dataset>
# Datasets: cuad, concurrentqa, wikihow, pga

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
    echo "Usage: $0 <dataset>" >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

if ! echo "$VALID_DATASETS" | grep -qw "$DATASET"; then
    echo "ERROR: Invalid dataset '$DATASET'." >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

# --- Map dataset to test class ---
declare -A BUILD_CLASSES=(
    [cuad]="benchmark_build.CuadBenchmarkBuild"
    [concurrentqa]="benchmark_build.ConcurrentQaBenchmarkBuild"
    [wikihow]="benchmark_build.WikihowBenchmarkBuild"
    [pga]="benchmark_build.PgaBenchmarkBuild"
)

TEST_CLASS="${BUILD_CLASSES[$DATASET]}"

echo "=== GraphRAG Benchmark: Build ==="
echo "Dataset:    $DATASET"
echo "Test class: $TEST_CLASS"
echo "S3 data:    $BENCHMARK_DATA_S3_URI"
echo "Neptune:    $NEPTUNE_ENDPOINT"
echo "OpenSearch: $OPENSEARCH_ENDPOINT"
echo "Region:     $AWS_REGION"
echo "===================================="

# --- Run graph build ---
export STACK_PREFIX
cd "$SCRIPT_DIR/../.."

python -m pytest "benchmark/${TEST_CLASS%%.*}.py::${TEST_CLASS##*.}" \
    --benchmark-data-s3-uri "$BENCHMARK_DATA_S3_URI" \
    -v --tb=short

echo "Build complete for dataset: $DATASET"
