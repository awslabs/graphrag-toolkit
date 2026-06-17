#!/usr/bin/env bash
# compare.sh — Generate a comparison report across datasets and retrievers.
# Usage: ./compare.sh [dataset]
# If no dataset is specified, generates a report across all datasets.
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
    AWS_REGION
    RESULTS_S3_URI
    BUCKET_NAME
)

for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: Required environment variable '$var' is missing or empty." >&2
        echo "       See .env.example for the full list of required variables." >&2
        exit 1
    fi
done

# --- Validate optional dataset parameter ---
DATASET="${1:-all}"
VALID_DATASETS="cuad concurrentqa wikihow pga all"

if ! echo "$VALID_DATASETS" | grep -qw "$DATASET"; then
    echo "ERROR: Invalid dataset '$DATASET'." >&2
    echo "Valid datasets: $VALID_DATASETS" >&2
    exit 1
fi

echo "=== GraphRAG Benchmark: Comparison Report ==="
echo "Dataset:     $DATASET"
echo "Results S3:  $RESULTS_S3_URI"
echo "Region:      $AWS_REGION"
echo "==============================================="

# --- Determine which datasets to include ---
if [[ "$DATASET" == "all" ]]; then
    DATASETS=("cuad" "concurrentqa" "wikihow" "pga")
else
    DATASETS=("$DATASET")
fi

# --- Sync results locally ---
RESULTS_DIR="$SCRIPT_DIR/../../benchmark-results"
mkdir -p "$RESULTS_DIR"

for ds in "${DATASETS[@]}"; do
    echo "Syncing results for $ds..."
    aws s3 sync "${RESULTS_S3_URI}${ds}-benchmark-results/" "$RESULTS_DIR/$ds/" \
        --region "$AWS_REGION" --quiet
done

# --- Generate comparison report ---
cd "$SCRIPT_DIR/../.."

python -c "
import json
import os
import sys
from pathlib import Path

results_dir = Path('$RESULTS_DIR')
datasets = $( printf '%s\n' "${DATASETS[@]}" | python3 -c "import sys,json; print(json.dumps([l.strip() for l in sys.stdin]))" )

report = {
    'datasets': datasets,
    'retrievers': [],
    'correctness': {},
}

for ds in datasets:
    ds_dir = results_dir / ds
    if not ds_dir.exists():
        print(f'WARNING: No results found for {ds}', file=sys.stderr)
        continue

    for retriever_dir in sorted(ds_dir.iterdir()):
        if not retriever_dir.is_dir():
            continue
        retriever = retriever_dir.name
        if retriever not in report['retrievers']:
            report['retrievers'].append(retriever)

        correctness_file = retriever_dir / 'correctness.json'
        if correctness_file.exists():
            with open(correctness_file) as f:
                data = json.load(f)
                report['correctness'].setdefault(retriever, {})[ds] = data.get('score', None)

output_path = results_dir / 'comparison_report.json'
with open(output_path, 'w') as f:
    json.dump(report, f, indent=2)

print(f'Comparison report written to: {output_path}')
print(f'Datasets: {len(datasets)}, Retrievers: {len(report[\"retrievers\"])}')
"

echo ""
echo "=== Comparison report generation complete ==="
