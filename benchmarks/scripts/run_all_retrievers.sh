#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# run_all_retrievers.sh — Run extract + build once, then query + evaluate for each retriever.
#
# Usage:
#   bash benchmarks/scripts/run_all_retrievers.sh <dataset>
#
# Where <dataset> is one of: cuad, concurrentqa, pga, pga_bio, pga_stat, wikihow

set -euo pipefail

DATASET="${1:-}"

if [[ -z "$DATASET" ]]; then
    echo "Usage: $0 <dataset>"
    echo "  dataset: cuad | concurrentqa | pga | pga_bio | pga_stat | wikihow"
    exit 1
fi

# Guard: --delete-on-pass is incompatible with multi-retriever mode because
# test_suite.py deletes the stack on the first passing run, leaving subsequent
# retrievers without infrastructure.
if [[ "${DELETE_ON_PASS:-}" == "True" || "${DELETE_ON_PASS:-}" == "true" ]]; then
    echo "ERROR: BENCHMARK_ALL_RETRIEVERS is incompatible with DELETE_ON_PASS=True."
    echo "  The stack would be torn down after the first passing retriever,"
    echo "  leaving remaining retrievers without infrastructure."
    echo "  Please set DELETE_ON_PASS=False or omit --delete-on-pass."
    exit 1
fi

# Map dataset argument to test class name prefixes
case "$DATASET" in
    cuad)
        EXTRACT_CLASS="benchmark_extract.CuadBenchmarkExtract"
        BUILD_CLASS="benchmark_build.CuadBenchmarkBuild"
        QUERY_CLASS="benchmark_query.CuadBenchmarkQuery"
        EVALUATE_CLASS="benchmark_evaluate.CuadBenchmarkEvaluate"
        ;;
    concurrentqa)
        EXTRACT_CLASS="benchmark_extract.ConcurrentQaBenchmarkExtract"
        BUILD_CLASS="benchmark_build.ConcurrentQaBenchmarkBuild"
        QUERY_CLASS="benchmark_query.ConcurrentQaBenchmarkQuery"
        EVALUATE_CLASS="benchmark_evaluate.ConcurrentQaBenchmarkEvaluate"
        ;;
    pga|pga_bio|pga_stat)
        EXTRACT_CLASS="benchmark_extract.PgaBenchmarkExtract"
        BUILD_CLASS="benchmark_build.PgaBenchmarkBuild"
        QUERY_CLASS="benchmark_query.PgaBenchmarkQuery"
        EVALUATE_CLASS="benchmark_evaluate.PgaBenchmarkEvaluate"
        ;;
    wikihow)
        EXTRACT_CLASS="benchmark_extract.WikihowBenchmarkExtract"
        BUILD_CLASS="benchmark_build.WikihowBenchmarkBuild"
        QUERY_CLASS="benchmark_query.WikihowBenchmarkQuery"
        EVALUATE_CLASS="benchmark_evaluate.WikihowBenchmarkEvaluate"
        ;;
    *)
        echo "ERROR: Unknown dataset '$DATASET'"
        echo "  Valid values: cuad, concurrentqa, pga, pga_bio, pga_stat, wikihow"
        exit 1
        ;;
esac

# All retrievers to benchmark in a single pass
RETRIEVERS=(
    traversal
    topic-beam-chunk_only
    topic_beam_search
    chunk_based_semantic
    entity_network
    chunk_based
    entity_based
    topic_based
)

echo "============================================================"
echo " Multi-Retriever Benchmark: $DATASET"
echo " Retrievers: ${RETRIEVERS[*]}"
echo "============================================================"
echo ""

# --- Phase 1: Extract + Build (run once) ---
echo ">>> Phase 1: Extract + Build"
echo "    Running: $EXTRACT_CLASS"
export TESTS="$EXTRACT_CLASS"
python test_suite.py "$EXTRACT_CLASS"

echo "    Running: $BUILD_CLASS"
export TESTS="$BUILD_CLASS"
python test_suite.py "$BUILD_CLASS"

echo ">>> Phase 1 complete."
echo ""

# --- Phase 2: Query + Evaluate per retriever ---
PASS_COUNT=0
FAIL_COUNT=0
FAILED_RETRIEVERS=()

for RETRIEVER in "${RETRIEVERS[@]}"; do
    echo "------------------------------------------------------------"
    echo ">>> Phase 2: Query + Evaluate [$RETRIEVER]"
    echo "------------------------------------------------------------"

    export BENCHMARK_RETRIEVER="$RETRIEVER"

    echo "    Running: $QUERY_CLASS (retriever=$RETRIEVER)"
    export TESTS="$QUERY_CLASS"
    if python test_suite.py "$QUERY_CLASS"; then
        echo "    Running: $EVALUATE_CLASS (retriever=$RETRIEVER)"
        export TESTS="$EVALUATE_CLASS"
        if python test_suite.py "$EVALUATE_CLASS"; then
            echo "    PASSED: $RETRIEVER"
            PASS_COUNT=$((PASS_COUNT + 1))
        else
            echo "    FAILED (evaluate): $RETRIEVER"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            FAILED_RETRIEVERS+=("$RETRIEVER")
        fi
    else
        echo "    FAILED (query): $RETRIEVER"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_RETRIEVERS+=("$RETRIEVER")
    fi

    echo ""
done

# --- Summary ---
echo "============================================================"
echo " SUMMARY: $DATASET"
echo "   Passed: $PASS_COUNT / ${#RETRIEVERS[@]}"
echo "   Failed: $FAIL_COUNT / ${#RETRIEVERS[@]}"
if [[ ${#FAILED_RETRIEVERS[@]} -gt 0 ]]; then
    echo "   Failed retrievers: ${FAILED_RETRIEVERS[*]}"
fi
echo "============================================================"

if [[ $FAIL_COUNT -gt 0 ]]; then
    exit 1
fi
