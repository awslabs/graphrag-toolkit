#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Runs the full benchmark pipeline: extract + build once, then query + evaluate
# for every retriever in the list.
#
# Usage: bash run_all_retrievers.sh <dataset>
#   dataset: one of cuad, concurrentqa, pga, wikihow
#
# This script is designed to run ON the SageMaker notebook after the initial
# setup (run_test_suite.sh handles setup). It sources .env.testing for
# connectivity and environment variables.

set -e

DATASET="${1:-concurrentqa}"

# Source environment variables
if [[ -f .env.testing ]]; then
  . ./.env.testing
fi
if [[ -f .env ]]; then
  . ./.env
fi

# Map dataset to test class names
case "$DATASET" in
  cuad)
    EXTRACT_CLASS=""
    BUILD_CLASS="benchmark_build.CuadBenchmarkBuild"
    QUERY_CLASS="benchmark_query.CuadBenchmarkQuery"
    EVAL_CLASS="benchmark_evaluate.CuadBenchmarkEvaluate"
    ;;
  concurrentqa)
    EXTRACT_CLASS="benchmark_extract.ConcurrentQaBenchmarkExtract"
    BUILD_CLASS="benchmark_build.ConcurrentQaBenchmarkBuild"
    QUERY_CLASS="benchmark_query.ConcurrentQaBenchmarkQuery"
    EVAL_CLASS="benchmark_evaluate.ConcurrentQaBenchmarkEvaluate"
    ;;
  pga)
    EXTRACT_CLASS="benchmark_extract.PgaBenchmarkExtract"
    BUILD_CLASS="benchmark_build.PgaBenchmarkBuild"
    QUERY_CLASS="benchmark_query.PgaBenchmarkQuery"
    EVAL_CLASS="benchmark_evaluate.PgaBenchmarkEvaluate"
    ;;
  wikihow)
    EXTRACT_CLASS="benchmark_extract.WikihowBenchmarkExtract"
    BUILD_CLASS="benchmark_build.WikihowBenchmarkBuild"
    QUERY_CLASS="benchmark_query.WikihowBenchmarkQuery"
    EVAL_CLASS="benchmark_evaluate.WikihowBenchmarkEvaluate"
    ;;
  *)
    echo "ERROR: Unknown dataset '$DATASET'. Use one of: cuad, concurrentqa, pga, wikihow"
    exit 1
    ;;
esac

# Retrievers to benchmark (skip semantic_guided — 18x slower, skip agentic — no benefit)
RETRIEVERS=(
  "traversal"
  "topic-beam-chunk_only"
  "topic_beam_search"
  "chunk_based_semantic"
  "entity_network"
  "chunk_based"
  "entity_based"
  "topic_based"
)

echo "============================================"
echo " Multi-Retriever Benchmark: $DATASET"
echo " Retrievers: ${RETRIEVERS[*]}"
echo "============================================"
echo ""

# Phase 1: Extract + Build (once)
echo "=== Phase 1: Extract + Build ==="
export BENCHMARK_RETRIEVER=traversal
if [[ -n "$EXTRACT_CLASS" ]]; then
  export TESTS="$EXTRACT_CLASS $BUILD_CLASS"
else
  export TESTS="$BUILD_CLASS"
fi
python test_suite.py

echo ""
echo "=== Phase 1 complete. Graph built. ==="
echo ""

# Phase 2: Query + Evaluate for each retriever
for retriever in "${RETRIEVERS[@]}"; do
  echo ""
  echo "=== Phase 2: Query + Evaluate with retriever: $retriever ==="
  export BENCHMARK_RETRIEVER="$retriever"
  export TESTS="$QUERY_CLASS $EVAL_CLASS"
  python test_suite.py || echo "WARNING: retriever $retriever failed, continuing..."
  echo "=== Completed: $retriever ==="
done

echo ""
echo "============================================"
echo " All retrievers complete for $DATASET"
echo "============================================"
