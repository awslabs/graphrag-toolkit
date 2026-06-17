# GraphRAG Benchmark Reproducibility Scripts

End-to-end scripts for reproducing GraphRAG Toolkit benchmark results across 4 datasets and 9 retrievers.

## Directory Structure

```
benchmark/
├── README.md                           # Benchmarking guide (how to run on AWS infra)
├── scripts/                            # CLI reproducibility scripts (this directory)
│   ├── extract.sh
│   ├── build.sh
│   ├── query.sh
│   ├── evaluate.sh
│   ├── run_all.sh
│   ├── compare.sh
│   ├── .env.example
│   └── README.md                       # This file
├── benchmark_extract.py                # Extraction test classes
├── benchmark_build.py                  # Graph build test classes
├── benchmark_query.py                  # Query test classes
├── benchmark_evaluate.py               # Evaluation test classes
├── utils/                              # Shared utilities
│   ├── __init__.py
│   ├── s3_utils.py
│   ├── metrics_summary.py
│   ├── hop_classifier.py
│   ├── token_tracker.py
│   ├── retriever_factory.py
│   ├── agentic_retriever.py
│   ├── comparison_report.py
│   ├── run_evaluation.py
│   ├── test_benchmark_query.py
│   ├── test_metrics_summary.py
│   ├── test_hop_classifier.py
│   ├── test_agentic_retriever.py
│   ├── test_comparison_report.py
│   ├── test_retriever_factory.py
│   └── test_token_tracker.py
├── evaluation/                         # Standalone evaluation scripts
│   ├── answer_eval.py
│   └── run_evaluation.py
└── test-configs/                       # Test file configs for build-tests.sh
    ├── benchmark.concurrentqa.prototype
    ├── benchmark.wikihow
    └── benchmark.pga
```

## Prerequisites

1. **AWS Infrastructure**: A provisioned CloudFormation stack with Neptune and OpenSearch Serverless.
   Deploy using `integration-tests/cloudformation-templates/graphrag-toolkit-tests.json` with:
   - Neptune instance: `db.r8g.2xlarge`
   - Notebook instance: `ml.m5.xlarge` (CUAD/PGA) or `ml.m5.4xlarge` (ConcurrentQA/WikiHow)

2. **AWS CLI** configured with credentials that have access to Neptune, OpenSearch, Bedrock, and S3.

3. **Python environment** with the graphrag-toolkit packages installed:
   ```bash
   pip install -e lexical-graph/
   pip install -e byokg-rag/
   pip install pytest
   ```

4. **Environment variables**: Copy `.env.example` to `.env` and fill in your values:
   ```bash
   cp .env.example .env
   # Edit .env with your endpoint values
   ```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NEPTUNE_ENDPOINT` | Neptune cluster endpoint | (required) |
| `OPENSEARCH_ENDPOINT` | OpenSearch Serverless endpoint | (required) |
| `AWS_REGION` | AWS region | `us-west-2` |
| `BENCHMARK_DATA_S3_URI` | S3 path to benchmark datasets | (required) |
| `RESULTS_S3_URI` | S3 path for storing results | (required) |
| `BUCKET_NAME` | S3 bucket name (no prefix) | (required) |
| `TEST_EXTRACTION_LLM` | LLM for extraction | `us.anthropic.claude-haiku-4-5-20251001-v1:0` |
| `TEST_RESPONSE_LLM` | LLM for response generation | `us.anthropic.claude-haiku-4-5-20251001-v1:0` |
| `BENCHMARK_JUDGE_LLM` | LLM for evaluation judging | `us.anthropic.claude-sonnet-4-6` |
| `STACK_PREFIX` | CloudFormation stack prefix | (required) |

## Datasets

| Dataset | Documents | Questions | Notes |
|---------|-----------|-----------|-------|
| `cuad` | 510 | 500 | Contract Understanding Atticus Dataset |
| `concurrentqa` | 13,501 | 400 | Multi-hop QA (needs 64GB notebook) |
| `wikihow` | 5,000 | 300 | Instructional articles (needs 64GB notebook) |
| `pga` | 507 | 240 | PGA Tour bio (150) + stats (90) |

## Retrievers

| ID | Description |
|----|-------------|
| `traversal` | Topic-first graph walk (default) |
| `topic-beam-chunk_only` | Vector seed + graph beam search (best correctness) |
| `chunk_based_semantic` | Pure vector search |
| `semantic_guided` | Heavy beam search (high accuracy, 18x slower) |
| `chunk_based` | Keyword-based (BM25/TF-IDF) |
| `entity_network` | Entity expansion via relationship network |
| `entity_based` | Direct entity lookup |
| `topic_based` | Topic-only retrieval |
| `agentic` | LLM-guided iterative retrieval |

## Execution Order

The benchmark pipeline runs in this order:

```
extract → build → query → evaluate
```

1. **Extract** — Run LLM extraction on documents to produce entities, topics, and statements
2. **Build** — Ingest extracted data into Neptune graph and OpenSearch index
3. **Query** — Run benchmark queries against the graph using one or more retrievers
4. **Evaluate** — Judge response correctness using the evaluation LLM

## CLI Commands

### Run individual stages

```bash
# Extract (prepare documents for graph building)
./extract.sh cuad

# Build (ingest into Neptune + OpenSearch)
./build.sh cuad

# Query (run all retrievers, or specify one)
./query.sh cuad
./query.sh cuad topic-beam-chunk_only

# Evaluate (judge responses, all retrievers or specify one)
./evaluate.sh cuad
./evaluate.sh cuad topic-beam-chunk_only
```

### Run the full pipeline

```bash
# Full pipeline for a dataset (extract → build → query → evaluate)
./run_all.sh cuad

# Full pipeline with a specific retriever
./run_all.sh pga topic-beam-chunk_only
```

### Generate comparison report

```bash
# Compare all datasets
./compare.sh

# Compare a single dataset
./compare.sh cuad
```

## Full Reproduction Commands

### CUAD (pre-extracted, build → query → evaluate)

```bash
export STACK_PREFIX=cuad
./build.sh cuad
./query.sh cuad
./evaluate.sh cuad
```

### ConcurrentQA (pre-extracted, build → query → evaluate)

```bash
export STACK_PREFIX=cqa
./build.sh concurrentqa
./query.sh concurrentqa
./evaluate.sh concurrentqa
```

### WikiHow (full pipeline)

```bash
export STACK_PREFIX=wiki
./run_all.sh wikihow
```

### PGA (full pipeline)

```bash
export STACK_PREFIX=pga
./run_all.sh pga
```

## Alternative: Using build-tests.sh Directly

These scripts wrap the existing `build-tests.sh` infrastructure. You can also invoke it directly:

```bash
cd integration-tests/

# CUAD
export STACK_PREFIX=cuad
sh build-tests.sh --neptune-instance-type db.r8g.2xlarge \
    --benchmark-data-s3-uri s3://ouss-benchmarking-tests/benchmark-data/ \
    --test "benchmark_build.CuadBenchmarkBuild benchmark_query.CuadBenchmarkQuery benchmark_evaluate.CuadBenchmarkEvaluate"

# PGA (with extraction)
export STACK_PREFIX=pga
sh build-tests.sh --neptune-instance-type db.r8g.2xlarge \
    --benchmark-data-s3-uri s3://ouss-benchmarking-tests/benchmark-data/ \
    --test-file benchmark.pga

# ConcurrentQA (larger notebook needed)
export STACK_PREFIX=cqa
sh build-tests.sh --neptune-instance-type db.r8g.2xlarge \
    --notebook-instance-type ml.m5.4xlarge \
    --benchmark-data-s3-uri s3://ouss-benchmarking-tests/benchmark-data/ \
    --test "benchmark_build.ConcurrentQaBenchmarkBuild benchmark_query.ConcurrentQaBenchmarkQuery benchmark_evaluate.ConcurrentQaBenchmarkEvaluate"

# WikiHow (larger notebook needed)
export STACK_PREFIX=wiki
sh build-tests.sh --neptune-instance-type db.r8g.2xlarge \
    --notebook-instance-type ml.m5.4xlarge \
    --benchmark-data-s3-uri s3://ouss-benchmarking-tests/benchmark-data/ \
    --test-file benchmark.wikihow
```

## Monitoring

```bash
# Check results in S3
aws s3 ls s3://ouss-benchmarking-tests/<stack-name>/results/ --recursive --region us-west-2

# Download results locally
aws s3 sync s3://ouss-benchmarking-tests/cuad-benchmark-results/ benchmark-results/cuad/ --region us-west-2

# Check batch inference jobs (for extraction)
aws bedrock list-model-invocation-jobs --region us-west-2 --status-equals InProgress --output table
```

## Troubleshooting

- **VPC limit**: Account has limited VPCs in us-west-2. Delete old stacks before creating new ones, or use VPC reuse parameters.
- **OOM on ConcurrentQA/WikiHow**: Use `ml.m5.4xlarge` (64GB) notebook instance.
- **Token tracking shows null**: Pydantic v2 monkey-patching issue. Fixed in current codebase for standard retrievers.
- **0% correctness on topic_based**: Known issue — retriever returns empty results. Pending re-evaluation with Sonnet 4.6.
