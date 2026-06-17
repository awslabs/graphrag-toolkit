# Benchmarking Guide

This guide explains how to run benchmarks for the graphrag-toolkit across all four datasets (CUAD, ConcurrentQA, WikiHow, PGA) and multiple retriever configurations.

## Overview

The benchmarking pipeline has up to four stages:

1. **Extract** — Extract propositions and topics from raw documents using LLM-based extraction (optional if pre-extracted data is available)
2. **Build** — Build graph and vector stores from extracted chunks
3. **Query** — Query the graph with QA pairs and collect responses
4. **Evaluate** — Score responses using LLM-as-judge (correctness and IDK metrics)

## Prerequisites

- AWS account with permissions for CloudFormation, SageMaker, Neptune, OpenSearch, Bedrock, and S3
- AWS CLI configured with appropriate credentials
- A clone of the graphrag-toolkit repository
- An S3 bucket for test assets and results
- Pre-extracted data uploaded to S3 (or raw documents if running extraction)

## Configuration

Create a `.env` file in the `integration-tests/` directory based on `env.template`:

```bash
cp env.template .env
# Edit .env with your bucket name, region, and model preferences
```

See `env.template` for all available configuration options.

## S3 Data Layout

When using `BENCHMARK_DATA_S3_URI`, the benchmark data must follow this structure:

```
s3://<BUCKET>/benchmark-data/
├── cuad/
│   ├── qa.json                          # QA pairs (questions + ground-truth answers)
│   ├── documents/                       # Raw documents (only needed for extraction)
│   └── extracted/
│       └── 2026-02-17/                  # Pre-extracted chunks (collection_id)
│           ├── doc-001.json
│           ├── doc-002.json
│           └── ...
├── concurrentqa/
│   ├── qa.json                          # QA pairs
│   ├── documents/                       # Raw documents (only needed for extraction)
│   └── extracted/
│       └── 20260513-174224/             # Pre-extracted chunks (collection_id)
│           ├── doc-001.json
│           ├── doc-002.json
│           └── ...
├── wikihow/
│   ├── qa.json                          # QA pairs (300 questions)
│   ├── documents/                       # Raw documents (5,000 .txt files)
│   └── extracted/                       # Created by extraction step
│       └── wikihow/                     # collection_id defaults to dataset name
│           └── ...
└── pga/
    ├── pga_bio.json                     # QA pairs (biographical questions)
    ├── pga_stat.json                    # QA pairs (statistical questions)
    ├── documents/                       # Raw documents (507 .txt files)
    └── extracted/
        └── pga/                         # Pre-extracted chunks
            └── ...
```

## Running Benchmarks

### CUAD (Build → Query → Evaluate)

CUAD is a contract understanding dataset with 510 documents and 500 QA pairs. Pre-extracted data is used by default.

```bash
cd integration-tests
source .env

export STACK_PREFIX=cuad
sh build-tests.sh \
  --neptune-instance-type db.r8g.2xlarge \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --test "benchmark_build.CuadBenchmarkBuild benchmark_query.CuadBenchmarkQuery benchmark_evaluate.CuadBenchmarkEvaluate"
```

**Resource requirements:**
- Notebook: `ml.m5.xlarge` (16GB) — sufficient for 510 documents
- Neptune: `db.r8g.2xlarge`

### ConcurrentQA (Build → Query → Evaluate)

ConcurrentQA is a larger dataset with 13,501 documents and 400 QA pairs.

```bash
cd integration-tests
source .env

export STACK_PREFIX=cqa
sh build-tests.sh \
  --neptune-instance-type db.r8g.2xlarge \
  --notebook-instance-type ml.m5.4xlarge \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --test "benchmark_build.ConcurrentQaBenchmarkBuild benchmark_query.ConcurrentQaBenchmarkQuery benchmark_evaluate.ConcurrentQaBenchmarkEvaluate"
```

**Resource requirements:**
- Notebook: `ml.m5.4xlarge` (64GB) — required for 13,501 documents
- Neptune: `db.r8g.2xlarge`

### ConcurrentQA with Extraction

If you need to extract from raw documents first (e.g., first-time setup or re-extraction with a different model):

```bash
cd integration-tests
source .env

export STACK_PREFIX=cqa
sh build-tests.sh \
  --neptune-instance-type db.r8g.2xlarge \
  --notebook-instance-type ml.m5.4xlarge \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --test "benchmark_extract.ConcurrentQaBenchmarkExtract benchmark_build.ConcurrentQaBenchmarkBuild benchmark_query.ConcurrentQaBenchmarkQuery benchmark_evaluate.ConcurrentQaBenchmarkEvaluate"
```

Extraction uses Bedrock batch inference for large datasets. The following environment variables are set automatically by the stack:
- `BATCH_INFERENCE_ROLE` — IAM role for batch inference jobs
- `S3_RESULTS_BUCKET` / `S3_RESULTS_PREFIX` — S3 location for batch job I/O
- `AWS_REGION_NAME` — Region for Bedrock API calls

### WikiHow (Extract → Build → Query → Evaluate)

WikiHow is a how-to instructional dataset with 5,000 documents and 300 QA pairs. Since there is no pre-extracted data, the full pipeline including extraction is required.

```bash
cd integration-tests
source .env

export STACK_PREFIX=wiki
sh build-tests.sh \
  --neptune-instance-type db.r8g.2xlarge \
  --notebook-instance-type ml.m5.4xlarge \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --test "benchmark_extract.WikihowBenchmarkExtract benchmark_build.WikihowBenchmarkBuild benchmark_query.WikihowBenchmarkQuery benchmark_evaluate.WikihowBenchmarkEvaluate"
```

Or using the test file:

```bash
sh build-tests.sh \
  --neptune-instance-type db.r8g.2xlarge \
  --notebook-instance-type ml.m5.4xlarge \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --test-file benchmark.wikihow
```

**Resource requirements:**
- Notebook: `ml.m5.4xlarge` (64GB) — recommended for 5,000 documents with extraction
- Neptune: `db.r8g.2xlarge`

### PGA (Build → Query → Evaluate)

PGA is a sports statistics dataset with 507 documents and 400 QA pairs (split across biographical and statistical questions). Pre-extracted data is used by default.

```bash
cd integration-tests
source .env

export STACK_PREFIX=pga
sh build-tests.sh \
  --neptune-instance-type db.r8g.2xlarge \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --test "benchmark_build.PgaBenchmarkBuild benchmark_query.PgaBenchmarkQuery benchmark_evaluate.PgaBenchmarkEvaluate"
```

**Resource requirements:**
- Notebook: `ml.m5.xlarge` (16GB) — sufficient for 507 documents
- Neptune: `db.r8g.2xlarge`

## Running Multi-Retriever Comparison

After the initial build + traversal run completes for a dataset, you can run all other retrievers against the same graph without rebuilding. This is done directly on the SageMaker notebook.

### Step 1: Get the notebook URL

```bash
aws sagemaker create-presigned-notebook-instance-url \
  --notebook-instance-name aws-neptune-<stack-name> \
  --region us-west-2 \
  --query 'AuthorizedUrl' --output text
```

### Step 2: Run all retrievers on the notebook

Open a terminal on the notebook and run:

Replace `<Dataset>` with the appropriate class name:
- CUAD: `Cuad`
- ConcurrentQA: `ConcurrentQa`
- WikiHow: `Wikihow`
- PGA: `Pga`

```bash
source /home/ec2-user/anaconda3/bin/activate JupyterSystemEnv
cd /home/ec2-user/SageMaker/graphrag-toolkit

# Source environment files (created by build-tests.sh during stack deployment)
source .env.testing
source .env

# Loop through all retrievers
for RETRIEVER in topic_based entity_based chunk_based entity_network chunk_based_semantic semantic_guided topic-beam-chunk_only semantic-path_weighted; do
  export BENCHMARK_RETRIEVER=$RETRIEVER
  export TESTS="benchmark_query.<Dataset>BenchmarkQuery benchmark_evaluate.<Dataset>BenchmarkEvaluate"
  echo "=== Running $RETRIEVER ==="
  python test_suite.py
done
```

### Available Retrievers

| Retriever ID | Type | Description |
|---|---|---|
| `traversal` | Composite | ChunkBasedSearch + EntityNetworkSearch (default) |
| `topic_based` | Single sub-retriever | TopicBasedSearch only |
| `entity_based` | Single sub-retriever | EntityBasedSearch only |
| `chunk_based` | Single sub-retriever | ChunkBasedSearch only |
| `entity_network` | Single sub-retriever | EntityNetworkSearch only |
| `chunk_based_semantic` | Single sub-retriever | ChunkBasedSemanticSearch only |
| `semantic_guided` | Beam search | StatementCosine + KeywordRanking + SemanticBeamGraph |
| `topic-beam-chunk_only` | Contributor config | ChunkCosine + SemanticChunkBeamGraph |
| `semantic-path_weighted` | Contributor config | StatementCosine + RerankingBeamGraph |

### Step 3: Copy results to S3

After all retrievers complete, copy results from the notebook to S3:

```bash
# On the notebook (replace <dataset> with cuad, concurrentqa, wikihow, or pga)
aws s3 sync /home/ec2-user/SageMaker/graphrag-toolkit/benchmark-results/<dataset>/ \
  s3://<your-bucket>/<dataset>-benchmark-results/ --region us-west-2
```

### Step 4: Download results locally and generate comparison report

```bash
# Locally
aws s3 sync s3://<your-bucket>/cuad-benchmark-results/ benchmark-results/cuad/ --region us-west-2
aws s3 sync s3://<your-bucket>/concurrentqa-benchmark-results/ benchmark-results/concurrentqa/ --region us-west-2
aws s3 sync s3://<your-bucket>/wikihow-benchmark-results/ benchmark-results/wikihow/ --region us-west-2
aws s3 sync s3://<your-bucket>/pga-benchmark-results/ benchmark-results/pga/ --region us-west-2

# Generate comparison reports
cd integration-tests/test-scripts
python -c "
from benchmark.utils.comparison_report import generate_comparison_report
generate_comparison_report('cuad', '/path/to/benchmark-results')
generate_comparison_report('concurrentqa', '/path/to/benchmark-results')
generate_comparison_report('wikihow', '/path/to/benchmark-results')
generate_comparison_report('pga', '/path/to/benchmark-results')
"
```

Reports are written to `benchmark-results/<dataset>/comparison_report.json`.

### Prototype Runs (Quick Validation)

Use prototype mode to validate the pipeline with a small subset of data (2 documents, limited QA pairs):

```bash
# CUAD prototype
sh build-tests.sh \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --benchmark-prototype \
  --test-file benchmark.cuad.prototype

# ConcurrentQA prototype (includes extraction)
sh build-tests.sh \
  --benchmark-data-s3-uri s3://my-benchmarking-bucket/benchmark-data/ \
  --benchmark-prototype \
  --test-file benchmark.concurrentqa.prototype
```

## Monitoring

### Check stack status

```bash
aws cloudformation describe-stacks \
  --stack-name <stack-name>-tests \
  --region us-west-2 \
  --query 'Stacks[0].StackStatus' --output text
```

### On the SageMaker notebook

Connect via the SageMaker console or use the presigned URL:

```bash
aws sagemaker create-presigned-notebook-instance-url \
  --notebook-instance-name aws-neptune-<stack-name> \
  --region us-west-2 \
  --query 'AuthorizedUrl' --output text
```

Then in a terminal on the notebook:

```bash
# Check build progress
grep "Running build pipeline" /home/ec2-user/SageMaker/graphrag-toolkit/test-logs/00-*Build.log | tail -1

# Check for errors
grep -i "error\|exception" /home/ec2-user/SageMaker/graphrag-toolkit/test-logs/00-*Build.log | tail -10

# Check memory usage
free -h

# Check test results as they complete
ls /home/ec2-user/SageMaker/graphrag-toolkit/test-results/
```

### Check results in S3

```bash
aws s3 ls s3://<BUCKET>/graphrag-toolkit-tests/<stack-name>/results/test-runs/ \
  --recursive --region us-west-2
```

## Build Configuration

The build stage uses conservative settings to avoid OOM on large datasets:

| Parameter | Value | Notes |
|-----------|-------|-------|
| `build_num_workers` | 2 | Parallel workers for graph writes |
| `build_batch_size` | 10 | Documents per batch |
| `build_batch_write_size` | 25 | Graph write batch size |

These are set in `benchmark_build.py` and apply to all benchmark datasets. For smaller datasets (like CUAD), these are more conservative than necessary but ensure stability.

## Dataset Details

| Dataset | Documents | QA Pairs | Extraction Model | Notes |
|---------|-----------|----------|------------------|-------|
| CUAD | 510 | 500 | Claude Sonnet | Contract understanding, legal terminology |
| ConcurrentQA | 13,501 | 400 | Claude Sonnet | Multi-document QA, temporal reasoning |
| WikiHow | 5,000 | 300 | Claude Sonnet | How-to instructional, procedural steps |
| PGA | 507 | 400 | Claude Sonnet | Sports statistics, entity ambiguity |

## Evaluation Metrics

- **Correctness** — LLM-as-judge grades whether the response correctly answers the question given the ground-truth answer
- **IDK (I Don't Know)** — Classifies responses as "answerable" or "unanswerable" to measure how often the system declines to answer
- **Latency** — End-to-end time per query in milliseconds (retrieval + LLM response generation), reported as avg/p50/p95
- **Token Usage** — Total input and output tokens across all queries (proxy for cost)
- **Estimated Cost** — USD per query using model-specific pricing
- **Hop Classification** — Each question classified as single-hop, multi-hop, or unknown for breakdown analysis

Results are written to `benchmark-results/<dataset>/<retriever>/` on the notebook:
- `responses.jsonl` — Per-query responses with latency, tokens, and hop classification
- `metrics_summary.json` — Aggregate latency (avg/p50/p95), total tokens, estimated cost
- `correctness.json` — Aggregate correctness score
- `idk.json` — Aggregate IDK rate
- `correctness_evals.json` — Per-question correctness grades
- `idk_evals.json` — Per-question IDK classifications

After running all retrievers, generate `comparison_report.json` at `benchmark-results/<dataset>/` with cross-retriever rankings.

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| Empty `.FAIL.json` file | Process killed before writing results | Check `dmesg` for OOM kills; increase notebook instance type |
| `BulkIndexError` during build | OpenSearch Serverless rate limiting | Reduce `build_num_workers` to 2 and `build_batch_write_size` to 25 |
| VPC limit exceeded | Account VPC quota reached | Delete old stacks before creating new ones |
| Batch inference model error | Model not supported for batch | Use a non-legacy model ID (e.g., `us.anthropic.claude-sonnet-4-6`) |
| IAM permission denied for model | Model not in CFN IAM policy | Add the model ARN to `graphrag-toolkit-tests.json` |
| Notebook crashes on ConcurrentQA | Insufficient memory | Use `ml.m5.4xlarge` (64GB) or reduce `build_batch_size` |

## Adding a New Benchmark Dataset

1. Add a dataset entry to `DATASET_CONFIG` in `benchmark_build.py`:
   ```python
   'my-dataset': {
       'num_docs': 100,
       'extracted_dir': 'extracted',
       'collection_id': 'my-collection-id',  # optional, defaults to dataset name
   }
   ```

2. Add QA file mapping in `benchmark_query.py`:
   ```python
   QA_FILE_MAP = {
       ...
       'my-dataset': ['qa.json'],
   }
   ```

3. Upload data to S3 following the layout in [S3 Data Layout](#s3-data-layout).

4. Create test classes for Build, Query, and Evaluate (follow the CUAD/ConcurrentQA patterns).

5. Optionally create a test file (e.g., `benchmark.my-dataset`) listing the test classes.
