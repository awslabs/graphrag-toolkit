---
inclusion: manual
---
# Test Hybrid-Dev Notebooks (lexical-graph-hybrid-dev)

## When to Use
When asked to test, validate, or run the lexical-graph-hybrid-dev notebooks.

## How to Run
```bash
cd examples/lexical-graph-hybrid-dev
bash scripts/test-hybrid-dev-notebooks.sh
```

## Configuration
Environment variables to customize behavior:
- `SKIP_CUDA=true|false` (default: true) — skip GPU/CUDA cells
- `SKIP_BATCH=true|false` (default: true) — skip batch inference cells requiring PDF source files
- `CLEANUP=true|false` (default: true) — cleanup all resources after test
- `DOCKER_MODE=standard|dev` (default: standard)
- `REPORT_DIR=path` (default: examples/lexical-graph-hybrid-dev/test-reports/)

## Prerequisites
- AWS CLI configured with valid credentials
- Docker running
- Bedrock model access enabled (Claude Sonnet, Cohere Embed English v3)

## What It Does
1. Detects platform (ARM/x86)
2. Creates .env from template with auto-detected AWS account/region
3. Creates AWS resources (S3 bucket, DynamoDB table, IAM role, Bedrock prompts)
4. Starts Docker containers (Neo4j, pgvector, Jupyter)
5. Executes all notebook cells (skipping CUDA and batch as configured)
6. Generates per-cell execution report (JSON + markdown)
7. Cleans up all resources (Docker, AWS, local)

## Notebooks Tested
- 00-Setup.ipynb — Environment setup, package installation, reader dependencies
- 01-Local-Extract-Batch.ipynb — Local and S3 extraction
- 02-Cloud-Setup.ipynb — Cloud dependency installation
- 03-Cloud-Build.ipynb — Graph building from extracted data
- 04-Cloud-Querying.ipynb — All query types (traversal, semantic, reranking, post-processors)

## Expected Results
- 78+ cells SUCCESS, 2 SKIPPED (CUDA + batch), 0 FAILED
- Reports in test-reports/ directory
