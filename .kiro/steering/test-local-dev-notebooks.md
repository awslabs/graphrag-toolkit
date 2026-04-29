---
inclusion: manual
---
# Test Local-Dev Notebooks

## When to Use
When asked to test, validate, or run the lexical-graph-local-dev notebooks.

## How to Run
```bash
cd examples/lexical-graph-local-dev
bash scripts/test-local-dev-notebooks.sh
```

## Configuration
Environment variables to customize behavior:
- `SKIP_GITHUB=true|false` (default: true) — skip GitHub reader cells (no token available)
- `SKIP_PPTX=true|false` (default: true) — skip PPTX reader cells (600s timeout)
- `SKIP_LONG_RUNNING=true|false` (default: true) — skip JSON/Wikipedia reader cells (extract_and_build timeout)
- `CLEANUP=true|false` (default: true) — cleanup all resources after test
- `DOCKER_MODE=standard|dev` (default: standard)
- `REPORT_DIR=path` (default: examples/lexical-graph-local-dev/test-reports/)

## Prerequisites
- AWS CLI configured with valid credentials
- Docker running
- Bedrock model access enabled (Claude Sonnet, Cohere Embed English v3)

## What It Does
1. Detects platform (ARM/x86)
2. Creates .env from template with auto-detected AWS account/region
3. Creates AWS resources (S3 bucket, Bedrock managed prompts)
4. Starts Docker containers (Neo4j, pgvector, Jupyter)
5. Executes all notebook cells (skipping GitHub and PPTX as configured)
6. Generates per-cell execution report (JSON + markdown)
7. Cleans up all resources (Docker, S3, Bedrock prompts, local .env)

## Notebooks Tested
- 00-Setup.ipynb — Environment setup, package installation, reader dependencies
- 01-Combined-Extract-and-Build.ipynb — Reader providers (web, PDF, YouTube, docx, markdown, JSON, Wikipedia, CSV, directory)
- 02-Querying.ipynb — TraversalBasedRetriever queries
- 03-Querying-with-Prompting.ipynb — Custom prompts (file, S3, Bedrock managed)
- 04-Advanced-Configuration-Examples.ipynb — Batch processing, custom metadata functions
- 05-S3-Directory-Reader-Provider.ipynb — S3 directory reader with prefix filtering and metadata

## Expected Results
- All executed cells SUCCESS, 0 FAILED
- Skipped cells depend on configuration (GitHub, PPTX, long-running by default)
- Reports in test-reports/ directory (execution_report.json + execution_report.md)
