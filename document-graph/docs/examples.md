# Examples & Notebooks

All notebooks are in `examples/cloud/notebooks/` and run on SageMaker with Neptune + OpenSearch.

## Setup & Utilities

| Notebook | Purpose |
|----------|---------|
| `00-Setup` | Verify Neptune connection, write test node |
| `00-Cleanup` | Remove old test tenants from Neptune |

## Core Pipeline

| Notebook | Purpose | Docs |
|----------|---------|------|
| `001-Combined-Extract-Load-Build` | Full pipeline: CSV → ingest → transform → Neptune | [Pipeline](pipeline.md) |
| `002-Full-Pipeline-Test` | Every stage tested individually | [Pipeline](pipeline.md) |

## Component Tests

| Notebook | Feature | Docs |
|----------|---------|------|
| `03a-Ingestors-Test` | Column select, rename, row filter | [Ingestors](ingestors.md) |
| `03b-Schema-Driven-Pipeline` | Schema → automatic pipeline | [Schema Providers](schema-providers.md) |
| `03c-JSON-Driven-Pipeline` | JSON mapping file → Neptune | [Schema Providers](schema-providers.md) |
| `03d-Field-Transformers-Test` | JSON flatten, UUID, regex | [Transformers](transformers.md) |
| `03e-Normalizers-Test` | Whitespace, nulls, case | [Transformers](transformers.md) |
| `03f-Constructors-Test` | Cypher generation, batch | [Graph Build](graph-build.md) |
| `03g-Document-Transformers-Test` | JSON→rows, text chunker, PII | [Transformers](transformers.md) |

## Feature Deep-Dives

| Notebook | Feature | Docs |
|----------|---------|------|
| `004-Graph-Write-and-Read` | Node/Edge CRUD + typed queries | [Graph Build](graph-build.md), [Query Engine](query-engine.md) |
| `005-Schema-Providers-Integration-Validation` | CSV, JSON, S3, Static, Factory, validation | [Schema Providers](schema-providers.md) |
| `006-Transformer-Deep-Dive` | All 7 transformer categories + custom | [Transformers](transformers.md) |

## Hybrid Integration (Document-Graph + Lexical-Graph)

| Notebook | Purpose | Docs |
|----------|---------|------|
| `07a-Lexical-Integration-Data-Processing` | Structured data → Neptune → LlamaIndex Documents (with lineage) | [Lexical-Graph Integration](lexical-graph-integration.md) |
| `07b-Lexical-Integration-Lexical-Setup` | Index into lexical-graph → semantic query → correlate back | [Lexical-Graph Integration](lexical-graph-integration.md) |

## Real-World Demos

| Notebook | Purpose |
|----------|---------|
| `08a-Nelson-Mandela-Hybrid-Build` | Wikipedia + structured events → hybrid graph |
| `08b-Nelson-Mandela-Hybrid-Query` | Semantic search + document-graph correlation |

## Multi-Tenancy

| Notebook | Purpose | Docs |
|----------|---------|------|
| `09-Multi-Tenant-Coexistence-Demo` | Two tenants, same graph, complete isolation | [Multi-Tenancy](multi-tenancy.md) |

## Running Notebooks

### On SageMaker

```bash
# Sync from S3
aws s3 sync s3://graphrag-artifacts-705909755305/document-graph-notebooks/ ~/SageMaker/document-graph/ --delete

# Install
pip install --no-deps --force-reinstall ~/SageMaker/document-graph/wheels/document_graph-*.whl
```

### Prerequisites

- Neptune Database (1.4.x+) — for graph storage
- OpenSearch Serverless — for vector search (hybrid notebooks only)
- IAM role with: `neptune-db:*`, `aoss:APIAccessAll`, `bedrock:InvokeModel`
