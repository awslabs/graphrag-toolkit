# Document Graph

> Structured data ETL for graph-enhanced GenAI applications. Extends [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit) with typed node support, schema providers, and hybrid search.

## What it does

| Input | Process | Output |
|-------|---------|--------|
| CSV, Excel, JSON, Parquet, XML | Deterministic ETL (no LLM) | Typed Neptune nodes + edges |

**Hybrid with Lexical Graph:** Same Neptune cluster holds both structured (document-graph) and unstructured (lexical-graph) data. Query both with a single semantic search.

## Features

| Feature | Module | Description |
|---------|--------|-------------|
| **Schema Providers** | `schema.providers` | CSV, JSON, S3, Static, File, Glue — auto-discover or define schemas |
| **Schema Discovery** | `schema.discovery` | Infer ETL schema from data files (CSV, Excel, JSON, Parquet, XML, YAML) |
| **Transformers** | `transform` | Normalizers, field, document, filter, graph, truncators (20+ built-in) |
| **Graph Build** | `graph_build` | Cypher generation (node_to_cypher, edge_to_cypher) with tenant scoping |
| **Query Engine** | `query` | Typed node queries with tenant isolation |
| **Multi-Tenancy** | Labels | `__Type__tenant_id__` — complete data isolation per tenant |
| **Hybrid Search** | Integration | Lexical-graph indexes document-graph content for semantic retrieval |

## Architecture

```
graphrag-toolkit (PyPI: graphrag-lexical-graph)
├── GraphStore, VectorStore, Factories
├── LexicalGraphIndex (unstructured → lexical graph)
└── LexicalGraphQueryEngine (semantic search)

document-graph (this package)
├── schema/           → ETL schema model, providers (CSV/JSON/S3/Static), discovery
├── transform/        → 20+ transformers (normalizers, field, document, filter, graph, truncators)
├── graph_build/      → Cypher generation with tenant-scoped labels
├── query/            → DocumentGraphQueryEngine (typed node queries)
├── pipeline/         → Extract providers (CSV, Excel, JSON, Parquet)
├── ingest/           → Column selectors, row filters, renamers
└── storage/          → ReadOnlyGraphStore, factory extensions
```

## Infrastructure

This project relies on [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit) for AWS infrastructure. Deploy the CloudFormation stack:

> https://github.com/awslabs/graphrag-toolkit/tree/main/examples/lexical-graph/cloudformation-templates

This provisions Neptune, OpenSearch Serverless, and a SageMaker notebook instance. All example notebooks (`examples/cloud/notebooks/`) must run on **SageMaker** within the provisioned VPC.

## Install

```bash
pip install document-graph                    # core (no heavy deps)
pip install document-graph[graphrag]          # with lexical-graph integration
pip install graphrag-lexical-graph            # for hybrid search
```

## Quick Start

### 1. Write nodes to Neptune

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from document_graph.graph_build import node_to_cypher
from document_graph import Node

gs = GraphStoreFactory.for_graph_store('neptune-db://endpoint:8182').__enter__()

node = Node(id='u1', labels=['User'], properties={'name': 'Alice', 'role': 'admin'})
cypher, params = node_to_cypher(node, tenant_id='my_app')
gs.execute_query(cypher, params)
```

### 2. Query nodes

```python
from document_graph.query import DocumentGraphQueryEngine

engine = DocumentGraphQueryEngine(gs, tenant_id='my_app')
users = engine.get_nodes('User')
admins = engine.find_by_property('User', 'role', 'admin')
```

### 3. Schema-driven pipeline

```python
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.providers.csv_schema_provider import CSVSchemaProvider

config = SchemaProviderConfig(type='csv', connection_config={'path': 'data/users.csv'})
provider = CSVSchemaProvider(config)
schema = provider.load_schema()  # Returns ETLSchema with discovered fields
```

### 4. Transform data

```python
from document_graph.transform.transformer_provider_config import TransformerProviderConfig
from document_graph.transform.normalizers.normalize_whitespace_provider import NormalizeWhitespaceProvider
from document_graph.transform.graph_transformers.row_to_node import RowToNodeTransformer

# Normalize
records = NormalizeWhitespaceProvider(TransformerProviderConfig(name='ws', args={})).transform(records)

# Convert to nodes
nodes = RowToNodeTransformer(TransformerProviderConfig(name='r2n', args={'type': 'User'})).transform(records)
```

### 5. Hybrid search (document-graph + lexical-graph)

```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex, LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from llama_index.core.schema import Document

# Index document-graph data into lexical-graph
docs = [Document(text=f'[User | {r["id"]} | my_app]\nname: {r["name"]}', metadata={...}) for r in records]
vector_store = VectorStoreFactory.for_vector_store('aoss://endpoint')
graph_index = LexicalGraphIndex(graph_store, vector_store)
graph_index.extract_and_build(docs, show_progress=True)

# Semantic query
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(graph_store, vector_store)
results = query_engine.retrieve('Who are the admin users?')
```

## Lexical-Graph Integration

Document-graph and lexical-graph coexist in the same Neptune database:

| | Lexical Graph | Document Graph |
|--|---------------|----------------|
| **Input** | Unstructured text (PDF, web) | Structured data (CSV, Excel, JSON) |
| **Extraction** | LLM-based (Claude) | Deterministic ETL (pandas) |
| **Graph Model** | Source → Chunk → Topic → Statement → Entity | Row → Typed Node + Edges |
| **Query** | Traversal-based semantic search | Cypher over typed nodes |
| **Labels** | `__Source__`, `__Chunk__`, `__Topic__` | `__User__tenant__`, `__Account__tenant__` |
| **Coexistence** | Same Neptune, different labels | Complete isolation |

### Lineage

When indexing document-graph data into lexical-graph, embed lineage in the text:

```python
text = f'[{node_type} | {node_id} | {tenant}]\n...'
```

This survives the lexical-graph chunking pipeline and enables correlation back to the original nodes.

## Multi-Tenancy

All operations use tenant-scoped labels: `__Type__tenant_id__`

```python
# Write to tenant A
node_to_cypher(node, tenant_id='acme_corp')  # → MERGE (n:`__User__acme_corp__` ...)

# Query tenant A only
engine = DocumentGraphQueryEngine(gs, tenant_id='acme_corp')
engine.get_nodes('User')  # Only sees acme_corp's users
```

## Schema Providers

| Provider | Source | Usage |
|----------|--------|-------|
| `CSVSchemaProvider` | CSV file | Auto-discover schema |
| `JSONSchemaProvider` | JSON file | Load pre-defined schema |
| `S3SchemaProvider` | S3 bucket | Shared schemas across environments |
| `StaticSchemaProvider` | Code | Programmatic definition |
| `SchemaProviderFactory` | Config dict | Unified creation |

## Transformers

| Category | Examples | Purpose |
|----------|----------|---------|
| Normalizers | whitespace, nulls, case, enum, timestamp | Clean & standardize |
| Field | json_flattener, uuid_gen, regex_clean | Reshape fields |
| Document | json_to_rows, text_chunker, pii_redactor | Split/transform docs |
| Filter | row_filter, column_pruner | Remove unwanted data |
| Graph | row_to_node, infer_edges | Create graph structures |
| Truncators | length, field_count, token | Limit data size |

All follow: `TransformerProvider(config).transform(records) → records`

## Testing

```bash
pip install pytest
pytest tests/ -q  # 59 tests
```

## Requirements

- Python >= 3.10
- `pydantic >= 2.0`
- `pandas >= 2.0`
- `boto3 >= 1.26`
- Optional: `graphrag-lexical-graph >= 3.18.0` (for hybrid search)

## License

Copyright © 2024-2026 Evan Erwee. All rights reserved.
