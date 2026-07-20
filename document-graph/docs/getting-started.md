# Getting Started

## Prerequisites

### Infrastructure

This project relies on [graphrag-toolkit](https://github.com/awslabs/graphrag-toolkit) for infrastructure provisioning. Deploy the CloudFormation stack from:

> https://github.com/awslabs/graphrag-toolkit/tree/main/examples/lexical-graph/cloudformation-templates

This provisions:
- **Amazon Neptune** (graph database)
- **Amazon OpenSearch Serverless** (vector store for hybrid search)
- **Amazon SageMaker Notebook** (for running the example notebooks)

### SageMaker Notebooks

All example notebooks in `examples/cloud/notebooks/` are designed to run on **SageMaker** within the VPC provisioned by the CloudFormation stack. They require:
- Network access to Neptune (port 8182)
- Network access to OpenSearch Serverless (port 443)
- IAM role with `neptune-db:*`, `aoss:APIAccessAll`, `bedrock:InvokeModel`

:::{note}
The notebooks will NOT work on a local machine — they require VPC-internal access to Neptune and OpenSearch.
:::

Get a working document graph in 5 minutes.

## Install

```bash
pip install document-graph
```

## Connect to Neptune

```python
from graphrag_toolkit.storage import GraphStoreFactory

gs = GraphStoreFactory.for_graph_store('neptune-db://your-endpoint:8182').__enter__()
```

## Write Your First Node

```python
from document_graph import Node
from document_graph.graph_build import node_to_cypher

node = Node(
    id="doc-001",
    labels=["Document"],
    properties={"title": "Getting Started", "status": "active"}
)

cypher, params = node_to_cypher(node, tenant_id="acme")
gs.execute_query(cypher, params)
```

## Write an Edge

```python
from document_graph import Edge
from document_graph.graph_build import edge_to_cypher

edge = Edge(
    id="edge-001",
    source_id="doc-001",
    target_id="doc-002",
    label="REFERENCES",
    properties={"weight": 0.9}
)

cypher, params = edge_to_cypher(edge, tenant_id="acme")
gs.execute_query(cypher, params)
```

## Query

```python
from document_graph.query import DocumentGraphQueryEngine

engine = DocumentGraphQueryEngine(gs, tenant_id="acme")

# Get all Document nodes
docs = engine.get_nodes("Document", limit=10)

# Find by property
results = engine.find_by_property("Document", "title", "Getting Started")

# Raw Cypher
results = engine.query("MATCH (n) RETURN n LIMIT 5")
```

## Next Steps

- {doc}`schema-providers` — Auto-discover schemas from data files
- {doc}`transformers` — Clean and enrich data before graph build
- {doc}`graph-build` — Batch operations and tenant scoping
- {doc}`multi-tenancy` — Isolation guarantees
