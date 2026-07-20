# Lexical-Graph Integration

Document-graph and lexical-graph coexist in the same Neptune database, enabling hybrid queries that combine structured data retrieval with semantic search.

## How It Works

```
┌─────────────────────────────────────────────────────────┐
│ Neptune Database                                         │
│                                                         │
│  Document-Graph (structured)    Lexical-Graph (semantic)│
│  __User__tenant__               __Source__              │
│  __Account__tenant__            __Chunk__               │
│  __Event__tenant__              __Topic__               │
│                                 __Statement__           │
│                                 __Entity__              │
│                                                         │
│  Connected by: shared Neptune + entity correlation      │
└─────────────────────────────────────────────────────────┘
         ↕                              ↕
    Cypher queries              Semantic vector search
    (exact match)               (OpenSearch Serverless)
```

## Setup

```python
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, VectorStoreFactory
from graphrag_toolkit.lexical_graph import LexicalGraphIndex, LexicalGraphQueryEngine

GRAPH_STORE = 'neptune-db://your-endpoint:8182'
VECTOR_STORE = 'aoss://your-collection.us-east-1.aoss.amazonaws.com'

graph_store = GraphStoreFactory.for_graph_store(GRAPH_STORE)
vector_store = VectorStoreFactory.for_vector_store(VECTOR_STORE)
```

## Step 1: Write structured data to Neptune

Use document-graph to write typed nodes:

```python
from document_graph.graph_build import node_to_cypher
from document_graph import Node

TENANT = 'my_app'
node = Node(id='u1', labels=['User'], properties={'name': 'Alice', 'role': 'admin'})
cypher, params = node_to_cypher(node, tenant_id=TENANT)
gs.execute_query(cypher, params)
```

## Step 2: Convert to LlamaIndex Documents

The key is embedding **lineage** in the document text so it survives the lexical-graph chunking pipeline:

```python
from llama_index.core.schema import Document

docs = []
for record in records:
    node_id = record['id']
    # Lineage header survives chunking
    text = f'[User | {node_id} | {TENANT}]\n'
    text += '\n'.join(f'{k}: {v}' for k, v in record.items() if v)

    doc = Document(
        text=text,
        metadata={
            'source': {
                'sourceId': f'User:{node_id}:{TENANT}',
                'metadata': {'node_type': 'User', 'node_id': node_id, 'tenant': TENANT}
            }
        }
    )
    docs.append(doc)
```

:::{note}
The `source.sourceId` and `source.metadata` structure is how graphrag-toolkit persists metadata to `__Source__` nodes in Neptune.
:::

## Step 3: Index into lexical-graph

```python
graph_index = LexicalGraphIndex(graph_store, vector_store)
graph_index.extract_and_build(docs, show_progress=True)
```

This creates the lexical graph (topics, statements, entities) alongside your document-graph nodes.

## Step 4: Semantic query

```python
query_engine = LexicalGraphQueryEngine.for_traversal_based_search(graph_store, vector_store)
results = query_engine.retrieve('Who are the admin users?')
```

## Step 5: Correlate back to document-graph

Extract lineage from query results and look up original nodes:

```python
import re

for r in results:
    text = r.node.text
    match = re.search(r'\[(\w+) \| ([\w-]+) \| (\w+)\]', text)
    if match:
        ntype, node_id, tenant = match.groups()
        label = f'__{ntype}__{tenant}__'
        original = gs.execute_query(
            f"MATCH (n:`{label}`) WHERE n.id = '{node_id}' RETURN properties(n) as props"
        )
        # original contains the full structured node data
```

## Lineage Strategies

| Strategy | Reliability | How |
|----------|------------|-----|
| **Text header** | High | `[Type \| node_id \| tenant]` in first line — survives chunking |
| **Source metadata** | Medium | `source.metadata` → stored on `__Source__` node |
| **Entity correlation** | Fallback | Match extracted entities (Person, Org) to Neptune nodes |

## Key Points

1. **Same database** — document-graph and lexical-graph share Neptune
2. **Different labels** — no collision (`__User__tenant__` vs `__Topic__`)
3. **Tenant isolation** — both respect tenant-scoped labels
4. **Lineage** — embed identifiers in text to survive chunking
5. **Entity bridge** — lexical-graph extracts entities that match document-graph nodes
