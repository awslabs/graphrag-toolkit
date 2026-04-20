[[Home](./)]

## Multi-Tenancy

### Topics

- [Overview](#overview)
- [Tenant Id](#tenant-id)
- [Indexing and multi-tenancy](#indexing-and-multi-tenancy)
- [Querying and multi-tenancy](#querying-and-multi-tenancy)
- [Implementation details](#implementation-details)

### Overview

Multi-tenancy allows you to host multiple separate lexical graphs in the same underlying graph and vector stores.

### Tenant Id

To use the multi-tenancy feature, supply a tenant id when creating a `LexicalGraphIndex` or `LexicalGraphQueryEngine`. A tenant id is a string of 1–25 lowercase letters, numbers, and periods (periods cannot appear at the start or end). If you don't supply a tenant id, the index and query engine use the _default tenant_ (a tenant id value of `None`).

See [`tenant_id.py`](../../lexical-graph/src/graphrag_toolkit/lexical_graph/tenant_id.py) for the validation logic.

### Indexing and multi-tenancy

The following example creates a `LexicalGraphIndex` for tenant `user123`:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphIndex

graph_store = ...
vector_store = ...

graph_index = LexicalGraphIndex(
    graph_store,
    vector_store,
    tenant_id='user123'
)
```

**Important:** the extract stage always writes under the _default_ tenant, regardless of the tenant id you set. This is intentional — it lets you extract once and build for multiple tenants from the same extracted output. Only the build stage applies the tenant id. A warning is logged when a non-default tenant id is set ([`lexical_graph_index.py:445`](../../lexical-graph/src/graphrag_toolkit/lexical_graph/lexical_graph_index.py#L445)).

### Querying and multi-tenancy

The following example creates a `LexicalGraphQueryEngine` for tenant `user123`:

```python
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine

graph_store = ...
vector_store = ...

query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
    graph_store,
    vector_store,
    tenant_id='user123'
)
```

If a lexical graph does not exist for the specified tenant id, the retrievers return an empty result set.

### Implementation details

Multi-tenancy works by using tenant-specific node labels and index names. For example, chunk nodes for tenant `user123` are labelled `__Chunk__user123__`, and the chunk vector index is named `chunk_user123`.
