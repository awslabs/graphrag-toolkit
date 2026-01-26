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

To use the multi-tenancy feature, you must supply a tenant id when creating a `LexicalGraphIndex` or `LexicalGraphQueryEngine`. A tenant id is a string containing 1-10 lower case characters and numbers. If you don't supply a tenant id, the index and query engine will use the _default tenant_ (i.e. a tenant id value of `None`).

### Indexing and multi-tenancy

The following example creates a `LexicalGraphIndex` for tenant 'user123':

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

The `LexicalGraphIndex` always uses the _default tenant_ for the [extract stage](https://github.com/awslabs/graphrag-toolkit/blob/main/docs/indexing.md#extract), even if you supply a different tenant id. The [build stage](https://github.com/awslabs/graphrag-toolkit/blob/main/docs/indexing.md#build), however, will use the tenant id. The reason for this is so that you can extract once, and then build many times, potentially for different tenants.

### Querying and multi-tenancy

The following example creates a `LexicalGraphQueryEngine` for tenant 'user123':

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

If a lexical graph does not exist for the specified tenant id, the underlying retrievers will return an empty set of results.

### Implementation details

Multi-tenancy works by using tenant-specific node labels for nodes in the graph, and tenant-specific indexes in the vector store. For example, chunk nodes in a graph belonging to tenant 'user123' will be labelled `__Chunk__user123__`, while the chunk vector index will be named `chunk_user123`.
