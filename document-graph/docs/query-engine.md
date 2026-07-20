# Query Engine

`DocumentGraphQueryEngine` provides typed queries over the document graph with automatic tenant isolation.

## Setup

```python
from graphrag_toolkit.storage import GraphStoreFactory
from document_graph.query import DocumentGraphQueryEngine

gs = GraphStoreFactory.for_graph_store('neptune-db://your-endpoint:8182').__enter__()
engine = DocumentGraphQueryEngine(gs, tenant_id="acme")
```

## get_nodes

Retrieve all nodes of a given type:

```python
docs = engine.get_nodes("Document", limit=50)
# Internally queries: MATCH (n:__Document__acme__) RETURN n LIMIT 50
```

## find_by_property

Find nodes matching a property value:

```python
results = engine.find_by_property("Person", "name", "Alice")
# Internally queries: MATCH (n:__Person__acme__ {name: $val}) RETURN n
```

## get_relationships

Traverse typed relationships:

```python
results = engine.get_relationships("Person", "WORKS_ON", "Project", limit=25)
# Internally queries: MATCH (a:__Person__acme__)-[r:WORKS_ON]->(b:__Project__acme__) RETURN a, r, b LIMIT 25
```

## query (Raw Cypher)

Execute arbitrary Cypher when you need full control:

```python
results = engine.query(
    "MATCH (n:__Document__acme__)-[:REFERENCES]->(m) RETURN n.title, m.id LIMIT 10"
)

# With parameters
results = engine.query(
    "MATCH (n:__Person__acme__) WHERE n.age > $min_age RETURN n",
    params={"min_age": 30}
)
```

## Tenant Isolation

The engine automatically scopes all typed queries to the configured `tenant_id`:

```python
# Tenant-scoped — only sees "acme" data
acme_engine = DocumentGraphQueryEngine(gs, tenant_id="acme")
acme_docs = acme_engine.get_nodes("Document")

# Different tenant — completely isolated
beta_engine = DocumentGraphQueryEngine(gs, tenant_id="beta")
beta_docs = beta_engine.get_nodes("Document")

# No tenant — queries raw labels (admin/cross-tenant)
admin_engine = DocumentGraphQueryEngine(gs)
all_docs = admin_engine.get_nodes("Document")  # Matches label "Document" directly
```

See {doc}`multi-tenancy` for isolation guarantees and cleanup.
