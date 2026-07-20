# Graph Build

Generate Cypher MERGE statements from `Node` and `Edge` models and execute them against Neptune.

## Models

### Node

```python
from document_graph import Node

node = Node(
    id="person-001",
    labels=["Person", "Employee"],
    properties={"name": "Alice", "department": "Engineering"}
)
```

### Edge

```python
from document_graph import Edge

edge = Edge(
    id="edge-001",
    source_id="person-001",
    target_id="project-042",
    label="WORKS_ON",
    properties={"since": "2024-01-15"}
)
```

## node_to_cypher

Generates a MERGE statement for a single node:

```python
from document_graph.graph_build import node_to_cypher

cypher, params = node_to_cypher(node, tenant_id="acme")
# cypher: "MERGE (n:__Person__acme__:__Employee__acme__ {id: $id_val}) SET n += $props"
# params: {"id_val": "person-001", "props": {"name": "Alice", "department": "Engineering"}}

gs.execute_query(cypher, params)
```

## edge_to_cypher

Generates a MERGE statement for a single edge:

```python
from document_graph.graph_build import edge_to_cypher

cypher, params = edge_to_cypher(edge, tenant_id="acme")
# cypher: "MATCH (a {id: $src_id}), (b {id: $tgt_id}) MERGE (a)-[r:WORKS_ON]->(b) SET r += $props"

gs.execute_query(cypher, params)
```

## Batch Operations

Process multiple nodes/edges efficiently:

```python
from document_graph.graph_build import batch_nodes_to_cypher, batch_edges_to_cypher

nodes = [
    Node(id="p1", labels=["Person"], properties={"name": "Alice"}),
    Node(id="p2", labels=["Person"], properties={"name": "Bob"}),
]

statements = batch_nodes_to_cypher(nodes, tenant_id="acme")
for cypher, params in statements:
    gs.execute_query(cypher, params)

# Same pattern for edges
edge_statements = batch_edges_to_cypher(edges, tenant_id="acme")
for cypher, params in edge_statements:
    gs.execute_query(cypher, params)
```

## Tenant ID Scoping

When `tenant_id` is provided, all labels are encoded as `__Label__tenant_id__`:

```python
node = Node(id="x", labels=["Document"])

# Without tenant
cypher, _ = node_to_cypher(node)
# → MERGE (n:Document {id: $id_val}) SET n += $props

# With tenant
cypher, _ = node_to_cypher(node, tenant_id="acme")
# → MERGE (n:__Document__acme__ {id: $id_val}) SET n += $props
```

This ensures complete data isolation between tenants at the label level. See {doc}`multi-tenancy` for details.
