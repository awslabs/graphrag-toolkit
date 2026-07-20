# Multi-Tenancy

Document-graph provides tenant isolation through label encoding in Neptune.

## How It Works

All node labels are encoded with the tenant ID using the pattern:

```
__Type__tenant_id__
```

For example, a `Document` node for tenant `acme`:

```
__Document__acme__
```

A node with multiple labels:

```python
Node(id="x", labels=["Document", "Report"])
# With tenant_id="acme" becomes labels: ["__Document__acme__", "__Report__acme__"]
```

## Isolation Guarantees

- **Label-level isolation**: Each tenant's nodes use unique labels, making it impossible to accidentally query across tenants.
- **No shared labels**: Tenant A's `__Document__tenantA__` never overlaps with Tenant B's `__Document__tenantB__`.
- **Query scoping**: `DocumentGraphQueryEngine(gs, tenant_id="acme")` automatically scopes all `get_nodes` and `find_by_property` calls.

```python
from document_graph.query import DocumentGraphQueryEngine

# These two engines see completely different data
engine_a = DocumentGraphQueryEngine(gs, tenant_id="tenant_a")
engine_b = DocumentGraphQueryEngine(gs, tenant_id="tenant_b")

docs_a = engine_a.get_nodes("Document")  # → MATCH (n:__Document__tenant_a__)
docs_b = engine_b.get_nodes("Document")  # → MATCH (n:__Document__tenant_b__)
```

## Admin Cross-Tenant View

Omit `tenant_id` to query raw labels (useful for admin/debugging):

```python
admin_engine = DocumentGraphQueryEngine(gs)

# Raw Cypher to find all tenant-scoped Document nodes
all_docs = admin_engine.query(
    "MATCH (n) WHERE any(l IN labels(n) WHERE l STARTS WITH '__Document__') RETURN n LIMIT 100"
)
```

## Tenant Cleanup

Remove all data for a specific tenant:

```python
tenant_id = "old_tenant"

# Delete all nodes with tenant-scoped labels
gs.execute_query(
    "MATCH (n) WHERE any(l IN labels(n) WHERE l CONTAINS $tid) DETACH DELETE n",
    {"tid": f"__{tenant_id}__"}
)
```

:::{warning}
Tenant cleanup is irreversible. Always verify the tenant ID before executing.
:::
