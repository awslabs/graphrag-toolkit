"""Document Graph Query Engine — Cypher queries over typed nodes.

Uses graphrag-toolkit's GraphStore for execution.
"""

from typing import Optional


class DocumentGraphQueryEngine:
    """Query typed document graph nodes via GraphStore."""

    def __init__(self, graph_store, tenant_id: Optional[str] = None):
        """Initialize with a graphrag-toolkit GraphStore.

        Args:
            graph_store: GraphStore from GraphStoreFactory.for_graph_store()
            tenant_id: Optional tenant scope for queries
        """
        self._store = graph_store
        self._tenant_id = tenant_id

    def query(self, cypher: str, params: dict = None) -> list[dict]:
        """Execute a Cypher query against the typed document graph."""
        return self._store.execute_query(cypher, params or {})

    def get_nodes(self, label: str, limit: int = 100) -> list[dict]:
        """Get all nodes of a given type."""
        scoped_label = f"__{label}__{self._tenant_id}__" if self._tenant_id else label
        return self.query(f"MATCH (n:{scoped_label}) RETURN n LIMIT $limit", {"limit": limit})

    def get_relationships(self, source_label: str, rel_type: str, target_label: str, limit: int = 100) -> list[dict]:
        """Get relationships between typed nodes."""
        src = f"__{source_label}__{self._tenant_id}__" if self._tenant_id else source_label
        tgt = f"__{target_label}__{self._tenant_id}__" if self._tenant_id else target_label
        return self.query(
            f"MATCH (a:{src})-[r:{rel_type}]->(b:{tgt}) RETURN a, r, b LIMIT $limit",
            {"limit": limit},
        )

    def find_by_property(self, label: str, key: str, value) -> list[dict]:
        """Find nodes by property value."""
        scoped_label = f"__{label}__{self._tenant_id}__" if self._tenant_id else label
        return self.query(f"MATCH (n:{scoped_label} {{{key}: $val}}) RETURN n", {"val": value})
