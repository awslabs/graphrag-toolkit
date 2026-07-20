"""Cypher Builder — generates openCypher MERGE statements from Node/Edge models.

Uses graphrag-toolkit's GraphStore.query() for execution.
"""

from typing import Optional
from ..model_elements import Node, Edge


def node_to_cypher(node: Node, tenant_id: Optional[str] = None) -> tuple[str, dict]:
    """Generate MERGE statement for a typed node."""
    labels = node.labels or ["Node"]
    if tenant_id:
        labels = [f"__{l}__{tenant_id}__" for l in labels]
    label_str = ":".join(labels)
    props = node.properties or {}
    id_val = node.id

    query = f"MERGE (n:{label_str} {{id: $id_val}}) SET n += $props"
    params = {"id_val": id_val, "props": props}
    return query, params


def edge_to_cypher(edge: Edge, tenant_id: Optional[str] = None) -> tuple[str, dict]:
    """Generate MERGE statement for a typed edge."""
    rel_type = edge.label

    query = (
        f"MATCH (a {{id: $src_id}}), (b {{id: $tgt_id}}) "
        f"MERGE (a)-[r:{rel_type}]->(b) SET r += $props"
    )
    params = {
        "src_id": edge.source_id,
        "tgt_id": edge.target_id,
        "props": edge.properties or {},
    }
    return query, params


def batch_nodes_to_cypher(nodes: list[Node], tenant_id: Optional[str] = None) -> list[tuple[str, dict]]:
    """Generate MERGE statements for a batch of nodes."""
    return [node_to_cypher(n, tenant_id) for n in nodes]


def batch_edges_to_cypher(edges: list[Edge], tenant_id: Optional[str] = None) -> list[tuple[str, dict]]:
    """Generate MERGE statements for a batch of edges."""
    return [edge_to_cypher(e, tenant_id) for e in edges]
