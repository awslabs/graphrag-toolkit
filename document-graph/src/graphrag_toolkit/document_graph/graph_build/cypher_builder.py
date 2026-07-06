"""Cypher Builder — generates openCypher MERGE statements from Node/Edge models.

Uses graphrag-toolkit's GraphStore.query() for execution.
"""

from typing import Optional, Union
from ..model_elements import Node, Edge

try:
    from graphrag_toolkit.lexical_graph import TenantId, to_tenant_id
    HAS_LEXICAL = True
except ImportError:
    HAS_LEXICAL = False


def _format_label(label: str, tenant_id: Optional[str] = None) -> str:
    """Format a label using TenantId.format_label() for consistency with lexical-graph.
    
    Produces backtick-quoted labels: `Label` (default tenant) or `Label{tenant}__` (scoped).
    Falls back to manual formatting if lexical-graph is not installed.
    """
    if not tenant_id:
        return f'`{label}`'
    if HAS_LEXICAL:
        tid = to_tenant_id(tenant_id)
        return tid.format_label(label)
    # Fallback when lexical-graph not installed — same format
    return f'`{label}{tenant_id}__`'


def node_to_cypher(node: Node, tenant_id: Optional[str] = None) -> tuple[str, dict]:
    """Generate MERGE statement for a typed node."""
    labels = node.labels or ["Node"]
    labels = [_format_label(l, tenant_id) for l in labels]
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
