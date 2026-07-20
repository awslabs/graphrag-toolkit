# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Graph Loader — loads CPG nodes and edges from artifact data into Neptune.

Converts raw artifact dicts (from nodes.jsonl / edges.jsonl) into typed CPG
models and writes them in batches to a graph store, applying tenant isolation.
"""

import logging
from typing import Any, Protocol, runtime_checkable

from .models import CPGNode, CPGEdge

logger = logging.getLogger(__name__)


@runtime_checkable
class GraphStoreProtocol(Protocol):
    """Protocol for graph store backends (e.g., Neptune)."""

    async def write_nodes(self, nodes: list[dict]) -> int:
        """Write a batch of node dicts to the graph store.

        Each dict has keys: 'id', 'label', 'properties'.

        Returns:
            Number of nodes written.
        """
        ...

    async def write_edges(self, edges: list[dict]) -> int:
        """Write a batch of edge dicts to the graph store.

        Each dict has keys: 'source_id', 'target_id', 'edge_type', 'properties'.

        Returns:
            Number of edges written.
        """
        ...


class GraphLoader:
    """Loads CPG nodes and edges from artifact data into a graph store.

    Converts raw artifact dicts into CPGNode/CPGEdge models, applies tenant
    isolation via tenant_id, and writes in configurable batches.

    Usage:
        loader = GraphLoader(
            graph_store=neptune_store,
            tenant_id="amigo-core__abc123",
            batch_size=200,
        )
        result = await loader.load(nodes_data, edges_data)
        # result = {"nodes_written": 1500, "edges_written": 3200}
    """

    def __init__(
        self,
        graph_store: GraphStoreProtocol,
        tenant_id: str,
        batch_size: int = 100,
        apm_id: str = "",
        app_name: str = "",
    ) -> None:
        """Initialize the GraphLoader.

        Args:
            graph_store: Backend implementing GraphStoreProtocol.
            tenant_id: Tenant identifier applied to all nodes/edges for isolation.
            batch_size: Number of nodes/edges per write batch. Default 100.
            apm_id: APM ID from client's application registry (primary scoping key).
            app_name: Application/repo name.
        """
        self._graph_store = graph_store
        self._tenant_id = tenant_id
        self._batch_size = batch_size
        self._apm_id = apm_id
        self._app_name = app_name

    async def load(
        self,
        nodes_data: list[dict],
        edges_data: list[dict],
    ) -> dict[str, Any]:
        """Load nodes and edges into the graph store.

        Converts raw artifact dicts to typed CPG models, applies tenant_id,
        batches writes, and returns summary counts.

        Args:
            nodes_data: List of node dicts from nodes.jsonl artifact.
            edges_data: List of edge dicts from edges.jsonl artifact.

        Returns:
            Dict with 'nodes_written' and 'edges_written' counts.
        """
        logger.info(
            "GraphLoader.load: tenant_id=%s, nodes=%d, edges=%d, batch_size=%d",
            self._tenant_id,
            len(nodes_data),
            len(edges_data),
            self._batch_size,
        )

        nodes_written = await self._load_nodes(nodes_data)
        edges_written = await self._load_edges(edges_data)

        logger.info(
            "GraphLoader.load complete: nodes_written=%d, edges_written=%d",
            nodes_written,
            edges_written,
        )

        return {
            "nodes_written": nodes_written,
            "edges_written": edges_written,
        }

    async def _load_nodes(self, nodes_data: list[dict]) -> int:
        """Convert and write nodes in batches.

        Args:
            nodes_data: Raw node dicts from artifact.

        Returns:
            Total number of nodes written.
        """
        total_written = 0
        total = len(nodes_data)

        for batch_start in range(0, total, self._batch_size):
            batch_end = min(batch_start + self._batch_size, total)
            raw_batch = nodes_data[batch_start:batch_end]

            write_batch = []
            for raw in raw_batch:
                node = CPGNode.from_artifact(raw)
                write_batch.append(self._node_to_write_dict(node))

            count = await self._graph_store.write_nodes(write_batch)
            total_written += count

            logger.debug(
                "Nodes batch [%d-%d] of %d: wrote %d",
                batch_start,
                batch_end,
                total,
                count,
            )

        return total_written

    async def _load_edges(self, edges_data: list[dict]) -> int:
        """Convert and write edges in batches.

        Args:
            edges_data: Raw edge dicts from artifact.

        Returns:
            Total number of edges written.
        """
        total_written = 0
        total = len(edges_data)

        for batch_start in range(0, total, self._batch_size):
            batch_end = min(batch_start + self._batch_size, total)
            raw_batch = edges_data[batch_start:batch_end]

            write_batch = []
            for raw in raw_batch:
                edge = CPGEdge.from_artifact(raw)
                write_batch.append(self._edge_to_write_dict(edge))

            count = await self._graph_store.write_edges(write_batch)
            total_written += count

            logger.debug(
                "Edges batch [%d-%d] of %d: wrote %d",
                batch_start,
                batch_end,
                total,
                count,
            )

        return total_written

    def _node_to_write_dict(self, node: CPGNode) -> dict:
        """Convert a CPGNode to the write dict format expected by GraphStoreProtocol.

        Applies tenant_id as both a label prefix and a property for isolation.

        Args:
            node: Typed CPGNode instance.

        Returns:
            Dict with 'id', 'label', 'properties' keys.
        """
        properties = {
            "tenant_id": self._tenant_id,
            "domain": "cpg",
            "apmId": self._apm_id,
            "appName": self._app_name,
            "full_name": node.full_name,
            "FULL_NAME": node.full_name,
            "NAME": node.name,
            "hash": node.hash,
            "filename": node.filename,
            "FILENAME": node.filename,
            "name": node.name,
            "code": node.code,
            "CODE": node.code,
            "signature": node.signature,
            "SIGNATURE": node.signature,
            "type_full_name": node.type_full_name,
            "TYPE_FULL_NAME": node.type_full_name,
            "is_external": node.is_external,
            "IS_EXTERNAL": node.is_external,
            "labelV": node.node_type,
        }

        # Include optional fields only when present (using client's field names)
        if node.line_number is not None:
            properties["line_number"] = node.line_number
            properties["LINE_NUMBER"] = node.line_number
        if node.line_number_end is not None:
            properties["line_number_end"] = node.line_number_end
            properties["LINE_NUMBER_END"] = node.line_number_end
        if node.order is not None:
            properties["order"] = node.order
            properties["ORDER"] = node.order

        # Client-required fields from Joern properties (explicit for index support)
        properties["MODIFIER_TYPE"] = node.properties.get("MODIFIER_TYPE", node.properties.get("modifierType", ""))
        properties["METHOD_FULL_NAME"] = node.properties.get("METHOD_FULL_NAME", node.properties.get("methodFullName", ""))

        # Merge any extra properties from the raw artifact
        for key, value in node.properties.items():
            if key not in properties:
                properties[key] = value

        return {
            "id": node.id,
            "label": f"{self._tenant_id}__{node.node_type}",
            "properties": properties,
        }

    def _edge_to_write_dict(self, edge: CPGEdge) -> dict:
        """Convert a CPGEdge to the write dict format expected by GraphStoreProtocol.

        Applies tenant_id as a property for isolation.

        Args:
            edge: Typed CPGEdge instance.

        Returns:
            Dict with 'source_id', 'target_id', 'edge_type', 'properties' keys.
        """
        properties = {
            "tenant_id": self._tenant_id,
            "domain": "cpg",
            "apmId": self._apm_id,
            "appName": self._app_name,
            **edge.properties,
        }

        return {
            "source_id": edge.source_id,
            "target_id": edge.target_id,
            "edge_type": edge.edge_type,
            "properties": properties,
        }
