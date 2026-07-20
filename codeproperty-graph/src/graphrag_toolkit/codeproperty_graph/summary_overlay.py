# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Summary Node Builder — creates :MethodSummary / :FileSummary nodes linked via HAS_SUMMARY.

Client schema conformance: summaries are separate graph nodes (not properties on METHOD/FILE).
Each summary node has:
  - shortDescription (first sentence)
  - longDescription (full summary text)
  - embedding (written separately by VectorLoader)
  - summarizedTime
  - embeddedTime

Linked to parent: (METHOD)-[:HAS_SUMMARY]->(:MethodSummary)
                   (FILE)-[:HAS_SUMMARY]->(:FileSummary)
"""

import asyncio
import logging
from typing import Any, Dict, List, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class GraphStoreProtocol(Protocol):
    """Protocol for graph store implementations."""

    async def write_nodes(self, nodes: list[dict]) -> int:
        ...

    async def write_edges(self, edges: list[dict]) -> int:
        ...

    async def update_node_properties(self, node_id: str, properties: dict) -> bool:
        ...


class SummaryOverlay:
    """Creates summary nodes and links them to their parent CPG nodes.

    For each summary record:
    1. Creates a :MethodSummary or :FileSummary node
    2. Creates a HAS_SUMMARY edge from the parent METHOD/FILE to the summary node
    3. Also writes shortDescription/longDescription as properties on the parent (for convenience queries)

    Args:
        graph_store: An implementation of GraphStoreProtocol.
        batch_size: Number of records to process per batch.
    """

    def __init__(self, graph_store: Any, batch_size: int = 50):
        self._graph_store = graph_store
        self._batch_size = batch_size

    async def apply(self, records: List[Dict[str, Any]]) -> int:
        """Create summary nodes and link to parent CPG nodes.

        Args:
            records: List of summary record dicts from summaries.jsonl.
                Each has: cpg_node_id, summary_type, text, generation_model, generation_prompt_version

        Returns:
            Number of summary nodes successfully created.
        """
        if not records:
            return 0

        count = 0
        summary_nodes = []
        summary_edges = []

        for record in records:
            cpg_node_id = record.get("cpg_node_id", "")
            summary_type = record.get("summary_type", "")
            text = record.get("text", "")
            generation_model = record.get("generation_model", "")
            prompt_version = record.get("generation_prompt_version", "")

            if not cpg_node_id or not text:
                logger.warning(f"Skipping summary record: missing cpg_node_id or text")
                continue

            # Determine node label based on summary_type
            if summary_type == "method_summary":
                label = "MethodSummary"
            elif summary_type == "file_summary":
                label = "FileSummary"
            elif summary_type == "class_summary":
                label = "ClassSummary"
            else:
                label = "Summary"

            # Summary node ID: parent_id + "::summary"
            summary_node_id = f"{cpg_node_id}::summary::{summary_type}"

            # Extract short description (first sentence)
            short_desc = text.split(".")[0] + "." if "." in text else text[:100]

            # Create the summary node
            summary_nodes.append({
                "id": summary_node_id,
                "label": label,
                "properties": {
                    "cpg_node_id": summary_node_id,
                    "parent_cpg_node_id": cpg_node_id,
                    "shortDescription": short_desc,
                    "longDescription": text,
                    "methodName": cpg_node_id.split(":")[-2] if ":" in cpg_node_id else "",
                    "summary_type": summary_type,
                    "generation_model": generation_model,
                    "generation_prompt_version": prompt_version,
                    "summarizedTime": record.get("summarized_time", ""),
                },
            })

            # Create HAS_SUMMARY edge from parent to summary
            summary_edges.append({
                "source_id": cpg_node_id,
                "target_id": summary_node_id,
                "edge_type": "HAS_SUMMARY",
                "properties": {"summary_type": summary_type},
            })

            # Also write shortDescription/longDescription on the parent node (convenience)
            try:
                await self._graph_store.update_node_properties(cpg_node_id, {
                    "shortDescription": short_desc,
                    "longDescription": text,
                })
            except Exception as e:
                logger.debug(f"Failed to write summary properties on parent {cpg_node_id}: {e}")

            count += 1

        # Batch write summary nodes
        if summary_nodes:
            try:
                written = await self._graph_store.write_nodes(summary_nodes)
                logger.info(f"Created {written} summary nodes")
            except Exception as e:
                logger.warning(f"Failed to write summary nodes: {e}")

        # Batch write HAS_SUMMARY edges
        if summary_edges:
            try:
                written = await self._graph_store.write_edges(summary_edges)
                logger.info(f"Created {written} HAS_SUMMARY edges")
            except Exception as e:
                logger.warning(f"Failed to write HAS_SUMMARY edges: {e}")

        return count
