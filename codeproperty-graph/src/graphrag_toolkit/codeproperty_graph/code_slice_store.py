# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Code Slice Store — attaches extracted code slices to CPG nodes in Neptune.

Reads CodeSliceRecord dicts (from code_slices.jsonl) and stores them as
properties on their corresponding Neptune nodes. Each slice becomes a set
of sub-properties keyed by slice_type (e.g. full_method, signature_only).

Property layout on a node:
    code_slice_full_method       = "<code text>"
    code_slice_full_method_lang  = "java"
    code_slice_full_method_line_start = 42
    code_slice_full_method_line_end   = 67
    code_slice_full_method_file_path  = "src/Main.java"

Multiple slice types per node are supported (full_method + signature_only).
"""

import asyncio
import logging
from typing import Any, Dict, List, Protocol, TypedDict, runtime_checkable

logger = logging.getLogger(__name__)


class CodeSliceRecord(TypedDict, total=False):
    """Schema for a single code slice record from code_slices.jsonl."""

    cpg_node_id: str
    slice_type: str  # e.g. "full_method", "signature_only"
    code: str
    language: str
    line_start: int
    line_end: int
    file_path: str


@runtime_checkable
class GraphStoreProtocol(Protocol):
    """Protocol for Neptune graph store — matches the interface used by delta_ingestor."""

    def execute_query(self, query: str, parameters: Dict[str, Any]) -> Any:
        ...


class CodeSliceStore:
    """Stores code slices as properties on existing CPG nodes in Neptune.

    Usage:
        store = CodeSliceStore(graph_store)
        count = await store.store(slices)
    """

    def __init__(self, graph_store: GraphStoreProtocol) -> None:
        """Initialize with a Neptune graph store.

        Args:
            graph_store: Neptune graph store instance with execute_query method.
        """
        self._graph_store = graph_store

    async def store(self, slices: List[CodeSliceRecord]) -> int:
        """Attach code slices to their corresponding CPG nodes.

        For each record, sets properties on the node identified by cpg_node_id:
            - code_slice_{slice_type}: the code text
            - code_slice_{slice_type}_lang: language
            - code_slice_{slice_type}_line_start: start line number
            - code_slice_{slice_type}_line_end: end line number
            - code_slice_{slice_type}_file_path: source file path

        Args:
            slices: List of CodeSliceRecord dicts parsed from code_slices.jsonl.

        Returns:
            Number of slices successfully stored.
        """
        if not slices:
            return 0

        stored = 0

        for record in slices:
            cpg_node_id = record.get("cpg_node_id")
            slice_type = record.get("slice_type")

            if not cpg_node_id or not slice_type:
                logger.warning(
                    "Skipping slice record with missing cpg_node_id or slice_type: %s",
                    record,
                )
                continue

            try:
                await self._store_single(record, cpg_node_id, slice_type)
                stored += 1
            except Exception as e:
                logger.error(
                    "Failed to store slice (node=%s, type=%s): %s",
                    cpg_node_id,
                    slice_type,
                    e,
                )

        logger.info("Stored %d/%d code slices", stored, len(slices))
        return stored

    async def _store_single(
        self, record: CodeSliceRecord, cpg_node_id: str, slice_type: str
    ) -> None:
        """Store a single code slice as properties on a Neptune node.

        Args:
            record: The code slice record dict.
            cpg_node_id: Neptune node identifier.
            slice_type: Slice type key (e.g. "full_method").
        """
        prefix = f"code_slice_{slice_type}"

        # Build the SET clause with parameterized values
        query = (
            f"MATCH (n) WHERE n.id = $node_id "
            f"SET n.`{prefix}` = $code, "
            f"n.`{prefix}_lang` = $language, "
            f"n.`{prefix}_line_start` = $line_start, "
            f"n.`{prefix}_line_end` = $line_end, "
            f"n.`{prefix}_file_path` = $file_path"
        )

        parameters: Dict[str, Any] = {
            "node_id": cpg_node_id,
            "code": record.get("code", ""),
            "language": record.get("language", ""),
            "line_start": record.get("line_start", 0),
            "line_end": record.get("line_end", 0),
            "file_path": record.get("file_path", ""),
        }

        await asyncio.to_thread(self._graph_store.execute_query, query, parameters)

    async def store_batch(self, slices: List[CodeSliceRecord], batch_size: int = 50) -> int:
        """Store slices in batches for improved throughput on large datasets.

        Groups slices by cpg_node_id to minimize round-trips when a single node
        has multiple slice types (e.g. full_method + signature_only).

        Args:
            slices: List of CodeSliceRecord dicts.
            batch_size: Number of slices per batch query.

        Returns:
            Number of slices successfully stored.
        """
        if not slices:
            return 0

        # Group slices by node to coalesce updates
        node_slices: Dict[str, List[CodeSliceRecord]] = {}
        for record in slices:
            node_id = record.get("cpg_node_id", "")
            if node_id:
                node_slices.setdefault(node_id, []).append(record)

        stored = 0

        for node_id, node_records in node_slices.items():
            try:
                set_clauses: List[str] = []
                parameters: Dict[str, Any] = {"node_id": node_id}

                for idx, record in enumerate(node_records):
                    slice_type = record.get("slice_type", "")
                    if not slice_type:
                        continue

                    prefix = f"code_slice_{slice_type}"
                    param_suffix = f"_{idx}"

                    set_clauses.extend([
                        f"n.`{prefix}` = $code{param_suffix}",
                        f"n.`{prefix}_lang` = $language{param_suffix}",
                        f"n.`{prefix}_line_start` = $line_start{param_suffix}",
                        f"n.`{prefix}_line_end` = $line_end{param_suffix}",
                        f"n.`{prefix}_file_path` = $file_path{param_suffix}",
                    ])

                    parameters[f"code{param_suffix}"] = record.get("code", "")
                    parameters[f"language{param_suffix}"] = record.get("language", "")
                    parameters[f"line_start{param_suffix}"] = record.get("line_start", 0)
                    parameters[f"line_end{param_suffix}"] = record.get("line_end", 0)
                    parameters[f"file_path{param_suffix}"] = record.get("file_path", "")

                if not set_clauses:
                    continue

                query = (
                    f"MATCH (n) WHERE n.id = $node_id "
                    f"SET {', '.join(set_clauses)}"
                )

                await asyncio.to_thread(self._graph_store.execute_query, query, parameters)
                stored += len(node_records)

            except Exception as e:
                logger.error("Batch store failed for node %s: %s", node_id, e)

        logger.info("Batch-stored %d/%d code slices", stored, len(slices))
        return stored
