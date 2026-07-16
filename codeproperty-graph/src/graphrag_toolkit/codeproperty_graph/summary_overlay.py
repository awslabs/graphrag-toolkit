# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Attaches summaries from summaries.jsonl to Neptune graph nodes as properties."""

import asyncio
import logging
from typing import Any, Dict, List, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class SummaryRecord:
    """Type alias documentation for summary record dicts.

    Expected keys:
        cpg_node_id: str - The Neptune node identifier.
        summary_type: str - The type of summary (e.g., "method_summary").
        summary_text: str - The generated summary content.
        generation_model: str - The model used for generation.
        generation_prompt_version: str - The prompt version used.
    """


@runtime_checkable
class GraphStoreProtocol(Protocol):
    """Protocol for graph store implementations that support node property updates."""

    async def update_node_properties(self, node_id: str, properties: dict) -> bool:
        """Update properties on a graph node.

        Args:
            node_id: The unique identifier of the node to update.
            properties: A dictionary of property names to values to set.

        Returns:
            True if the update was successful, False otherwise.
        """
        ...


class SummaryOverlay:
    """Applies summary records as properties on Neptune graph nodes.

    This class takes a collection of summary records and attaches them
    to their corresponding nodes in the graph store. Summaries are stored
    as node properties with the naming convention `summary_{summary_type}`.

    Args:
        graph_store: An implementation of GraphStoreProtocol.
        batch_size: Number of updates to process concurrently. Defaults to 50.
    """

    DEFAULT_BATCH_SIZE = 50

    def __init__(
        self,
        graph_store: GraphStoreProtocol,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self._graph_store = graph_store
        self._batch_size = batch_size

    async def apply(self, records: List[Dict[str, Any]]) -> int:
        """Apply summary records to graph nodes as properties.

        For each record, sets the following properties on the node:
            - summary_{summary_type}: The summary text.
            - generation_model: The model used for generation.
            - generation_prompt_version: The prompt version used.

        Args:
            records: A list of summary record dicts. Each dict must contain
                keys: cpg_node_id, summary_type, summary_text,
                generation_model, generation_prompt_version.

        Returns:
            The count of summaries successfully applied.
        """
        if not records:
            logger.info("No summary records to apply.")
            return 0

        logger.info("Applying %d summary records in batches of %d.", len(records), self._batch_size)

        applied_count = 0

        for batch_start in range(0, len(records), self._batch_size):
            batch = records[batch_start : batch_start + self._batch_size]
            results = await self._apply_batch(batch)
            applied_count += sum(results)

            logger.debug(
                "Batch %d-%d: %d/%d applied.",
                batch_start,
                batch_start + len(batch),
                sum(results),
                len(batch),
            )

        logger.info("Summary overlay complete: %d/%d records applied.", applied_count, len(records))
        return applied_count

    async def _apply_batch(self, batch: List[Dict[str, Any]]) -> List[bool]:
        """Process a batch of summary records concurrently.

        Args:
            batch: A subset of summary records to process.

        Returns:
            A list of booleans indicating success/failure for each record.
        """
        tasks = [self._apply_single(record) for record in batch]
        return await asyncio.gather(*tasks)

    async def _apply_single(self, record: Dict[str, Any]) -> bool:
        """Apply a single summary record to a graph node.

        Args:
            record: A summary record dict.

        Returns:
            True if the update succeeded, False otherwise.
        """
        try:
            node_id = record["cpg_node_id"]
            summary_type = record["summary_type"]
            summary_text = record["summary_text"]
            generation_model = record["generation_model"]
            generation_prompt_version = record["generation_prompt_version"]
        except KeyError as e:
            logger.warning("Skipping record with missing key %s: %s", e, record)
            return False

        property_name = f"summary_{summary_type}"
        properties = {
            property_name: summary_text,
            "generation_model": generation_model,
            "generation_prompt_version": generation_prompt_version,
        }

        try:
            success = await self._graph_store.update_node_properties(node_id, properties)
            if not success:
                logger.warning("Failed to update node %s (store returned False).", node_id)
            return success
        except Exception:
            logger.exception("Error updating node %s.", node_id)
            return False
