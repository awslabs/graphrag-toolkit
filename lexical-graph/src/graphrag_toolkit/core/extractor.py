# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extractor abstract base class."""

from __future__ import annotations

from abc import ABC, abstractmethod

from graphrag_toolkit.core.async_utils import run_async
from graphrag_toolkit.core.types import Node
from graphrag_toolkit.core.transform import Transform


class Extractor(Transform, ABC):
    """Base class for extractors that derive structured data from nodes."""

    @abstractmethod
    async def extract(self, nodes: list[Node]) -> list[dict]:
        """Extract structured data from nodes."""

    def extract_sync(self, nodes: list[Node]) -> list[dict]:
        """Synchronous extract. Default runs async in a new event loop."""
        return run_async(self.extract(nodes))

    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        """Run extraction and merge results into node metadata.

        Makes Extractor compatible with pipeline_utils._run_transformations
        which expects callable transforms.
        """
        metadata_list = self.extract_sync(nodes)
        for node, metadata in zip(nodes, metadata_list):
            node.metadata.update(metadata)
        return nodes
