# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostProcessor abstract base class."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from graphrag_toolkit.core.types import NodeWithScore, QueryBundle


class PostProcessor(ABC):
    """Base class for post-processors that filter or rerank retrieved nodes."""

    @abstractmethod
    def process(self, nodes: list[NodeWithScore], query: QueryBundle) -> list[NodeWithScore]:
        """Process nodes (filter, rerank, etc.)."""

    async def async_process(self, nodes: list[NodeWithScore], query: QueryBundle) -> list[NodeWithScore]:
        """Async process. Default delegates via thread."""
        return await asyncio.to_thread(self.process, nodes, query)
