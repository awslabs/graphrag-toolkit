# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Retriever abstract base class."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from graphrag_toolkit.core.types import NodeWithScore, QueryBundle


class Retriever(ABC):
    """Base class for retrievers that fetch relevant nodes for a query."""

    @abstractmethod
    def retrieve(self, query: QueryBundle) -> list[NodeWithScore]:
        """Retrieve nodes relevant to the query."""

    async def async_retrieve(self, query: QueryBundle) -> list[NodeWithScore]:
        """Async retrieve. Default delegates via thread."""
        return await asyncio.to_thread(self.retrieve, query)
