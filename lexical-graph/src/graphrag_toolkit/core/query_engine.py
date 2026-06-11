# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""QueryEngine abstract base class."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Iterator


class QueryEngine(ABC):
    """Base class for query engines that perform end-to-end QA."""

    @abstractmethod
    def query(self, query_str: str) -> str:
        """Execute a query and return the response."""

    @abstractmethod
    def stream(self, query_str: str) -> Iterator[str]:
        """Stream query response chunks."""

    async def async_query(self, query_str: str) -> str:
        """Async query. Default delegates via thread."""
        return await asyncio.to_thread(self.query, query_str)
