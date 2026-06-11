# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extractor abstract base class."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from graphrag_toolkit.core.types import Node


class Extractor(ABC):
    """Base class for extractors that derive structured data from nodes."""

    @abstractmethod
    async def extract(self, nodes: list[Node]) -> list[dict]:
        """Extract structured data from nodes."""

    def extract_sync(self, nodes: list[Node]) -> list[dict]:
        """Synchronous extract. Default runs async in a new event loop."""
        return asyncio.run(self.extract(nodes))
