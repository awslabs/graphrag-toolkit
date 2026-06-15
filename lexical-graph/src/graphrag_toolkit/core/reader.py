# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reader abstract base class."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from graphrag_toolkit.core.types import Document


class Reader(ABC):
    """Base class for readers that load documents from a source."""

    @abstractmethod
    def load(self, **kwargs) -> list[Document]:
        """Load documents."""

    async def async_load(self, **kwargs) -> list[Document]:
        """Async load. Default delegates via thread."""
        return await asyncio.to_thread(self.load, **kwargs)
