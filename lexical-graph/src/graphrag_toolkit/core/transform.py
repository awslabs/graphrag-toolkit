# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Transform abstract base class."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from graphrag_toolkit.core.types import Node


class Transform(ABC):
    """Base class for transforms that process nodes."""

    @abstractmethod
    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        """Transform nodes."""

    async def async_call(self, nodes: list[Node], **kwargs) -> list[Node]:
        """Async transform. Default delegates via thread."""
        return await asyncio.to_thread(self.__call__, nodes, **kwargs)
