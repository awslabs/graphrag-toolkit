# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pipeline runner for sequential node transforms."""

from __future__ import annotations

import asyncio

from graphrag_toolkit.core.transform import Transform
from graphrag_toolkit.core.types import Node


def run_pipeline(nodes: list[Node], transforms: list[Transform]) -> list[Node]:
    """Run transforms sequentially on nodes."""
    for transform in transforms:
        nodes = transform(nodes)
    return nodes


async def async_run_pipeline(nodes: list[Node], transforms: list[Transform]) -> list[Node]:
    """Async version — runs each transform via thread."""
    for transform in transforms:
        nodes = await asyncio.to_thread(transform, nodes)
    return nodes


class Pipeline:
    """Orchestrator that holds a list of transforms and runs them."""

    def __init__(self, transforms: list[Transform]):
        self.transforms = transforms

    def run(self, nodes: list[Node]) -> list[Node]:
        """Run the pipeline."""
        return run_pipeline(nodes, self.transforms)

    async def async_run(self, nodes: list[Node]) -> list[Node]:
        """Async run the pipeline."""
        return await async_run_pipeline(nodes, self.transforms)
