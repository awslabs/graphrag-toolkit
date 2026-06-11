# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.postprocessor."""

import asyncio

import pytest

from graphrag_toolkit.core.postprocessor import PostProcessor
from graphrag_toolkit.core.types import Node, NodeWithScore, QueryBundle


class TestPostProcessorABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            PostProcessor()


class _ThresholdFilter(PostProcessor):
    def process(self, nodes: list[NodeWithScore], query: QueryBundle) -> list[NodeWithScore]:
        return [n for n in nodes if n.score >= 0.5]


class TestConcretePostProcessor:
    def test_process_filters(self):
        pp = _ThresholdFilter()
        nodes = [
            NodeWithScore(node=Node(text="a"), score=0.8),
            NodeWithScore(node=Node(text="b"), score=0.2),
            NodeWithScore(node=Node(text="c"), score=0.6),
        ]
        result = pp.process(nodes, QueryBundle(query_str="q"))
        assert len(result) == 2
        assert all(n.score >= 0.5 for n in result)

    def test_async_process(self):
        pp = _ThresholdFilter()
        nodes = [NodeWithScore(node=Node(text="x"), score=0.9)]
        result = asyncio.run(pp.async_process(nodes, QueryBundle(query_str="q")))
        assert len(result) == 1
