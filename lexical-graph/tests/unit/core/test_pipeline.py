# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.pipeline."""

import asyncio

from graphrag_toolkit.core.pipeline import Pipeline, async_run_pipeline, run_pipeline
from graphrag_toolkit.core.transform import Transform
from graphrag_toolkit.core.types import Node


class _UpperTransform(Transform):
    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        return [Node(text=n.text.upper(), node_id=n.node_id) for n in nodes]


class _FilterShort(Transform):
    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        return [n for n in nodes if len(n.text) > 3]


class TestRunPipeline:
    def test_no_transforms(self):
        nodes = [Node(text="a")]
        assert run_pipeline(nodes, []) == nodes

    def test_single_transform(self):
        nodes = [Node(text="hello", node_id="1")]
        result = run_pipeline(nodes, [_UpperTransform()])
        assert result[0].text == "HELLO"

    def test_multiple_transforms(self):
        nodes = [Node(text="hi", node_id="1"), Node(text="hello", node_id="2")]
        result = run_pipeline(nodes, [_FilterShort(), _UpperTransform()])
        assert len(result) == 1
        assert result[0].text == "HELLO"

    def test_async_run_pipeline(self):
        nodes = [Node(text="async", node_id="1")]
        result = asyncio.run(async_run_pipeline(nodes, [_UpperTransform()]))
        assert result[0].text == "ASYNC"


class TestPipelineClass:
    def test_run(self):
        p = Pipeline(transforms=[_UpperTransform()])
        result = p.run([Node(text="test", node_id="1")])
        assert result[0].text == "TEST"

    def test_async_run(self):
        p = Pipeline(transforms=[_FilterShort(), _UpperTransform()])
        nodes = [Node(text="ab", node_id="1"), Node(text="world", node_id="2")]
        result = asyncio.run(p.async_run(nodes))
        assert len(result) == 1
        assert result[0].text == "WORLD"
