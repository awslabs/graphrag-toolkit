# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.transform."""

import asyncio

import pytest

from graphrag_toolkit.core.transform import Transform
from graphrag_toolkit.core.types import Node


class TestTransformABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Transform()


class _UpperTransform(Transform):
    def __call__(self, nodes: list[Node], **kwargs) -> list[Node]:
        return [Node(text=n.text.upper(), node_id=n.node_id) for n in nodes]


class TestConcreteTransform:
    def test_call(self):
        t = _UpperTransform()
        nodes = [Node(text="hello", node_id="1")]
        result = t(nodes)
        assert result[0].text == "HELLO"

    def test_async_call(self):
        t = _UpperTransform()
        nodes = [Node(text="async", node_id="2")]
        result = asyncio.run(t.async_call(nodes))
        assert result[0].text == "ASYNC"
