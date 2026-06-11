# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.extractor."""

import asyncio

import pytest

from graphrag_toolkit.core.extractor import Extractor
from graphrag_toolkit.core.types import Node


class TestExtractorABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Extractor()


class _SimpleExtractor(Extractor):
    async def extract(self, nodes: list[Node]) -> list[dict]:
        return [{"text": n.text, "length": len(n.text)} for n in nodes]


class TestConcreteExtractor:
    def test_extract(self):
        e = _SimpleExtractor()
        nodes = [Node(text="hello"), Node(text="world")]
        results = asyncio.run(e.extract(nodes))
        assert len(results) == 2
        assert results[0] == {"text": "hello", "length": 5}

    def test_extract_sync(self):
        e = _SimpleExtractor()
        nodes = [Node(text="sync")]
        results = e.extract_sync(nodes)
        assert results == [{"text": "sync", "length": 4}]
