# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.retriever."""

import asyncio

import pytest

from graphrag_toolkit.core.retriever import Retriever
from graphrag_toolkit.core.types import Node, NodeWithScore, QueryBundle


class TestRetrieverABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Retriever()


class _SimpleRetriever(Retriever):
    def retrieve(self, query: QueryBundle) -> list[NodeWithScore]:
        return [NodeWithScore(node=Node(text=query.query_str), score=1.0)]


class TestConcreteRetriever:
    def test_retrieve(self):
        r = _SimpleRetriever()
        results = r.retrieve(QueryBundle(query_str="test"))
        assert len(results) == 1
        assert results[0].node.text == "test"
        assert results[0].score == 1.0

    def test_async_retrieve(self):
        r = _SimpleRetriever()
        results = asyncio.run(r.async_retrieve(QueryBundle(query_str="async")))
        assert len(results) == 1
        assert results[0].node.text == "async"
