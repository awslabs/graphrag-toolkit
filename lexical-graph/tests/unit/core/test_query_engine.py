# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.query_engine."""

import asyncio
from typing import Iterator

import pytest

from graphrag_toolkit.core.query_engine import QueryEngine


class TestQueryEngineABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            QueryEngine()


class _SimpleQueryEngine(QueryEngine):
    def query(self, query_str: str) -> str:
        return f"Answer to: {query_str}"

    def stream(self, query_str: str) -> Iterator[str]:
        for word in query_str.split():
            yield word


class TestConcreteQueryEngine:
    def test_query(self):
        qe = _SimpleQueryEngine()
        assert qe.query("hello") == "Answer to: hello"

    def test_stream(self):
        qe = _SimpleQueryEngine()
        chunks = list(qe.stream("hello world"))
        assert chunks == ["hello", "world"]

    def test_async_query(self):
        qe = _SimpleQueryEngine()
        result = asyncio.run(qe.async_query("test"))
        assert result == "Answer to: test"
