# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.reader."""

import asyncio

import pytest

from graphrag_toolkit.core.reader import Reader
from graphrag_toolkit.core.types import Document


class TestReaderABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Reader()


class _StaticReader(Reader):
    def load(self, **kwargs) -> list[Document]:
        return [Document(text="doc1"), Document(text="doc2")]


class TestConcreteReader:
    def test_load(self):
        r = _StaticReader()
        docs = r.load()
        assert len(docs) == 2
        assert all(isinstance(d, Document) for d in docs)

    def test_async_load(self):
        r = _StaticReader()
        docs = asyncio.run(r.async_load())
        assert len(docs) == 2
