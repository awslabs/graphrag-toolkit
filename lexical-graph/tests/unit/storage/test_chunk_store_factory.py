# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ChunkStoreFactory.

Mirrors test_graph_store_factory.py: registration, dispatch to the default
in-graph factory, and custom factory registration.
"""

import pytest
from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.storage.chunk_store_factory import ChunkStoreFactory
from graphrag_toolkit.lexical_graph.storage.chunk import (
    ChunkStore,
    ChunkStoreFactoryMethod,
    InGraphChunkStore,
)
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore


class TestChunkStoreFactoryRegister:

    def test_register_factory_class(self):
        class MockChunkStoreFactory(ChunkStoreFactoryMethod):
            def try_create(self, chunk_info, **kwargs):
                return None

        ChunkStoreFactory.register(MockChunkStoreFactory)

    def test_register_factory_instance(self):
        class MockChunkStoreFactory(ChunkStoreFactoryMethod):
            def try_create(self, chunk_info, **kwargs):
                return None

        ChunkStoreFactory.register(MockChunkStoreFactory())

    def test_register_invalid_class_raises_error(self):
        class InvalidFactory:
            pass

        with pytest.raises(ValueError, match="must inherit from ChunkStoreFactoryMethod"):
            ChunkStoreFactory.register(InvalidFactory)

    def test_register_invalid_instance_raises_error(self):
        class InvalidFactory:
            pass

        with pytest.raises(ValueError, match="must inherit from ChunkStoreFactoryMethod"):
            ChunkStoreFactory.register(InvalidFactory())


class TestChunkStoreFactoryForChunkStore:

    def test_factory_returns_existing_chunk_store_instance(self):
        mock_store = Mock(spec=ChunkStore)

        result = ChunkStoreFactory.for_chunk_store(mock_store)

        assert result is mock_store

    def test_factory_creates_in_graph_store_by_default(self):
        graph_client = Mock(spec=GraphStore)

        result = ChunkStoreFactory.for_chunk_store(None, graph_store=graph_client)

        assert isinstance(result, InGraphChunkStore)

    def test_factory_invalid_type_raises_error(self):
        with pytest.raises(ValueError, match="Unrecognized chunk store info"):
            ChunkStoreFactory.for_chunk_store("invalid://unknown")

    def test_factory_missing_graph_store_kwarg_raises_specific_error(self):
        with pytest.raises(ValueError, match="InGraphChunkStoreFactory requires a graph_store"):
            ChunkStoreFactory.for_chunk_store(None)


class TestChunkStoreFactoryCustomFactory:

    def test_custom_factory_can_create_store(self):
        class CustomChunkStoreFactory(ChunkStoreFactoryMethod):
            def try_create(self, chunk_info, **kwargs):
                if chunk_info == 'custom://':
                    return Mock(spec=ChunkStore)
                return None

        ChunkStoreFactory.register(CustomChunkStoreFactory)

        result = ChunkStoreFactory.for_chunk_store('custom://')

        assert isinstance(result, ChunkStore)

    def test_registered_factory_is_tried_before_in_graph_default(self):
        class FalsyChunkInfoFactory(ChunkStoreFactoryMethod):
            def try_create(self, chunk_info, **kwargs):
                if not chunk_info:
                    return Mock(spec=ChunkStore)
                return None

        ChunkStoreFactory.register(FalsyChunkInfoFactory)

        graph_client = Mock(spec=GraphStore)
        result = ChunkStoreFactory.for_chunk_store(None, graph_store=graph_client)

        assert not isinstance(result, InGraphChunkStore)
