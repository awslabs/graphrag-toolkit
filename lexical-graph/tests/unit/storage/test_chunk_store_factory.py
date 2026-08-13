# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ChunkStoreFactory.

Mirrors test_graph_store_factory.py: registration, dispatch to the default
in-graph factory, and custom factory registration.
"""

import pytest
from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.storage import chunk_store_factory
from graphrag_toolkit.lexical_graph.storage.chunk_store_factory import ChunkStoreFactory
from graphrag_toolkit.lexical_graph.storage.chunk import (
    ChunkStore,
    ChunkStoreFactoryMethod,
    InGraphChunkStore,
    S3ChunkStore,
)
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore


@pytest.fixture(autouse=True)
def isolate_chunk_store_registry():
    """Registrations live in a module-level dict, so without this a test
    that registers a broadly-matching factory changes what every later
    test sees."""
    saved = dict(chunk_store_factory._chunk_store_factories)
    yield
    chunk_store_factory._chunk_store_factories.clear()
    chunk_store_factory._chunk_store_factories.update(saved)


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

    def test_in_graph_default_not_used_for_unrecognized_chunk_info(self):
        # The in-graph store is the default for empty chunk_info only. An
        # unrecognized backend URI is an error, not something to silently
        # fall back on - a typo'd URI should fail loudly.
        with pytest.raises(ValueError, match="Unrecognized chunk store info"):
            ChunkStoreFactory.for_chunk_store('unknown://somewhere', graph_store=Mock(spec=GraphStore))

    def test_factory_missing_graph_store_kwarg_raises_specific_error(self):
        with pytest.raises(ValueError, match="InGraphChunkStoreFactory requires a graph_store"):
            ChunkStoreFactory.for_chunk_store(None)

    def test_factory_accepts_a_duck_typed_graph_client(self):
        # GraphBatchClient (the object real builders pass as graph_client) does
        # not subclass GraphStore, only duck-types it - the factory must not
        # reject it via isinstance.
        class DuckTypedGraphClient:
            def node_id(self, name):
                return name

            def execute_query_with_retry(self, query, params, **kwargs):
                pass

        result = ChunkStoreFactory.for_chunk_store(None, graph_store=DuckTypedGraphClient())

        assert isinstance(result, InGraphChunkStore)


class TestS3ChunkStoreFactory:

    def test_s3_uri_creates_an_s3_chunk_store(self):
        result = ChunkStoreFactory.for_chunk_store('s3://my-bucket/chunks', graph_store=Mock(spec=GraphStore))

        assert isinstance(result, S3ChunkStore)
        assert result.bucket_name == 'my-bucket'
        assert result.prefix == 'chunks'

    def test_s3_uri_without_a_prefix_is_accepted(self):
        result = ChunkStoreFactory.for_chunk_store('s3://my-bucket', graph_store=Mock(spec=GraphStore))

        assert result.bucket_name == 'my-bucket'
        assert result.prefix is None

    def test_kms_key_arn_is_read_from_the_query_string(self):
        key_arn = 'arn:aws:kms:us-east-1:123456789012:key/12345678'

        result = ChunkStoreFactory.for_chunk_store(
            f's3://my-bucket/chunks?kmsKeyArn={key_arn}',
            graph_store=Mock(spec=GraphStore),
        )

        assert result.kms_key_arn == key_arn

    def test_fallback_is_wired_to_the_in_graph_store(self):
        # Dual read: chunks written before a migration are still inline on the
        # graph node, so the S3 store has to be able to reach them.
        graph_client = Mock(spec=GraphStore)

        result = ChunkStoreFactory.for_chunk_store('s3://my-bucket/chunks', graph_store=graph_client)

        assert isinstance(result.fallback, InGraphChunkStore)
        assert result.fallback.graph_client is graph_client

    def test_s3_store_without_a_graph_store_has_no_fallback(self):
        result = ChunkStoreFactory.for_chunk_store('s3://my-bucket/chunks')

        assert isinstance(result, S3ChunkStore)
        assert result.fallback is None

    def test_non_s3_info_is_left_to_other_factories(self):
        with pytest.raises(ValueError, match="Unrecognized chunk store info"):
            ChunkStoreFactory.for_chunk_store('unknown://somewhere', graph_store=Mock(spec=GraphStore))


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
