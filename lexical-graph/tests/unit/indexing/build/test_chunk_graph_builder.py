# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from llama_index.core.schema import NodeRelationship


class TestChunkGraphBuilderInitialization:
    """Tests for ChunkGraphBuilder initialization."""

    def test_initialization(self):
        """Verify ChunkGraphBuilder initializes correctly."""
        builder = ChunkGraphBuilder()
        assert builder is not None

    def test_index_key(self):
        """Verify index_key returns 'chunk'."""
        assert ChunkGraphBuilder.index_key() == 'chunk'


class TestChunkGraphBuilding:
    """Tests for chunk graph building functionality."""

    def _make_graph_client(self):
        client = Mock()
        client.node_id = Mock(side_effect=lambda field: f'params.{field}')
        client.execute_query_with_retry = Mock()
        return client

    def _make_chunk_node(self, chunk_id='chunk_001', text='Sample text', source_id='src_001'):
        """Create a mock node with chunk metadata and SOURCE relationship."""
        node = Mock()
        node.node_id = chunk_id
        node.text = text
        node.metadata = {
            'chunk': {
                'chunkId': chunk_id,
                'metadata': {},
            }
        }
        source_info = Mock()
        source_info.node_id = source_id
        node.relationships = {
            NodeRelationship.SOURCE: source_info,
        }
        return node

    def test_build_inserts_chunk(self):
        """Verify build calls execute_query_with_retry for chunk insertion."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node()
        client = self._make_graph_client()

        builder.build(node, client)

        assert client.execute_query_with_retry.called

    def test_build_inserts_chunk_source_relationship(self):
        """Verify build creates chunk-source relationship."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node()
        client = self._make_graph_client()

        builder.build(node, client)

        # At least 2 calls: chunk insert + chunk-source relationship
        assert client.execute_query_with_retry.call_count >= 2

    def test_build_with_missing_chunk_id(self):
        """Verify build logs warning for missing chunk ID."""
        builder = ChunkGraphBuilder()
        node = Mock()
        node.node_id = 'n1'
        node.metadata = {'chunk': {}}
        node.relationships = {}

        client = self._make_graph_client()
        builder.build(node, client)

        # Should not execute queries without chunk_id
        assert not client.execute_query_with_retry.called

    def test_build_with_previous_next_relationships(self):
        """Verify build handles PREVIOUS and NEXT relationships."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node()

        prev_info = Mock()
        prev_info.node_id = 'chunk_000'
        next_info = Mock()
        next_info.node_id = 'chunk_002'
        node.relationships[NodeRelationship.PREVIOUS] = prev_info
        node.relationships[NodeRelationship.NEXT] = next_info

        client = self._make_graph_client()
        builder.build(node, client)

        # chunk insert + source rel + previous rel + next rel = at least 4
        assert client.execute_query_with_retry.call_count >= 4

    def test_build_with_external_properties(self):
        """Verify build includes external chunk metadata properties."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node()
        node.metadata['chunk']['metadata'] = {'custom_prop': 'custom_value'}

        client = self._make_graph_client()
        builder.build(node, client)

        # Verify the chunk insert query includes the custom property
        first_call_params = client.execute_query_with_retry.call_args_list[0][0][1]
        assert first_call_params['params'][0].get('custom_prop') == 'custom_value'
