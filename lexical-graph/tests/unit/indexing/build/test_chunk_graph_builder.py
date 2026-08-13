# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
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
        # spec'd so the mock does not auto-create buffer_chunk_write, which
        # only a GraphBatchClient has - these tests exercise the plain-store
        # path where build() calls chunk_store.put() directly.
        client = Mock(spec=GraphStore)
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

        # The custom property is set in its own query, separate from the
        # chunk-text write (which routes through ChunkStore.put()).
        calls_with_custom_prop = [
            call for call in client.execute_query_with_retry.call_args_list
            if call[0][1]['params'] and call[0][1]['params'][0].get('custom_prop') == 'custom_value'
        ]
        assert len(calls_with_custom_prop) == 1

    def test_build_with_metadata_value_key_does_not_overwrite_chunk_text(self):
        """A chunk_metadata key literally named 'value' must not clobber the chunk text ChunkStore.put() writes."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node(chunk_id='chunk_001', text='Sample text')
        node.metadata['chunk']['metadata'] = {'value': 'attacker-controlled', 'other_prop': 'kept'}

        client = self._make_graph_client()
        mock_chunk_store = Mock()

        with patch(
            'graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder.ChunkStoreFactory.for_chunk_store',
            return_value=mock_chunk_store,
        ):
            builder.build(node, client)

        mock_chunk_store.put.assert_called_once_with('chunk_001', 'Sample text')

        # the metadata-only query must never touch chunk.value
        for call in client.execute_query_with_retry.call_args_list:
            query = call[0][0]
            assert 'chunk.value' not in query

        # other_prop still gets set as usual
        calls_with_other_prop = [
            call for call in client.execute_query_with_retry.call_args_list
            if call[0][1]['params'] and call[0][1]['params'][0].get('other_prop') == 'kept'
        ]
        assert len(calls_with_other_prop) == 1

    def test_build_writes_chunk_text_via_chunk_store(self):
        """Verify build routes chunk text through ChunkStoreFactory/ChunkStore.put()."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node(chunk_id='chunk_001', text='Sample text')

        client = self._make_graph_client()
        mock_chunk_store = Mock()

        with patch(
            'graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder.ChunkStoreFactory.for_chunk_store',
            return_value=mock_chunk_store,
        ) as mock_for_chunk_store:
            builder.build(node, client)

        mock_for_chunk_store.assert_called_once_with(None, graph_store=client)
        mock_chunk_store.put.assert_called_once_with('chunk_001', 'Sample text')

    def test_build_passes_the_configured_chunk_store_to_the_factory(self):
        """A configured backend has to reach the factory, or the opt-in does nothing."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node(chunk_id='chunk_001', text='Sample text')

        client = self._make_graph_client()

        with patch(
            'graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder.ChunkStoreFactory.for_chunk_store',
            return_value=Mock(),
        ) as mock_for_chunk_store, patch(
            'graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder.GraphRAGConfig'
        ) as mock_config:
            mock_config.s3_chunk_store = 's3://my-bucket/chunks'
            builder.build(node, client)

        mock_for_chunk_store.assert_called_once_with('s3://my-bucket/chunks', graph_store=client)

    def test_build_skips_metadata_query_when_no_extra_properties(self):
        """Verify build doesn't issue an empty SET query when there's no extra chunk metadata."""
        builder = ChunkGraphBuilder()
        node = self._make_chunk_node()

        client = self._make_graph_client()
        builder.build(node, client)

        # chunk-text write (via ChunkStore) + source relationship = 2, no
        # separate (and otherwise-empty) metadata SET query.
        assert client.execute_query_with_retry.call_count == 2
