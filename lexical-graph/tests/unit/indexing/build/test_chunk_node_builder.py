# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.chunk_node_builder import ChunkNodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.metadata import DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.core.types import Node, NodeRef
from graphrag_toolkit.core.compat import NodeRelationship


def _make_builder():
    """Create a ChunkNodeBuilder with required dependencies."""
    tenant = TenantId()
    id_gen = IdGenerator(tenant_id=tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)
    build_filters = BuildFilters()
    source_metadata_formatter = DefaultSourceMetadataFormatter()
    return ChunkNodeBuilder(
        id_generator=id_gen,
        build_filters=build_filters,
        source_metadata_formatter=source_metadata_formatter,
    )


def _make_node(node_id='chunk_001', text='Sample chunk text', source_id='source_001', source_metadata=None, topics=None):
    """Create a TextNode with source relationship for chunk building."""
    metadata = {}
    if topics:
        metadata[TOPICS_KEY] = {'topics': [{'value': t} for t in topics]}
    node = Node(node_id=node_id, text=text, metadata=metadata)
    node.relationships[NodeRelationship.SOURCE] = NodeRef(
        node_id=source_id,
        metadata=source_metadata or {},
    )
    return node


class TestChunkNodeBuilderInitialization:
    """Tests for ChunkNodeBuilder initialization."""

    def test_initialization(self):
        """Verify ChunkNodeBuilder initializes correctly."""
        builder = _make_builder()
        assert builder is not None

    def test_name(self):
        """Verify name returns 'ChunkNodeBuilder'."""
        assert ChunkNodeBuilder.name() == 'ChunkNodeBuilder'

    def test_metadata_keys(self):
        """Verify metadata_keys includes TOPICS_KEY."""
        assert TOPICS_KEY in ChunkNodeBuilder.metadata_keys()


class TestChunkNodeCreation:
    """Tests for chunk node creation functionality."""

    def test_build_nodes_returns_chunk_with_correct_id(self):
        """Verify build_nodes preserves the chunk ID in metadata."""
        builder = _make_builder()
        node = _make_node(node_id='chunk_001')
        results = builder.build_nodes([node])

        assert len(results) == 1
        assert results[0].metadata['chunk']['chunkId'] == 'chunk_001'

    def test_build_nodes_sets_source_id(self):
        """Verify build_nodes records the source ID in metadata."""
        builder = _make_builder()
        node = _make_node(source_id='src_42')
        results = builder.build_nodes([node])

        assert results[0].metadata['source']['sourceId'] == 'src_42'

    def test_build_nodes_includes_topics(self):
        """Verify build_nodes extracts topics from metadata."""
        builder = _make_builder()
        node = _make_node(topics=['AI', 'Graphs'])
        results = builder.build_nodes([node])

        assert 'AI' in results[0].metadata['topics']
        assert 'Graphs' in results[0].metadata['topics']

    def test_topics_excluded_from_embedding_but_kept_for_llm(self):
        """Topics must not pollute the chunk embedding, but stay available to the LLM
        and in metadata. Chunk embeddings should be the chunk content only."""
        from llama_index.core.schema import MetadataMode
        builder = _make_builder()
        node = _make_node(text='Quarterly cash flow rose sharply.', topics=['Liquidity', 'Cash Flow'])
        chunk = builder.build_nodes([node])[0]

        # topics excluded from the embedding text, NOT from the LLM text
        assert 'topics' in chunk.excluded_embed_metadata_keys
        assert 'topics' not in chunk.excluded_llm_metadata_keys
        # the embedded text contains the content but not the topic names / "topics:" label
        embed_text = chunk.get_content(metadata_mode=MetadataMode.EMBED)
        assert 'Quarterly cash flow rose sharply.' in embed_text
        assert 'topics:' not in embed_text and 'Liquidity' not in embed_text
        # topics still present in metadata for downstream use
        assert chunk.metadata['topics'] == ['Liquidity', 'Cash Flow']

    def test_build_nodes_sets_index_key(self):
        """Verify build_nodes sets the INDEX_KEY to 'chunk'."""
        builder = _make_builder()
        node = _make_node()
        results = builder.build_nodes([node])

        assert results[0].metadata[INDEX_KEY]['index'] == 'chunk'

    def test_build_multiple_nodes(self):
        """Verify build_nodes handles multiple input nodes."""
        builder = _make_builder()
        nodes = [_make_node(node_id=f'c_{i}', source_id=f's_{i}') for i in range(3)]
        results = builder.build_nodes(nodes)

        assert len(results) == 3

    def test_build_nodes_preserves_text(self):
        """Verify build_nodes preserves the original text content."""
        builder = _make_builder()
        node = _make_node(text='Hello world')
        results = builder.build_nodes([node])

        assert results[0].text == 'Hello world'


class TestChunkNodeBuilderEdgeCases:
    """Tests for chunk node builder edge cases."""

    def test_build_nodes_with_empty_list(self):
        """Verify build_nodes returns empty list for empty input."""
        builder = _make_builder()
        results = builder.build_nodes([])
        assert results == []

    def test_build_nodes_with_empty_text(self):
        """Verify build_nodes handles empty text."""
        builder = _make_builder()
        node = _make_node(text='')
        results = builder.build_nodes([node])

        assert len(results) == 1
        assert results[0].text == ''

    def test_build_nodes_with_unicode(self):
        """Verify build_nodes handles unicode text."""
        builder = _make_builder()
        node = _make_node(text='Unicode: 你好世界 🌍 café')
        results = builder.build_nodes([node])

        assert '你好世界' in results[0].text

    def test_build_nodes_with_no_topics(self):
        """Verify build_nodes handles nodes without topics."""
        builder = _make_builder()
        node = _make_node()
        results = builder.build_nodes([node])

        assert results[0].metadata['topics'] == []
