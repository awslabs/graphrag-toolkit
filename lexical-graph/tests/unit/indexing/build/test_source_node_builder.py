# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.build.source_node_builder import SourceNodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.metadata import DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.indexing.constants import SOURCE_HASH_PROPERTY
from graphrag_toolkit.lexical_graph.indexing.source_id_collision import SourceIdCollisionError
from llama_index.core.schema import TextNode, NodeRelationship, RelatedNodeInfo


def _make_builder():
    """Create a SourceNodeBuilder with required dependencies."""
    tenant = TenantId()
    id_gen = IdGenerator(tenant_id=tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)
    return SourceNodeBuilder(
        id_generator=id_gen,
        build_filters=BuildFilters(),
        source_metadata_formatter=DefaultSourceMetadataFormatter(),
    )


def _make_node(node_id='chunk_001', source_id='source_001', source_metadata=None):
    """Create a TextNode with a SOURCE relationship."""
    node = TextNode(id_=node_id, text='chunk text')
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
        node_id=source_id,
        metadata=source_metadata or {'title': 'Test Doc'},
    )
    return node


class TestSourceNodeBuilderInitialization:
    """Tests for SourceNodeBuilder initialization."""

    def test_initialization(self):
        """Verify SourceNodeBuilder initializes correctly."""
        builder = _make_builder()
        assert builder is not None

    def test_name(self):
        """Verify name returns 'SourceNodeBuilder'."""
        assert SourceNodeBuilder.name() == 'SourceNodeBuilder'


class TestSourceNodeCreation:
    """Tests for source node creation functionality."""

    def test_build_nodes_creates_source_node(self):
        """Verify build_nodes creates a source node from chunk's SOURCE relationship."""
        builder = _make_builder()
        node = _make_node(source_id='src_42')
        results = builder.build_nodes([node])

        assert len(results) == 1
        assert results[0].metadata['source']['sourceId'] == 'src_42'

    def test_build_nodes_sets_index_key(self):
        """Verify build_nodes sets INDEX_KEY to 'source'."""
        builder = _make_builder()
        node = _make_node()
        results = builder.build_nodes([node])

        assert results[0].metadata[INDEX_KEY]['index'] == 'source'

    def test_build_nodes_deduplicates_by_source_id(self):
        """Verify build_nodes deduplicates nodes sharing the same source."""
        builder = _make_builder()
        nodes = [
            _make_node(node_id='c1', source_id='shared_source'),
            _make_node(node_id='c2', source_id='shared_source'),
        ]
        results = builder.build_nodes(nodes)

        assert len(results) == 1
        assert results[0].metadata['source']['sourceId'] == 'shared_source'

    def test_build_nodes_multiple_sources(self):
        """Verify build_nodes creates separate nodes for different sources."""
        builder = _make_builder()
        nodes = [
            _make_node(node_id='c1', source_id='src_a'),
            _make_node(node_id='c2', source_id='src_b'),
        ]
        results = builder.build_nodes(nodes)

        assert len(results) == 2
        source_ids = {r.metadata['source']['sourceId'] for r in results}
        assert source_ids == {'src_a', 'src_b'}

    def test_build_nodes_includes_source_metadata(self):
        """Verify build_nodes includes formatted source metadata."""
        builder = _make_builder()
        node = _make_node(source_metadata={'author': 'Test Author'})
        results = builder.build_nodes([node])

        assert 'metadata' in results[0].metadata['source']


class TestSourceNodeBuilderEdgeCases:
    """Tests for source node builder edge cases."""

    def test_build_nodes_empty_list(self):
        """Verify build_nodes returns empty list for empty input."""
        builder = _make_builder()
        results = builder.build_nodes([])
        assert results == []


class TestSourceNodeCarriesTheSourceHash:
    """The hash on the SOURCE relationship reaches the source node, so the graph
    write can record which document owns the id."""

    def test_the_hash_is_copied_onto_the_source_dict(self):
        node = _make_node()
        node.relationships[NodeRelationship.SOURCE].hash = 'abc123'

        results = _make_builder().build_nodes([node])

        assert results[0].metadata['source'][SOURCE_HASH_PROPERTY] == 'abc123'

    def test_a_relationship_without_a_hash_adds_nothing(self):
        results = _make_builder().build_nodes([_make_node()])

        assert SOURCE_HASH_PROPERTY not in results[0].metadata['source']

    def test_the_hash_stays_out_of_the_metadata_the_graph_and_vector_stores_write(self):
        # metadata['source']['metadata'] is what becomes graph properties and what
        # the vector serializers carry into a payload. The hash is not part of it.
        node = _make_node()
        node.relationships[NodeRelationship.SOURCE].hash = 'abc123'

        results = _make_builder().build_nodes([node])

        assert SOURCE_HASH_PROPERTY not in results[0].metadata['source']['metadata']

    def test_chunks_that_disagree_on_the_hash_raise(self):
        # Two documents whose ids collide share a storage prefix, so a read of that
        # prefix returns one document holding both sets of chunks. The builder writes
        # one source node per id, so only the first hash would otherwise be seen.
        a = _make_node('chunk_a')
        a.relationships[NodeRelationship.SOURCE].hash = 'hash-a'
        b = _make_node('chunk_b')
        b.relationships[NodeRelationship.SOURCE].hash = 'hash-b'

        with pytest.raises(SourceIdCollisionError) as raised:
            _make_builder().build_nodes([a, b])

        assert 'hash-a and hash-b' in str(raised.value)
        assert 'source_001' in str(raised.value)

    def test_chunks_that_agree_on_the_hash_do_not_raise(self):
        a = _make_node('chunk_a')
        a.relationships[NodeRelationship.SOURCE].hash = 'hash-a'
        b = _make_node('chunk_b')
        b.relationships[NodeRelationship.SOURCE].hash = 'hash-a'

        results = _make_builder().build_nodes([a, b])

        assert results[0].metadata['source'][SOURCE_HASH_PROPERTY] == 'hash-a'

    def test_a_chunk_without_a_hash_does_not_count_as_disagreement(self):
        a = _make_node('chunk_a')
        a.relationships[NodeRelationship.SOURCE].hash = None
        b = _make_node('chunk_b')
        b.relationships[NodeRelationship.SOURCE].hash = 'hash-b'

        results = _make_builder().build_nodes([a, b])

        assert results[0].metadata['source'][SOURCE_HASH_PROPERTY] == 'hash-b'
