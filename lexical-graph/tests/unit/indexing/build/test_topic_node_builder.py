# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.indexing.build.topic_node_builder import (
    TopicNodeBuilder,
)
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY
from graphrag_toolkit.lexical_graph.metadata import DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY
from graphrag_toolkit.lexical_graph.tenant_id import TenantId


def _builder(build_filters=None):
    return TopicNodeBuilder(
        id_generator=IdGenerator(tenant_id=TenantId()),
        build_filters=build_filters or BuildFilters(),
        source_metadata_formatter=DefaultSourceMetadataFormatter(),
    )


def _chunk(chunk_id, topics, source_id='source-1', source_metadata=None):
    metadata = {TOPICS_KEY: {'topics': topics}} if topics is not None else {}
    node = TextNode(id_=chunk_id, text='chunk text', metadata=metadata)
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
        node_id=source_id,
        metadata=source_metadata or {},
    )
    return node


def _topic(value, *statements):
    return {'value': value, 'statements': list(statements)}


def _statement(value, details=None):
    return {'value': value, 'details': details or []}


def _by_text(nodes):
    return {n.text: n for n in nodes}


class TestTopicNodeBuilder:

    def test_name_and_metadata_keys(self):
        assert TopicNodeBuilder.name() == 'TopicNodeBuilder'
        assert TopicNodeBuilder.metadata_keys() == [TOPICS_KEY]

    def test_builds_one_node_per_topic_with_id_source_and_index_key(self):
        builder = _builder()
        expected_id = builder.id_generator.create_topic_id('source-1', 'Neptune')

        [node] = builder.build_nodes([_chunk('c1', [_topic('Neptune', _statement('a'))])])

        assert node.id_ == expected_id
        assert node.text == 'Neptune'
        assert node.metadata['source']['sourceId'] == 'source-1'
        assert node.metadata['topic']['topicId'] == expected_id
        assert node.metadata['topic']['value'] == 'Neptune'
        assert node.metadata['topic']['chunkIds'] == ['c1']
        assert node.metadata[INDEX_KEY] == {'index': 'topic', 'key': builder._clean_id(expected_id)}

    def test_same_topic_in_two_chunks_of_one_source_is_one_node(self):
        builder = _builder()
        chunks = [
            _chunk('c1', [_topic('Neptune', _statement('first'))]),
            _chunk('c2', [_topic('Neptune', _statement('second'))]),
            _chunk('c1', [_topic('Neptune', _statement('first'))]),
        ]

        [node] = builder.build_nodes(chunks)

        assert node.metadata['topic']['chunkIds'] == ['c1', 'c2']
        assert node.metadata['statements'] == 'first second'

    def test_same_topic_in_two_sources_is_two_nodes(self):
        builder = _builder()
        chunks = [
            _chunk('c1', [_topic('Neptune')], source_id='source-1'),
            _chunk('c2', [_topic('Neptune')], source_id='source-2'),
        ]

        nodes = builder.build_nodes(chunks)

        assert sorted(n.metadata['source']['sourceId'] for n in nodes) == ['source-1', 'source-2']
        assert len({n.id_ for n in nodes}) == 2

    def test_statements_carry_their_details_and_keep_first_seen_order(self):
        builder = _builder()
        chunk = _chunk('c1', [_topic(
            'Neptune',
            _statement('b'),
            _statement('a', details=['x', 'y']),
            _statement('b'),
        )])

        [node] = builder.build_nodes([chunk])

        assert node.metadata['statements'] == 'b a (x, y)'

    def test_ignored_topics_and_statements_are_skipped(self):
        filters = BuildFilters(
            topic_filter_fn=lambda t: t == 'skip me',
            statement_filter_fn=lambda s: s == 'drop',
        )
        chunk = _chunk('c1', [
            _topic('skip me', _statement('kept elsewhere')),
            _topic('Neptune', _statement('keep'), _statement('drop')),
        ])

        nodes = _builder(filters).build_nodes([chunk])

        assert [n.text for n in nodes] == ['Neptune']
        assert nodes[0].metadata['statements'] == 'keep'

    def test_chunks_without_topics_produce_nothing(self):
        empty = _chunk('c2', None)
        empty.metadata[TOPICS_KEY] = {}

        assert _builder().build_nodes([_chunk('c1', None), empty, _chunk('c3', [])]) == []

    def test_source_metadata_is_added_to_the_topic(self):
        builder = _builder()
        chunk = _chunk('c1', [_topic('Neptune')], source_metadata={'url': 'https://example.com'})

        [node] = builder.build_nodes([chunk])

        assert node.metadata['source']['metadata'] == {'url': 'https://example.com'}

    def test_topic_and_source_metadata_are_kept_out_of_embedding_and_llm_text(self):
        builder = _builder()

        [node] = builder.build_nodes([_chunk('c1', [_topic('Neptune', _statement('a'))])])

        for key in (INDEX_KEY, 'topic', 'source'):
            assert key in node.excluded_embed_metadata_keys
            assert key in node.excluded_llm_metadata_keys
