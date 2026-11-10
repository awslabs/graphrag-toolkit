# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `TopicGraphBuilder`.

The `Topic` metadata here is spelled as the model actually defines it - `value`
and `chunkIds`, not `name` and `metadata`. The earlier version of this file used
the latter and every test in it raised `ValidationError`; nothing noticed, because
pytest's default `norecursedirs` contains `build`, so this whole directory was
skipped unless its path was named explicitly.
"""

from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.indexing.build.topic_graph_builder import TopicGraphBuilder

from llama_index.core.schema import TextNode

def topic_node(topic:dict, node_id:str='topic-1') -> TextNode:
    node = TextNode(text=topic.get('value', ''), id_=node_id)
    node.metadata = {'topic': topic}
    return node

def graph_client() -> Mock:
    client = Mock()
    client.node_id = Mock(side_effect=lambda field: f'params.{field}')
    client.execute_query_with_retry = Mock(return_value=[])
    return client

class TestTopicGraphBuilderInitialization:
    """Tests for TopicGraphBuilder initialization."""

    def test_initialization(self):
        """Verify TopicGraphBuilder initializes correctly."""
        assert TopicGraphBuilder() is not None

    def test_index_key(self):
        """Verify the builder claims the topic index."""
        assert TopicGraphBuilder.index_key() == 'topic'

class TestTopicGraphBuilding:
    """Tests for topic graph building functionality."""

    def test_build_topic_node(self):
        """Verify building a topic node writes it with its value."""
        client = graph_client()

        TopicGraphBuilder().build(
            topic_node({'topicId': 'topic-1', 'value': 'Artificial Intelligence'}),
            client
        )

        client.execute_query_with_retry.assert_called_once()
        (_, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0] == {
            'topic_id': 'topic-1',
            'title': 'Artificial Intelligence',
            'chunk_ids': [],
        }

    def test_build_topic_with_chunks(self):
        """Verify each chunk the topic was mentioned in is bound as a parameter."""
        client = graph_client()

        TopicGraphBuilder().build(
            topic_node({
                'topicId': 'topic-2',
                'value': 'Machine Learning',
                'chunkIds': ['chunk-1', 'chunk-2'],
            }),
            client
        )

        (query, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0]['chunk_ids'] == [{'chunk_id': 'chunk-1'}, {'chunk_id': 'chunk-2'}]
        assert '__MENTIONED_IN__' in query

    def test_build_multiple_topics(self):
        """Verify building multiple topic nodes."""
        client = graph_client()
        builder = TopicGraphBuilder()

        for i in range(3):
            builder.build(topic_node({'topicId': f't{i}', 'value': f'Topic {i}'}, node_id=f't{i}'), client)

        assert client.execute_query_with_retry.call_count == 3

    def test_build_tolerates_unknown_kwargs(self):
        """Verify the shared build kwargs the pipeline passes are ignored safely.

        Every builder receives the same kwargs, so a builder that does not read
        one must still accept it.
        """
        client = graph_client()

        TopicGraphBuilder().build(
            topic_node({'topicId': 'topic-1', 'value': 'AI'}),
            client,
            include_domain_labels=False,
            include_local_entities=True,
            typed_properties='subject',
        )

        assert client.execute_query_with_retry.called

class TestTopicGraphBuilderErrorHandling:
    """Tests for topic graph builder error handling."""

    def test_build_with_no_topic_metadata_writes_nothing(self):
        """Verify a node carrying no topic is left alone."""
        client = graph_client()

        node = TextNode(text='', id_='topic-1')
        node.metadata = {}

        TopicGraphBuilder().build(node, client)

        assert not client.execute_query_with_retry.called

    def test_build_with_missing_topic_id_still_writes(self):
        """Verify the id is not treated as required.

        `Topic.topicId` is optional, so the write goes ahead with a null id rather
        than being skipped. Asserted because it is surprising, not because it is
        desirable - a caller relying on the builder to reject an unidentified
        topic would be relying on something it does not do.
        """
        client = graph_client()

        TopicGraphBuilder().build(topic_node({'value': 'Topic without ID'}), client)

        (_, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0]['topic_id'] is None

    def test_build_with_empty_topic_value(self):
        """Verify an empty value is written rather than skipped."""
        client = graph_client()

        TopicGraphBuilder().build(topic_node({'topicId': 'topic-1', 'value': ''}), client)

        (_, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0]['title'] == ''
