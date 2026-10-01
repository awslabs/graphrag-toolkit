# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `StatementGraphBuilder`.

The `Statement` metadata here is spelled as the model actually defines it -
`value`, `details`, `chunkId` - not `text` and `entities`. The earlier version of
this file used the latter and every test in it raised `ValidationError`; nothing
noticed, because pytest's default `norecursedirs` contains `build`, so this whole
directory was skipped unless its path was named explicitly.
"""

from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.indexing.build.statement_graph_builder import StatementGraphBuilder

from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

def statement_node(statement:dict, node_id:str='stmt-1') -> TextNode:
    node = TextNode(text=statement.get('value', ''), id_=node_id)
    node.metadata = {'statement': statement}
    return node

def graph_client() -> Mock:
    client = Mock()
    client.node_id = Mock(side_effect=lambda field: f'params.{field}')
    client.execute_query_with_retry = Mock(return_value=[])
    return client

class TestStatementGraphBuilderInitialization:
    """Tests for StatementGraphBuilder initialization."""

    def test_initialization(self):
        """Verify StatementGraphBuilder initializes correctly."""
        assert StatementGraphBuilder() is not None

    def test_index_key(self):
        """Verify the builder claims the statement index."""
        assert StatementGraphBuilder.index_key() == 'statement'

class TestStatementGraphBuilding:
    """Tests for statement graph building functionality."""

    def test_build_statement_node(self):
        """Verify building a statement node writes its value and details."""
        client = graph_client()

        StatementGraphBuilder().build(
            statement_node({
                'statementId': 'stmt-1',
                'value': 'GraphRAG combines knowledge graphs with RAG',
                'details': ['first detail', 'second detail'],
            }),
            client
        )

        client.execute_query_with_retry.assert_called_once()
        (_, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0] == {
            'statement_id': 'stmt-1',
            'value': 'GraphRAG combines knowledge graphs with RAG',
            'details': 'first detail\nsecond detail',
        }

    def test_build_statement_with_chunk_writes_the_relationship(self):
        """Verify a statement carrying a chunk id also gets linked to that chunk."""
        client = graph_client()

        StatementGraphBuilder().build(
            statement_node({
                'statementId': 'stmt-2',
                'value': 'Knowledge graphs store structured information',
                'chunkId': 'chunk-1',
            }),
            client
        )

        assert client.execute_query_with_retry.call_count == 2
        (query, params) = client.execute_query_with_retry.call_args_list[1].args
        assert '__MENTIONED_IN__' in query
        assert params['params'][0] == {'statement_id': 'stmt-2', 'chunk_id': 'chunk-1'}

    def test_build_without_chunk_writes_only_the_statement(self):
        """Verify no chunk relationship is invented when there is no chunk id."""
        client = graph_client()

        StatementGraphBuilder().build(
            statement_node({'statementId': 'stmt-3', 'value': 'A statement'}),
            client
        )

        assert client.execute_query_with_retry.call_count == 1

    def test_build_multiple_statements(self):
        """Verify building multiple statement nodes."""
        client = graph_client()
        builder = StatementGraphBuilder()

        for i in range(2):
            builder.build(
                statement_node({'statementId': f's{i}', 'value': f'Statement {i}'}, node_id=f's{i}'),
                client
            )

        assert client.execute_query_with_retry.call_count == 2

    def test_build_reads_the_previous_statement_relationship(self):
        """Verify a PREVIOUS relationship carrying a statement is parsed, not ignored.

        The builder validates the previous node's statement metadata, so a
        malformed one would raise here rather than at the write - worth covering,
        because the relationship is optional and easy to leave untested.
        """
        client = graph_client()

        node = statement_node({'statementId': 'stmt-2', 'value': 'Second'})
        node.relationships[NodeRelationship.PREVIOUS] = RelatedNodeInfo(
            node_id='stmt-1',
            metadata={'statement': {'statementId': 'stmt-1', 'value': 'First'}},
        )

        StatementGraphBuilder().build(node, client)

        assert client.execute_query_with_retry.called

    def test_build_tolerates_unknown_kwargs(self):
        """Verify the shared build kwargs the pipeline passes are ignored safely."""
        client = graph_client()

        StatementGraphBuilder().build(
            statement_node({'statementId': 'stmt-1', 'value': 'A statement'}),
            client,
            include_domain_labels=False,
            include_local_entities=True,
            typed_properties='subject',
        )

        assert client.execute_query_with_retry.called

class TestStatementGraphBuilderErrorHandling:
    """Tests for statement graph builder error handling."""

    def test_build_with_no_statement_metadata_writes_nothing(self):
        """Verify a node carrying no statement is left alone."""
        client = graph_client()

        node = TextNode(text='', id_='stmt-1')
        node.metadata = {}

        StatementGraphBuilder().build(node, client)

        assert not client.execute_query_with_retry.called

    def test_build_with_missing_statement_id_still_writes(self):
        """Verify the id is not treated as required.

        `Statement.statementId` is optional, so the write goes ahead with a null
        id. Asserted because it is surprising, not because it is desirable.
        """
        client = graph_client()

        StatementGraphBuilder().build(statement_node({'value': 'Statement without ID'}), client)

        (_, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0]['statement_id'] is None

    def test_build_with_empty_statement_value(self):
        """Verify an empty value is written rather than skipped."""
        client = graph_client()

        StatementGraphBuilder().build(statement_node({'statementId': 'stmt-1', 'value': ''}), client)

        (_, params) = client.execute_query_with_retry.call_args.args
        assert params['params'][0]['value'] == ''
