# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `FactGraphBuilder`.

`Fact.subject` and `Fact.predicate` are an `Entity` and a `Relation`, not strings,
and `build` reads `include_local_entities` with a hard subscript. The earlier
version of this file passed strings and omitted the kwarg, so every test in it
raised; nothing noticed, because pytest's default `norecursedirs` contains
`build`, so this whole directory was skipped unless its path was named explicitly.
"""

from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.indexing.build.fact_graph_builder import FactGraphBuilder

from llama_index.core.schema import TextNode

RELATION_FACT = {
    'factId': 'fact-1',
    'statementId': 'stmt-1',
    'subject': {'entityId': 's-1', 'value': 'GraphRAG', 'classification': 'Framework'},
    'predicate': {'value': 'combines'},
    'object': {'entityId': 'o-1', 'value': 'knowledge graphs', 'classification': 'Technology'},
}

ATTRIBUTE_FACT = {
    'factId': 'fact-2',
    'statementId': 'stmt-1',
    'subject': {'entityId': 's-1', 'value': 'GraphRAG', 'classification': 'Framework'},
    'predicate': {'value': 'RELEASE YEAR'},
    'complement': {'entityId': 'c-1', 'value': '2024', 'classification': '__Local_Entity__'},
}

def fact_node(fact:dict, text:str='GraphRAG combines knowledge graphs') -> TextNode:
    node = TextNode(text=text, id_=fact.get('factId', 'fact-1'))
    node.metadata = {'fact': fact}
    return node

def graph_client() -> Mock:
    client = Mock()
    client.node_id = Mock(side_effect=lambda field: f'params.{field}')
    client.execute_query_with_retry = Mock(return_value=[])
    return client

def queries(client:Mock) -> list:
    return [call.args[0] for call in client.execute_query_with_retry.call_args_list]

class TestFactGraphBuilderInitialization:
    """Tests for FactGraphBuilder initialization."""

    def test_initialization(self):
        """Verify FactGraphBuilder initializes correctly."""
        assert FactGraphBuilder() is not None

    def test_index_key(self):
        """Verify the builder claims the fact index."""
        assert FactGraphBuilder.index_key() == 'fact'

class TestFactGraphBuilding:
    """Tests for fact graph building functionality."""

    def test_build_fact_node(self):
        """Verify the fact itself is written, with the node's text as its value."""
        client = graph_client()

        FactGraphBuilder().build(fact_node(RELATION_FACT), client, include_local_entities=False)

        (query, params) = client.execute_query_with_retry.call_args_list[0].args
        assert '__SUPPORTS__' in query
        assert params['params'][0] == {
            'statement_id': 'stmt-1',
            'fact_id': 'fact-1',
            'fact': 'GraphRAG combines knowledge graphs',
        }

    def test_build_links_subject_and_object_entities(self):
        """Verify both ends of a relation fact are linked to it."""
        client = graph_client()

        FactGraphBuilder().build(fact_node(RELATION_FACT), client, include_local_entities=False)

        bound = [
            call.args[1]['params'][0]
            for call in client.execute_query_with_retry.call_args_list
            if call.args[1]['params'] and 'entity_id' in call.args[1]['params'][0]
        ]

        assert {b['entity_id'] for b in bound} == {'s-1', 'o-1'}

    def test_complement_is_linked_only_with_local_entities(self):
        """Verify the complement is skipped when local entities are excluded.

        The complement node is created by `EntityGraphBuilder` only when
        `include_local_entities` is on, so linking to it otherwise would point at
        a node that does not exist.
        """
        for (include_local_entities, expected) in [(False, {'s-1'}), (True, {'s-1', 'c-1'})]:
            client = graph_client()

            FactGraphBuilder().build(
                fact_node(ATTRIBUTE_FACT, text='GraphRAG RELEASE YEAR 2024'),
                client,
                include_local_entities=include_local_entities
            )

            bound = [
                call.args[1]['params'][0]
                for call in client.execute_query_with_retry.call_args_list
                if call.args[1]['params'] and 'entity_id' in call.args[1]['params'][0]
            ]

            assert {b['entity_id'] for b in bound} == expected, include_local_entities

    def test_build_multiple_facts(self):
        """Verify building multiple fact nodes."""
        client = graph_client()
        builder = FactGraphBuilder()

        for fact in (RELATION_FACT, ATTRIBUTE_FACT):
            builder.build(fact_node(fact), client, include_local_entities=False)

        assert len([q for q in queries(client) if '__SUPPORTS__' in q]) == 2

    def test_build_tolerates_typed_properties(self):
        """Verify the kwarg this builder does not read is accepted anyway.

        Every builder receives the same kwargs, so a setting only
        `EntityGraphBuilder` acts on must not break the others.
        """
        client = graph_client()

        FactGraphBuilder().build(
            fact_node(RELATION_FACT),
            client,
            include_local_entities=True,
            typed_properties='subject',
        )

        assert client.execute_query_with_retry.called

class TestFactGraphBuilderErrorHandling:
    """Tests for fact graph builder error handling."""

    def test_build_with_no_fact_metadata_writes_nothing(self):
        """Verify a node carrying no fact is left alone."""
        client = graph_client()

        node = TextNode(text='', id_='fact-1')
        node.metadata = {}

        FactGraphBuilder().build(node, client, include_local_entities=False)

        assert not client.execute_query_with_retry.called

    def test_build_requires_include_local_entities(self):
        """Verify the kwarg is read with a hard subscript.

        Pinned deliberately: `BuildPipeline` always supplies it, but a caller who
        does not gets a `KeyError` rather than a default. The same read in
        `EntityGraphBuilder` was made tolerant for `typed_properties`; this one was
        left alone, and a change either way should be a visible decision.
        """
        import pytest

        client = graph_client()

        with pytest.raises(KeyError):
            FactGraphBuilder().build(fact_node(RELATION_FACT), client)
