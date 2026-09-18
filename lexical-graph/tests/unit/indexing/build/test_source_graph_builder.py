# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import escape_cypher_label


def _make_graph_client():
    client = Mock(spec=GraphStore)
    client.node_id = Mock(side_effect=lambda field: field)
    # Identity assignment fn, like the base GraphStore.
    client.property_assigment_fn = Mock(side_effect=lambda key, value: (lambda x: x))
    client.execute_query_with_retry = Mock()
    return client


def _make_source_node(metadata, source_id='src_001'):
    node = Mock()
    node.node_id = source_id
    node.metadata = {'source': {'sourceId': source_id, 'metadata': metadata}}
    return node


def _setter_query(client):
    setters = [
        call[0][0] for call in client.execute_query_with_retry.call_args_list
        if 'ON CREATE SET' in call[0][0]
    ]
    assert len(setters) == 1
    return setters[0]


class TestSourceGraphBuilderPropertyInjection:
    """A metadata key is a Cypher identifier interpolated into the SET clause,
    so it must be backtick-quoted and escaped or it can inject Cypher."""

    def test_backtick_key_is_escaped(self):
        malicious = 'x`);//'  # no spaces, so strip/replace leaves it intact
        client = _make_graph_client()

        SourceGraphBuilder().build(_make_source_node({malicious: 'v'}), client)

        query = _setter_query(client)
        escaped = escape_cypher_label(malicious)
        assert f'source.`{escaped}` = params.`{escaped}`' in query
        # The raw, unquoted breakout form must not reach the query.
        assert 'source.x`' not in query
        params = client.execute_query_with_retry.call_args_list[0][0][1]['params'][0]
        assert params[malicious] == 'v'

    def test_plain_key_is_backtick_quoted(self):
        client = _make_graph_client()

        SourceGraphBuilder().build(_make_source_node({'author': 'bob'}), client)

        query = _setter_query(client)
        assert 'source.`author` = params.`author`' in query


class TestSourceGraphBuilderSourceIdBinding:
    """source_id is the merge key; it must be bound as a param, not inlined into
    a single-quoted Cypher literal where a quote could inject."""

    def test_source_id_is_bound_not_inlined(self):
        client = _make_graph_client()
        malicious = "s'}) DETACH DELETE n //"

        SourceGraphBuilder().build(_make_source_node({}, source_id=malicious), client)

        call = client.execute_query_with_retry.call_args_list[0]
        query, params = call[0][0], call[0][1]['params'][0]
        assert 'params.sourceId' in query
        assert 'DETACH DELETE n' not in query   # value never inlined into Cypher
        assert params['sourceId'] == malicious   # bound instead

    def test_metadata_sourceid_does_not_override_merge_key(self):
        client = _make_graph_client()

        SourceGraphBuilder().build(
            _make_source_node({'sourceId': 'ATTACKER'}, source_id='real'), client
        )

        params = client.execute_query_with_retry.call_args_list[0][0][1]['params'][0]
        assert params['sourceId'] == 'real'


class TestSourceGraphBuilderRecordsTheDocumentHash:
    """The first document to claim an id owns it."""

    @staticmethod
    def _source_with_hash(document_hash):
        node = _make_source_node({'author': 'bob'})
        node.metadata['source']['documentHash'] = document_hash
        return node

    def test_the_hash_is_set_on_create_and_kept_on_match(self):
        client = _make_graph_client()

        SourceGraphBuilder().build(self._source_with_hash('h1'), client)

        query = _setter_query(client)
        create_clause, match_clause = query.split('ON MATCH SET')
        assert 'source.documentHash = params.documentHash' in create_clause
        assert 'source.documentHash = coalesce(source.documentHash, params.documentHash)' in match_clause
        params = client.execute_query_with_retry.call_args[0][1]['params'][0]
        assert params['documentHash'] == 'h1'

    def test_a_source_without_a_hash_writes_the_same_query_as_before(self):
        client = _make_graph_client()

        SourceGraphBuilder().build(_make_source_node({'author': 'bob'}), client)

        query = _setter_query(client)
        assert 'documentHash' not in query
        assert 'documentHash' not in client.execute_query_with_retry.call_args[0][1]['params'][0]
