# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.entity_relation_graph_builder import EntityRelationGraphBuilder


class TestEntityRelationGraphBuilderInitialization:
    """Tests for EntityRelationGraphBuilder initialization."""

    def test_initialization(self):
        """Verify EntityRelationGraphBuilder initializes correctly."""
        builder = EntityRelationGraphBuilder()
        assert builder is not None

    def test_index_key(self):
        """Verify index_key returns 'fact'."""
        assert EntityRelationGraphBuilder.index_key() == 'fact'


class TestEntityRelationBuilding:
    """Tests for entity relation building functionality."""

    def _make_spo_node(self, fact_id='f1', subject_id='e1', object_id='e2', predicate='is'):
        """Create a mock node with SPO fact metadata."""
        node = Mock()
        node.node_id = fact_id
        node.metadata = {
            'fact': {
                'factId': fact_id,
                'subject': {
                    'entityId': subject_id,
                    'value': 'Subject',
                    'classification': None,
                },
                'predicate': {'value': predicate},
                'object': {
                    'entityId': object_id,
                    'value': 'Object',
                    'classification': None,
                },
            }
        }
        return node

    def _make_graph_client(self):
        client = Mock()
        client.node_id = Mock(side_effect=lambda field: f'params.{field}')
        client.execute_query_with_retry = Mock()
        return client

    def test_build_creates_spo_relation(self):
        """Verify build creates relation between subject and object."""
        builder = EntityRelationGraphBuilder()
        node = self._make_spo_node()
        client = self._make_graph_client()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        assert client.execute_query_with_retry.called

    def test_build_passes_predicate_in_properties(self):
        """Verify build includes predicate value in query properties."""
        builder = EntityRelationGraphBuilder()
        node = self._make_spo_node(predicate='relates_to')
        client = self._make_graph_client()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        call_args = client.execute_query_with_retry.call_args
        params = call_args[0][1]  # second positional arg is properties
        assert params['params'][0]['p'] == 'relates_to'

    def test_build_multiple_relations(self):
        """Verify build handles multiple fact nodes."""
        builder = EntityRelationGraphBuilder()
        client = self._make_graph_client()

        for i in range(3):
            node = self._make_spo_node(fact_id=f'f{i}', subject_id=f's{i}', object_id=f'o{i}')
            builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        assert client.execute_query_with_retry.call_count >= 3


class TestEntityRelationGraphBuilderErrorHandling:
    """Tests for entity relation builder error handling."""

    def test_build_with_empty_fact_metadata(self):
        """Verify build handles missing fact metadata gracefully."""
        builder = EntityRelationGraphBuilder()
        node = Mock()
        node.node_id = 'node_123'
        node.metadata = {}

        client = Mock()
        client.execute_query_with_retry = Mock()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        assert not client.execute_query_with_retry.called
