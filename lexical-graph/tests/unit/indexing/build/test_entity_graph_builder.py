# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import EntityGraphBuilder


class TestEntityGraphBuilderInitialization:
    """Tests for EntityGraphBuilder initialization."""

    def test_initialization(self):
        """Verify EntityGraphBuilder initializes correctly."""
        builder = EntityGraphBuilder()
        assert builder is not None

    def test_index_key(self):
        """Verify index_key returns 'fact'."""
        assert EntityGraphBuilder.index_key() == 'fact'


class TestEntityGraphBuilding:
    """Tests for entity graph building functionality."""

    def _make_fact_node(self, fact_id='f1', subject_id='e1', subject_val='GraphRAG',
                        object_id='e2', object_val='framework', predicate='is',
                        subject_class=None, object_class=None):
        """Create a mock node with valid fact metadata."""
        node = Mock()
        node.node_id = fact_id
        node.metadata = {
            'fact': {
                'factId': fact_id,
                'subject': {
                    'entityId': subject_id,
                    'value': subject_val,
                    'classification': subject_class,
                },
                'predicate': {'value': predicate},
                'object': {
                    'entityId': object_id,
                    'value': object_val,
                    'classification': object_class,
                },
            }
        }
        return node

    def _make_graph_client(self):
        client = Mock()
        client.node_id = Mock(side_effect=lambda field: f'params.{field}')
        client.execute_query_with_retry = Mock()
        return client

    def test_build_inserts_entities(self):
        """Verify build calls execute_query_with_retry for entity insertion."""
        builder = EntityGraphBuilder()
        node = self._make_fact_node()
        client = self._make_graph_client()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        assert client.execute_query_with_retry.called

    def test_build_inserts_both_subject_and_object(self):
        """Verify build inserts both subject and object entities."""
        builder = EntityGraphBuilder()
        node = self._make_fact_node(subject_id='s1', object_id='o1')
        client = self._make_graph_client()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        # Should have at least 2 calls: one for subject, one for object
        assert client.execute_query_with_retry.call_count >= 2

    def test_build_skips_duplicate_object(self):
        """Verify build skips object when it has same entityId as subject."""
        builder = EntityGraphBuilder()
        node = self._make_fact_node(subject_id='same', object_id='same')
        client = self._make_graph_client()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        # Only subject should be inserted
        assert client.execute_query_with_retry.call_count == 1


class TestEntityGraphBuilderErrorHandling:
    """Tests for entity graph builder error handling."""

    def test_build_with_empty_fact_metadata(self):
        """Verify build logs warning for missing fact metadata."""
        builder = EntityGraphBuilder()
        node = Mock()
        node.node_id = 'node_123'
        node.metadata = {}

        client = Mock()
        client.execute_query_with_retry = Mock()

        builder.build(node, client, include_domain_labels=False, include_local_entities=False)

        assert not client.execute_query_with_retry.called
