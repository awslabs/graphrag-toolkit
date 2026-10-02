# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for storage/graph/multi_tenant_graph_store."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.multi_tenant_graph_store import (
    MultiTenantGraphStore,
)
from graphrag_toolkit.lexical_graph.storage.graph.graph_query_operation import GraphQueryOperation
from graphrag_toolkit.lexical_graph.storage.graph.query_tree import Query, QueryTree


def _wrap(tenant_value=None, labels=None):
    inner = MagicMock(spec=DummyGraphStore)
    tenant_id = TenantId() if tenant_value is None else TenantId(value=tenant_value)
    return MultiTenantGraphStore(
        inner=inner,
        tenant_id=tenant_id,
        labels=labels or ['Source', 'Chunk'],
    ), inner


class TestWrap:
    def test_returns_existing_multi_tenant_store_unchanged(self):
        inner = MagicMock(spec=DummyGraphStore)
        existing = MultiTenantGraphStore(inner=inner, tenant_id=TenantId(value='t1'))
        result = MultiTenantGraphStore.wrap(existing, TenantId(value='t2'))
        assert result is existing

    def test_wraps_plain_graph_store(self):
        inner = MagicMock(spec=DummyGraphStore)
        wrapped = MultiTenantGraphStore.wrap(inner, TenantId(value='acme'))
        assert isinstance(wrapped, MultiTenantGraphStore)
        assert wrapped.inner is inner


class TestRewriteQuery:
    def test_default_tenant_passes_query_through(self):
        store, _ = _wrap(tenant_value=None)
        cypher = 'MATCH (n:`Source`) RETURN n'
        assert store._rewrite_query(cypher) == cypher

    def test_non_default_tenant_appends_tenant_to_labels(self):
        store, _ = _wrap(tenant_value='acme', labels=['Source', 'Chunk'])
        cypher = 'MATCH (s:`Source`)-[]->(c:`Chunk`) RETURN s, c'
        rewritten = store._rewrite_query(cypher)
        assert '`Sourceacme__`' in rewritten
        assert '`Chunkacme__`' in rewritten
        assert '`Source`' not in rewritten

    def test_only_labels_in_list_are_rewritten(self):
        store, _ = _wrap(tenant_value='acme', labels=['Source'])
        cypher = 'MATCH (s:`Source`), (o:`Other`) RETURN s, o'
        rewritten = store._rewrite_query(cypher)
        assert '`Sourceacme__`' in rewritten
        assert '`Other`' in rewritten


class TestDelegation:
    @pytest.mark.parametrize('operation', [None, GraphQueryOperation.GET_FACTS])
    def test_native_execution_preserves_tenant_labels_and_correlation_id(self, operation):
        inner = DummyGraphStore()
        store = MultiTenantGraphStore(
            inner=inner, tenant_id=TenantId(value='acme'), labels=['Source'],
        )
        parameters = {'sourceId': 's1'}
        results = [{'sourceId': 's1'}]

        with patch(
            'graphrag_toolkit.lexical_graph.storage.graph.graph_store.uuid.uuid4',
            return_value=Mock(hex='abcde12345'),
        ), patch.object(
            DummyGraphStore, '_execute_query', autospec=True, return_value=results,
        ) as execute_query:
            result = store.execute_query_with_retry(
                'MATCH (n:`Source`) RETURN n',
                parameters,
                max_attempts=1,
                max_wait=0,
                correlation_id='request-1',
                operation=operation,
            )

        assert result == results
        execute_query.assert_called_once_with(
            inner,
            'MATCH (n:`Sourceacme__`) RETURN n',
            parameters,
            correlation_id='request-1/abcde',
        )

    def test_operation_override_receives_tenant_context(self):
        inner = DummyGraphStore()
        store = MultiTenantGraphStore(
            inner=inner, tenant_id=TenantId(value='acme'), labels=['Source'],
        )
        parameters = {'sourceId': 's1'}
        results = [{'sourceId': 's1'}]

        with patch(
            'graphrag_toolkit.lexical_graph.storage.graph.graph_store.uuid.uuid4',
            return_value=Mock(hex='abcde12345'),
        ), patch.object(
            DummyGraphStore, '_execute_operation', autospec=True, return_value=results,
        ) as execute_operation:
            result = store.execute_query_with_retry(
                'MATCH (n:`Source`) RETURN n',
                parameters,
                max_attempts=1,
                max_wait=0,
                correlation_id='request-1',
                operation=GraphQueryOperation.GET_FACTS,
            )

        assert result == results
        execute_operation.assert_called_once_with(
            inner,
            GraphQueryOperation.GET_FACTS,
            'MATCH (n:`Sourceacme__`) RETURN n',
            parameters,
            correlation_id='request-1/abcde',
            tenant_id='acme',
        )

    def test_execute_query_with_retry_rewrites_and_delegates(self):
        store, inner = _wrap(tenant_value='acme', labels=['Source'])
        store.execute_query_with_retry('MATCH (n:`Source`)', {'k': 1})
        called_query = inner.execute_query_with_retry.call_args.kwargs['query']
        assert '`Sourceacme__`' in called_query
        assert inner.execute_query_with_retry.call_args.kwargs['parameters'] == {'k': 1}
        assert 'tenant_id' not in inner.execute_query_with_retry.call_args.kwargs

    def test_operation_receives_tenant_id(self):
        store, inner = _wrap(tenant_value='acme', labels=['Source'])

        store.execute_query_with_retry(
            'MATCH (n:`Source`)',
            {'k': 1},
            operation=GraphQueryOperation.GET_FACTS,
        )

        kwargs = inner.execute_query_with_retry.call_args.kwargs
        assert kwargs['operation'] is GraphQueryOperation.GET_FACTS
        assert kwargs['tenant_id'] == 'acme'

    def test_query_tree_operations_receive_tenant_id(self):
        store, inner = _wrap(tenant_value='acme', labels=['Source'])
        inner.execute_query_with_retry.return_value = []
        tree = QueryTree(
            'lookup',
            Query('MATCH (n:`Source`)', operation=GraphQueryOperation.GET_FACTS),
        )

        list(store.execute_query_with_retry(tree, {'statementIds': ['s1']}))

        kwargs = inner.execute_query_with_retry.call_args.kwargs
        assert kwargs['operation'] is GraphQueryOperation.GET_FACTS
        assert kwargs['tenant_id'] == 'acme'

    def test_execute_query_rewrites_and_delegates(self):
        store, inner = _wrap(tenant_value='acme', labels=['Source'])
        store._execute_query('MATCH (n:`Source`)', {'k': 1})
        assert '`Sourceacme__`' in inner._execute_query.call_args.kwargs['cypher']

    def test_node_id_delegates(self):
        store, inner = _wrap()
        store.node_id('n.id')
        inner.node_id.assert_called_once_with(id_name='n.id')

    def test_property_assigment_fn_delegates(self):
        store, inner = _wrap()
        store.property_assigment_fn('k', 'v')
        inner.property_assigment_fn.assert_called_with('k', 'v')

    def test_logging_prefix_delegates(self):
        store, inner = _wrap()
        store._logging_prefix('q1', correlation_id='c1')
        inner._logging_prefix.assert_called_once_with(query_id='q1', correlation_id='c1')

    def test_init_passes_outer_store_as_self(self):
        store, inner = _wrap()
        store.init()
        inner.init.assert_called_once_with(store)

    def test_init_uses_provided_store(self):
        store, inner = _wrap()
        explicit = MagicMock()
        store.init(graph_store=explicit)
        inner.init.assert_called_once_with(explicit)
