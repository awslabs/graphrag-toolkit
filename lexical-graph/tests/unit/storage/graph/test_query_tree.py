# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for query_tree."""

from unittest.mock import MagicMock

import pytest

from graphrag_toolkit.lexical_graph.storage.graph.query_tree import (
    DEFAULT_PARAMS_ADAPTER,
    Job,
    Query,
    QueryTree,
    _default_params_adapter,
)


class TestDefaultParamsAdapter:
    def test_dict_passthrough(self):
        assert _default_params_adapter({'a': 1}) == {'a': 1}

    def test_list_wrapped_under_params_key(self):
        result = _default_params_adapter(['x', 'y'])
        assert result == {'params': ['x', 'y']}

    def test_list_dedups_case_insensitive(self):
        # Duplicates keyed by str(p).lower() are collapsed; later wins.
        result = _default_params_adapter(['Foo', 'foo', 'BAR'])
        assert result == {'params': ['foo', 'BAR']}

    def test_generator_drained_and_deduped(self):
        def gen():
            yield 'Foo'
            yield 'foo'
            yield 'baz'

        result = _default_params_adapter(gen())
        assert result == {'params': ['foo', 'baz']}

    def test_empty_list(self):
        assert _default_params_adapter([]) == {'params': []}

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match='Invalid input parameters'):
            _default_params_adapter('not-a-list')

    def test_default_constant_aliases_underscore_function(self):
        assert DEFAULT_PARAMS_ADAPTER is _default_params_adapter


class TestQuery:
    def test_defaults(self):
        q = Query('MATCH (n) RETURN n')
        assert q.query == 'MATCH (n) RETURN n'
        assert q.params_adapter is DEFAULT_PARAMS_ADAPTER
        assert q.child_queries == []

    def test_custom_adapter_kept(self):
        adapter = lambda v: {'custom': v}
        q = Query('MATCH (n)', params_adapter=adapter)
        assert q.params_adapter is adapter

    def test_child_queries_kept(self):
        child = Query('MATCH (c)')
        q = Query('MATCH (n)', child_queries=[child])
        assert q.child_queries == [child]


class TestJob:
    def test_run_invokes_store_with_adapted_params(self):
        q = Query('MATCH (n) RETURN n')
        job = Job(q, params=['Foo', 'foo'])
        store = MagicMock(return_value=[{'n': 1}])

        result = job.run(store)

        store.assert_called_once_with('MATCH (n) RETURN n', {'params': ['foo']})
        assert result == [{'n': 1}]

    def test_run_uses_custom_adapter(self):
        adapter = lambda v: {'wrapped': v}
        q = Query('MATCH (n)', params_adapter=adapter)
        job = Job(q, params='raw')
        store = MagicMock(return_value=[])

        job.run(store)

        store.assert_called_once_with('MATCH (n)', {'wrapped': 'raw'})


class TestQueryTree:
    def test_id_is_prefixed(self):
        tree = QueryTree('lookup', Query('MATCH (n)'))
        assert tree.id == 'query-tree-lookup'

    def test_leaf_query_yields_each_result(self):
        root = Query('MATCH (n) RETURN n')
        store = MagicMock(return_value=[{'n': 1}, {'n': 2}, {'n': 3}])
        tree = QueryTree('leaf', root)

        results = list(tree.run({'k': 'v'}, store))

        store.assert_called_once_with('MATCH (n) RETURN n', {'k': 'v'})
        assert results == [{'n': 1}, {'n': 2}, {'n': 3}]

    def test_child_query_consumes_parent_results(self):
        child = Query('MATCH (c)')
        root = Query('MATCH (n)', child_queries=[child])

        store = MagicMock(side_effect=[['Foo', 'foo', 'Bar'], [{'c': 'final'}]])
        tree = QueryTree('with-child', root)

        results = list(tree.run({'k': 'v'}, store))

        assert store.call_count == 2
        assert store.call_args_list[0].args == ('MATCH (n)', {'k': 'v'})
        assert store.call_args_list[1].args == ('MATCH (c)', {'params': ['foo', 'Bar']})
        assert results == [{'c': 'final'}]

    def test_multiple_child_queries_all_run(self):
        child_a = Query('MATCH (a)')
        child_b = Query('MATCH (b)')
        root = Query('MATCH (n)', child_queries=[child_a, child_b])

        store = MagicMock(
            side_effect=[
                [{'r': 1}],
                [{'b': 'leaf-b'}],
                [{'a': 'leaf-a'}],
            ]
        )
        tree = QueryTree('multi-child', root)

        results = list(tree.run({}, store))

        assert store.call_count == 3
        # Order is LIFO because the implementation uses list.pop() with no index.
        assert results == [{'b': 'leaf-b'}, {'a': 'leaf-a'}]
