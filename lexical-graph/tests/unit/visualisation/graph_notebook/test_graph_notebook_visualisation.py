# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from llama_index.core.vector_stores.types import (
    FilterCondition,
    MetadataFilter,
    MetadataFilters,
)

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
import graphrag_toolkit.lexical_graph.visualisation.graph_notebook.graph_notebook_visualisation as visualisation


@pytest.fixture(params=[
    (FilterCondition.AND, False),
    (FilterCondition.AND, True),
    (FilterCondition.OR, True),
], ids=['and', 'and-of-empty-and', 'or-of-empty-and'])
def empty_filter_config(request):
    condition, nested = request.param
    filters = (
        [MetadataFilters(filters=[], condition=FilterCondition.AND)]
        if nested
        else []
    )
    return FilterConfig(source_filters=MetadataFilters(
        filters=filters, condition=condition,
    ))


def test_empty_filter_does_not_add_where_clause(empty_filter_config):
    query, params = visualisation.get_sources_query(
        TenantId(), filter=empty_filter_config,
    )

    assert 'WHERE' not in query
    assert params == {}


def test_empty_filter_does_not_add_leading_or_before_source_ids(
    empty_filter_config,
):
    query, params = visualisation.get_sources_query(
        TenantId(), source_ids=['source-1'], filter=empty_filter_config,
    )

    assert 'WHERE (id(source) in $source_ids)' in query
    assert 'WHERE  OR' not in query
    assert params == {'source_ids': ['source-1']}


def test_empty_or_filter_matches_no_sources():
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[], condition=FilterCondition.OR,
    ))

    query, _ = visualisation.get_sources_query(TenantId(), filter=filter_config)

    assert 'WHERE false' in query


def test_empty_or_filter_still_returns_the_sources_asked_for_by_id():
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[], condition=FilterCondition.OR,
    ))

    query, params = visualisation.get_sources_query(
        TenantId(), source_ids=['source-1'], filter=filter_config,
    )

    assert 'WHERE false OR (id(source) in $source_ids)' in query
    assert params == {'source_ids': ['source-1']}


def test_an_or_holding_an_empty_and_adds_no_where_clause():
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[
            MetadataFilters(filters=[], condition=FilterCondition.AND),
            MetadataFilter(key='category', value='tech'),
        ],
        condition=FilterCondition.OR,
    ))

    query, _ = visualisation.get_sources_query(TenantId(), filter=filter_config)

    assert 'WHERE' not in query


def test_nested_empty_and_is_ignored_next_to_valid_filter():
    condition = FilterCondition.AND
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[
            MetadataFilters(filters=[], condition=FilterCondition.AND),
            MetadataFilter(key='category', value='tech'),
        ],
        condition=condition,
    ))

    query, _ = visualisation.get_sources_query(TenantId(), filter=filter_config)

    assert "source.`category` = 'tech'" in query
    assert 'WHERE ( AND ' not in query
    assert 'WHERE ( OR ' not in query


def test_source_ids_are_bound_not_written_into_the_query():
    source_id = "x'] OR true //"

    query, params = visualisation.get_sources_query(TenantId(), source_ids=[source_id])

    assert source_id not in query
    assert params == {'source_ids': [source_id]}


def test_source_ids_given_as_a_string_are_refused():
    with pytest.raises(TypeError, match='list of source ids'):
        visualisation.get_sources_query(TenantId(), source_ids='source-1')


def test_get_query_binds_every_id_and_drops_repeats():
    source_topic = {'startId': "s'1", 'intermediateIds': ['c1'], 'endId': 't1'}
    query_parameters = {
        'source_topic_ids': [source_topic, dict(source_topic)],
        'topic_statement_ids': [{'startId': 't1', 'endId': 'l1'}],
        'statement_id_sets': [{'startId': 'l1', 'endIds': ['l2']}],
        'entity_statement_ids': [{'startId': 'e1', 'endIds': ['l1']}],
    }

    query, params = visualisation.get_query(query_parameters)

    for name in query_parameters:
        assert f'UNWIND ${name} AS {name}' in query
    assert "s'1" not in query
    assert params['source_topic_ids'] == [source_topic]
    assert params['topic_statement_ids'] == query_parameters['topic_statement_ids']


def test_get_query_without_sources_binds_no_source_ids():
    query_parameters = {
        'source_topic_ids': [{'startId': 's1', 'intermediateIds': [], 'endId': 't1'}],
        'topic_statement_ids': [],
        'statement_id_sets': [],
        'entity_statement_ids': [],
    }

    query, params = visualisation.get_query(query_parameters, include_sources=False)

    assert '$source_topic_ids' not in query
    assert 'source_topic_ids' not in params


def test_entity_context_query_binds_ids():
    query, params = visualisation.get_entity_context_query({
        'entity_ids': ['e1', "e'2"],
        'entity_entity_ids': [{'startId': 'e1', 'endId': "e'2"}],
    })

    assert 'WHERE id(e) in $entity_ids' in query
    assert 'UNWIND $entity_entity_ids AS entity_entity_ids' in query
    assert "e'2" not in query
    assert params == {
        'entity_ids': ['e1', "e'2"],
        'entity_entity_ids': [{'startId': 'e1', 'endId': "e'2"}],
    }


@pytest.mark.parametrize('depth', [1, 3])
def test_entity_paths_query_binds_one_entity(depth):
    query, params = visualisation.get_entity_paths_query(TenantId(), 'Alice', depth=depth)

    assert f'*1..{depth}]' in query
    assert 'starts with $entity_1' in query
    assert params == {'entity_1': 'alice'}


@pytest.mark.parametrize('depth', [1, 3])
def test_entity_paths_query_binds_two_entities(depth):
    query, params = visualisation.get_entity_paths_query(TenantId(), 'Alice', 'Bob', depth=depth)

    assert f'*1..{depth}]' in query
    assert 'starts with $entity_2' in query
    assert params == {'entity_1': 'alice', 'entity_2': 'bob'}


@pytest.mark.parametrize('depth', [0, 4, -1])
def test_entity_paths_query_refuses_depth_outside_range(depth):
    with pytest.raises(ValueError, match='depth must be between 1-3'):
        visualisation.get_entity_paths_query(TenantId(), 'alice', 'bob', depth=depth)


@pytest.mark.parametrize('depth', ['3', '3]-() DETACH DELETE e1 //', True, None, 2.0])
def test_entity_paths_query_refuses_depth_that_is_not_an_int(depth):
    with pytest.raises(TypeError, match='depth must be an int'):
        visualisation.get_entity_paths_query(TenantId(), 'alice', 'bob', depth=depth)


def _visualisation_with_mock_graph():
    g = MagicMock()

    @contextmanager
    def get_graph(formatting_config):
        yield g

    vis = visualisation.GraphNotebookVisualisation()
    vis._get_graph = get_graph
    return vis, g


def test_display_passes_params_to_the_oc_magic():
    vis, g = _visualisation_with_mock_graph()

    vis.display_sources(source_ids=['source-1'])

    line = g.oc.call_args.args[0]
    assert line.endswith(' -qp params')
    assert g.oc.call_args.kwargs['local_ns'] == {'params': {'source_ids': ['source-1']}}


def test_display_without_params_adds_no_query_parameters():
    vis, g = _visualisation_with_mock_graph()

    vis.display_entities()

    assert '-qp' not in g.oc.call_args.args[0]
    assert g.oc.call_args.kwargs['local_ns'] == {}
