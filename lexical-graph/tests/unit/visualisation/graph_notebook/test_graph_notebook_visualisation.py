# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
    query = visualisation.get_sources_query(
        TenantId(), filter=empty_filter_config,
    )

    assert 'WHERE' not in query


def test_empty_filter_does_not_add_leading_or_before_source_ids(
    empty_filter_config,
):
    query = visualisation.get_sources_query(
        TenantId(), source_ids=['source-1'], filter=empty_filter_config,
    )

    assert "WHERE (id(source) in ['source-1'])" in query
    assert 'WHERE  OR' not in query


def test_empty_or_filter_matches_no_sources():
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[], condition=FilterCondition.OR,
    ))

    query = visualisation.get_sources_query(TenantId(), filter=filter_config)

    assert 'WHERE false' in query


def test_empty_or_filter_still_returns_the_sources_asked_for_by_id():
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[], condition=FilterCondition.OR,
    ))

    query = visualisation.get_sources_query(
        TenantId(), source_ids=['source-1'], filter=filter_config,
    )

    assert "WHERE false OR (id(source) in ['source-1'])" in query


def test_an_or_holding_an_empty_and_adds_no_where_clause():
    filter_config = FilterConfig(source_filters=MetadataFilters(
        filters=[
            MetadataFilters(filters=[], condition=FilterCondition.AND),
            MetadataFilter(key='category', value='tech'),
        ],
        condition=FilterCondition.OR,
    ))

    query = visualisation.get_sources_query(TenantId(), filter=filter_config)

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

    query = visualisation.get_sources_query(TenantId(), filter=filter_config)

    assert "source.`category` = 'tech'" in query
    assert 'WHERE ( AND ' not in query
    assert 'WHERE ( OR ' not in query


def test_source_ids_render_as_a_cypher_list():
    query = visualisation.get_sources_query(
        TenantId(), source_ids=['source-1', 'source-2'],
    )

    assert "WHERE (id(source) in ['source-1', 'source-2'])" in query


def test_quote_in_source_id_is_escaped():
    query = visualisation.get_sources_query(
        TenantId(), source_ids=["x'] OR true //"],
    )

    assert "WHERE (id(source) in ['x\\'] OR true //'])" in query


def test_backslash_before_quote_in_source_id_cannot_close_the_literal():
    query = visualisation.get_sources_query(
        TenantId(), source_ids=["x\\'] OR true //"],
    )

    assert "WHERE (id(source) in ['x\\\\\\'] OR true //'])" in query


def test_non_string_source_id_is_refused():
    with pytest.raises(TypeError):
        visualisation.get_sources_query(TenantId(), source_ids=[1])


def test_format_params_renders_ids_as_before():
    params = [
        {'startId': 's1', 'intermediateIds': ['c1', 'c2'], 'endId': 't1'},
        {'startId': 'e1', 'endIds': ['l1', 'l2']},
    ]

    rendered = visualisation.format_params(params)

    assert "{startId: 's1', intermediateIds: ['c1', 'c2'], endId: 't1'}" in rendered
    assert "{startId: 'e1', endIds: ['l1', 'l2']}" in rendered


def test_format_params_escapes_quote_in_id():
    rendered = visualisation.format_params([{'startId': "x'} OR true //", 'endId': 't1'}])

    assert rendered == "[{startId: 'x\\'} OR true //', endId: 't1'}]"


def test_format_params_refuses_non_identifier_key():
    with pytest.raises(ValueError, match='identifier'):
        visualisation.format_params([{'startId: 1} //': 's1'}])


def test_get_query_escapes_quote_in_id():
    query_parameters = {
        'source_topic_ids': [{'startId': "s'1", 'intermediateIds': ["c'1"], 'endId': "t'1"}],
        'topic_statement_ids': [{'startId': "t'1", 'endId': "l'1"}],
        'statement_id_sets': [{'startId': "l'1", 'endIds': ["l'2"]}],
        'entity_statement_ids': [{'startId': "e'1", 'endIds': ["l'1"]}],
    }

    query = visualisation.get_query(query_parameters)

    for escaped in ["'s\\'1'", "['c\\'1']", "'t\\'1'", "'l\\'1'", "['l\\'2']", "'e\\'1'"]:
        assert escaped in query
    assert '"' not in query


def test_entity_context_query_escapes_quote_in_id():
    query = visualisation.get_entity_context_query({
        'entity_ids': ['e1', "e'2"],
        'entity_entity_ids': [{'startId': 'e1', 'endId': "e'2"}],
    })

    assert "WHERE id(e) in ['e1', 'e\\'2']" in query
    assert "{startId: 'e1', endId: 'e\\'2'}" in query


@pytest.mark.parametrize('value', [1, None, {'startId': 1}, [b'id']])
def test_to_cypher_literal_refuses_non_string_values(value):
    with pytest.raises(TypeError):
        visualisation.to_cypher_literal(value)


@pytest.mark.parametrize('depth', [1, 3])
def test_entity_paths_query_accepts_depth_in_range(depth):
    query = visualisation.get_entity_paths_query(TenantId(), 'alice', depth=depth)

    assert f'*1..{depth}]' in query


@pytest.mark.parametrize('depth', [0, 4, '3', '3]-() DETACH DELETE e1 //', True, None])
def test_entity_paths_query_refuses_depth_outside_range(depth):
    with pytest.raises(ValueError, match='depth must be between 1-3'):
        visualisation.get_entity_paths_query(TenantId(), 'alice', 'bob', depth=depth)
