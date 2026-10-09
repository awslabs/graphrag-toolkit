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
