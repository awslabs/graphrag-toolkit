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
    (FilterCondition.OR, False),
    (FilterCondition.AND, True),
    (FilterCondition.OR, True),
], ids=['and', 'or', 'nested-and', 'nested-or'])
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


@pytest.mark.parametrize('condition', [FilterCondition.AND, FilterCondition.OR])
def test_nested_empty_filter_is_ignored_next_to_valid_filter(condition):
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
