# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from llama_index.core.vector_stores.types import FilterCondition, MetadataFilters

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
import graphrag_toolkit.lexical_graph.visualisation.graph_notebook.graph_notebook_visualisation as visualisation


@pytest.fixture(params=[FilterCondition.AND, FilterCondition.OR])
def empty_filter_config(request):
    return FilterConfig(source_filters=MetadataFilters(
        filters=[], condition=request.param,
    ))


def test_empty_filter_does_not_add_where_clause(monkeypatch, empty_filter_config):
    # PR #415 changes the graph helper to return an empty string for this input.
    # Stub that result here so this test isolates the notebook query consumer.
    monkeypatch.setattr(
        visualisation,
        'filter_config_to_opencypher_filters',
        lambda _: '',
    )

    query = visualisation.get_sources_query(
        TenantId(), filter=empty_filter_config,
    )

    assert 'WHERE' not in query


def test_empty_filter_does_not_add_leading_or_before_source_ids(
    monkeypatch,
    empty_filter_config,
):
    # PR #415 changes the graph helper to return an empty string for this input.
    # Stub that result here so this test isolates the notebook query consumer.
    monkeypatch.setattr(
        visualisation,
        'filter_config_to_opencypher_filters',
        lambda _: '',
    )

    query = visualisation.get_sources_query(
        TenantId(), source_ids=['source-1'], filter=empty_filter_config,
    )

    assert "WHERE (id(source) in ['source-1'])" in query
    assert 'WHERE  OR' not in query
