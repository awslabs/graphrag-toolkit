# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/summary/graph_summary."""

import time
from unittest.mock import MagicMock

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.retrieval.summary.graph_summary import (
    GraphSummary,
    get_domain,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache


def _summary(llm_response='Domain: x', cached=None):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    llm = MagicMock(spec=LLMCache)
    llm.predict.return_value = llm_response
    summary = GraphSummary(graph_store=graph_store, llm=llm)
    return summary, graph_store, llm


class TestGetDomain:
    def test_extracts_domain_line(self):
        assert get_domain('Domain: Networking\nScope: x') == 'Networking'

    def test_returns_none_when_absent(self):
        assert get_domain('Scope: only this') is None


class TestGetCachedSummary:
    def test_returns_none_when_no_rows(self):
        s, store, _ = _summary()
        store.execute_query.return_value = []
        assert s._get_cached_summary(TenantId(value='t1')) is None

    def test_returns_none_when_stale(self):
        s, store, _ = _summary()
        store.execute_query.return_value = [{
            'summary': 'old',
            'last_updated_datetime': 0,  # 1970 = stale
        }]
        assert s._get_cached_summary(TenantId(value='t1')) is None

    def test_returns_fresh_cached_summary(self):
        s, store, _ = _summary()
        store.execute_query.return_value = [{
            'summary': 'recent',
            'last_updated_datetime': int(time.time()),
        }]
        assert s._get_cached_summary(TenantId(value='t1')) == 'recent'


class TestCreateGraphSummary:
    def test_returns_cached_summary_when_available(self):
        s, store, llm = _summary()
        store.execute_query.return_value = [{
            'summary': 'cached', 'last_updated_datetime': int(time.time()),
        }]
        result = s.create_graph_summary(tenant_id='t1')
        assert result == 'cached'
        llm.predict.assert_not_called()

    def test_generates_summary_when_no_cache(self):
        s, store, llm = _summary(llm_response='Domain: fresh')
        # cache lookup is the only direct execute_query call (the entity/path
        # lookups go through MultiTenantGraphStore.execute_query_with_retry).
        store.execute_query.return_value = []
        store.execute_query_with_retry.side_effect = [
            [{'entity': 'apple [fruit]'}],
            [{'path': '(a)-[r]->(b)'}],
            None,  # cache write
        ]
        result = s.create_graph_summary(tenant_id='t1')
        assert result == 'Domain: fresh'
        llm.predict.assert_called_once()

    def test_returns_none_when_no_entities_or_paths(self):
        s, store, _ = _summary()
        store.execute_query.return_value = []
        store.execute_query_with_retry.side_effect = [[], []]
        assert s.create_graph_summary(tenant_id='t1') is None

    def test_refresh_bypasses_cache(self):
        s, store, llm = _summary(llm_response='Domain: latest')
        store.execute_query_with_retry.side_effect = [
            [{'entity': 'a [x]'}],
            [{'path': '(a)-[r]->(b)'}],
            None,
        ]
        result = s.create_graph_summary(tenant_id='t1', refresh=True)
        assert result == 'Domain: latest'
