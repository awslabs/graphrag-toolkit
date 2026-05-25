# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for storage/vector/neptune_vector_indexes."""

from unittest.mock import MagicMock, patch

import pytest
from llama_index.core.schema import QueryBundle, TextNode

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import (
    NeptuneAnalyticsClient,
)
from graphrag_toolkit.lexical_graph.storage.vector import neptune_vector_indexes as mod
from graphrag_toolkit.lexical_graph.storage.vector.neptune_vector_indexes import (
    NEPTUNE_ANALYTICS,
    NeptuneAnalyticsVectorIndexFactory,
    NeptuneIndex,
)


def _stub_neptune_client():
    client = MagicMock(spec=NeptuneAnalyticsClient)
    client.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    return client


def _make_index(index_name='chunk', tenant_value=None):
    client = _stub_neptune_client()
    with patch.object(mod, 'GraphStoreFactory') as factory:
        factory.for_graph_store.return_value = client
        idx = NeptuneIndex.for_index(index_name, f'{NEPTUNE_ANALYTICS}my-graph')
    if tenant_value is not None:
        idx.tenant_id = TenantId(value=tenant_value)
    return idx, client


class TestNeptuneAnalyticsVectorIndexFactory:
    def test_returns_none_when_uri_unmatched(self):
        factory = NeptuneAnalyticsVectorIndexFactory()
        assert factory.try_create(['chunk'], 'other://x') is None

    def test_creates_indexes_for_each_name(self):
        factory = NeptuneAnalyticsVectorIndexFactory()
        with patch.object(NeptuneIndex, 'for_index') as for_idx:
            for_idx.return_value = MagicMock()
            result = factory.try_create(['chunk', 'topic'], f'{NEPTUNE_ANALYTICS}g')
        assert len(result) == 2
        assert for_idx.call_count == 2


class TestForIndex:
    def test_chunk_index_sets_source_join_path(self):
        idx, _ = _make_index('chunk')
        assert idx.label == '__Chunk__'
        assert 'EXTRACTED_FROM' in idx.path

    def test_statement_index_sets_statement_path(self):
        idx, _ = _make_index('statement')
        assert idx.label == '__Statement__'
        assert 'MENTIONED_IN' in idx.path

    def test_topic_index_sets_topic_path(self):
        idx, _ = _make_index('topic')
        assert idx.label == '__Topic__'

    def test_invalid_index_name_raises(self):
        client = _stub_neptune_client()
        with patch.object(mod, 'GraphStoreFactory') as factory:
            factory.for_graph_store.return_value = client
            with pytest.raises(ValueError, match='Invalid index name'):
                NeptuneIndex.for_index('bogus', f'{NEPTUNE_ANALYTICS}g')


class TestNeptuneClientWrapping:
    def test_default_tenant_returns_unwrapped_client(self):
        idx, client = _make_index('chunk')
        assert idx._neptune_client() is client

    def test_non_default_tenant_returns_multi_tenant_wrapper(self):
        idx, client = _make_index('chunk', tenant_value='acme')
        wrapper = idx._neptune_client()
        from graphrag_toolkit.lexical_graph.storage.graph.multi_tenant_graph_store import (
            MultiTenantGraphStore,
        )
        assert isinstance(wrapper, MultiTenantGraphStore)
        assert wrapper.inner is client


class TestAddEmbeddings:
    def test_read_only_index_raises(self):
        idx, _ = _make_index('chunk')
        idx.writeable = False
        with pytest.raises(IndexError, match='read-only'):
            idx.add_embeddings([])

    def test_executes_upsert_per_node(self):
        idx, client = _make_index('chunk')
        node_a = TextNode(id_='n1', text='hi', embedding=[0.1, 0.2])
        node_b = TextNode(id_='n2', text='bye', embedding=[0.3, 0.4])
        with patch.object(mod, 'embed_nodes', return_value={
            'n1': [0.1, 0.2], 'n2': [0.3, 0.4],
        }):
            idx.add_embeddings([node_a, node_b])
        assert client.execute_query_with_retry.call_count == 2


class TestTopK:
    def test_returns_result_field_from_each_row(self):
        idx, client = _make_index('chunk')
        client.execute_query.return_value = [
            {'result': {'score': 0.9, 'chunk': {'chunkId': 'c1'}}},
            {'result': {'score': 0.8, 'chunk': {'chunkId': 'c2'}}},
        ]
        with patch.object(mod, 'to_embedded_query') as q:
            q.return_value = QueryBundle(query_str='q', embedding=[0.1, 0.2])
            result = idx.top_k(QueryBundle('q'), top_k=2)
        assert [r['chunk']['chunkId'] for r in result] == ['c1', 'c2']


class TestGetEmbeddings:
    def test_runs_one_query_per_unique_id(self):
        idx, client = _make_index('chunk')
        client.execute_query.return_value = [
            {'result': {'embedding': [0.1], 'chunk': {'chunkId': 'c1'}}},
        ]
        idx.get_embeddings(['c1', 'c1', 'c2'])
        assert client.execute_query.call_count == 2


class TestDeferredMethods:
    def test_update_versioning_returns_empty(self):
        idx, _ = _make_index('chunk')
        assert idx.update_versioning(0) == []

    def test_enable_for_versioning_returns_empty(self):
        idx, _ = _make_index('chunk')
        assert idx.enable_for_versioning() == []

    def test_delete_embeddings_returns_empty(self):
        idx, _ = _make_index('chunk')
        assert idx.delete_embeddings() == []
