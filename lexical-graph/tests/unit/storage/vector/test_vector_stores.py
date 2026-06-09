# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for VectorStore base + MultiTenantVectorStore + ReadOnlyVectorStore."""

import pytest

from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import (
    DummyVectorIndex,
)
from graphrag_toolkit.lexical_graph.storage.vector.multi_tenant_vector_store import (
    MultiTenantVectorStore,
)
from graphrag_toolkit.lexical_graph.storage.vector.read_only_vector_store import (
    ReadOnlyVectorStore,
)
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore


class TestVectorStore:
    def test_unknown_index_name_raises(self):
        store = VectorStore()
        with pytest.raises(ValueError, match='Invalid index name'):
            store.get_index('bogus')

    def test_missing_known_index_returns_dummy(self):
        store = VectorStore()
        result = store.get_index('chunk')
        assert isinstance(result, DummyVectorIndex)

    def test_registered_index_is_returned(self):
        index = DummyVectorIndex(index_name='chunk')
        store = VectorStore(indexes={'chunk': index})
        assert store.get_index('chunk') is index

    def test_all_indexes_returns_registered_values(self):
        a = DummyVectorIndex(index_name='chunk')
        b = DummyVectorIndex(index_name='statement')
        store = VectorStore(indexes={'chunk': a, 'statement': b})
        result = store.all_indexes()
        assert len(result) == 2
        assert a in result and b in result

    def test_context_manager_yields_self(self):
        store = VectorStore()
        with store as ctx:
            assert ctx is store


class TestMultiTenantVectorStore:
    def test_wrap_returns_existing_wrapper_unchanged(self):
        inner = VectorStore()
        existing = MultiTenantVectorStore(inner=inner, tenant_id=TenantId(value='a'))
        result = MultiTenantVectorStore.wrap(existing, TenantId(value='b'))
        assert result is existing

    def test_wrap_wraps_plain_store(self):
        inner = VectorStore()
        wrapped = MultiTenantVectorStore.wrap(inner, TenantId(value='acme'))
        assert isinstance(wrapped, MultiTenantVectorStore)
        assert wrapped.inner is inner

    def test_get_index_stamps_tenant_id(self):
        index = DummyVectorIndex(index_name='chunk')
        inner = VectorStore(indexes={'chunk': index})
        wrapped = MultiTenantVectorStore(inner=inner, tenant_id=TenantId(value='acme'))
        out = wrapped.get_index('chunk')
        assert out.tenant_id.value == 'acme'

    def test_all_indexes_returns_tenant_stamped_indexes(self):
        a = DummyVectorIndex(index_name='chunk')
        b = DummyVectorIndex(index_name='statement')
        inner = VectorStore(indexes={'chunk': a, 'statement': b})
        wrapped = MultiTenantVectorStore(inner=inner, tenant_id=TenantId(value='acme'))
        result = wrapped.all_indexes()
        assert len(result) == 2
        assert all(idx.tenant_id.value == 'acme' for idx in result)


class TestReadOnlyVectorStore:
    def test_wrap_returns_existing_wrapper_unchanged(self):
        inner = VectorStore()
        existing = ReadOnlyVectorStore(inner=inner)
        result = ReadOnlyVectorStore.wrap(existing)
        assert result is existing

    def test_wrap_wraps_plain_store(self):
        inner = VectorStore()
        wrapped = ReadOnlyVectorStore.wrap(inner)
        assert isinstance(wrapped, ReadOnlyVectorStore)

    def test_get_index_marks_index_not_writeable(self):
        index = DummyVectorIndex(index_name='chunk')
        index.writeable = True
        inner = VectorStore(indexes={'chunk': index})
        wrapped = ReadOnlyVectorStore(inner=inner)
        out = wrapped.get_index('chunk')
        assert out.writeable is False

    def test_all_indexes_marks_each_not_writeable(self):
        a = DummyVectorIndex(index_name='chunk')
        b = DummyVectorIndex(index_name='statement')
        a.writeable = True
        b.writeable = True
        inner = VectorStore(indexes={'chunk': a, 'statement': b})
        wrapped = ReadOnlyVectorStore(inner=inner)
        for idx in wrapped.all_indexes():
            assert idx.writeable is False
