# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for NeptuneIndex._neptune_client.

Covers:
  - Default tenant returns the raw neptune_client
  - Custom tenant wraps it in MultiTenantGraphStore
"""

import pytest
from unittest.mock import MagicMock, patch


class TestNeptuneIndexNeptuneClient:

    def _make_index(self, tenant_value=None):
        from graphrag_toolkit.lexical_graph.storage.vector.neptune_vector_indexes import NeptuneIndex
        from graphrag_toolkit.lexical_graph.storage.graph.neptune_graph_stores import NeptuneAnalyticsClient
        from graphrag_toolkit.lexical_graph.tenant_id import TenantId

        neptune_client = NeptuneAnalyticsClient(graph_id="test-graph-id")
        mock_embed = MagicMock()

        index = NeptuneIndex(
            index_name="chunk",
            neptune_client=neptune_client,
            embed_model=mock_embed,
            dimensions=1024,
            id_name="chunkId",
            label="__Chunk__",
            path="(chunk)",
            return_fields="chunk: {chunkId: chunk.chunkId}",
            tenant_id=TenantId(tenant_value) if tenant_value else TenantId(),
        )
        return index, neptune_client

    def test_default_tenant_returns_raw_client(self):
        index, mock_neptune = self._make_index()
        result = index._neptune_client()
        assert result is mock_neptune

    def test_custom_tenant_wraps_in_multi_tenant_store(self):
        from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore

        index, mock_neptune = self._make_index(tenant_value="acme")

        mock_wrapped = MagicMock()
        with patch.object(MultiTenantGraphStore, "wrap", return_value=mock_wrapped) as mock_wrap:
            result = index._neptune_client()
            assert result is mock_wrapped
            mock_wrap.assert_called_once()
