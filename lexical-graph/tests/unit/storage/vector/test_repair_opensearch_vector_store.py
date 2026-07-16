# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for repair_opensearch_vector_store()'s per-index error handling.

Covers:
  - A ValueError from index_exists() (e.g. a NextGen-misconfigured collection)
    skips that index and continues the batch instead of aborting the whole run
"""

from unittest.mock import MagicMock, patch

import graphrag_toolkit.lexical_graph.storage.vector.repair_opensearch_vector_store as rovs
from graphrag_toolkit.lexical_graph.storage.constants import ALL_EMBEDDING_INDEXES


def test_valueerror_from_one_index_does_not_abort_the_batch():
    def fake_index_exists(tenant_id, index_name, vector_store):
        if index_name == ALL_EMBEDDING_INDEXES[0]:
            raise ValueError(f"misconfigured NextGen collection for {index_name}")
        return False

    with patch.object(rovs, "GraphStoreFactory") as mock_graph_factory, \
         patch.object(rovs, "VectorStoreFactory") as mock_vector_factory, \
         patch.object(rovs, "index_exists", side_effect=fake_index_exists) as mock_index_exists:
        mock_graph_factory.for_graph_store.return_value = MagicMock()
        mock_vector_factory.for_vector_store.return_value = MagicMock()

        result = rovs.repair_opensearch_vector_store(
            graph_store_info="fake-graph-store",
            vector_store_info="fake-vector-store",
            tenant_ids=["testtenant"],
        )

    # index_exists() was called for every index despite the first one raising
    assert mock_index_exists.call_count == len(ALL_EMBEDDING_INDEXES)
    assert result["results"] == []
