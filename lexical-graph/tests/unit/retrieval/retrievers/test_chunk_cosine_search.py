# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/retrievers/chunk_cosine_search."""

from unittest.mock import MagicMock

import numpy as np
from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_cosine_search import (
    ChunkCosineSimilaritySearch,
)


def _retriever(chunk_results, embeddings):
    graph_store = MagicMock()
    vector_store = MagicMock()
    chunk_index = MagicMock()
    chunk_index.top_k.return_value = chunk_results
    vector_store.get_index.return_value = chunk_index

    embedding_cache = MagicMock()
    embedding_cache.get_embeddings.return_value = embeddings

    return ChunkCosineSimilaritySearch(
        vector_store=vector_store,
        graph_store=graph_store,
        embedding_cache=embedding_cache,
        top_k=2,
    )


class TestChunkCosineSimilaritySearch:
    def test_returns_top_k_nodes_with_scores(self):
        chunk_results = [
            {'chunk': {'chunkId': 'c1'}},
            {'chunk': {'chunkId': 'c2'}},
            {'chunk': {'chunkId': 'c3'}},
        ]
        embeddings = {
            'c1': np.array([1.0, 0.0]),
            'c2': np.array([0.0, 1.0]),
            'c3': np.array([1.0, 1.0]),
        }
        retriever = _retriever(chunk_results, embeddings)
        query = QueryBundle(query_str='q', embedding=[1.0, 0.0])

        nodes = retriever._retrieve(query)

        assert len(nodes) == 2
        # Highest similarity is 'c1' against query [1,0]
        assert nodes[0].node.metadata['chunk']['chunkId'] == 'c1'

    def test_metadata_includes_search_type(self):
        chunk_results = [{'chunk': {'chunkId': 'c1'}}]
        embeddings = {'c1': np.array([1.0, 0.0])}
        retriever = _retriever(chunk_results, embeddings)

        nodes = retriever._retrieve(QueryBundle(query_str='q', embedding=[1.0, 0.0]))

        assert nodes[0].node.metadata['search_type'] == 'cosine_similarity'

    def test_empty_chunk_results_returns_empty(self):
        retriever = _retriever([], {})
        nodes = retriever._retrieve(QueryBundle(query_str='q', embedding=[1.0, 0.0]))
        assert nodes == []
