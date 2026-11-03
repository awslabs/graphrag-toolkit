# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Example custom retriever: traversal search with a concurrent chunk prefetch.

This packages the change proposed in PR #221 as a custom retriever, so it can be
benchmarked without modifying lexical-graph. In CompositeTraversalBasedRetriever,
the entity-context phase (``_init``) runs before each sub-retriever's
``get_start_node_ids``. ChunkBasedSearch's start nodes come from a chunk vector
search that does not depend on the entity contexts, so it can start alongside
``_init`` instead of after it.

Benchmark it against the same retriever with the prefetch turned off, so the two
runs differ only in the prefetch::

    export BENCHMARK_RETRIEVER=custom:benchmarks.examples.prefetch_traversal_retriever.PrefetchTraversalRetriever
    # ... run the query and evaluate stages, then:
    export BENCHMARK_RETRIEVER=custom:benchmarks.examples.prefetch_traversal_retriever.without_prefetch

Each run writes ``benchmark-results/<dataset>/<BENCHMARK_RETRIEVER>/``. Pass
ProcessorArgs (e.g. ``vss_top_k``) through ``BENCHMARK_CUSTOM_RETRIEVER_KWARGS``,
for example ``'{"vss_top_k": 10}'``.
"""

import concurrent.futures
import dataclasses
import functools
from typing import Any, Dict, Optional

from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.retrieval.retrievers import ChunkBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.composite_traversal_based_retriever import (
    CompositeTraversalBasedRetriever,
)
from graphrag_toolkit.lexical_graph.retrieval.utils.vector_utils import get_diverse_vss_elements


class PrefetchedChunkBasedSearch(ChunkBasedSearch):
    """ChunkBasedSearch that takes its start-node chunks from a prefetch future, if given one."""

    def __init__(self, *args, prefetched_chunks: Optional[concurrent.futures.Future] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefetched_chunks = prefetched_chunks

    def get_start_node_ids(self, query_bundle):
        if self.prefetched_chunks is None:
            return super().get_start_node_ids(query_bundle)
        # Consume once, so a reused instance falls back to the inline search.
        future, self.prefetched_chunks = self.prefetched_chunks, None
        return [chunk['chunk']['chunkId'] for chunk in future.result()]


class PrefetchingCompositeTraversalBasedRetriever(CompositeTraversalBasedRetriever):
    """Starts ChunkBasedSearch's chunk vector search concurrently with ``_init``."""

    def _retrieve(self, query_bundle):
        chunk_slots = [
            i for i, wr in enumerate(self.weighted_retrievers)
            if wr.retriever is ChunkBasedSearch
        ]
        # Subqueries would need their own vector searches, and with no
        # ChunkBasedSearch there is nothing to prefetch.
        if self.args.derive_subqueries or not chunk_slots:
            return super()._retrieve(query_bundle)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix='chunk-prefetch') as executor:
            # Same call ChunkBasedSearch.get_start_node_ids makes; the sub-retriever
            # inherits this retriever's args, stores and filter config.
            prefetch = executor.submit(
                get_diverse_vss_elements,
                'chunk',
                query_bundle,
                self.vector_store,
                self.args.vss_diversity_factor,
                self.args.vss_top_k,
                self.filter_config,
                graph_store=self.graph_store,
            )
            original = self.weighted_retrievers
            self.weighted_retrievers = [
                dataclasses.replace(
                    wr, retriever=functools.partial(PrefetchedChunkBasedSearch, prefetched_chunks=prefetch)
                ) if i in chunk_slots else wr
                for i, wr in enumerate(original)
            ]
            try:
                return super()._retrieve(query_bundle)
            finally:
                self.weighted_retrievers = original


class PrefetchTraversalRetriever:
    """Traversal-based search (ChunkBasedSearch + EntityNetworkSearch) as a custom retriever.

    Satisfies benchmarks.utils.custom_retriever.CustomRetriever: ``query`` returns
    the LexicalGraphQueryEngine response, and ``get_config`` reports the settings
    recorded in metrics_summary.json.
    """

    def __init__(self, graph_store, vector_store, llm=None, prefetch: bool = True, **processor_args):
        self.prefetch = prefetch
        self.processor_args = processor_args
        retriever = PrefetchingCompositeTraversalBasedRetriever if prefetch else CompositeTraversalBasedRetriever
        self._query_engine = LexicalGraphQueryEngine(
            graph_store,
            vector_store,
            retriever=retriever,
            context_format='text',
            **({'llm': llm} if llm else {}),
            **processor_args,
        )

    def query(self, question: str):
        return self._query_engine.query(question)

    def get_config(self) -> Dict[str, Any]:
        return {'prefetch': self.prefetch, **self.processor_args}


def without_prefetch(graph_store, vector_store, llm=None, **processor_args) -> PrefetchTraversalRetriever:
    """The same retriever with the prefetch turned off: the baseline for comparison."""
    return PrefetchTraversalRetriever(graph_store, vector_store, llm=llm, prefetch=False, **processor_args)
