# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import warnings
import importlib

from .chunk_based_search import ChunkBasedSearch
from .chunk_based_semantic_search import ChunkBasedSemanticSearch
from .entity_based_search import EntityBasedSearch
from .entity_context_search import EntityContextSearch
from .entity_network_search import EntityNetworkSearch
from .topic_based_search import TopicBasedSearch
from .composite_traversal_based_retriever import CompositeTraversalBasedRetriever, WeightedTraversalBasedRetrieverType
from .query_mode_retriever import QueryModeRetriever
from .chunk_cosine_search import ChunkCosineSimilaritySearch
from .semantic_chunk_beam_search import SemanticChunkBeamGraphSearch

# Mapping of deprecated class names to their module paths within the deprecated sub-package
_DEPRECATED_NAMES = {
    'SemanticGuidedRetriever': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever',
    'SemanticGuidedRetrieverType': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever',
    'SemanticGuidedChunkRetriever': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever',
    'SemanticGuidedChunkRetrieverType': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever',
    'KeywordRankingSearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.keyword_ranking_search',
    'RerankingBeamGraphSearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.rerank_beam_search',
    'SemanticBeamGraphSearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_beam_search',
    'StatementCosineSimilaritySearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.statement_cosine_seach',
}


def __getattr__(name):
    if name in _DEPRECATED_NAMES:
        module_path = _DEPRECATED_NAMES[name]
        module = importlib.import_module(module_path)
        attr = getattr(module, name)
        warnings.warn(
            f"Importing {name} from graphrag_toolkit.lexical_graph.retrieval.retrievers is deprecated. "
            f"Import from {module_path} instead. This import path will be removed in a future release.",
            DeprecationWarning,
            stacklevel=2
        )
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
