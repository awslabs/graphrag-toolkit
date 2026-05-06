# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for deprecated semantic-guided retriever modules.

This module verifies:
- Each deprecated module is importable from the deprecated/ sub-package
- Backward-compatible imports resolve correctly from the retrievers package
"""

import importlib

import pytest
from unittest.mock import Mock, patch, MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

class TestDeprecatedModuleImports:
    """Verify each deprecated module is importable from the deprecated/ sub-package."""

    def test_import_semantic_guided_retriever(self):
        """SemanticGuidedRetriever is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever import (
            SemanticGuidedRetriever,
            SemanticGuidedRetrieverType,
        )
        assert SemanticGuidedRetriever is not None
        assert SemanticGuidedRetrieverType is not None

    def test_import_semantic_guided_base_retriever(self):
        """SemanticGuidedBaseRetriever is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_base_retriever import (
            SemanticGuidedBaseRetriever,
        )
        assert SemanticGuidedBaseRetriever is not None

    def test_import_semantic_guided_chunk_retriever(self):
        """SemanticGuidedChunkRetriever is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever import (
            SemanticGuidedChunkRetriever,
            SemanticGuidedChunkRetrieverType,
        )
        assert SemanticGuidedChunkRetriever is not None
        assert SemanticGuidedChunkRetrieverType is not None

    def test_import_semantic_guided_base_chunk_retriever(self):
        """SemanticGuidedBaseChunkRetriever is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_base_chunk_retriever import (
            SemanticGuidedBaseChunkRetriever,
        )
        assert SemanticGuidedBaseChunkRetriever is not None

    def test_import_semantic_beam_search(self):
        """SemanticBeamGraphSearch is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_beam_search import (
            SemanticBeamGraphSearch,
        )
        assert SemanticBeamGraphSearch is not None

    def test_import_statement_cosine_search(self):
        """StatementCosineSimilaritySearch is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.statement_cosine_seach import (
            StatementCosineSimilaritySearch,
        )
        assert StatementCosineSimilaritySearch is not None

    def test_import_keyword_ranking_search(self):
        """KeywordRankingSearch is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.keyword_ranking_search import (
            KeywordRankingSearch,
        )
        assert KeywordRankingSearch is not None

    def test_import_rerank_beam_search(self):
        """RerankingBeamGraphSearch is importable from deprecated sub-package."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.rerank_beam_search import (
            RerankingBeamGraphSearch,
        )
        assert RerankingBeamGraphSearch is not None

    def test_import_all_from_deprecated_init(self):
        """All deprecated classes are importable from the deprecated __init__."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated import (
            SemanticGuidedRetriever,
            SemanticGuidedRetrieverType,
            SemanticGuidedBaseRetriever,
            SemanticGuidedChunkRetriever,
            SemanticGuidedChunkRetrieverType,
            SemanticGuidedBaseChunkRetriever,
            SemanticBeamGraphSearch,
            StatementCosineSimilaritySearch,
            KeywordRankingSearch,
            RerankingBeamGraphSearch,
        )
        assert all(cls is not None for cls in [
            SemanticGuidedRetriever,
            SemanticGuidedRetrieverType,
            SemanticGuidedBaseRetriever,
            SemanticGuidedChunkRetriever,
            SemanticGuidedChunkRetrieverType,
            SemanticGuidedBaseChunkRetriever,
            SemanticBeamGraphSearch,
            StatementCosineSimilaritySearch,
            KeywordRankingSearch,
            RerankingBeamGraphSearch,
        ])


# The set of deprecated class names that should be backward-compatible
_DEPRECATED_CLASS_NAMES = [
    'SemanticGuidedRetriever',
    'SemanticGuidedRetrieverType',
    'SemanticGuidedChunkRetriever',
    'SemanticGuidedChunkRetrieverType',
    'KeywordRankingSearch',
    'RerankingBeamGraphSearch',
    'SemanticBeamGraphSearch',
    'StatementCosineSimilaritySearch',
]

# Mapping from class name to the deprecated module path
_DEPRECATED_MODULE_PATHS = {
    'SemanticGuidedRetriever': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever',
    'SemanticGuidedRetrieverType': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever',
    'SemanticGuidedChunkRetriever': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever',
    'SemanticGuidedChunkRetrieverType': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever',
    'KeywordRankingSearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.keyword_ranking_search',
    'RerankingBeamGraphSearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.rerank_beam_search',
    'SemanticBeamGraphSearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_beam_search',
    'StatementCosineSimilaritySearch': 'graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.statement_cosine_seach',
}


class TestBackwardCompatibleImports:
    """Backward-compatible imports resolve correctly.

    For any deprecated class name, importing from the original retrievers package should
    resolve to the same class object as importing from the deprecated sub-package.
    """

    @given(name=st.sampled_from(_DEPRECATED_CLASS_NAMES))
    def test_backward_compatible_import_resolves(self, name):
        """Backward-compatible imports resolve to the correct class."""
        # Import from the new (deprecated) location directly
        deprecated_module_path = _DEPRECATED_MODULE_PATHS[name]
        deprecated_module = importlib.import_module(deprecated_module_path)
        expected_class = getattr(deprecated_module, name)

        # Import from the old location (retrievers package) via __getattr__
        retrievers_module = importlib.import_module(
            'graphrag_toolkit.lexical_graph.retrieval.retrievers'
        )

        actual_class = getattr(retrievers_module, name)

        # The resolved class should be the same object
        assert actual_class is expected_class, (
            f"Importing {name} from retrievers package did not resolve to the same "
            f"class as importing from {deprecated_module_path}"
        )
