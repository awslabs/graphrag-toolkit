# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for deprecated semantic-guided retriever modules.

This module verifies:
- Each deprecated module is importable from the deprecated/ sub-package (Task 5.5)
- SemanticGuidedRetriever.__init__ emits DeprecationWarning (Task 5.6)
- SemanticGuidedChunkRetriever.__init__ emits DeprecationWarning (Task 5.7)
- LexicalGraphQueryEngine.for_semantic_guided_search() emits DeprecationWarning (Task 5.8)
- Property 1: backward-compatible imports resolve correctly and emit warnings (Task 5.9)
"""

import importlib
import warnings

import pytest
from unittest.mock import Mock, patch, MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Task 5.5: Verify each deprecated module is importable from deprecated/ sub-package
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Task 5.6: SemanticGuidedRetriever.__init__ emits DeprecationWarning
# ---------------------------------------------------------------------------

class TestSemanticGuidedRetrieverDeprecation:
    """Verify SemanticGuidedRetriever emits DeprecationWarning on instantiation."""

    def _make_mock_vector_store(self):
        """Create a mock VectorStore with a statement index."""
        from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
        from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndex

        mock_store = MagicMock(spec=VectorStore)
        mock_index = MagicMock(spec=DummyVectorIndex)
        mock_store.get_index.return_value = mock_index
        return mock_store

    def _make_mock_graph_store(self):
        """Create a mock GraphStore."""
        from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

        mock_store = MagicMock(spec=GraphStore)
        return mock_store

    def test_init_emits_deprecation_warning(self):
        """SemanticGuidedRetriever.__init__ emits DeprecationWarning."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever import (
            SemanticGuidedRetriever,
        )

        vector_store = self._make_mock_vector_store()
        graph_store = self._make_mock_graph_store()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            SemanticGuidedRetriever(
                vector_store=vector_store,
                graph_store=graph_store,
                retrievers=[],
            )

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1

        # Find the warning from SemanticGuidedRetriever specifically
        retriever_warnings = [
            x for x in deprecation_warnings
            if "SemanticGuidedRetriever" in str(x.message)
        ]
        assert len(retriever_warnings) == 1
        msg = str(retriever_warnings[0].message)
        assert "CompositeTraversalBasedRetriever" in msg
        assert "deprecated" in msg

    def test_init_warning_mentions_removal(self):
        """Warning message mentions future removal."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_retriever import (
            SemanticGuidedRetriever,
        )

        vector_store = self._make_mock_vector_store()
        graph_store = self._make_mock_graph_store()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            SemanticGuidedRetriever(
                vector_store=vector_store,
                graph_store=graph_store,
                retrievers=[],
            )

        retriever_warnings = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "SemanticGuidedRetriever" in str(x.message)
        ]
        assert len(retriever_warnings) == 1
        assert "removed in a future release" in str(retriever_warnings[0].message)


# ---------------------------------------------------------------------------
# Task 5.7: SemanticGuidedChunkRetriever.__init__ emits DeprecationWarning
# ---------------------------------------------------------------------------

class TestSemanticGuidedChunkRetrieverDeprecation:
    """Verify SemanticGuidedChunkRetriever emits DeprecationWarning on instantiation."""

    def _make_mock_vector_store(self):
        """Create a mock VectorStore with a chunk index."""
        from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
        from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndex

        mock_store = MagicMock(spec=VectorStore)
        mock_index = MagicMock(spec=DummyVectorIndex)
        mock_store.get_index.return_value = mock_index
        return mock_store

    def _make_mock_graph_store(self):
        """Create a mock GraphStore."""
        from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

        mock_store = MagicMock(spec=GraphStore)
        return mock_store

    def test_init_emits_deprecation_warning(self):
        """SemanticGuidedChunkRetriever.__init__ emits DeprecationWarning."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever import (
            SemanticGuidedChunkRetriever,
        )

        vector_store = self._make_mock_vector_store()
        graph_store = self._make_mock_graph_store()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            SemanticGuidedChunkRetriever(
                vector_store=vector_store,
                graph_store=graph_store,
                retrievers=[],
            )

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1

        retriever_warnings = [
            x for x in deprecation_warnings
            if "SemanticGuidedChunkRetriever" in str(x.message)
        ]
        assert len(retriever_warnings) == 1
        msg = str(retriever_warnings[0].message)
        assert "deprecated" in msg
        assert "CompositeTraversalBasedRetriever" in msg

    def test_init_warning_mentions_removal(self):
        """Warning message mentions future removal."""
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_chunk_retriever import (
            SemanticGuidedChunkRetriever,
        )

        vector_store = self._make_mock_vector_store()
        graph_store = self._make_mock_graph_store()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            SemanticGuidedChunkRetriever(
                vector_store=vector_store,
                graph_store=graph_store,
                retrievers=[],
            )

        retriever_warnings = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "SemanticGuidedChunkRetriever" in str(x.message)
        ]
        assert len(retriever_warnings) == 1
        assert "removed in a future release" in str(retriever_warnings[0].message)


# ---------------------------------------------------------------------------
# Task 5.8: LexicalGraphQueryEngine.for_semantic_guided_search() emits DeprecationWarning
# ---------------------------------------------------------------------------

class TestForSemanticGuidedSearchDeprecation:
    """Verify LexicalGraphQueryEngine.for_semantic_guided_search() emits DeprecationWarning."""

    def test_for_semantic_guided_search_emits_deprecation_warning(self):
        """for_semantic_guided_search() emits DeprecationWarning recommending for_traversal_based_search()."""
        from graphrag_toolkit.lexical_graph.lexical_graph_query_engine import LexicalGraphQueryEngine

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.GraphStoreFactory.for_graph_store"
            ) as mock_gs_factory, patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.MultiTenantGraphStore.wrap"
            ) as mock_mt_gs, patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.VectorStoreFactory.for_vector_store"
            ) as mock_vs_factory, patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.MultiTenantVectorStore.wrap"
            ) as mock_mt_vs, patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.ReadOnlyVectorStore.wrap"
            ) as mock_ro_vs, patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.LLMCache",
                autospec=True,
            ), patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.ChatPromptTemplate",
                autospec=True,
            ):
                mock_graph_store = MagicMock()
                mock_vector_store = MagicMock()
                mock_vector_store.get_index.return_value = MagicMock()

                mock_gs_factory.return_value = mock_graph_store
                mock_mt_gs.return_value = mock_graph_store
                mock_vs_factory.return_value = mock_vector_store
                mock_mt_vs.return_value = mock_vector_store
                mock_ro_vs.return_value = mock_vector_store

                try:
                    LexicalGraphQueryEngine.for_semantic_guided_search(
                        graph_store="dummy://",
                        vector_store="dummy://",
                    )
                except Exception:
                    # We only care about the warning being emitted, not full execution
                    pass

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        # Find the specific warning from for_semantic_guided_search
        method_warnings = [
            x for x in deprecation_warnings
            if "for_semantic_guided_search" in str(x.message)
        ]
        assert len(method_warnings) >= 1
        msg = str(method_warnings[0].message)
        assert "for_traversal_based_search" in msg
        assert "deprecated" in msg


# ---------------------------------------------------------------------------
# Task 5.9: Property-based test (Hypothesis) for Property 1
# ---------------------------------------------------------------------------

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


class TestBackwardCompatibleImportsProperty:
    """Property-based test: backward-compatible imports resolve correctly and emit warnings.

    Feature: deprecate-semantic-guided-retriever
    Property 1: Backward-compatible imports resolve correctly and emit warnings

    For any deprecated class name, importing from the original retrievers package should:
    (a) resolve to the same class object as importing from the deprecated sub-package
    (b) emit a DeprecationWarning whose message contains the new import path
    """

    @given(name=st.sampled_from(_DEPRECATED_CLASS_NAMES))
    def test_backward_compatible_import_resolves_and_warns(self, name):
        """Property 1: backward-compatible imports resolve correctly and emit warnings."""
        # Import from the new (deprecated) location directly
        deprecated_module_path = _DEPRECATED_MODULE_PATHS[name]
        deprecated_module = importlib.import_module(deprecated_module_path)
        expected_class = getattr(deprecated_module, name)

        # Import from the old location (retrievers package) via __getattr__
        retrievers_module = importlib.import_module(
            'graphrag_toolkit.lexical_graph.retrieval.retrievers'
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            actual_class = getattr(retrievers_module, name)

        # (a) The resolved class should be the same object
        assert actual_class is expected_class, (
            f"Importing {name} from retrievers package did not resolve to the same "
            f"class as importing from {deprecated_module_path}"
        )

        # (b) A DeprecationWarning should have been emitted
        deprecation_warnings = [
            x for x in w if issubclass(x.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) >= 1, (
            f"No DeprecationWarning emitted when importing {name} from retrievers package"
        )

        # The warning message should mention the new import path
        warning_messages = [str(x.message) for x in deprecation_warnings]
        assert any(deprecated_module_path in msg for msg in warning_messages), (
            f"DeprecationWarning for {name} does not mention the new import path "
            f"'{deprecated_module_path}'. Got: {warning_messages}"
        )
