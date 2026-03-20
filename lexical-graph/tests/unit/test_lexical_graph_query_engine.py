# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for lexical_graph_query_engine.py module.

This module tests query execution and retrieval operations.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from graphrag_toolkit.lexical_graph.lexical_graph_query_engine import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.tenant_id import TenantId


def _make_engine(extra_patches=None, **kwargs):
    """Context manager helper – not used directly; prefer inline patches."""
    pass


def _engine_base_patches():
    """Return module-level patch targets needed to construct a LexicalGraphQueryEngine."""
    return [
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.GraphStoreFactory',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.VectorStoreFactory',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.MultiTenantGraphStore',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.MultiTenantVectorStore',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.ReadOnlyVectorStore',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.LLMCache',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.CompositeTraversalBasedRetriever',
        'graphrag_toolkit.lexical_graph.lexical_graph_query_engine.PromptProviderFactory',
    ]


class TestLexicalGraphQueryEngineInitialization:
    """Tests for LexicalGraphQueryEngine initialization."""






class TestLexicalGraphQueryEngineFactoryMethods:
    """Tests for factory methods."""



class TestLexicalGraphQueryEngineQuery:
    """Tests for query execution."""




class TestLexicalGraphQueryEngineErrorHandling:
    """Tests for error handling."""

    def test_query_engine_initialization_error(self):
        """Verify error handling during initialization."""
        with patch('graphrag_toolkit.lexical_graph.lexical_graph_query_engine.GraphStoreFactory') as mock_factory, \
             patch('graphrag_toolkit.lexical_graph.lexical_graph_query_engine.VectorStoreFactory'):

            # Simulate factory error before MultiTenantGraphStore.wrap is reached
            mock_factory.for_graph_store.side_effect = ValueError("Invalid graph store")

            with pytest.raises(ValueError, match="Invalid graph store"):
                LexicalGraphQueryEngine(
                    graph_store="invalid",
                    vector_store=Mock()
                )



class TestLexicalGraphQueryEngineAsync:
    """Tests verifying async query behavior."""


