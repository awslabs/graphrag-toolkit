# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for lexical_graph_query_engine.py module.

This module tests query execution and retrieval operations.
"""

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.lexical_graph_query_engine import LexicalGraphQueryEngine


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


class TestLexicalGraphQueryEngine:

    def test_init_invokes_graph_store_init_hook(self):
        graph_store = Mock()
        vector_store = Mock()
        prompt_provider = Mock()
        prompt_provider.get_system_prompt.return_value = "system"
        prompt_provider.get_user_prompt.return_value = "user"

        with (
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.GraphStoreFactory.for_graph_store",
                return_value=graph_store,
            ),
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.MultiTenantGraphStore.wrap",
                return_value=graph_store,
            ),
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.VectorStoreFactory.for_vector_store",
                return_value=vector_store,
            ),
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.MultiTenantVectorStore.wrap",
                return_value=vector_store,
            ),
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.ReadOnlyVectorStore.wrap",
                return_value=vector_store,
            ),
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.LLMCache",
                autospec=True,
            ),
            patch(
                "graphrag_toolkit.lexical_graph.lexical_graph_query_engine.ChatPromptTemplate",
                autospec=True,
            ),
        ):
            LexicalGraphQueryEngine(
                graph_store="dummy://",
                vector_store="dummy://",
                retriever=Mock(),
                prompt_provider=prompt_provider,
            )

        graph_store.init.assert_called_once_with()
