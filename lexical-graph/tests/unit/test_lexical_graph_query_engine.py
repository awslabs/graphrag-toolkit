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
