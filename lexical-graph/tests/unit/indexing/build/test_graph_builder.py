# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder


class TestGraphBuilderInitialization:
    """Tests for GraphBuilder initialization."""

    def test_is_abstract(self):
        """Verify GraphBuilder cannot be instantiated directly."""
        with pytest.raises(TypeError):
            GraphBuilder()


class TestGraphBuilderHelpers:
    """Tests for GraphBuilder helper methods via a concrete subclass."""

    def test_to_params_wraps_dict(self):
        """Verify _to_params wraps a dict in {'params': [dict]}."""
        # Use ChunkGraphBuilder as a concrete subclass
        from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
        builder = ChunkGraphBuilder()
        result = builder._to_params({'key': 'value'})
        assert result == {'params': [{'key': 'value'}]}

    def test_to_params_empty_dict(self):
        """Verify _to_params returns empty params list for falsy input."""
        from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
        builder = ChunkGraphBuilder()
        result = builder._to_params({})
        assert result == {'params': []}

    def test_to_params_none(self):
        """Verify _to_params returns empty params list for None."""
        from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
        builder = ChunkGraphBuilder()
        result = builder._to_params(None)
        assert result == {'params': []}

    def test_index_key_on_subclass(self):
        """Verify index_key works on concrete subclass."""
        from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
        assert ChunkGraphBuilder.index_key() == 'chunk'
