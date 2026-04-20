# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder
from graphrag_toolkit.lexical_graph.indexing.constants import DEFAULT_CLASSIFICATION
from graphrag_toolkit.lexical_graph.indexing.build.chunk_node_builder import ChunkNodeBuilder
from graphrag_toolkit.lexical_graph.indexing.build.build_filters import BuildFilters
from graphrag_toolkit.lexical_graph.metadata import DefaultSourceMetadataFormatter
from graphrag_toolkit.lexical_graph.indexing import IdGenerator
from graphrag_toolkit.lexical_graph.tenant_id import TenantId


def _make_concrete_builder():
    """Create a concrete NodeBuilder subclass (ChunkNodeBuilder) for testing base methods."""
    tenant = TenantId()
    id_gen = IdGenerator(tenant_id=tenant, include_classification_in_entity_id=True, use_chunk_id_delimiter=False)
    return ChunkNodeBuilder(
        id_generator=id_gen,
        build_filters=BuildFilters(),
        source_metadata_formatter=DefaultSourceMetadataFormatter(),
    )


class TestNodeBuilderInitialization:
    """Tests for NodeBuilder initialization."""

    def test_is_abstract(self):
        """Verify NodeBuilder cannot be instantiated directly."""
        # NodeBuilder has abstract methods, so direct instantiation should fail
        with pytest.raises(TypeError):
            NodeBuilder()


class TestNodeBuilderHelpers:
    """Tests for NodeBuilder helper methods via a concrete subclass."""

    def test_clean_id_removes_non_alphanumeric(self):
        """Verify _clean_id strips non-alphanumeric characters."""
        builder = _make_concrete_builder()
        assert builder._clean_id('abc-123_def!@#') == 'abc123def'

    def test_clean_id_preserves_alphanumeric(self):
        """Verify _clean_id preserves purely alphanumeric strings."""
        builder = _make_concrete_builder()
        assert builder._clean_id('abc123') == 'abc123'

    def test_clean_id_empty_string(self):
        """Verify _clean_id handles empty string."""
        builder = _make_concrete_builder()
        assert builder._clean_id('') == ''

    def test_format_classification_default(self):
        """Verify _format_classification returns empty for default classification."""
        builder = _make_concrete_builder()
        assert builder._format_classification(DEFAULT_CLASSIFICATION) == ''

    def test_format_classification_none(self):
        """Verify _format_classification returns empty for None."""
        builder = _make_concrete_builder()
        assert builder._format_classification(None) == ''

    def test_format_classification_custom(self):
        """Verify _format_classification wraps custom classification in parens."""
        builder = _make_concrete_builder()
        assert builder._format_classification('Technology') == ' (Technology)'

    def test_format_fact(self):
        """Verify _format_fact produces 'subject predicate object' string."""
        builder = _make_concrete_builder()
        result = builder._format_fact('GraphRAG', 'Tech', 'is', 'framework', 'Software')
        assert result == 'GraphRAG is framework'
