# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests for the retriever_factory module.

# Feature: retriever-comparison-benchmarks, Property 5: Sub-retriever factory correctness
"""

from unittest.mock import patch, MagicMock

from hypothesis import given, settings
from hypothesis.strategies import sampled_from

from graphrag_toolkit_tests.benchmark_utils.retriever_factory import (
    create_query_engine,
    _SUB_RETRIEVER_MAP,
    _SUB_RETRIEVER_PROCESSOR_ARGS,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers import (
    TopicBasedSearch,
    EntityBasedSearch,
    ChunkBasedSearch,
    EntityNetworkSearch,
    ChunkBasedSemanticSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.composite_traversal_based_retriever import (
    WeightedTraversalBasedRetriever,
)


# The sub-retriever IDs that should produce a CompositeTraversalBasedRetriever
# with a single retriever of the corresponding type
SUB_RETRIEVER_IDS = ['topic_based', 'entity_based', 'chunk_based', 'entity_network', 'chunk_based_semantic']

# Expected mapping from ID to search class
EXPECTED_CLASS_MAP = {
    'topic_based': TopicBasedSearch,
    'entity_based': EntityBasedSearch,
    'chunk_based': ChunkBasedSearch,
    'entity_network': EntityNetworkSearch,
    'chunk_based_semantic': ChunkBasedSemanticSearch,
}


class TestSubRetrieverFactoryCorrectnessProperty:
    """
    Property 5: Sub-retriever factory correctness

    For any sub-retriever identifier in {topic_based, entity_based, chunk_based,
    entity_network, chunk_based_semantic}, the factory SHALL create a
    CompositeTraversalBasedRetriever with exactly one retriever of the corresponding
    type, weight 1.0, and ProcessorArgs with reranker='tfidf', vss_top_k=10,
    max_search_results=5, max_statements=200, derive_subqueries=False.

    **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7**
    """

    @settings(max_examples=100)
    @given(retriever_id=sampled_from(SUB_RETRIEVER_IDS))
    def test_sub_retriever_factory_creates_correct_configuration(self, retriever_id):
        """
        **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7**

        For each sub-retriever ID, verify the factory calls for_traversal_based_search
        with exactly one retriever of the corresponding type, weight 1.0, and correct
        ProcessorArgs (reranker='tfidf', vss_top_k=10, max_search_results=5,
        max_statements=200, derive_subqueries=False).
        """
        mock_graph_store = MagicMock()
        mock_vector_store = MagicMock()
        mock_query_engine = MagicMock()

        with patch(
            'graphrag_toolkit_tests.benchmark_utils.retriever_factory.LexicalGraphQueryEngine.for_traversal_based_search',
            return_value=mock_query_engine,
        ) as mock_for_traversal:
            result = create_query_engine(
                retriever_id=retriever_id,
                graph_store=mock_graph_store,
                vector_store=mock_vector_store,
            )

            # Verify for_traversal_based_search was called exactly once
            mock_for_traversal.assert_called_once()

            # Get the call arguments
            call_args = mock_for_traversal.call_args

            # Verify graph_store and vector_store are passed as positional args
            assert call_args[0][0] is mock_graph_store, (
                f"Expected graph_store as first positional arg for '{retriever_id}'"
            )
            assert call_args[0][1] is mock_vector_store, (
                f"Expected vector_store as second positional arg for '{retriever_id}'"
            )

            # Verify retrievers kwarg contains exactly one WeightedTraversalBasedRetriever
            retrievers = call_args[1]['retrievers']
            assert len(retrievers) == 1, (
                f"Expected exactly 1 retriever for '{retriever_id}', got {len(retrievers)}"
            )

            weighted_retriever = retrievers[0]
            assert isinstance(weighted_retriever, WeightedTraversalBasedRetriever), (
                f"Expected WeightedTraversalBasedRetriever for '{retriever_id}', "
                f"got {type(weighted_retriever)}"
            )

            # Verify the retriever class matches the expected type
            expected_class = EXPECTED_CLASS_MAP[retriever_id]
            assert weighted_retriever.retriever is expected_class, (
                f"Expected retriever class {expected_class.__name__} for '{retriever_id}', "
                f"got {weighted_retriever.retriever}"
            )

            # Verify weight is 1.0
            assert weighted_retriever.weight == 1.0, (
                f"Expected weight 1.0 for '{retriever_id}', got {weighted_retriever.weight}"
            )

            # Verify ProcessorArgs kwargs are passed correctly
            call_kwargs = call_args[1]
            assert call_kwargs.get('reranker') == 'tfidf', (
                f"Expected reranker='tfidf' for '{retriever_id}', "
                f"got reranker='{call_kwargs.get('reranker')}'"
            )
            assert call_kwargs.get('vss_top_k') == 10, (
                f"Expected vss_top_k=10 for '{retriever_id}', "
                f"got vss_top_k={call_kwargs.get('vss_top_k')}"
            )
            assert call_kwargs.get('max_search_results') == 5, (
                f"Expected max_search_results=5 for '{retriever_id}', "
                f"got max_search_results={call_kwargs.get('max_search_results')}"
            )
            assert call_kwargs.get('max_statements') == 200, (
                f"Expected max_statements=200 for '{retriever_id}', "
                f"got max_statements={call_kwargs.get('max_statements')}"
            )
            assert call_kwargs.get('derive_subqueries') is False, (
                f"Expected derive_subqueries=False for '{retriever_id}', "
                f"got derive_subqueries={call_kwargs.get('derive_subqueries')}"
            )

        # Verify the returned engine is the one from for_traversal_based_search
        assert result is mock_query_engine


# --- Property 9: Retriever ID validation ---

import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import text

from graphrag_toolkit_tests.benchmark_utils.retriever_factory import (
    VALID_RETRIEVER_IDS,
)


# Feature: retriever-comparison-benchmarks, Property 9: Retriever ID validation
class TestRetrieverIDValidationProperty:
    """
    Property 9: Retriever ID validation

    For any string that is not a member of the valid retriever identifier set,
    the factory SHALL raise a ValueError whose message contains the invalid
    identifier and lists all valid identifiers.

    **Validates: Requirements 1.4**
    """

    @settings(max_examples=100)
    @given(invalid_id=text(min_size=1))
    def test_invalid_retriever_id_raises_value_error(self, invalid_id):
        """
        **Validates: Requirements 1.4**

        For any string not in VALID_RETRIEVER_IDS, verify ValueError is raised
        with message containing the invalid ID and listing all valid IDs.
        """
        assume(invalid_id not in VALID_RETRIEVER_IDS)

        mock_graph_store = MagicMock()
        mock_vector_store = MagicMock()

        with pytest.raises(ValueError) as exc_info:
            create_query_engine(
                retriever_id=invalid_id,
                graph_store=mock_graph_store,
                vector_store=mock_vector_store,
            )

        error_message = str(exc_info.value)

        # Verify the error message contains the invalid identifier
        assert invalid_id in error_message, (
            f"Error message should contain the invalid ID '{invalid_id}', "
            f"but got: {error_message}"
        )

        # Verify the error message lists all valid identifiers
        for valid_id in VALID_RETRIEVER_IDS:
            assert valid_id in error_message, (
                f"Error message should list valid ID '{valid_id}', "
                f"but got: {error_message}"
            )
