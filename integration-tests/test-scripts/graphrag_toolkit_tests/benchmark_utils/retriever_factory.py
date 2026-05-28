# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
RetrieverFactory module for benchmark retriever parameterization.

Maps retriever identifiers (from BENCHMARK_RETRIEVER env var) to query engine
construction logic, enabling multi-retriever comparison benchmarks.
"""

import logging
import time
from typing import Optional, Union

from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine

from graphrag_toolkit_tests.benchmark_utils.agentic_retriever import AgenticRetriever
from graphrag_toolkit.lexical_graph.retrieval.retrievers import (
    ChunkBasedSearch,
    ChunkBasedSemanticSearch,
    ChunkCosineSimilaritySearch,
    EntityBasedSearch,
    EntityNetworkSearch,
    RerankingBeamGraphSearch,
    SemanticChunkBeamGraphSearch,
    SemanticGuidedChunkRetriever,
    SemanticGuidedRetriever,
    StatementCosineSimilaritySearch,
    TopicBasedSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.composite_traversal_based_retriever import (
    WeightedTraversalBasedRetriever,
)

logger = logging.getLogger(__name__)

VALID_RETRIEVER_IDS = [
    'traversal',
    'topic_based',
    'entity_based',
    'chunk_based',
    'entity_network',
    'chunk_based_semantic',
    'semantic_guided',
    'topic-beam-chunk_only',
    'semantic-path_weighted',
    'agentic',
    'byokg_agentic',
]

# Maps sub-retriever IDs to their corresponding search class
_SUB_RETRIEVER_MAP = {
    'topic_based': TopicBasedSearch,
    'entity_based': EntityBasedSearch,
    'chunk_based': ChunkBasedSearch,
    'entity_network': EntityNetworkSearch,
    'chunk_based_semantic': ChunkBasedSemanticSearch,
}

# Common ProcessorArgs kwargs for individual sub-retriever benchmarks
_SUB_RETRIEVER_PROCESSOR_ARGS = {
    'reranker': 'tfidf',
    'vss_top_k': 10,
    'max_search_results': 5,
    'max_statements': 200,
    'derive_subqueries': False,
}


def create_query_engine(
    retriever_id: str,
    graph_store,
    vector_store,
    response_llm: str = 'us.anthropic.claude-sonnet-4-6',
    agentic_max_iterations: int = 3,
    byokg_max_iterations: int = 2,
    llm=None,
) -> Union[LexicalGraphQueryEngine, AgenticRetriever, 'ByoKGQueryEngineWrapper']:
    """
    Creates a configured query engine for the given retriever identifier.

    Args:
        retriever_id: One of VALID_RETRIEVER_IDS identifying the retrieval strategy.
        graph_store: Graph store instance (already opened/connected).
        vector_store: Vector store instance (already opened/connected).
        response_llm: Bedrock model ID for response generation.
        agentic_max_iterations: Max iterations for agentic retriever (1-10).
        byokg_max_iterations: Max iterations for BYOKG agentic retriever.
        llm: Optional pre-configured LLMCache instance (e.g. TokenTrackingLLMCache)
            to use for response generation. If provided, it is passed directly to
            the query engine constructor.

    Returns:
        A configured LexicalGraphQueryEngine instance.

    Raises:
        ValueError: If retriever_id is not in VALID_RETRIEVER_IDS.
    """
    if retriever_id not in VALID_RETRIEVER_IDS:
        raise ValueError(
            f"Invalid retriever identifier: '{retriever_id}'. "
            f"Valid identifiers are: {VALID_RETRIEVER_IDS}"
        )

    if retriever_id == 'traversal':
        return LexicalGraphQueryEngine.for_traversal_based_search(
            graph_store,
            vector_store,
            **({"llm": llm} if llm else {}),
        )

    if retriever_id in _SUB_RETRIEVER_MAP:
        retriever_class = _SUB_RETRIEVER_MAP[retriever_id]
        return LexicalGraphQueryEngine.for_traversal_based_search(
            graph_store,
            vector_store,
            retrievers=[WeightedTraversalBasedRetriever(retriever=retriever_class, weight=1.0)],
            **({"llm": llm} if llm else {}),
            **_SUB_RETRIEVER_PROCESSOR_ARGS,
        )

    if retriever_id == 'semantic_guided':
        return LexicalGraphQueryEngine.for_semantic_guided_search(
            graph_store,
            vector_store,
            **({"llm": llm} if llm else {}),
        )

    if retriever_id == 'topic-beam-chunk_only':
        return _create_topic_beam_chunk_only(graph_store, vector_store, llm=llm)

    if retriever_id == 'semantic-path_weighted':
        return _create_semantic_path_weighted(graph_store, vector_store, llm=llm)

    if retriever_id == 'agentic':
        return AgenticRetriever(
            graph_store,
            vector_store,
            max_iterations=agentic_max_iterations,
        )

    if retriever_id == 'byokg_agentic':
        return _create_byokg_agentic(
            graph_store, vector_store, byokg_max_iterations=byokg_max_iterations,
            response_llm=response_llm, llm=llm,
        )


def _create_topic_beam_chunk_only(graph_store, vector_store, llm=None) -> LexicalGraphQueryEngine:
    """
    Creates a query engine using SemanticGuidedChunkRetriever with
    ChunkCosineSimilaritySearch (initial) + SemanticChunkBeamGraphSearch (graph expansion).
    Uses default parameters: chunk_cosine_top_k=50, beam_width=10, max_depth=3.
    """
    retriever = SemanticGuidedChunkRetriever(
        vector_store=vector_store,
        graph_store=graph_store,
        retrievers=[
            ChunkCosineSimilaritySearch(
                vector_store=vector_store,
                graph_store=graph_store,
            ),
            SemanticChunkBeamGraphSearch(
                vector_store=vector_store,
                graph_store=graph_store,
            ),
        ],
    )

    return LexicalGraphQueryEngine(
        graph_store,
        vector_store,
        retriever=retriever,
        context_format='bedrock_xml',
        **({"llm": llm} if llm else {}),
    )


def _create_semantic_path_weighted(graph_store, vector_store, llm=None) -> LexicalGraphQueryEngine:
    """
    Creates a query engine using SemanticGuidedRetriever with
    StatementCosineSimilaritySearch (initial) + RerankingBeamGraphSearch (graph expansion).
    Uses default parameters: beam_width=10, max_depth=3.
    """
    retriever = SemanticGuidedRetriever(
        vector_store=vector_store,
        graph_store=graph_store,
        retrievers=[
            StatementCosineSimilaritySearch(
                vector_store=vector_store,
                graph_store=graph_store,
            ),
            RerankingBeamGraphSearch(
                vector_store=vector_store,
                graph_store=graph_store,
            ),
        ],
    )

    return LexicalGraphQueryEngine(
        graph_store,
        vector_store,
        retriever=retriever,
        context_format='bedrock_xml',
        **({"llm": llm} if llm else {}),
    )


def _create_byokg_agentic(graph_store, vector_store, byokg_max_iterations: int = 2,
                           response_llm: str = 'us.anthropic.claude-sonnet-4-6',
                           llm=None):
    """
    Creates a BYOKG agentic query engine wrapper that uses ByoKGQueryEngine
    against the same Neptune/OpenSearch endpoints used by GraphRAG toolkit
    deterministic retrievers.

    The wrapper produces a Response-compatible object with the same mandatory
    JSONL fields (raw_example, response) plus BYOKG-specific metadata
    (retrieval_iterations, entities_per_iteration, llm_calls).

    On failure, writes empty string for response, logs error, and continues.

    Args:
        graph_store: Graph store instance (already opened/connected).
        vector_store: Vector store instance (already opened/connected).
        byokg_max_iterations: Max iterations for BYOKG retrieval (default 2).
            Controlled via BYOKG_MAX_ITERATIONS env var.
        response_llm: Bedrock model ID for response generation.
        llm: Optional pre-configured LLMCache instance (unused for BYOKG but
            accepted for interface consistency).

    Returns:
        A ByoKGQueryEngineWrapper instance that implements the query() interface
        expected by run_benchmark_query().

    Raises:
        ImportError: If the graphrag_toolkit.byokg_rag package is not available.
    """
    try:
        from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine
        from graphrag_toolkit.byokg_rag.llm.bedrock_llms import BedrockGenerator
        from graphrag_toolkit.byokg_rag.graphstore.neptune import (
            NeptuneAnalyticsGraphStore,
            NeptuneDBGraphStore,
        )
    except ImportError as e:
        raise ImportError(
            "The 'graphrag_toolkit.byokg_rag' package is required for the "
            "'byokg_agentic' retriever but is not installed. "
            "Install it with: pip install -e byokg-rag/ "
            f"(Original error: {e})"
        ) from e

    # Create a BYOKG-compatible graph store from the existing graph store connection.
    # The graph_store passed in is a graphrag_toolkit.lexical_graph graph store.
    # We need to create a BYOKG NeptuneDBGraphStore or NeptuneAnalyticsGraphStore
    # that connects to the same Neptune endpoint.
    byokg_graph_store = _create_byokg_graph_store(graph_store)

    # Create the LLM generator for BYOKG
    import os
    region = os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
    llm_generator = BedrockGenerator(
        model_name=response_llm,
        region_name=region,
    )

    # Create the ByoKGQueryEngine
    byokg_engine = ByoKGQueryEngine(
        graph_store=byokg_graph_store,
        llm_generator=llm_generator,
    )

    return ByoKGQueryEngineWrapper(
        byokg_engine=byokg_engine,
        max_iterations=byokg_max_iterations,
    )


def _create_byokg_graph_store(graph_store):
    """
    Creates a BYOKG-compatible graph store from the existing lexical graph store.

    Inspects the graph_store to determine the Neptune endpoint type and creates
    the appropriate BYOKG graph store wrapper.

    Args:
        graph_store: The graphrag_toolkit.lexical_graph graph store instance.

    Returns:
        A BYOKG-compatible graph store (NeptuneDBGraphStore or NeptuneAnalyticsGraphStore).
    """
    import os

    try:
        from graphrag_toolkit.byokg_rag.graphstore.neptune import (
            NeptuneAnalyticsGraphStore,
            NeptuneDBGraphStore,
        )
    except ImportError as e:
        raise ImportError(
            "The 'graphrag_toolkit.byokg_rag' package is required for BYOKG graph store creation."
        ) from e

    # Try to extract connection info from the existing graph store
    # The lexical graph store wraps Neptune connections; we need to determine
    # whether it's Neptune Analytics or Neptune DB and get the endpoint.
    region = os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

    # Check if the graph store has a graph_identifier (Neptune Analytics)
    if hasattr(graph_store, 'graph_identifier'):
        return NeptuneAnalyticsGraphStore(
            graph_identifier=graph_store.graph_identifier,
            region=region,
        )

    # Check if the graph store has an endpoint_url (Neptune DB)
    if hasattr(graph_store, 'endpoint_url'):
        return NeptuneDBGraphStore(
            endpoint_url=graph_store.endpoint_url,
            region=region,
        )

    # Try to get connection info from the GRAPH_STORE env var
    graph_store_conn = os.environ.get('GRAPH_STORE', '')
    if 'neptune-graph://' in graph_store_conn:
        # Neptune Analytics: neptune-graph://graph-id
        graph_id = graph_store_conn.replace('neptune-graph://', '').strip('/')
        return NeptuneAnalyticsGraphStore(
            graph_identifier=graph_id,
            region=region,
        )
    elif 'neptune-db://' in graph_store_conn or 'wss://' in graph_store_conn or 'https://' in graph_store_conn:
        # Neptune DB: neptune-db://endpoint:port or wss://endpoint:port/gremlin
        endpoint = graph_store_conn.replace('neptune-db://', '').strip('/')
        if not endpoint.startswith('https://'):
            endpoint = f'https://{endpoint}'
        return NeptuneDBGraphStore(
            endpoint_url=endpoint,
            region=region,
        )

    raise ValueError(
        "Unable to determine Neptune connection type from graph store. "
        "Ensure GRAPH_STORE environment variable is set to a valid Neptune "
        "connection string (neptune-graph://... or neptune-db://...)."
    )


class ByoKGQueryEngineWrapper:
    """
    Wraps ByoKGQueryEngine to provide a query() interface compatible with
    the benchmark pipeline's run_benchmark_query() function.

    Produces a Response-like object with:
    - .response: The generated answer text (empty string on failure)
    - .metadata: Dict with timing and BYOKG-specific metadata

    BYOKG-specific metadata fields:
    - retrieval_iterations: Number of retrieval iterations performed
    - entities_per_iteration: List of entity counts discovered per iteration
    - llm_calls: Number of LLM calls made during retrieval
    """

    def __init__(self, byokg_engine, max_iterations: int = 2):
        """
        Initialize the wrapper.

        Args:
            byokg_engine: A configured ByoKGQueryEngine instance.
            max_iterations: Maximum retrieval iterations (default 2).
        """
        self.byokg_engine = byokg_engine
        self.max_iterations = max_iterations

    def query(self, question: str):
        """
        Query the BYOKG engine and return a Response-compatible object.

        On failure, returns a response with empty string and logs the error.

        Args:
            question: The question to answer.

        Returns:
            A ByoKGResponse object with .response and .metadata attributes.
        """
        try:
            start_time = time.perf_counter()

            # Run BYOKG retrieval
            retrieved_context = self.byokg_engine.query(
                query=question,
                iterations=self.max_iterations,
            )

            retrieve_end = time.perf_counter()
            retrieve_ms = (retrieve_end - start_time) * 1000

            # Generate response using the retrieved context
            context_str = "\n".join(retrieved_context) if retrieved_context else ""
            answers, full_response = self.byokg_engine.generate_response(
                query=question,
                graph_context=context_str,
            )

            end_time = time.perf_counter()
            answer_ms = (end_time - retrieve_end) * 1000
            total_ms = (end_time - start_time) * 1000

            # Extract the answer text
            response_text = answers[0] if answers else ''

            metadata = {
                'retrieve_ms': retrieve_ms,
                'answer_ms': answer_ms,
                'total_ms': total_ms,
                'retrieval_iterations': self.max_iterations,
                'entities_per_iteration': [],
                'llm_calls': 0,
            }

            return ByoKGResponse(response=response_text, metadata=metadata)

        except Exception as e:
            logger.error(f"BYOKG query failed for question: '{question[:100]}...': {e}")
            return ByoKGResponse(response='', metadata={
                'retrieve_ms': None,
                'answer_ms': None,
                'total_ms': None,
                'retrieval_iterations': 0,
                'entities_per_iteration': [],
                'llm_calls': 0,
            })


class ByoKGResponse:
    """
    Response object compatible with the benchmark pipeline's expectations.

    Mimics the llama_index Response interface with .response and .metadata.
    """

    def __init__(self, response: str, metadata: dict):
        self.response = response
        self.metadata = metadata
