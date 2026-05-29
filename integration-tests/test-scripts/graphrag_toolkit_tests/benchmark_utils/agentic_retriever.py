# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Agentic retriever module for iterative, LLM-guided retrieval.

Performs multi-turn retrieval where an LLM analyzes initial results and
determines additional entities or subqueries to issue against the graph,
repeating until max_iterations is reached or the LLM determines no more
follow-ups are needed (early stop).
"""

import json
import logging
import time
from dataclasses import dataclass
from typing import List, Optional

from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.lexical_graph_query_engine import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.retrieval.retrievers import (
    ChunkBasedSearch,
    EntityNetworkSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.composite_traversal_based_retriever import (
    WeightedTraversalBasedRetriever,
)
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache

logger = logging.getLogger(__name__)


@dataclass
class AgenticQueryResult:
    """Result from an agentic retrieval query, including response and tracked metrics."""

    response: str
    retrieval_iterations: int
    agentic_retrieval_ms: float
    agentic_input_tokens: int
    agentic_output_tokens: int


# Prompt for the LLM to analyze retrieval results and determine follow-up queries
_ANALYSIS_PROMPT = PromptTemplate(
    "You are analyzing retrieval results to determine if follow-up queries are needed.\n\n"
    "Original question: {question}\n\n"
    "Retrieved context so far:\n{context}\n\n"
    "Based on the retrieved context, determine if additional queries are needed to fully "
    "answer the original question. If the context is sufficient, respond with:\n"
    '```json\n{{"needs_followup": false, "reason": "..."}}\n```\n\n'
    "If additional information is needed, respond with:\n"
    '```json\n{{"needs_followup": true, "follow_up_queries": ["query1", "query2"], '
    '"reason": "..."}}\n```\n\n'
    "Respond ONLY with the JSON block. Generate at most 3 follow-up queries."
)


def _validate_max_iterations(max_iterations: int) -> None:
    """
    Validate that max_iterations is within the allowed range [1, 10].

    Args:
        max_iterations: The maximum number of retrieval iterations.

    Raises:
        ValueError: If max_iterations is not in range [1, 10].
    """
    if not isinstance(max_iterations, int) or max_iterations < 1 or max_iterations > 10:
        raise ValueError(
            f"AGENTIC_MAX_ITERATIONS must be an integer in range [1, 10], "
            f"got: {max_iterations}"
        )


class AgenticRetriever:
    """
    Performs iterative retrieval using an LLM to plan follow-up queries.

    The retrieval loop:
    1. Initial retrieval using CompositeTraversalBasedRetriever
    2. LLM analyzes results and identifies missing entities/subqueries
    3. Issues follow-up retrievals if needed
    4. Repeats until max_iterations reached or LLM determines no more follow-ups needed

    Tracks:
    - retrieval_iterations: int (actual number of iterations performed)
    - agentic_retrieval_ms: float (cumulative retrieval latency across all iterations)
    - agentic_input_tokens: int (cumulative LLM tokens for planning/analysis)
    - agentic_output_tokens: int (cumulative LLM tokens for planning/analysis)
    """

    def __init__(self, graph_store, vector_store, max_iterations: int = 3):
        """
        Initialize the AgenticRetriever.

        Args:
            graph_store: Graph store instance (already opened/connected).
            vector_store: Vector store instance (already opened/connected).
            max_iterations: Maximum number of retrieval iterations (1-10, default 3).

        Raises:
            ValueError: If max_iterations is not in range [1, 10].
        """
        _validate_max_iterations(max_iterations)

        self.graph_store = graph_store
        self.vector_store = vector_store
        self.max_iterations = max_iterations

        # Create the base query engine for retrieval
        self._query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
            graph_store,
            vector_store,
            retrievers=[
                WeightedTraversalBasedRetriever(retriever=ChunkBasedSearch, weight=1.0),
                WeightedTraversalBasedRetriever(retriever=EntityNetworkSearch, weight=1.0),
            ],
        )

        # Create a separate LLM for analysis (planning) calls
        self._analysis_llm = LLMCache(
            llm=GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache,
        )

    def query(self, question: str) -> AgenticQueryResult:
        """
        Perform iterative agentic retrieval for the given question.

        Executes the retrieval loop: initial retrieval → LLM analysis →
        follow-up queries → repeat until max_iterations or early stop.

        Args:
            question: The question to answer.

        Returns:
            AgenticQueryResult with the final response and all tracked metrics.
        """
        cumulative_retrieval_ms = 0.0
        cumulative_input_tokens = 0
        cumulative_output_tokens = 0
        iteration_count = 0
        all_contexts: List[str] = []

        # Iteration 1: Initial retrieval
        iteration_count += 1
        retrieval_start = time.perf_counter()
        results = self._query_engine.retrieve(question)
        retrieval_end = time.perf_counter()
        cumulative_retrieval_ms += (retrieval_end - retrieval_start) * 1000

        # Collect context from initial retrieval
        context_text = self._extract_context(results)
        all_contexts.append(context_text)

        # Iterative loop: analyze and issue follow-ups
        while iteration_count < self.max_iterations:
            # LLM analysis to determine if follow-up queries are needed
            combined_context = "\n\n---\n\n".join(all_contexts)
            analysis_response, input_tokens, output_tokens = self._analyze_results(
                question, combined_context
            )
            cumulative_input_tokens += input_tokens
            cumulative_output_tokens += output_tokens

            # Parse the LLM's analysis response
            follow_up_queries = self._parse_analysis(analysis_response)

            # Early stop: no follow-up queries needed
            if not follow_up_queries:
                break

            # Issue follow-up retrievals
            iteration_count += 1
            for follow_up_query in follow_up_queries:
                retrieval_start = time.perf_counter()
                follow_up_results = self._query_engine.retrieve(follow_up_query)
                retrieval_end = time.perf_counter()
                cumulative_retrieval_ms += (retrieval_end - retrieval_start) * 1000

                follow_up_context = self._extract_context(follow_up_results)
                if follow_up_context.strip():
                    all_contexts.append(follow_up_context)

        # Generate final response using the query engine with all accumulated context
        final_response = self._generate_final_response(question, all_contexts)

        return AgenticQueryResult(
            response=final_response,
            retrieval_iterations=iteration_count,
            agentic_retrieval_ms=cumulative_retrieval_ms,
            agentic_input_tokens=cumulative_input_tokens,
            agentic_output_tokens=cumulative_output_tokens,
        )

    def _extract_context(self, results) -> str:
        """Extract text context from retrieval results."""
        context_parts = []
        for node_with_score in results:
            text = getattr(node_with_score.node, 'text', '') or ''
            if text.strip():
                context_parts.append(text)
        return "\n".join(context_parts)

    def _analyze_results(self, question: str, context: str) -> tuple:
        """
        Use the analysis LLM to determine if follow-up queries are needed.

        Returns:
            Tuple of (analysis_response_text, input_tokens, output_tokens)
        """
        from llama_index.core.base.llms.types import ChatMessage, MessageRole

        input_tokens = 0
        output_tokens = 0

        # Format the prompt
        formatted_prompt = _ANALYSIS_PROMPT.format(
            question=question,
            context=context[:8000],  # Truncate context to avoid token limits
        )

        # Call chat() directly on the LLM to get the full ChatResponse with token usage
        llm = self._analysis_llm.llm if hasattr(self._analysis_llm, 'llm') else None

        if llm is not None:
            from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
            from llama_index.llms.bedrock_converse import BedrockConverse
            from botocore.config import Config

            if isinstance(llm, BedrockConverse):
                if not hasattr(llm, '_client') or llm._client is None:
                    config = Config(
                        retries={'max_attempts': 10, 'mode': 'standard'},
                        connect_timeout=60.0,
                        read_timeout=60.0,
                    )
                    session = GraphRAGConfig.session
                    llm._client = session.client('bedrock-runtime', config=config)

                messages = [ChatMessage(role=MessageRole.USER, content=formatted_prompt)]
                chat_response = llm.chat(messages)
                response_text = chat_response.message.content if chat_response.message else ''

                # Extract token usage
                raw = getattr(chat_response, 'raw', None)
                if raw and isinstance(raw, dict):
                    usage = raw.get('usage')
                    if usage and isinstance(usage, dict):
                        input_tokens = usage.get('inputTokens', 0)
                        output_tokens = usage.get('outputTokens', 0)
            else:
                response_text = self._analysis_llm.predict(
                    _ANALYSIS_PROMPT,
                    question=question,
                    context=context[:8000],
                )
        else:
            response_text = self._analysis_llm.predict(
                _ANALYSIS_PROMPT,
                question=question,
                context=context[:8000],
            )

        return response_text, input_tokens, output_tokens

    def _parse_analysis(self, analysis_response: str) -> List[str]:
        """
        Parse the LLM analysis response to extract follow-up queries.

        Returns:
            List of follow-up query strings, or empty list if no follow-ups needed.
        """
        try:
            # Try to extract JSON from the response
            json_str = analysis_response.strip()

            # Handle markdown code blocks
            if '```json' in json_str:
                json_str = json_str.split('```json')[1].split('```')[0].strip()
            elif '```' in json_str:
                json_str = json_str.split('```')[1].split('```')[0].strip()

            parsed = json.loads(json_str)

            if not parsed.get('needs_followup', False):
                return []

            follow_ups = parsed.get('follow_up_queries', [])
            # Limit to at most 3 follow-up queries
            return follow_ups[:3] if isinstance(follow_ups, list) else []

        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            logger.warning("Failed to parse agentic analysis response, stopping iteration")
            return []

    def _generate_final_response(self, question: str, all_contexts: List[str]) -> str:
        """
        Generate the final response using the query engine.

        Uses the full query engine (which includes response generation) to produce
        the final answer based on the original question.
        """
        response = self._query_engine.query(question)
        return response.response or ''
