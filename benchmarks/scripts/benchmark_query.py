# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import math
import os
import unittest
from contextlib import nullcontext
from typing import Dict, Any, Optional, List
import logging

from benchmarks.scripts.integration_test_base import IntegrationTestBase
from benchmarks.scripts.integration_test_handler import IntegrationTestHandler
from benchmarks.utils.s3_utils import sync_benchmark_data_from_s3
from benchmarks.utils.retriever_factory import create_query_engine, get_retriever_config, ByoKGQueryEngineWrapper
from benchmarks.utils.token_tracker import TokenTrackingLLMCache, extract_token_usage
from benchmarks.utils.metrics_summary import compute_metrics_summary
from benchmarks.utils.hop_classifier import classify_hop
from benchmarks.utils.agentic_retriever import AgenticRetriever, AgenticQueryResult

from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

QA_FILE_MAP = {
    'cuad': ['qa.json'],
    'cuad-prototype': ['qa.json'],
    'pga': ['pga_bio.json', 'pga_stat.json'],
    'concurrentqa': ['qa.json'],
    'concurrentqa-prototype': ['qa.json'],
    'wikihow': ['qa.json'],
}

BENCHMARK_DATA_DIR = 'source-data'


def load_qa_pairs(data_dir: str, dataset: str, qa_files: List[str], limit: Optional[int] = None):
    pairs = []
    for f in qa_files:
        path = os.path.join(data_dir, dataset, f)
        with open(path) as fh:
            pairs.extend(json.load(fh))
    if limit:
        pairs = pairs[:limit]
    return pairs


def run_benchmark_query(handler: IntegrationTestHandler,
                        params: Dict[str, Any],
                        dataset: str, 
                        data_dir: str,
                        graph_store_conn: Optional[str] = None,
                        vector_store_conn: Optional[str] = None,
                        response_llm: str = 'us.anthropic.claude-sonnet-4-6',
                        qa_limit: Optional[int] = None,
                        retriever_id: str = 'traversal',
                        agentic_max_iterations: int = 3,
                        byokg_max_iterations: int = 2):
    """
    Queries a benchmark dataset's QA pairs and writes responses in run_evaluation.py format.

    Loads QA pairs from the dataset's JSON files (mapped via QA_FILE_MAP), queries each
    question through a LexicalGraphQueryEngine (configured via RetrieverFactory), and writes a JSONL file
    with one line per question in the format:
        {"raw_example": {"question": "...", "answer": "..."}, "response": "..."}

    Either store connection can be omitted — a nullcontext is used for the missing store.
    The assertion verifies that all questions received a non-empty response.

    Results are written to benchmark-results/<dataset>/<retriever_id>/responses.jsonl and the path is
    stored in params['benchmark_responses_path'] for downstream BenchmarkEvaluate.

    Args:
        handler: Integration test handler for recording assertions and output.
        params: Shared params dict passed between pipeline stages. This function sets
            'benchmark_responses_path' and 'benchmark_num_questions' for downstream use.
        dataset: Dataset key (e.g. 'cuad', 'pga', 'concurrentqa'). Must have a
            corresponding entry in QA_FILE_MAP.
        data_dir: Root path to the benchmark data directory containing dataset subdirectories.
        graph_store_conn: Optional graph store connection string (e.g. 'neptune-db://...').
        vector_store_conn: Optional vector store connection string (e.g. 'aoss://...').
        response_llm: Bedrock model ID for generating query responses.
        qa_limit: Optional cap on the number of QA pairs to query (for prototype runs).
        retriever_id: Retriever identifier to use (default 'traversal'). Must be one of
            the valid IDs defined in RetrieverFactory.VALID_RETRIEVER_IDS.
        agentic_max_iterations: Maximum iterations for the agentic retriever (default 3,
            range 1-10). Only used when retriever_id is 'agentic'.
        byokg_max_iterations: Maximum iterations for the BYOKG agentic retriever
            (default 2). Only used when retriever_id is 'byokg_agentic'.
    """
    sync_benchmark_data_from_s3(dataset, data_dir)

    qa_files = QA_FILE_MAP.get(dataset, ['qa.json'])
    qa_pairs = load_qa_pairs(data_dir, dataset, qa_files, qa_limit)

    GraphRAGConfig.response_llm = response_llm

    graph_ctx = GraphStoreFactory.for_graph_store(
        graph_store_conn, log_formatting=NonRedactedGraphQueryLogFormatting()
    ) if graph_store_conn else nullcontext()

    vector_ctx = VectorStoreFactory.for_vector_store(
        vector_store_conn
    ) if vector_store_conn else nullcontext()

    with graph_ctx as graph_store, vector_ctx as vector_store:
        # Vector store connectivity check: ensure stores are populated when reusing existing graph data
        if retriever_id != 'traversal' and vector_store is not None:
            connectivity_results = vector_store.get_index('chunk').top_k(QueryBundle(query_str='test'), top_k=1)
            if len(connectivity_results) == 0:
                raise RuntimeError(
                    f"Vector store connectivity check failed: no results returned. "
                    f"Ensure graph and vector stores are populated before running the "
                    f"query phase with BENCHMARK_RETRIEVER={retriever_id}"
                )

        llm_cache = TokenTrackingLLMCache(
            llm=GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache,
        )

        query_engine = create_query_engine(
            retriever_id,
            graph_store,
            vector_store,
            response_llm=response_llm,
            agentic_max_iterations=agentic_max_iterations,
            byokg_max_iterations=byokg_max_iterations,
            llm=llm_cache,
        )

        retriever_dir = os.path.join('benchmark-results', dataset, retriever_id)
        os.makedirs(retriever_dir, exist_ok=True)
        responses_path = os.path.join(retriever_dir, 'responses.jsonl')

        num_empty = 0
        per_query_data = []
        is_agentic = isinstance(query_engine, AgenticRetriever)
        is_byokg = isinstance(query_engine, ByoKGQueryEngineWrapper)

        with open(responses_path, 'w') as out:
            for i, item in enumerate(qa_pairs):
                question = item['input']
                answer = item['output'][0]['text']

                response = query_engine.query(question)

                # Extract response text based on query engine type
                if is_agentic:
                    # AgenticRetriever returns AgenticQueryResult dataclass
                    response_text = response.response or ''
                else:
                    # Standard query engines and ByoKGQueryEngineWrapper
                    response_text = response.response or ''

                if not response_text.strip():
                    num_empty += 1

                # Extract per-query latency from response metadata
                metadata = getattr(response, 'metadata', None) or {}
                raw_retrieve_ms = metadata.get('retrieve_ms')
                raw_answer_ms = metadata.get('answer_ms')
                raw_total_ms = metadata.get('total_ms')

                retrieval_ms = math.floor(raw_retrieve_ms) if raw_retrieve_ms is not None else None
                response_ms = math.floor(raw_answer_ms) if raw_answer_ms is not None else None
                total_ms = math.floor(raw_total_ms) if raw_total_ms is not None else None

                # Extract per-query token usage
                input_tokens, output_tokens, retrieval_context_tokens = extract_token_usage(llm_cache)

                # Classify question hop complexity
                hop_classification = classify_hop(question)

                # Extract agentic-specific fields
                if is_agentic:
                    # AgenticQueryResult has these fields directly
                    agentic_retrieval_iterations = response.retrieval_iterations
                    agentic_retrieval_ms_val = response.agentic_retrieval_ms
                    agentic_input_tokens_val = response.agentic_input_tokens
                    agentic_output_tokens_val = response.agentic_output_tokens
                elif is_byokg:
                    # ByoKGQueryEngineWrapper returns metadata with retrieval_iterations
                    agentic_retrieval_iterations = metadata.get('retrieval_iterations')
                    agentic_retrieval_ms_val = None
                    agentic_input_tokens_val = None
                    agentic_output_tokens_val = None
                else:
                    # Non-agentic retrievers: all fields are null
                    agentic_retrieval_iterations = None
                    agentic_retrieval_ms_val = None
                    agentic_input_tokens_val = None
                    agentic_output_tokens_val = None

                per_query_data.append({
                    'retrieval_ms': retrieval_ms,
                    'response_ms': response_ms,
                    'total_ms': total_ms,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'retrieval_context_tokens': retrieval_context_tokens,
                    'hop_classification': hop_classification,
                })

                out.write(json.dumps({
                    'raw_example': {'question': question, 'answer': answer},
                    'response': response_text,
                    'retrieval_ms': retrieval_ms,
                    'response_ms': response_ms,
                    'total_ms': total_ms,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'retrieval_context_tokens': retrieval_context_tokens,
                    'hop_classification': hop_classification,
                    'retrieval_iterations': agentic_retrieval_iterations,
                    'agentic_retrieval_ms': agentic_retrieval_ms_val,
                    'agentic_input_tokens': agentic_input_tokens_val,
                    'agentic_output_tokens': agentic_output_tokens_val,
                }) + '\n')

                handler.add_output(f'q{i}', {
                    'question': question,
                    'response': response_text[:500]
                })

        # Compute and write aggregate metrics summary, recording the retriever
        # hyperparameters used so the run is reproducible from the JSON alone.
        retriever_config = get_retriever_config(
            retriever_id,
            response_llm=response_llm,
            agentic_max_iterations=agentic_max_iterations,
            byokg_max_iterations=byokg_max_iterations,
        )
        metrics_summary = compute_metrics_summary(
            per_query_data, retriever_id, dataset, response_llm, num_empty,
            retriever_config=retriever_config,
        )
        metrics_summary_path = os.path.join(retriever_dir, 'metrics_summary.json')
        with open(metrics_summary_path, 'w') as f:
            json.dump(metrics_summary, f, indent=2)

        params['benchmark_responses_path'] = retriever_dir
        params['benchmark_num_questions'] = len(qa_pairs)

        handler.add_output('total_questions', len(qa_pairs))
        handler.add_output('empty_responses', num_empty)

        class BenchmarkQueryAssertions(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls._num_questions = len(qa_pairs)
                cls._num_empty = num_empty

            def test_all_questions_answered(self):
                """All questions received a non-empty response"""
                self.assertEqual(self._num_empty, 0,
                                 f'{self._num_empty}/{self._num_questions} questions got empty responses')

        handler.run_assertions(BenchmarkQueryAssertions)


class CuadBenchmarkQuery(IntegrationTestBase):

    @property
    def description(self):
        return 'Query CUAD benchmark QA pairs and write responses JSONL'

    def wait(self) -> bool:
        vector_store_conn = os.environ.get('VECTOR_STORE')
        if not vector_store_conn:
            return False
        with VectorStoreFactory.for_vector_store(vector_store_conn) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='contract'), top_k=1)) == 0

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        limit_str = os.environ.get('BENCHMARK_QA_LIMIT')
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'cuad-prototype' if is_prototype == 'true' else 'cuad'
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')
        agentic_max_iterations = int(os.environ.get('AGENTIC_MAX_ITERATIONS', '3'))
        byokg_max_iterations = int(os.environ.get('BYOKG_MAX_ITERATIONS', '2'))

        run_benchmark_query(
            handler, 
            params,
            dataset=dataset_name,
            data_dir=BENCHMARK_DATA_DIR,
            graph_store_conn=os.environ.get('GRAPH_STORE'),
            vector_store_conn=os.environ.get('VECTOR_STORE'),
            response_llm=os.environ.get('TEST_RESPONSE_LLM', 'us.anthropic.claude-sonnet-4-6'),
            qa_limit=int(limit_str) if limit_str else None,
            retriever_id=retriever_id,
            agentic_max_iterations=agentic_max_iterations,
            byokg_max_iterations=byokg_max_iterations,
        )


class ConcurrentQaBenchmarkQuery(IntegrationTestBase):

    @property
    def description(self):
        return 'Query ConcurrentQA benchmark QA pairs and write responses JSONL'

    def wait(self) -> bool:
        vector_store_conn = os.environ.get('VECTOR_STORE')
        if not vector_store_conn:
            return False
        with VectorStoreFactory.for_vector_store(vector_store_conn) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='pipeline'), top_k=1)) == 0

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        limit_str = os.environ.get('BENCHMARK_QA_LIMIT')
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'concurrentqa-prototype' if is_prototype == 'true' else 'concurrentqa'
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')
        agentic_max_iterations = int(os.environ.get('AGENTIC_MAX_ITERATIONS', '3'))
        byokg_max_iterations = int(os.environ.get('BYOKG_MAX_ITERATIONS', '2'))

        run_benchmark_query(
            handler,
            params,
            dataset=dataset_name,
            data_dir=BENCHMARK_DATA_DIR,
            graph_store_conn=os.environ.get('GRAPH_STORE'),
            vector_store_conn=os.environ.get('VECTOR_STORE'),
            response_llm=os.environ.get('TEST_RESPONSE_LLM', 'us.anthropic.claude-sonnet-4-6'),
            qa_limit=int(limit_str) if limit_str else None,
            retriever_id=retriever_id,
            agentic_max_iterations=agentic_max_iterations,
            byokg_max_iterations=byokg_max_iterations,
        )


class WikihowBenchmarkQuery(IntegrationTestBase):

    @property
    def description(self):
        return 'Query WikiHow benchmark QA pairs and write responses JSONL'

    def wait(self) -> bool:
        vector_store_conn = os.environ.get('VECTOR_STORE')
        if not vector_store_conn:
            return False
        with VectorStoreFactory.for_vector_store(vector_store_conn) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='how to'), top_k=1)) == 0

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        limit_str = os.environ.get('BENCHMARK_QA_LIMIT')
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')
        agentic_max_iterations = int(os.environ.get('AGENTIC_MAX_ITERATIONS', '3'))
        byokg_max_iterations = int(os.environ.get('BYOKG_MAX_ITERATIONS', '2'))

        run_benchmark_query(
            handler,
            params,
            dataset='wikihow',
            data_dir=BENCHMARK_DATA_DIR,
            graph_store_conn=os.environ.get('GRAPH_STORE'),
            vector_store_conn=os.environ.get('VECTOR_STORE'),
            response_llm=os.environ.get('TEST_RESPONSE_LLM', 'us.anthropic.claude-sonnet-4-6'),
            qa_limit=int(limit_str) if limit_str else None,
            retriever_id=retriever_id,
            agentic_max_iterations=agentic_max_iterations,
            byokg_max_iterations=byokg_max_iterations,
        )


class PgaBenchmarkQuery(IntegrationTestBase):

    @property
    def description(self):
        return 'Query PGA benchmark QA pairs and write responses JSONL'

    def wait(self) -> bool:
        vector_store_conn = os.environ.get('VECTOR_STORE')
        if not vector_store_conn:
            return False
        with VectorStoreFactory.for_vector_store(vector_store_conn) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='golf'), top_k=1)) == 0

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        limit_str = os.environ.get('BENCHMARK_QA_LIMIT')
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')
        agentic_max_iterations = int(os.environ.get('AGENTIC_MAX_ITERATIONS', '3'))
        byokg_max_iterations = int(os.environ.get('BYOKG_MAX_ITERATIONS', '2'))

        run_benchmark_query(
            handler,
            params,
            dataset='pga',
            data_dir=BENCHMARK_DATA_DIR,
            graph_store_conn=os.environ.get('GRAPH_STORE'),
            vector_store_conn=os.environ.get('VECTOR_STORE'),
            response_llm=os.environ.get('TEST_RESPONSE_LLM', 'us.anthropic.claude-sonnet-4-6'),
            qa_limit=int(limit_str) if limit_str else None,
            retriever_id=retriever_id,
            agentic_max_iterations=agentic_max_iterations,
            byokg_max_iterations=byokg_max_iterations,
        )
