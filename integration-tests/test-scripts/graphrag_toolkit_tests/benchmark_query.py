# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import unittest
from contextlib import nullcontext
from typing import Dict, Any, Optional, List

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting

from llama_index.core.schema import QueryBundle

QA_FILE_MAP = {
    'cuad': ['qa.json'],
    'cuad-prototype': ['qa.json'],
    'pga': ['pga_bio.json', 'pga_stat.json'],
    'concurrentqa': ['qa.json'],
}


def load_qa_pairs(data_dir: str, dataset: str, qa_files: List[str], limit: Optional[int] = None):
    pairs = []
    for f in qa_files:
        path = os.path.join(data_dir, dataset, f)
        with open(path) as fh:
            pairs.extend(json.load(fh))
    if limit:
        pairs = pairs[:limit]
    return pairs


def run_benchmark_query(handler: IntegrationTestHandler, params: Dict[str, Any],
                        dataset: str, data_dir: str,
                        graph_store_conn: Optional[str] = None,
                        vector_store_conn: Optional[str] = None,
                        response_llm: str = 'anthropic.claude-sonnet-4-20250514-v1:0',
                        qa_limit: Optional[int] = None):
    """
    Queries a benchmark dataset's QA pairs and writes responses in run_evaluation.py format.

    Loads QA pairs from the dataset's JSON files (mapped via QA_FILE_MAP), queries each
    question through a traversal-based LexicalGraphQueryEngine, and writes a JSONL file
    with one line per question in the format:
        {"raw_example": {"question": "...", "answer": "..."}, "response": "..."}

    Either store connection can be omitted — a nullcontext is used for the missing store.
    The assertion verifies that all questions received a non-empty response.

    Results are written to benchmark-results/<dataset>/responses.jsonl and the path is
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
    """
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
        query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
            graph_store,
            vector_store
        )

        responses_path = os.path.join('benchmark-results', dataset, 'responses.jsonl')
        os.makedirs(os.path.dirname(responses_path), exist_ok=True)

        num_empty = 0

        with open(responses_path, 'w') as out:
            for i, item in enumerate(qa_pairs):
                question = item['input']
                answer = item['output'][0]['text']

                response = query_engine.query(question)
                response_text = response.response or ''

                if not response_text.strip():
                    num_empty += 1

                out.write(json.dumps({
                    'raw_example': {'question': question, 'answer': answer},
                    'response': response_text
                }) + '\n')

                handler.add_output(f'q{i}', {
                    'question': question,
                    'response': response_text[:500]
                })

        params['benchmark_responses_path'] = responses_path
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
        data_dir = os.environ.get('BENCHMARK_DATA_DIR')
        limit_str = os.environ.get('BENCHMARK_QA_LIMIT')
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'cuad-prototype' if is_prototype == 'true' else 'cuad'

        run_benchmark_query(
            handler, params,
            dataset=dataset_name,
            data_dir=data_dir,
            graph_store_conn=os.environ.get('GRAPH_STORE'),
            vector_store_conn=os.environ.get('VECTOR_STORE'),
            response_llm=os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0'),
            qa_limit=int(limit_str) if limit_str else None,
        )
