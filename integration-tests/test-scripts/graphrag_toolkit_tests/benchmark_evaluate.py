# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Benchmark evaluation for GraphRAG benchmark results.

This module provides:
- run_benchmark_evaluate(): Evaluation function used by pipeline test classes
- Test classes: CuadBenchmarkEvaluate, ConcurrentQaBenchmarkEvaluate,
  WikihowBenchmarkEvaluate, PgaBenchmarkEvaluate

Environment Variables:
    BENCHMARK_JUDGE_LLM: Model ID for evaluation judge (default: us.anthropic.claude-sonnet-4-6)
"""
import json
import logging
import os
import unittest
from typing import Dict, Any, Optional, List

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler
from graphrag_toolkit_tests.benchmark_utils.run_evaluation import evaluate_responses

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline evaluation helpers
# ---------------------------------------------------------------------------


def run_benchmark_evaluate(handler: IntegrationTestHandler, params: Dict[str, Any],
                           dataset: str, responses_path: str,
                           metrics: Optional[List[str]] = None):
    """
    Evaluates benchmark query responses against ground-truth answers using LLM-as-judge.

    Reads the responses JSONL file produced by run_benchmark_query, runs each specified
    metric evaluator (correctness, idk, correctness_on_answerable), and writes per-item
    grades and aggregate scores to the retriever-specific directory
    benchmark-results/<dataset>/<retriever>/.

    Output files per metric:
        - benchmark-results/<dataset>/<retriever>/<metric>_evals.json — per-item grading array
        - benchmark-results/<dataset>/<retriever>/<metric>.json — aggregate score, e.g. {"correctness": 0.72}

    The 'correctness_on_answerable' metric requires both 'correctness' and 'idk' to have
    been run first (it reads their output files). If included in the metrics list, it must
    come after both.

    Args:
        handler: Integration test handler for recording assertions and output.
        params: Shared params dict passed between pipeline stages.
        dataset: Dataset key (e.g. 'cuad', 'pga', 'concurrentqa').
        responses_path: Path to the retriever-specific results directory or responses JSONL
            file from the query stage. If a directory, responses.jsonl is read from it.
        metrics: List of metrics to run. Defaults to ['correctness', 'idk'].
    """
    if metrics is None:
        metrics = ['correctness', 'idk']

    # Determine the retriever-specific results directory
    retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')

    if params.get('benchmark_responses_path'):
        # benchmark_responses_path is set by the query phase (points to retriever-specific dir)
        results_dir = params['benchmark_responses_path']
    else:
        # Standalone run: construct from BENCHMARK_RETRIEVER env var
        results_dir = os.path.join('benchmark-results', dataset, retriever_id)

    os.makedirs(results_dir, exist_ok=True)

    # Determine the responses.jsonl path
    if os.path.isdir(responses_path):
        responses_file = os.path.join(responses_path, 'responses.jsonl')
    else:
        responses_file = responses_path

    data = []
    with open(responses_file) as fin:
        for line in fin:
            data.append(json.loads(line))

    judge_model_id = os.environ.get('BENCHMARK_JUDGE_LLM', 'us.anthropic.claude-sonnet-4-6')

    scores = evaluate_responses(data, results_dir, judge_model_id, metrics)

    for metric, score in scores.items():
        handler.add_output(metric, score)

    params['benchmark_scores'] = scores

    class BenchmarkEvaluateAssertions(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls._scores = scores

        def test_scores_are_plausible(self):
            """All metric scores are between 0 and 1"""
            for metric, score in self._scores.items():
                self.assertGreaterEqual(score, 0.0, f'{metric} score is negative')
                self.assertLessEqual(score, 1.0, f'{metric} score exceeds 1.0')

    handler.run_assertions(BenchmarkEvaluateAssertions)


class CuadBenchmarkEvaluate(IntegrationTestBase):

    @property
    def description(self):
        return 'Evaluate CUAD benchmark responses using LLM-as-judge correctness and IDK metrics'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'cuad-prototype' if is_prototype == 'true' else 'cuad'
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')

        responses_path = params.get('benchmark_responses_path',
                                    os.path.join('benchmark-results', dataset_name, 'responses.jsonl'))

        run_benchmark_evaluate(
            handler, params,
            dataset=dataset_name,
            responses_path=responses_path,
            metrics=['correctness', 'idk'],
        )


class ConcurrentQaBenchmarkEvaluate(IntegrationTestBase):

    @property
    def description(self):
        return 'Evaluate ConcurrentQA benchmark responses using LLM-as-judge correctness and IDK metrics'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'concurrentqa-prototype' if is_prototype == 'true' else 'concurrentqa'
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')

        responses_path = params.get('benchmark_responses_path',
                                    os.path.join('benchmark-results',
                                                 dataset_name,
                                                 retriever_id))

        run_benchmark_evaluate(
            handler,
            params,
            dataset=dataset_name,
            responses_path=responses_path,
            metrics=['correctness', 'idk'],
        )


class WikihowBenchmarkEvaluate(IntegrationTestBase):

    @property
    def description(self):
        return 'Evaluate WikiHow benchmark responses using LLM-as-judge correctness and IDK metrics'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')

        responses_path = params.get('benchmark_responses_path',
                                    os.path.join('benchmark-results',
                                                 'wikihow',
                                                 retriever_id))

        run_benchmark_evaluate(
            handler,
            params,
            dataset='wikihow',
            responses_path=responses_path,
            metrics=['correctness', 'idk'],
        )


class PgaBenchmarkEvaluate(IntegrationTestBase):

    @property
    def description(self):
        return 'Evaluate PGA benchmark responses using LLM-as-judge correctness and IDK metrics'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        retriever_id = os.environ.get('BENCHMARK_RETRIEVER', 'traversal')

        responses_path = params.get('benchmark_responses_path',
                                    os.path.join('benchmark-results',
                                                 'pga',
                                                 retriever_id))

        run_benchmark_evaluate(
            handler,
            params,
            dataset='pga',
            responses_path=responses_path,
            metrics=['correctness', 'idk'],
        )
