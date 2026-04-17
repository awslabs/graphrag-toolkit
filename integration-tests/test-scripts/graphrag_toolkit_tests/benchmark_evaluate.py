# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import os
import unittest
from typing import Dict, Any, Optional, List

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler
from graphrag_toolkit_tests.benchmark_utils.run_evaluation import CorrectnessEvaluator, IDKEvaluator


def run_benchmark_evaluate(handler: IntegrationTestHandler, params: Dict[str, Any],
                           dataset: str, responses_path: str,
                           metrics: Optional[List[str]] = None):
    """
    Evaluates benchmark query responses against ground-truth answers using LLM-as-judge.

    Reads the responses JSONL file produced by run_benchmark_query, runs each specified
    metric evaluator (correctness, idk, correctness_on_answerable), and writes per-item
    grades and aggregate scores to benchmark-results/<dataset>/.

    Output files per metric:
        - benchmark-results/<dataset>/<metric>_evals.json — per-item grading array
        - benchmark-results/<dataset>/<metric>.json — aggregate score, e.g. {"correctness": 0.72}

    The 'correctness_on_answerable' metric requires both 'correctness' and 'idk' to have
    been run first (it reads their output files). If included in the metrics list, it must
    come after both.

    Args:
        handler: Integration test handler for recording assertions and output.
        params: Shared params dict passed between pipeline stages.
        dataset: Dataset key (e.g. 'cuad', 'pga', 'concurrentqa').
        responses_path: Path to the responses JSONL file from the query stage.
        metrics: List of metrics to run. Defaults to ['correctness', 'idk'].
    """
    if metrics is None:
        metrics = ['correctness', 'idk']

    results_dir = os.path.join('benchmark-results', dataset)
    os.makedirs(results_dir, exist_ok=True)

    data = []
    with open(responses_path) as fin:
        for line in fin:
            data.append(json.loads(line))

    scores = {}

    for metric in metrics:
        if metric == 'correctness_on_answerable':
            correctness_path = os.path.join(results_dir, 'correctness_evals.json')
            idk_path = os.path.join(results_dir, 'idk_evals.json')
            with open(correctness_path) as f:
                correctness_data = json.load(f)
            with open(idk_path) as f:
                idk_data = json.load(f)
            total, count = 0, 0
            for c_eval, i_eval in zip(correctness_data, idk_data):
                if i_eval['label'] == 'answerable':
                    total += 1
                    if c_eval['llmCorrectnessGrade'] == 'correct':
                        count += 1
            score = count / total if total > 0 else 0.0
        else:
            if metric == 'correctness':
                evaluator = CorrectnessEvaluator(model_id='anthropic.claude-3-sonnet-20240229-v1:0')
            elif metric == 'idk':
                evaluator = IDKEvaluator(model_id='anthropic.claude-3-sonnet-20240229-v1:0')

            evals = []
            for example in data:
                question = example['raw_example']['question']
                answer = example['raw_example']['answer']
                response = example['response']
                evals.append(evaluator.evaluate(question, answer, response))

            evals_path = os.path.join(results_dir, f'{metric}_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(evals, f, indent=2)

            count, total = 0, len(evals)
            for e in evals:
                if metric == 'correctness' and e.get('llmCorrectnessGrade') == 'correct':
                    count += 1
                elif metric == 'idk' and e.get('label') == 'unanswerable':
                    count += 1
            score = count / total if total > 0 else 0.0

        scores[metric] = score
        score_path = os.path.join(results_dir, f'{metric}.json')
        with open(score_path, 'w') as f:
            json.dump({metric: score}, f, indent=2)

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

        responses_path = params.get('benchmark_responses_path',
                                    os.path.join('benchmark-results', dataset_name, 'responses.jsonl'))

        run_benchmark_evaluate(
            handler, params,
            dataset=dataset_name,
            responses_path=responses_path,
            metrics=['correctness', 'idk'],
        )
