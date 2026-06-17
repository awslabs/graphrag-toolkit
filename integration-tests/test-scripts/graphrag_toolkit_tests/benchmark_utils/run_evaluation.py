# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Single source of truth for all benchmark evaluation logic.

This module provides:
- call_bedrock_invoke_model: Bedrock invocation with retry and parse-failure handling
- CorrectnessEvaluator: LLM-as-judge correctness grading
- IDKEvaluator: Detects "I don't know" / unanswerable responses
- evaluate_responses: Shared evaluation loop used by all callers
- BKB_CORRECTNESS_GRADING / IDK_DETECTION prompt templates

Environment Variables:
    BENCHMARK_JUDGE_LLM: Model ID for evaluation judge (default: us.anthropic.claude-sonnet-4-6).
        Uses cross-region inference profile; requires it enabled in the account.
    AWS_REGION: AWS region for Bedrock calls (default: us-west-2)
    REGION_NAME: Fallback for AWS_REGION
"""
from boto3 import Session
from botocore.config import Config
import logging
import json
import os
import time
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_REGION = os.environ.get('AWS_REGION', os.environ.get('REGION_NAME', 'us-west-2'))


def call_bedrock_invoke_model(prompt, bedrock, model_id, is_json_output=True, max_retries=10):
    """Invoke a Bedrock model with retry logic and parse-failure handling.

    Args:
        prompt: The prompt string to send to the model.
        bedrock: A boto3 bedrock-runtime client.
        model_id: The Bedrock model ID to invoke.
        is_json_output: If True, parse the response as JSON and return a dict.
        max_retries: Maximum number of retry attempts on transient failures.

    Returns:
        If is_json_output=True: a dict with 'grade', 'justification', and 'llm_response' keys.
        If is_json_output=False: the raw response text string.

    Raises:
        The last exception encountered if all retries are exhausted.
    """
    last_exception = None
    for _attempt in range(max_retries):
        try:
            accept = 'application/json'
            contentType = 'application/json'

            payload_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2048,
                # Error: `temperature` and `top_p` cannot both be specified for this model. Please use only one.
                # "temperature": 0.0,
                "top_p": 1,
                "top_k": 50,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ]
                    }
                ]
            }
            body = json.dumps(payload_body)

            response = bedrock.invoke_model(body=body, modelId=model_id, accept=accept, contentType=contentType)

            response = response['body'].read().decode('utf-8')
            response = json.loads(response)
            response_text = response['content'][0]['text']
            if is_json_output:
                try:
                    start_idx = response_text.find("{")
                    end_idx = response_text.find("}")
                    parsed_completion = response_text[start_idx:end_idx + 1]
                    parsed_json = json.loads(parsed_completion)
                    parsed_json['llm_response'] = response_text
                    return parsed_json
                except (ValueError, KeyError, json.JSONDecodeError, TypeError):
                    logger.error(f"PARSING_FAILURE: Could not parse judge response: {response_text[:200]}")
                    last_exception = ValueError(f"Parse failure: {response_text[:200]}")
                    time.sleep(3)
                    continue
            else:
                return response_text
        except Exception as e:
            last_exception = e
            logger.error(str(e))
            time.sleep(3)

    # All retries exhausted
    if last_exception and isinstance(last_exception, ValueError):
        return {
            "grade": "incorrect",
            "justification": "PARSING_FAILURE: All retries returned unparseable responses",
            "llm_response": ""
        }
    raise last_exception


BKB_CORRECTNESS_GRADING = """
Human:
You are a teacher grading a quiz. 
You are given a question, the student's answer, and the true answer, and are asked to score the student answer as either Correct or Incorrect.

Example Format:
QUESTION: question here
STUDENT ANSWER: student's answer here
TRUE ANSWER: true answer here
GRADE: Correct or Incorrect here

Grade the student answers based ONLY on their factual accuracy. Ignore differences in punctuation and phrasing between the student answer and true answer. It is OK if the student answer contains more information than the true answer, as long as it does not contain any conflicting statements. If the student fails to answers or claims that the search results do not mention the answer then mark as incorrect. Begin! 

QUESTION: {query}
STUDENT ANSWER: {answer}
TRUE ANSWER: {expected_answer}
GRADE:

Your response should be in json format as follows:
{{
    "grade": (correct or incorrect),
    "justification": (Without mentioning the student/teacher framing of this prompt, explain why the STUDENT ANSWER is Correct or Incorrect. Use one or two sentences maximum. Keep the answer as concise as possible.)
}}


Assistant:
"""

IDK_DETECTION = """You are a teacher grading a quiz. Based on students' response, you are asked to determine if the students think they can not answer the question because some information are missing.
Response: {response}
Please output "Unanswerable" if the students identify that they can not answer the question. Otherwise, output "Answerable".
"""


class GenerationEvaluator:
    """Base evaluator class that holds a shared Bedrock client."""

    bedrock = Session().client(
        service_name='bedrock-runtime',
        region_name=_REGION,
        config=Config(
            max_pool_connections=50,
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=500,
            read_timeout=500,
            region_name=_REGION
        ))

    def __init__(self, model_id):
        self.model_id = model_id


class CorrectnessEvaluator(GenerationEvaluator):
    """Evaluates whether a response is factually correct against a ground-truth answer."""

    def __init__(self, model_id):
        super().__init__(model_id)
    
    def evaluate(self, question, answer, response):
        grading = {}
        grading.update(self._llm_evaluate(question, answer, response))
        return grading

    def _llm_evaluate(self, question, answer, response):
        prompt = BKB_CORRECTNESS_GRADING.format(
            query=question,
            answer=response,
            expected_answer=answer
        )
        completion = call_bedrock_invoke_model(prompt, self.bedrock, model_id=self.model_id)
        if answer == "":
            completion['grade'] = "incorrect"
            completion['justification'] = "No answer was provided"

        if not completion or not completion.get('grade') or not completion.get('justification'):
            logger.error("Failed to grade")
            logger.error(str(completion))
            return {
                'question': question,
                'llmCorrectnessGrade': "incorrect",
                'llmCorrectnessGradeJustification': "PARSING_FAILURE: Judge returned invalid or empty grade structure",
                'llm_response': completion.get('llm_response', str(completion)) if completion else ''
            }

        try:
            grading = {
                'question': question,
                'llmCorrectnessGrade': completion['grade'].lower(),
                'llmCorrectnessGradeJustification': completion['justification'].replace("\"", "\\\""),
                'llm_response': completion.get('llm_response', str(completion))
            }
            return grading
        except (AttributeError, KeyError, TypeError) as e:
            logger.info(str(e))
            return {
                'question': question,
                'llmCorrectnessGrade': "incorrect",
                'llmCorrectnessGradeJustification': f"PARSING_FAILURE: {str(e)}",
                'llm_response': completion.get('llm_response', str(completion)) if completion else ''
            }


class IDKEvaluator(GenerationEvaluator):
    """Detects whether a response indicates the model couldn't answer the question."""

    def __init__(self, model_id):
        super().__init__(model_id)

    def evaluate(self, question, answer, response):
        grading = {}
        grading.update(self._llm_evaluate(question, answer, response))
        return grading

    def _llm_evaluate(self, question, answer, response):
        prompt = IDK_DETECTION.format(
            question=question,
            answer=answer,
            response=response
        )
        completion = call_bedrock_invoke_model(prompt, self.bedrock, model_id=self.model_id, is_json_output=False)
        if "Unanswerable" in completion:
            return {
                "label": "unanswerable"
            }
        else:
            return {
                "label": "answerable"
            }


def evaluate_responses(data: List[dict], output_dir: str, judge_model_id: str,
                       metrics: Optional[List[str]] = None) -> Dict[str, float]:
    """Evaluate a list of response records and write results to output_dir.

    This is the shared evaluation loop used by benchmark_evaluate.py.

    Args:
        data: List of response dicts, each with 'raw_example' (question/answer) and 'response'.
        output_dir: Directory to write metric_evals.json and metric.json files.
        judge_model_id: Bedrock model ID for the evaluation judge.
        metrics: List of metrics to compute. Defaults to ['correctness', 'idk'].

    Returns:
        Dict of {metric_name: score} for each computed metric.
    """
    if metrics is None:
        metrics = ['correctness', 'idk']

    # if "correctness_on_answerable" in metrics, make sure it's last in the last
    if "correctness_on_answerable" in metrics:
        for dep in ["correctness", "idk"]:
            if dep not in metrics:
                metrics.insert(metrics.index("correctness_on_answerable"), dep)

    os.makedirs(output_dir, exist_ok=True)

    scores = {}

    for metric in metrics:
        if metric == 'correctness_on_answerable':
            # Requires correctness and idk to have been run first
            correctness_path = os.path.join(output_dir, 'correctness_evals.json')
            idk_path = os.path.join(output_dir, 'idk_evals.json')
            if not os.path.exists(correctness_path) or not os.path.exists(idk_path):
                logger.warning("Cannot compute correctness_on_answerable without correctness and idk evals")
                continue
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
                evaluator = CorrectnessEvaluator(model_id=judge_model_id)
            elif metric == 'idk':
                evaluator = IDKEvaluator(model_id=judge_model_id)
            else:
                logger.warning(f"Unknown metric: {metric} — skipping")
                continue

            evals = []
            for example in data:
                question = example['raw_example']['question']
                answer = example['raw_example']['answer']
                response = example['response']
                evals.append(evaluator.evaluate(question, answer, response))

            # Write per-item evaluation results
            evals_path = os.path.join(output_dir, f'{metric}_evals.json')
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

        # Write aggregate score file
        score_path = os.path.join(output_dir, f'{metric}.json')
        with open(score_path, 'w') as f:
            json.dump({metric: score}, f, indent=2)

        logger.info(f"  {metric}: {score:.4f}")

    return scores
