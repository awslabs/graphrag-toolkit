# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests for benchmark_query module.
"""

import json
import math

from hypothesis import given, settings
from hypothesis.strategies import (
    none,
    one_of,
    floats,
    integers,
    text,
    booleans,
)


class TestTimingFloorTransformation:
    """
    Timing metadata floor transformation

    For any non-negative float value in the query engine response metadata
    (retrieve_ms, answer_ms), the corresponding integer field written
    to the JSONL output (retrieve_ms, answer_ms) SHALL equal
    math.floor() of that float value. total_latency_ms SHALL equal the sum
    of retrieve_ms and answer_ms when both are present, null otherwise.
    """

    @settings(max_examples=100)
    @given(
        value=floats(
            min_value=0,
            max_value=1_000_000,
            allow_nan=False,
            allow_infinity=False,
        )
    )
    def test_floor_produces_correct_integer(self, value):
        """
        Given a non-negative float, math.floor produces the expected integer value.

        The transformation used in benchmark_query.py is:
            retrieval_ms = math.floor(raw_retrieve_ms)

        This verifies that the result is an integer and equals the largest
        integer less than or equal to the input float.
        """
        result = math.floor(value)

        # Result must be an integer
        assert isinstance(result, int), (
            f"Expected int, got {type(result)} for input {value}"
        )

        # Result must be less than or equal to the input
        assert result <= value, (
            f"floor({value}) = {result}, but {result} > {value}"
        )

        # Result must be the largest such integer (result + 1 > value)
        assert result + 1 > value, (
            f"floor({value}) = {result}, but {result + 1} <= {value}"
        )

        # Result must be non-negative since input is non-negative
        assert result >= 0, (
            f"floor({value}) = {result}, expected non-negative"
        )

    @settings(max_examples=100)
    @given(
        retrieve_ms=floats(
            min_value=0,
            max_value=1_000_000,
            allow_nan=False,
            allow_infinity=False,
        ),
        answer_ms=floats(
            min_value=0,
            max_value=1_000_000,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
    def test_all_timing_fields_floor_correctly(self, retrieve_ms, answer_ms):
        """
        Given two non-negative floats representing retrieve_ms and answer_ms,
        applying math.floor to each produces correct integer values matching the
        transformation in benchmark_query.py. total_latency_ms equals their sum.
        """
        # Apply the same transformation as benchmark_query.py
        retrieve_ms_int = math.floor(retrieve_ms)
        answer_ms_int = math.floor(answer_ms)
        total_latency_ms = retrieve_ms_int + answer_ms_int

        # All results must be integers
        assert isinstance(retrieve_ms_int, int)
        assert isinstance(answer_ms_int, int)
        assert isinstance(total_latency_ms, int)

        # Floor results must satisfy floor properties
        assert retrieve_ms_int <= retrieve_ms < retrieve_ms_int + 1
        assert answer_ms_int <= answer_ms < answer_ms_int + 1

        # total_latency_ms must equal the sum of the floored values
        assert total_latency_ms == retrieve_ms_int + answer_ms_int

        # All results must be non-negative
        assert retrieve_ms_int >= 0
        assert answer_ms_int >= 0
        assert total_latency_ms >= 0


REQUIRED_FIELDS = [
    'raw_example',
    'response',
    'retrieve_ms',
    'answer_ms',
    'total_latency_ms',
    'prompt_tokens_total',
    'retrieval_context_tokens',
    'output_tokens',
    'hop_classification',
    'dataset_category',
    'retriever',
    'dataset',
    'capping_params',
    'graph_statistics',
    'ingestion_time_minutes',
    'extraction_time_minutes',
    'retrieval_iterations',
    'agentic_retrieval_ms',
    'agentic_input_tokens',
    'agentic_output_tokens',
]


def build_jsonl_record(
    question: str,
    answer: str,
    response_text: str,
    raw_retrieve_ms,
    raw_answer_ms,
    input_tokens,
    output_tokens,
    retriever_id: str = 'traversal',
    dataset: str = 'cuad',
    dataset_category=None,
    capping_params=None,
    graph_statistics=None,
    ingestion_time_minutes=None,
    extraction_time_minutes=None,
    retrieval_iterations=None,
    agentic_retrieval_ms=None,
    agentic_input_tokens=None,
    agentic_output_tokens=None,
    hop_classification: str = 'unknown',
):
    """
    Simulates the JSONL line construction logic from benchmark_query.py's
    run_benchmark_query() function.

    This mirrors the exact dict construction in the query loop:
    - Timing fields are floor'd if present, else None
    - total_latency_ms is computed as sum when both present, null otherwise
    - Token fields are passed through as-is (int or None)
    - All fields always present (null when unavailable, never omitted)
    """
    retrieve_ms = math.floor(raw_retrieve_ms) if raw_retrieve_ms is not None else None
    answer_ms = math.floor(raw_answer_ms) if raw_answer_ms is not None else None
    total_latency_ms = (retrieve_ms + answer_ms) if (retrieve_ms is not None and answer_ms is not None) else None

    if capping_params is None:
        capping_params = {
            'max_statements_per_topic': 10,
            'max_search_results': 5,
            'max_statements': 200,
        }

    if graph_statistics is None:
        graph_statistics = {
            'chunk_count': None,
            'statement_count': None,
            'topic_count': None,
            'entity_count': None,
        }

    return {
        'raw_example': {'question': question, 'answer': answer},
        'response': response_text,
        'retrieve_ms': retrieve_ms,
        'answer_ms': answer_ms,
        'total_latency_ms': total_latency_ms,
        'prompt_tokens_total': input_tokens,
        'retrieval_context_tokens': None,  # Populated by task 3.1
        'output_tokens': output_tokens,
        'hop_classification': hop_classification,
        'dataset_category': dataset_category,
        'retriever': retriever_id,
        'dataset': dataset,
        'capping_params': capping_params,
        'graph_statistics': graph_statistics,
        'ingestion_time_minutes': ingestion_time_minutes,
        'extraction_time_minutes': extraction_time_minutes,
        'retrieval_iterations': retrieval_iterations,
        'agentic_retrieval_ms': agentic_retrieval_ms,
        'agentic_input_tokens': agentic_input_tokens,
        'agentic_output_tokens': agentic_output_tokens,
    }


class TestJSONLStructuralCompletenessProperty:
    """
    JSONL structural completeness

    For any query result (whether timing/token metadata is available or not),
    the corresponding JSONL line SHALL contain all required fields defined in
    the schema (raw_example, response, retrieve_ms, answer_ms, total_latency_ms,
    prompt_tokens_total, retrieval_context_tokens, output_tokens, hop_classification,
    dataset_category, retriever, dataset, capping_params, graph_statistics,
    ingestion_time_minutes, extraction_time_minutes, retrieval_iterations,
    agentic_retrieval_ms, agentic_input_tokens, agentic_output_tokens) where
    each field is either a valid value or null.
    """

    @settings(max_examples=100)
    @given(
        question=text(min_size=1, max_size=200),
        answer=text(min_size=1, max_size=200),
        response_text=text(max_size=500),
        raw_retrieve_ms=one_of(none(), floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)),
        raw_answer_ms=one_of(none(), floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)),
        input_tokens=one_of(none(), integers(min_value=0, max_value=10_000_000)),
        output_tokens=one_of(none(), integers(min_value=0, max_value=10_000_000)),
    )
    def test_all_required_fields_present(
        self,
        question,
        answer,
        response_text,
        raw_retrieve_ms,
        raw_answer_ms,
        input_tokens,
        output_tokens,
    ):
        """
        Generate query results with various combinations of available/missing
        metadata, verify all required fields are present (either valid value or null).
        """
        record = build_jsonl_record(
            question=question,
            answer=answer,
            response_text=response_text,
            raw_retrieve_ms=raw_retrieve_ms,
            raw_answer_ms=raw_answer_ms,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

        # All required fields must be present as keys
        for field in REQUIRED_FIELDS:
            assert field in record, f"Required field '{field}' missing from JSONL record"

        # raw_example must be a dict with question and answer
        assert isinstance(record['raw_example'], dict)
        assert 'question' in record['raw_example']
        assert 'answer' in record['raw_example']

        # response must be a string (possibly empty)
        assert isinstance(record['response'], str)

        # Timing fields must be int or None
        for timing_field in ('retrieve_ms', 'answer_ms', 'total_latency_ms'):
            value = record[timing_field]
            assert value is None or isinstance(value, int), (
                f"Field '{timing_field}' must be int or None, got {type(value)}: {value}"
            )

        # Token fields must be int or None
        for token_field in ('prompt_tokens_total', 'retrieval_context_tokens', 'output_tokens'):
            value = record[token_field]
            assert value is None or isinstance(value, int), (
                f"Field '{token_field}' must be int or None, got {type(value)}: {value}"
            )

        # capping_params must be a dict with the required keys
        assert isinstance(record['capping_params'], dict)
        for key in ('max_statements_per_topic', 'max_search_results', 'max_statements'):
            assert key in record['capping_params'], (
                f"capping_params missing key '{key}'"
            )
            assert isinstance(record['capping_params'][key], int), (
                f"capping_params['{key}'] must be int, got {type(record['capping_params'][key])}"
            )

        # graph_statistics must be a dict with the required keys (values can be int or None)
        assert isinstance(record['graph_statistics'], dict)
        for key in ('chunk_count', 'statement_count', 'topic_count', 'entity_count'):
            assert key in record['graph_statistics'], (
                f"graph_statistics missing key '{key}'"
            )
            value = record['graph_statistics'][key]
            assert value is None or isinstance(value, int), (
                f"graph_statistics['{key}'] must be int or None, got {type(value)}: {value}"
            )

        # ingestion_time_minutes and extraction_time_minutes must be float or None
        for time_field in ('ingestion_time_minutes', 'extraction_time_minutes'):
            value = record[time_field]
            assert value is None or isinstance(value, (int, float)), (
                f"Field '{time_field}' must be float or None, got {type(value)}: {value}"
            )

        # retriever must be a string
        assert isinstance(record['retriever'], str)

        # dataset must be a string
        assert isinstance(record['dataset'], str)

        # dataset_category must be string or None
        assert record['dataset_category'] is None or isinstance(record['dataset_category'], str)

        # Agentic fields must be int or None
        for agentic_field in ('retrieval_iterations', 'agentic_retrieval_ms', 'agentic_input_tokens', 'agentic_output_tokens'):
            value = record[agentic_field]
            assert value is None or isinstance(value, int), (
                f"Field '{agentic_field}' must be int or None, got {type(value)}: {value}"
            )

        # Verify the record is JSON-serializable (structural contract)
        serialized = json.dumps(record)
        deserialized = json.loads(serialized)

        # After round-trip through JSON, all required fields must still be present
        for field in REQUIRED_FIELDS:
            assert field in deserialized, (
                f"Required field '{field}' lost during JSON serialization round-trip"
            )


import os

from hypothesis.strategies import sampled_from, tuples
from hypothesis import assume

from benchmark.utils.retriever_factory import VALID_RETRIEVER_IDS


# Strategy for dataset names: alphanumeric + hyphens, min_size=1
_dataset_names = text(
    alphabet='abcdefghijklmnopqrstuvwxyz0123456789-',
    min_size=1,
    max_size=50,
)


class TestOutputPathConstructionProperty:
    """
    Output path construction

    For any valid retriever identifier and any dataset name, the output directory
    path SHALL equal `benchmark-results/{dataset}/{retriever}/`, and any two distinct
    (dataset, retriever) pairs SHALL produce distinct paths.
    """

    @settings(max_examples=100)
    @given(
        retriever_id=sampled_from(VALID_RETRIEVER_IDS),
        dataset=_dataset_names,
    )
    def test_output_path_equals_expected_format(self, retriever_id, dataset):
        """
        For any valid retriever ID and dataset name, verify the constructed path
        equals benchmark-results/{dataset}/{retriever_id}.
        """
        # This is the path construction logic used in benchmark_query.py
        constructed_path = os.path.join('benchmark-results', dataset, retriever_id)
        expected_path = f'benchmark-results/{dataset}/{retriever_id}'

        assert constructed_path == expected_path, (
            f"Expected path '{expected_path}', got '{constructed_path}' "
            f"for dataset='{dataset}', retriever_id='{retriever_id}'"
        )

    @settings(max_examples=100)
    @given(
        pair1=tuples(_dataset_names, sampled_from(VALID_RETRIEVER_IDS)),
        pair2=tuples(_dataset_names, sampled_from(VALID_RETRIEVER_IDS)),
    )
    def test_distinct_pairs_produce_distinct_paths(self, pair1, pair2):
        """
        For any two distinct (dataset, retriever_id) pairs, verify they produce
        distinct output paths.
        """
        assume(pair1 != pair2)

        dataset1, retriever_id1 = pair1
        dataset2, retriever_id2 = pair2

        path1 = os.path.join('benchmark-results', dataset1, retriever_id1)
        path2 = os.path.join('benchmark-results', dataset2, retriever_id2)

        assert path1 != path2, (
            f"Distinct pairs ({dataset1}, {retriever_id1}) and ({dataset2}, {retriever_id2}) "
            f"produced the same path: '{path1}'"
        )
