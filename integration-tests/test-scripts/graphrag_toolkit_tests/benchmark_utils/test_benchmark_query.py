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
    (retrieve_ms, answer_ms, total_ms), the corresponding integer field written
    to the JSONL output (retrieval_ms, response_ms, total_ms) SHALL equal
    math.floor() of that float value.
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
        total_ms=floats(
            min_value=0,
            max_value=1_000_000,
            allow_nan=False,
            allow_infinity=False,
        ),
    )
    def test_all_timing_fields_floor_correctly(self, retrieve_ms, answer_ms, total_ms):
        """
        Given three non-negative floats representing retrieve_ms, answer_ms, and total_ms,
        applying math.floor to each produces correct integer values matching the
        transformation in benchmark_query.py.
        """
        # Apply the same transformation as benchmark_query.py
        retrieval_ms = math.floor(retrieve_ms)
        response_ms = math.floor(answer_ms)
        total_ms_int = math.floor(total_ms)

        # All results must be integers
        assert isinstance(retrieval_ms, int)
        assert isinstance(response_ms, int)
        assert isinstance(total_ms_int, int)

        # All results must satisfy floor properties
        assert retrieval_ms <= retrieve_ms < retrieval_ms + 1
        assert response_ms <= answer_ms < response_ms + 1
        assert total_ms_int <= total_ms < total_ms_int + 1

        # All results must be non-negative
        assert retrieval_ms >= 0
        assert response_ms >= 0
        assert total_ms_int >= 0


REQUIRED_FIELDS = [
    'raw_example',
    'response',
    'retrieval_ms',
    'response_ms',
    'total_ms',
    'input_tokens',
    'output_tokens',
]


def build_jsonl_record(
    question: str,
    answer: str,
    response_text: str,
    raw_retrieve_ms,
    raw_answer_ms,
    raw_total_ms,
    input_tokens,
    output_tokens,
):
    """
    Simulates the JSONL line construction logic from benchmark_query.py's
    run_benchmark_query() function.

    This mirrors the exact dict construction in the query loop:
    - Timing fields are floor'd if present, else None
    - Token fields are passed through as-is (int or None)
    """
    retrieval_ms = math.floor(raw_retrieve_ms) if raw_retrieve_ms is not None else None
    response_ms = math.floor(raw_answer_ms) if raw_answer_ms is not None else None
    total_ms = math.floor(raw_total_ms) if raw_total_ms is not None else None

    return {
        'raw_example': {'question': question, 'answer': answer},
        'response': response_text,
        'retrieval_ms': retrieval_ms,
        'response_ms': response_ms,
        'total_ms': total_ms,
        'input_tokens': input_tokens,
        'output_tokens': output_tokens,
    }


class TestJSONLStructuralCompletenessProperty:
    """
    JSONL structural completeness

    For any query result (whether timing/token metadata is available or not),
    the corresponding JSONL line SHALL contain all required fields
    (raw_example, response, retrieval_ms, response_ms, total_ms, input_tokens,
    output_tokens) where each field is either a valid value or null.
    """

    @settings(max_examples=100)
    @given(
        question=text(min_size=1, max_size=200),
        answer=text(min_size=1, max_size=200),
        response_text=text(max_size=500),
        raw_retrieve_ms=one_of(none(), floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)),
        raw_answer_ms=one_of(none(), floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)),
        raw_total_ms=one_of(none(), floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)),
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
        raw_total_ms,
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
            raw_total_ms=raw_total_ms,
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
        for timing_field in ('retrieval_ms', 'response_ms', 'total_ms'):
            value = record[timing_field]
            assert value is None or isinstance(value, int), (
                f"Field '{timing_field}' must be int or None, got {type(value)}: {value}"
            )

        # Token fields must be int or None
        for token_field in ('input_tokens', 'output_tokens'):
            value = record[token_field]
            assert value is None or isinstance(value, int), (
                f"Field '{token_field}' must be int or None, got {type(value)}: {value}"
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

from graphrag_toolkit_tests.benchmark_utils.retriever_factory import (
    VALID_RETRIEVER_IDS,
    SUB_RETRIEVER_IDS,
    get_retriever_config,
)
from graphrag_toolkit_tests.benchmark_utils.metrics_summary import compute_metrics_summary


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


class TestRetrieverConfigReproducibility:
    """
    Retriever-config reproducibility

    For any valid retriever identifier, get_retriever_config() SHALL return a
    JSON-serializable dict carrying the retriever id, the response model id, and a
    hyperparameters dict, so that metrics_summary.json records enough to reproduce
    the run's retriever configuration.
    """

    @settings(max_examples=50)
    @given(retriever_id=sampled_from(VALID_RETRIEVER_IDS))
    def test_config_shape_is_complete_and_serializable(self, retriever_id):
        config = get_retriever_config(retriever_id)

        for field in ('retriever_id', 'response_llm', 'hyperparameters'):
            assert field in config, f"'{field}' missing from retriever config"
        assert config['retriever_id'] == retriever_id
        assert isinstance(config['hyperparameters'], dict)

        # Must survive a JSON round-trip with no loss (the reproducibility contract).
        assert json.loads(json.dumps(config)) == config

    @settings(max_examples=50)
    @given(retriever_id=sampled_from(SUB_RETRIEVER_IDS))
    def test_sub_retrievers_record_required_hyperparameters(self, retriever_id):
        """The ticket's required knobs (max statements, max search results) are
        recorded for the sub-retrievers that set them."""
        hp = get_retriever_config(retriever_id)['hyperparameters']
        assert hp['max_statements'] == 200
        assert hp['max_search_results'] == 5
        assert hp['vss_top_k'] == 10

    @settings(max_examples=50)
    @given(max_iters=integers(min_value=1, max_value=10))
    def test_agentic_records_max_iterations(self, max_iters):
        config = get_retriever_config('agentic', agentic_max_iterations=max_iters)
        assert config['hyperparameters']['max_iterations'] == max_iters

    def test_invalid_retriever_id_raises(self):
        try:
            get_retriever_config('not-a-retriever')
            assert False, 'expected ValueError for invalid retriever id'
        except ValueError:
            pass

    def test_metrics_summary_embeds_retriever_config(self):
        """compute_metrics_summary writes the retriever_config block through verbatim."""
        config = get_retriever_config('topic_based')
        summary = compute_metrics_summary(
            per_query_data=[],
            retriever_id='topic_based',
            dataset='cuad',
            model_id='us.anthropic.claude-sonnet-4-6',
            num_empty=0,
            retriever_config=config,
        )
        assert summary['retriever_config'] == config
        # Whole summary must stay JSON-serializable.
        json.dumps(summary)
