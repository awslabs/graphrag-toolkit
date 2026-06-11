# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the pure helpers in storage/vector/s3_vector_indexes."""

import json

import pytest
from graphrag_toolkit.core.types import Node
from graphrag_toolkit.core.vector_store_types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes import (
    _node_to_s3_vector,
    filter_config_to_s3_filters,
    formatter_for_type,
    node_to_s3_vector,
    parse_metadata_filters_recursive,
    s3_vector_to_dict,
    to_s3_operator,
    validate_metadata_limits,
)


def _eq(key, value):
    return MetadataFilter(key=key, value=value, operator=FilterOperator.EQ)


class TestToS3Operator:
    @pytest.mark.parametrize('op,expected', [
        (FilterOperator.EQ, '$eq'),
        (FilterOperator.GT, '$gt'),
        (FilterOperator.LT, '$lt'),
        (FilterOperator.NE, '$ne'),
        (FilterOperator.GTE, '$gte'),
        (FilterOperator.LTE, '$lte'),
        (FilterOperator.IS_EMPTY, '$exists'),
    ])
    def test_known_operators(self, op, expected):
        operator, _ = to_s3_operator(op)
        assert operator == expected

    def test_unsupported_operator_raises(self):
        with pytest.raises(ValueError, match='Unsupported filter operator'):
            to_s3_operator(FilterOperator.TEXT_MATCH)


class TestFormatterForType:
    def test_text_wraps_in_double_quotes(self):
        assert formatter_for_type('text')('hello') == '"hello"'

    def test_numeric_passthrough(self):
        assert formatter_for_type('int')(5) == 5
        assert formatter_for_type('float')(2.5) == 2.5

    def test_timestamp_wraps_in_quotes(self):
        result = formatter_for_type('timestamp')('2024-01-15T10:30:00')
        assert result.startswith('"2024-01-15')

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match='Unsupported type name'):
            formatter_for_type('boolean')


class TestParseMetadataFiltersRecursive:
    def test_eq_filter_produces_eq_clause(self):
        result = parse_metadata_filters_recursive(MetadataFilters(
            filters=[_eq('category', 'tech')], condition=FilterCondition.AND,
        ))
        # The text formatter writes "tech" with embedded quotes; json.loads unwraps them.
        assert result == {'$and': [{'source.metadata.category': {'$eq': 'tech'}}]}

    def test_and_condition(self):
        result = parse_metadata_filters_recursive(MetadataFilters(
            filters=[_eq('a', 'x'), _eq('b', 'y')], condition=FilterCondition.AND,
        ))
        assert '$and' in result

    def test_or_condition(self):
        result = parse_metadata_filters_recursive(MetadataFilters(
            filters=[_eq('a', 'x'), _eq('b', 'y')], condition=FilterCondition.OR,
        ))
        assert '$or' in result

    def test_not_condition_with_bare_filter_raises(self):
        filters = MetadataFilters(filters=[_eq('a', 'x')], condition=FilterCondition.NOT)
        with pytest.raises(ValueError, match='Expected MetadataFilters'):
            parse_metadata_filters_recursive(filters)

    def test_unsupported_condition_raises(self):
        filters = MetadataFilters(filters=[_eq('a', 'x')], condition=FilterCondition.NOT)
        # NOT is not supported in S3-style filters, even with nested MetadataFilters.
        inner = MetadataFilters(filters=[_eq('a', 'x')], condition=FilterCondition.AND)
        outer = MetadataFilters(filters=[inner], condition=FilterCondition.NOT)
        with pytest.raises(ValueError, match='Unsupported filters condition'):
            parse_metadata_filters_recursive(outer)


class TestFilterConfigToS3Filters:
    def test_none_returns_none(self):
        assert filter_config_to_s3_filters(None) is None

    def test_no_source_filters_returns_none(self):
        assert filter_config_to_s3_filters(FilterConfig()) is None

    def test_passes_through_to_recursive(self):
        config = FilterConfig(source_filters=MetadataFilters(
            filters=[_eq('a', 'x')], condition=FilterCondition.AND,
        ))
        result = filter_config_to_s3_filters(config)
        assert '$and' in result


class TestNodeToS3Vector:
    def test_extracts_versioning_into_top_level_keys(self):
        node = Node(node_id='n1', text='hi', metadata={
            'source': {
                'versioning': {'valid_from': 100, 'valid_to': 200},
                'metadata': {'category': 'tech'},
            },
        })
        result = node_to_s3_vector(node, [0.1, 0.2])
        meta = result['metadata']
        assert meta['source.versioning.valid_from'] == 100
        assert meta['source.versioning.valid_to'] == 200
        assert meta['source.metadata.category'] == 'tech'
        assert result['key'] == 'n1'
        assert result['data']['float32'] == [0.1, 0.2]

    def test_missing_versioning_uses_default_bounds(self):
        result = _node_to_s3_vector('id', 'text', [0.0], {})
        meta = result['metadata']
        # Default valid_from is TIMESTAMP_LOWER_BOUND, valid_to is TIMESTAMP_UPPER_BOUND.
        assert isinstance(meta['source.versioning.valid_from'], int)
        assert isinstance(meta['source.versioning.valid_to'], int)


class TestS3VectorToDict:
    def test_round_trip_recovers_versioning_and_metadata(self):
        node = Node(node_id='n1', text='hi', metadata={
            'source': {
                'versioning': {'valid_from': 100, 'valid_to': 200},
                'metadata': {'category': 'tech'},
            },
        })
        s3 = node_to_s3_vector(node, [0.1, 0.2])
        out = s3_vector_to_dict(s3)
        assert out['id'] == 'n1'
        assert out['embedding'] == [0.1, 0.2]
        assert out['source']['versioning']['valid_from'] == 100
        assert out['source']['metadata']['category'] == 'tech'

    def test_handles_missing_metadata_key(self):
        s3 = {'key': 'n1', 'data': {}, 'metadata': {}}
        out = s3_vector_to_dict(s3)
        assert out['id'] == 'n1'
        assert out['source']['metadata'] == {}


class TestValidateMetadataLimits:
    def test_non_dict_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_metadata_limits([], max_tags=50, vector_id='n1')

    def test_under_limit_passes(self):
        validate_metadata_limits({'k': 'v'}, max_tags=50, vector_id='n1')

    def test_over_limit_raises_value_error(self):
        meta = {f'k{i}': i for i in range(60)}
        with pytest.raises(ValueError):
            validate_metadata_limits(meta, max_tags=50, vector_id='n1')
