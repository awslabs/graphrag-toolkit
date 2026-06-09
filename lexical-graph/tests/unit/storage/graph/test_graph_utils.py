# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for graph_utils."""

import pytest
from llama_index.core.vector_stores.types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import (
    filter_config_to_opencypher_filters,
    formatter_for_type,
    label_from,
    new_query_var,
    node_result,
    parse_metadata_filters_recursive,
    relationship_name_from,
    search_string_from,
    to_opencypher_operator,
)


class TestNewQueryVar:
    def test_starts_with_n_prefix(self):
        assert new_query_var().startswith('n')

    def test_hex_body(self):
        var = new_query_var()
        # 'n' + 32 hex chars
        assert len(var) == 33
        int(var[1:], 16)


class TestSearchStringFrom:
    def test_lowercases(self):
        assert search_string_from('Hello') == 'hello'

    def test_strips_punctuation(self):
        assert search_string_from('hello, world!') == 'hello world'

    def test_collapses_multiple_spaces(self):
        assert search_string_from('a    b     c') == 'a b c'

    def test_strips_underscores(self):
        assert search_string_from('foo_bar_baz') == 'foo bar baz'

    def test_trims_outer_whitespace(self):
        assert search_string_from('   padded   ') == 'padded'

    def test_empty_string(self):
        assert search_string_from('') == ''

    def test_only_punctuation(self):
        assert search_string_from('!!!---???') == ''


class TestLabelFrom:
    def test_capwords_and_joins(self):
        assert label_from('source document') == 'SourceDocument'

    def test_dunder_passthrough(self):
        assert label_from('__internal__') == '__internal__'

    def test_strips_punctuation(self):
        assert label_from('aws::graph::index') == 'AwsGraphIndex'

    def test_single_word(self):
        assert label_from('chunk') == 'Chunk'


class TestRelationshipNameFrom:
    def test_uppercases(self):
        assert relationship_name_from('next') == 'NEXT'

    def test_replaces_non_alnum_with_underscore(self):
        assert relationship_name_from('mentioned in') == 'MENTIONED_IN'
        assert relationship_name_from('belongs-to') == 'BELONGS_TO'

    def test_keeps_digits(self):
        assert relationship_name_from('rev2 of') == 'REV2_OF'


class TestNodeResult:
    def test_default_star_properties(self):
        result = node_result('source')
        assert result == 'source: source{.*}'

    def test_custom_key_name(self):
        result = node_result('source', key_name='src')
        assert result == 'src: source{.*}'

    def test_explicit_properties(self):
        result = node_result('chunk', properties=['value', 'text'])
        assert result == 'chunk: chunk{.value, .text}'

    def test_property_based_node_id_with_star_omits_key(self):
        # is_property_based=True and '*' in properties -> no extra selector for key
        node_id = NodeId('chunkId', 'c.chunkId', is_property_based=True)
        result = node_result('chunk', node_id=node_id, properties=['*'])
        assert result == 'chunk: chunk{.*}'

    def test_property_based_node_id_prepends_key_when_absent(self):
        # is_property_based and key not in properties -> prepend .key
        node_id = NodeId('chunkId', 'c.chunkId', is_property_based=True)
        result = node_result('chunk', node_id=node_id, properties=['text'])
        assert result == 'chunk: chunk{.chunkId, .text}'

    def test_non_property_based_node_id(self):
        # is_property_based=False -> 'key: value' selector
        node_id = NodeId('id', 'abc', is_property_based=False)
        result = node_result('chunk', node_id=node_id, properties=['text'])
        assert result == 'chunk: chunk{id: abc, .text}'


class TestToOpencypherOperator:
    @pytest.mark.parametrize(
        'op,expected',
        [
            (FilterOperator.EQ, '='),
            (FilterOperator.GT, '>'),
            (FilterOperator.LT, '<'),
            (FilterOperator.NE, '<>'),
            (FilterOperator.GTE, '>='),
            (FilterOperator.LTE, '<='),
            (FilterOperator.TEXT_MATCH, 'CONTAINS'),
            (FilterOperator.TEXT_MATCH_INSENSITIVE, 'CONTAINS'),
            (FilterOperator.IS_EMPTY, 'IS NULL'),
        ],
    )
    def test_maps_supported_operators(self, op, expected):
        operator, _ = to_opencypher_operator(op)
        assert operator == expected

    def test_default_value_formatter_is_identity(self):
        _, formatter = to_opencypher_operator(FilterOperator.EQ)
        assert formatter('Foo') == 'Foo'

    def test_text_match_insensitive_lowercases(self):
        _, formatter = to_opencypher_operator(FilterOperator.TEXT_MATCH_INSENSITIVE)
        assert formatter('Hello World') == 'hello world'

    def test_unsupported_operator_raises(self):
        with pytest.raises(ValueError, match='Unsupported filter operator'):
            to_opencypher_operator(FilterOperator.IN)


class TestFormatterForType:
    def test_text_wraps_in_quotes(self):
        assert formatter_for_type('text')('hello') == "'hello'"

    def test_number_passthrough(self):
        assert formatter_for_type('number')(42) == 42

    def test_int_alias(self):
        assert formatter_for_type('int')(7) == 7

    def test_float_alias(self):
        assert formatter_for_type('float')(3.14) == 3.14

    def test_timestamp_wraps_in_datetime_call(self):
        result = formatter_for_type('timestamp')('2024-01-15T10:30:00')
        assert result.startswith("datetime('2024-01-15T10:30:00")
        assert result.endswith("')")

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match='Unsupported type name'):
            formatter_for_type('boolean')


def _eq_filter(key: str, value):
    return MetadataFilter(key=key, value=value, operator=FilterOperator.EQ)


class TestParseMetadataFiltersRecursive:
    def test_single_eq_filter_with_and_condition(self):
        filters = MetadataFilters(
            filters=[_eq_filter('category', 'tech')],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(filters)
        assert result == "((source.category = 'tech'))"

    def test_numeric_value_not_quoted(self):
        filters = MetadataFilters(
            filters=[MetadataFilter(key='count', value=5, operator=FilterOperator.GT)],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(filters)
        assert "source.count > 5" in result

    def test_float_value_not_quoted(self):
        filters = MetadataFilters(
            filters=[MetadataFilter(key='ratio', value=0.5, operator=FilterOperator.LTE)],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(filters)
        assert "source.ratio <= 0.5" in result

    def test_and_condition_joins_with_and_keyword(self):
        filters = MetadataFilters(
            filters=[_eq_filter('category', 'tech'), _eq_filter('lang', 'en')],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(filters)
        assert ' AND ' in result
        assert "source.category = 'tech'" in result
        assert "source.lang = 'en'" in result

    def test_or_condition_joins_with_or_keyword(self):
        filters = MetadataFilters(
            filters=[_eq_filter('category', 'tech'), _eq_filter('category', 'science')],
            condition=FilterCondition.OR,
        )
        result = parse_metadata_filters_recursive(filters)
        assert ' OR ' in result

    def test_not_wraps_nested_filters(self):
        inner = MetadataFilters(
            filters=[_eq_filter('category', 'tech')],
            condition=FilterCondition.AND,
        )
        outer = MetadataFilters(filters=[inner], condition=FilterCondition.NOT)
        result = parse_metadata_filters_recursive(outer)
        assert result.startswith('(NOT ')

    def test_not_rejects_bare_metadata_filter(self):
        filters = MetadataFilters(
            filters=[_eq_filter('category', 'tech')],
            condition=FilterCondition.NOT,
        )
        with pytest.raises(ValueError, match='Expected MetadataFilters for FilterCondition.NOT'):
            parse_metadata_filters_recursive(filters)

    def test_is_empty_emits_is_null(self):
        filters = MetadataFilters(
            filters=[
                MetadataFilter(key='archived_at', value=None, operator=FilterOperator.IS_EMPTY)
            ],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(filters)
        assert 'source.archived_at IS NULL' in result

    def test_text_match_insensitive_emits_tolower_on_key_and_lowercased_value(self):
        filters = MetadataFilters(
            filters=[
                MetadataFilter(
                    key='title',
                    value='Hello',
                    operator=FilterOperator.TEXT_MATCH_INSENSITIVE,
                )
            ],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(filters)
        assert 'source.title.toLower() CONTAINS' in result
        assert "'hello'" in result

    def test_nested_filters_recurse(self):
        inner = MetadataFilters(
            filters=[_eq_filter('lang', 'en'), _eq_filter('lang', 'fr')],
            condition=FilterCondition.OR,
        )
        outer = MetadataFilters(
            filters=[_eq_filter('category', 'tech'), inner],
            condition=FilterCondition.AND,
        )
        result = parse_metadata_filters_recursive(outer)
        assert 'source.category' in result
        assert 'source.lang' in result
        assert ' AND ' in result
        assert ' OR ' in result


class TestFilterConfigToOpencypherFilters:
    def test_none_config_returns_empty_string(self):
        assert filter_config_to_opencypher_filters(None) == ''

    def test_empty_source_filters_returns_empty_string(self):
        config = FilterConfig()
        assert filter_config_to_opencypher_filters(config) == ''

    def test_passes_through_to_recursive_parser(self):
        config = FilterConfig(
            source_filters=MetadataFilters(
                filters=[_eq_filter('category', 'tech')],
                condition=FilterCondition.AND,
            )
        )
        result = filter_config_to_opencypher_filters(config)
        assert "source.category = 'tech'" in result
